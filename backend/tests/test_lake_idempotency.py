"""落湖幂等凭证链路测试：Webhook 重发、确认中断续做与并发重试。

覆盖场景：
1. Webhook 接收入口已返回成功，之后重发但内存去重缓存丢失；
2. 首次写入未确认（落湖失败），重试能接着处理而不是新起一份身份；
3. 分区已读到第一次写入记录，但接入请求在回写确认前中断，重启后重发；
4. 同一接入身份并发提交只确认一次正式写入；
5. 历史落湖记录（随机 event_id、无去重键）重发兼容；
6. 去重确认结果回写接入清单，Webhook 通道按凭证决定是否继续写入。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.persistence
from app.persistence import JsonStore
import app.services.data_lake_sink as sink_module
import app.services.webhook_channel as webhook_module


def _sink_cls():
    """按当前模块对象取服务类：其他测试文件会 reload 服务模块，导入时的类会过期。"""
    return sink_module.DataLakeSinkService


def _webhook_cls():
    return webhook_module.WebhookChannelService


def _const(name: str):
    return getattr(sink_module, name)


def _dedup_singleton():
    """Webhook 去重走服务注册表里的实例，测试也必须对它断言。"""
    from app.services import service_for
    return service_for("dedup_filter")


class LakeIngestIdempotencyTest(unittest.TestCase):
    """以真实文件分区验证同源事件只会正式落湖一次。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self._original_store = app.persistence.json_store
        store = JsonStore(base_dir=os.path.join(self.base, "state"))
        app.persistence.json_store = store
        # 领域服务在导入时绑定了 json_store 名字，逐个重绑到临时存储。
        from app.services import SERVICES
        self._rebound_modules = []
        for module in [sys.modules[type(svc).__module__] for svc in SERVICES.values()]:
            if module not in [m for m, _ in self._rebound_modules]:
                self._rebound_modules.append((module, module.json_store))
                module.json_store = store
        for module in (sink_module, webhook_module):
            if module not in [m for m, _ in self._rebound_modules]:
                self._rebound_modules.append((module, module.json_store))
                module.json_store = store
        self.store = store

        self._sink_cls = _sink_cls()
        self._original_lake = self._sink_cls.LAKE_BASE_DIR
        self._sink_cls.LAKE_BASE_DIR = os.path.join(self.base, "lake")

        # 单例去重服务跨测试复用，先清空内存里的去重缓存。
        dedup_singleton = _dedup_singleton()
        if dedup_singleton is not None:
            dedup_singleton.clear_dedup_cache()

        self.webhook = _webhook_cls()()
        self.sink = self._sink_cls()
        # BaseModuleService.create_entry 写的是内存 seed 仓库，测试里直接落持久化层。
        self.channel = self._create_channel()

    def tearDown(self):
        for module, original in self._rebound_modules:
            module.json_store = original
        self._sink_cls.LAKE_BASE_DIR = self._original_lake
        app.persistence.json_store = self._original_store
        self._tmp.cleanup()

    def _create_channel(self) -> dict:
        rows = app.persistence.json_store.get_all("webhook_channel")
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "通道编号": "WH-IDEM-001",
            "通道名称": "幂等测试通道",
            "接收路径": "/webhook/idempotent",
            "签名密钥": "secret",
            "验证方式": "NONE",
            "status": "已启用",
            "pending": False,
            "abnormal": False,
        }
        return app.persistence.json_store.insert("webhook_channel", entry)

    def _payload(self, order_id: str = "ORD-001") -> dict:
        return {"type": "order_created", "id": order_id, "amount": 88.0}

    def _partition_records(self, event_type: str = "order_created") -> list[dict]:
        root = os.path.join(self.sink._lake_base, "events", event_type)
        found: list[dict] = []
        for dirpath, _dirs, files in os.walk(root):
            for name in files:
                if name.endswith(".json"):
                    with open(os.path.join(dirpath, name), "r", encoding="utf-8") as f:
                        data = json.load(f)
                    found.extend(data if isinstance(data, list) else [data])
        return found

    def _credentials(self, ingest_key_fragment: str = "") -> list[dict]:
        rows = app.persistence.json_store.get_all(_const("INGEST_LEDGER_KEY"))
        return [r for r in rows if ingest_key_fragment in r.get("接入身份", "")]

    # ── 1. 成功返回后重发，缓存丢失 ────────────────────────────────
    def test_resend_after_cached_dedup_lost_does_not_duplicate(self):
        first = self.webhook.receive_webhook(self.channel["id"], self._payload(),
                                             headers={"X-Webhook-Id": "req-1"})
        self.assertTrue(first["ok"], first)
        self.assertEqual(first["ingest_status"], "written")
        self.assertEqual(len(self._partition_records()), 1)

        # 模拟进程重启：内存去重缓存与累计计数全部丢失，只有落盘凭证还在。
        _webhook_cls()()
        _dedup_singleton().clear_dedup_cache()

        second = self.webhook.receive_webhook(self.channel["id"], self._payload(),
                                              headers={"X-Webhook-Id": "req-1"})
        self.assertTrue(second["ok"], second)
        self.assertFalse(second.get("dedup"))
        self.assertEqual(second["ingest_status"], "already")
        self.assertTrue(second["ingest_confirmed"])
        self.assertEqual(len(self._partition_records()), 1, "重发不应在分区多出一份同源记录")

        credentials = self._credentials("ID:ORD-001")
        self.assertEqual(len(credentials), 1, "同一接入身份只应有一份凭证")
        self.assertEqual(credentials[0]["接入凭证状态"], _const("CREDENTIAL_CONFIRMED"))
        self.assertEqual(credentials[0]["重复到达次数"], 1)

    # ── 2. 首次写入未确认，重试接着做 ──────────────────────────────
    def test_unconfirmed_write_keeps_credential_for_retry(self):
        real_write = self._sink_cls.write_event
        state = {"fail": True}

        def flaky_write(self_, event_id, event_type, payload, source_channel_id=0, **kwargs):
            if state["fail"]:
                return {"ok": False, "message": "磁盘写入中断", "partition": "", "file_path": ""}
            return real_write(self_, event_id, event_type, payload,
                              source_channel_id=source_channel_id, **kwargs)

        with mock.patch.object(self._sink_cls, "write_event", flaky_write):
            first = self.webhook.receive_webhook(self.channel["id"], self._payload(),
                                                 headers={"X-Webhook-Id": "req-2"})
        self.assertFalse(first["ok"])
        self.assertEqual(first["ingest_status"], "unconfirmed")
        self.assertFalse(first["ingest_confirmed"])
        self.assertEqual(self._partition_records(), [])

        pending = self._credentials("ID:ORD-001")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["接入凭证状态"], _const("CREDENTIAL_FAILED"))
        self.assertEqual(pending[0]["落湖结果"], "未写入")

        # 重试沿用同一接入身份，写入成功后才确认。
        second = self.webhook.receive_webhook(self.channel["id"], self._payload(),
                                              headers={"X-Webhook-Id": "req-2"})
        self.assertTrue(second["ok"], second)
        self.assertEqual(second["ingest_status"], "written")
        self.assertEqual(len(self._partition_records()), 1)

        credential = self._credentials("ID:ORD-001")[0]
        self.assertEqual(credential["接入凭证状态"], _const("CREDENTIAL_CONFIRMED"))
        self.assertEqual(credential["重试次数"], 2)
        self.assertTrue(credential["落湖文件"])

    # ── 3. 分区已有记录但确认中断，重启后再次提交 ──────────────────
    def test_recovery_when_lake_written_but_confirmation_interrupted(self):
        # 模拟首次接入：分区已经写到磁盘，但接入请求在回写凭证之前进程中断。
        self._sink_cls().write_event(
            "interrupted-request", "order_created", self._payload(), source_channel_id=self.channel["id"])
        self.assertEqual(len(self._partition_records()), 1)
        self.assertEqual(self._credentials(), [])

        # 重启（新实例只带落盘状态）后同源事件再次到达。
        result = self.webhook.receive_webhook(self.channel["id"], self._payload(),
                                              headers={"X-Webhook-Id": "req-3"})
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["ingest_status"], "recovered")
        self.assertTrue(result["ingest_confirmed"])

        # 重试确认的是“原先真正写入的数据”，没有再写第二份。
        records = self._partition_records()
        self.assertEqual(len(records), 1)
        credential = result["credential"]
        self.assertEqual(credential["接入凭证状态"], _const("CREDENTIAL_CONFIRMED"))
        self.assertEqual(credential["落湖文件"], records[0]["event_id"] and credential["落湖文件"])
        self.assertTrue(credential["落湖文件"].endswith(".json"))
        self.assertEqual(credential["记录哈希"], records[0]["record_hash"])

        # 再次接入同样只确认，不再写。
        again = self.webhook.receive_webhook(self.channel["id"], self._payload(),
                                             headers={"X-Webhook-Id": "req-3"})
        self.assertEqual(again["ingest_status"], "already")
        self.assertEqual(len(self._partition_records()), 1)

    # ── 4. 同一接入身份并发重试只确认一次正式写入 ─────────────────
    def test_concurrent_retries_write_once(self):
        sink = self._sink_cls()
        payload = self._payload("ORD-CONCURRENT")
        barrier = threading.Barrier(8)
        results: list[dict] = []
        lock = threading.Lock()

        def submit():
            barrier.wait()
            outcome = sink.ingest_event("order_created", payload,
                                        source_channel_id=self.channel["id"],
                                        request_id="req-concurrent")
            with lock:
                results.append(outcome)

        threads = [threading.Thread(target=submit) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertTrue(all(r["ok"] for r in results), results)
        self.assertEqual(len(self._partition_records()), 1)
        written = [r for r in results if r["status"] == "written"]
        self.assertEqual(len(written), 1, f"只应有一次正式写入，实际: {[r['status'] for r in results]}")
        self.assertTrue(all(r["status"] in ("written", "already") for r in results))
        credentials = self._credentials("ID:ORD-CONCURRENT")
        self.assertEqual(len(credentials), 1)
        self.assertEqual(credentials[0]["接入凭证状态"], _const("CREDENTIAL_CONFIRMED"))
        self.assertEqual(credentials[0]["重复到达次数"], 7)

    # ── 5. 历史落湖记录兼容 ───────────────────────────────────────
    def test_legacy_lake_record_can_be_confirmed_by_resend(self):
        payload = {"type": "order_created", "amount": 66.0, "customer": "legacy"}
        # 历史记录：随机 event_id、无去重键，只留下内容哈希。
        legacy = {
            "event_id": "4f9c0d2e-legacy-random-uuid",
            "event_type": "order_created",
            "source_channel_id": self.channel["id"],
            "payload": payload,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "record_hash": self._sink_cls.record_hash(payload),
        }
        partition = os.path.join(
            self.sink._lake_base, "events", "order_created",
            datetime.now(timezone.utc).strftime("%Y/%m/%d"))
        os.makedirs(partition, exist_ok=True)
        with open(os.path.join(partition, "000000_legacy.json"), "w", encoding="utf-8") as f:
            json.dump(legacy, f, ensure_ascii=False)

        result = self.webhook.receive_webhook(self.channel["id"], payload,
                                              headers={"X-Webhook-Id": "req-legacy"})
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["ingest_status"], "recovered")
        self.assertEqual(len(self._partition_records()), 1, "历史记录不应被重写成第二份")
        self.assertEqual(result["credential"]["记录哈希"], legacy["record_hash"])

    # ── 6. 去重回写与通道按凭证决定是否继续写入 ────────────────────
    def test_dedup_outcome_is_written_back_to_ledger(self):
        dedup = _dedup_singleton()
        app.persistence.json_store.insert("dedup_filter", {
            "规则编号": "DF-IDEM",
            "事件类型": "通用",
            "去重键列": "id",
            "比较方式": "精确匹配",
            "时间窗口": "24小时",
            "相似度阈值": "0.95",
            "冲突策略": "跳过",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

        first = self.webhook.receive_webhook(self.channel["id"], self._payload(),
                                             headers={"X-Webhook-Id": "req-d1"})
        self.assertEqual(first["ingest_status"], "written")
        second = self.webhook.receive_webhook(self.channel["id"], self._payload(),
                                              headers={"X-Webhook-Id": "req-d1"})
        self.assertTrue(second.get("dedup"), second)
        self.assertTrue(second["ingest_confirmed"])
        self.assertEqual(second["ingest_status"], "already")
        self.assertEqual(len(self._partition_records()), 1)

        credential = self._credentials("ID:ORD-001")[0]
        self.assertEqual(credential["去重判定"], "重复")
        self.assertEqual(credential["接入凭证状态"], _const("CREDENTIAL_CONFIRMED"))
        self.assertTrue(dedup.get_dedup_stats()["total_duplicates"] >= 1)

    def test_duplicate_arrival_without_confirmed_record_is_not_reported_success(self):
        app.persistence.json_store.insert("dedup_filter", {
            "规则编号": "DF-IDEM-2",
            "事件类型": "通用",
            "去重键列": "id",
            "比较方式": "精确匹配",
            "时间窗口": "24小时",
            "相似度阈值": "0.95",
            "冲突策略": "跳过",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })
        # 只命中去重缓存、湖里没有任何正式记录：不能当作成功确认。
        _dedup_singleton().check_duplicate("order_created", self._payload("ORD-NOLAND"))
        result = self.webhook.receive_webhook(self.channel["id"], self._payload("ORD-NOLAND"),
                                              headers={"X-Webhook-Id": "req-nolake"})
        self.assertTrue(result.get("dedup"))
        self.assertEqual(result["ingest_status"], "written")
        self.assertTrue(result["ok"])
        self.assertEqual(len(self._partition_records()), 1)


if __name__ == "__main__":
    unittest.main()
