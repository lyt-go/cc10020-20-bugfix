"""数据落湖领域服务：数据写入本地存储、分区组织与生命周期管理。

将接入的数据按日期/来源/类型分区写入本地JSON文件，
支持多种写入格式和压缩策略。
跨模块关联：webhook_channel（事件来源）、batch_ingest（批量来源）、event_archive（归档接口）。
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import zlib
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService

# 落湖接入清单（幂等凭证）所在分区。凭证是落湖幂等的唯一可信来源：
# 去重缓存只存在内存里，进程重启或清缓存后会丢失，不能用来判断是否已经落湖。
INGEST_LEDGER_KEY = "lake_ingest_ledger"
CREDENTIAL_PENDING = "待确认"
CREDENTIAL_CONFIRMED = "已确认"
CREDENTIAL_FAILED = "写入失败"

# 同一进程内的所有服务实例共享同一把落湖锁，保证并发重试只真正写入一次。
_LAKE_INGEST_LOCK = threading.RLock()


class DataLakeSinkService(BaseModuleService):
    MODULE = "data_lake_sink"
    ENTITY = "落湖配置"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["配置编号", "数据来源", "目标路径", "分区策略", "写入格式", "压缩算法", "保留天数", "落湖水位"]
    REQUIRED_FIELDS = ["配置编号", "数据来源", "目标路径"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    # 允许测试或部署把湖目录指到别处，默认仍在 backend/data_store/lake。
    LAKE_BASE_DIR: str | None = None

    def __init__(self) -> None:
        super().__init__()
        self._lake_base = self.LAKE_BASE_DIR or os.environ.get("LAKE_BASE_DIR") or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data_store", "lake")
        os.makedirs(self._lake_base, exist_ok=True)
        self._write_stats: dict[str, int] = {}

    # ── 落湖幂等凭证（接入清单）────────────────────────────────────────
    # 同源事件可能因为接入方重发、接入请求在回写确认前中断而重复到达。
    # 每次接入都先按“接入身份”（通道 + 事件类型 + 去重键）在清单里登记一条
    # 凭证，再由凭证决定是否真正写分区：
    #   * 分区里已有正式记录 -> 只把凭证从“待确认”推进到“已确认”，返回重复到达；
    #   * 分区里还没有记录   -> 抢占凭证后写入，写成功即确认，写失败保持待确认；
    # 凭证落盘在 lake_ingest_ledger 分区，重启后仍能关联到历史落湖记录。

    def ingest_candidates(self, source_channel_id: int, event_type: str,
                          payload: dict[str, Any]) -> list[str]:
        """计算同源事件可能对应的历史接入身份（兼容历史落湖记录）。

        新记录按 payload 的 id/event_id 去重；历史 webhook 落湖记录用的是随机
        event_id、payload 里没有去重键，只留下记录哈希，因此同时给出内容哈希候选，
        按顺序命中第一个已确认的凭证。
        """
        candidates: list[str] = []
        for field in ("id", "event_id", "eventId"):
            value = payload.get(field)
            if value not in (None, ""):
                candidates.append(f"ID:{value}")
        record_hash = self.record_hash(payload)
        candidates.append(f"HASH:{record_hash}")
        return candidates

    @staticmethod
    def record_hash(payload: dict[str, Any]) -> str:
        """与分区记录里的 record_hash 保持同一算法，便于和历史记录对齐。"""
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()[:16]

    @staticmethod
    def build_ingest_key(source_channel_id: int, event_type: str, candidate: str) -> str:
        """接入身份：同一通道、同一事件类型下的同源事件共享一个身份。"""
        return f"{source_channel_id}|{event_type}|{candidate}"

    def find_credential(self, ingest_key: str) -> dict[str, Any] | None:
        for row in json_store.get_all(INGEST_LEDGER_KEY):
            if row.get("接入身份") == ingest_key:
                return dict(row)
        return None

    def list_credentials(self, source_channel_id: int | None = None) -> list[dict[str, Any]]:
        rows = json_store.get_all(INGEST_LEDGER_KEY)
        if source_channel_id is not None:
            rows = [r for r in rows if int(r.get("source_channel_id", 0)) == int(source_channel_id)]
        return [dict(r) for r in rows]

    def _register_credential(self, ingest_key: str, event_type: str,
                             source_channel_id: int, request_id: str) -> dict[str, Any]:
        """登记一条待确认的接入凭证（每个接入身份只登记一次）。"""
        existing = self.find_credential(ingest_key)
        if existing is not None:
            return existing
        try:
            return json_store.insert(INGEST_LEDGER_KEY, {
                "接入身份": ingest_key,
                "source_channel_id": source_channel_id,
                "事件类型": event_type,
                "接入凭证状态": CREDENTIAL_PENDING,
                "接入请求": request_id,
                "落湖结果": "",
                "分区": "",
                "落湖文件": "",
                "记录哈希": "",
                "首次接入时间": datetime.now(timezone.utc).isoformat(),
                "确认时间": "",
                "重试次数": 0,
                "重复到达次数": 0,
                "最近接入请求": request_id,
                "最近接入时间": datetime.now(timezone.utc).isoformat(),
            })
        except Exception:
            # 并发登记时另一方已经写入，读取已存在的凭证即可。
            found = self.find_credential(ingest_key)
            if found is None:
                raise
            return found

    def _update_credential(self, ingest_key: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        for row in json_store.get_all(INGEST_LEDGER_KEY):
            if row.get("接入身份") == ingest_key:
                return json_store.update(INGEST_LEDGER_KEY, int(row["id"]), updates)
        return None

    def _find_lake_record(self, event_type: str, payload: dict[str, Any],
                          source_channel_id: int = 0,
                          event_id: str | None = None) -> dict[str, Any] | None:
        """在湖里查找已经正式落地的同源记录。

        分区文件一旦出现就代表正式写入已完成（原子 rename），因此查到即确认。
        按来源通道限定范围，避免把别的通道的同内容事件误认成本次接入的落湖结果。
        """
        content_hash = self.record_hash(payload)
        wanted_ids = {event_id} if event_id else set()
        wanted_ids.discard(None)

        for root, _dirs, files in os.walk(self._lake_base):
            parts = os.path.relpath(root, self._lake_base).split(os.sep)
            if "events" not in parts:
                continue
            idx = parts.index("events")
            if len(parts) <= idx + 1 or parts[idx + 1] != event_type:
                continue
            for fname in sorted(files):
                if not fname.endswith(".json"):
                    continue
                try:
                    with open(os.path.join(root, fname), "r", encoding="utf-8") as f:
                        data = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue
                for record in data if isinstance(data, list) else [data]:
                    if source_channel_id and record.get("source_channel_id") not in (None, source_channel_id):
                        continue
                    record_id = record.get("event_id")
                    id_hit = record_id is not None and record_id in wanted_ids
                    hash_hit = record.get("record_hash") == content_hash
                    if id_hit or hash_hit:
                        return {
                            "event_id": record_id,
                            "file_path": os.path.join(root, fname),
                            "partition": os.path.relpath(root, self._lake_base),
                            "record_hash": record.get("record_hash", content_hash),
                            "ingested_at": record.get("ingested_at"),
                        }
        return None

    def ingest_event(self, event_type: str, payload: dict[str, Any],
                     source_channel_id: int = 0,
                     request_id: str = "",
                     dedup_hit: bool | None = None) -> dict[str, Any]:
        """带幂等凭证的事件落湖入口（Webhook 通道使用）。

        返回 status：
          * ``written``     首次写入已完成并确认；
          * ``already``     同源事件此前已确认落湖，本次只确认凭证、不重复写；
          * ``recovered``   分区里已有正式记录但确认中断（含历史记录），本次补确认；
          * ``unconfirmed`` 抢占凭证后写入未成功，凭证保持待确认，重试可继续。

        只有写入/补确认成功才返回 ok=True 并推进凭证状态；否则凭证指向实际结果，
        接入方据此重试。
        """
        candidates = self.ingest_candidates(source_channel_id, event_type, payload)
        digest = self.record_hash(payload)
        dedup_update = {}
        if dedup_hit is not None:
            # 去重判定结果回写接入清单，便于回溯“当时为什么当作重复事件”。
            dedup_update = {
                "去重判定": "重复" if dedup_hit else "首次",
                "去重判定时间": datetime.now(timezone.utc).isoformat(),
            }

        with _LAKE_INGEST_LOCK:
            for candidate in candidates:
                ingest_key = self.build_ingest_key(source_channel_id, event_type, candidate)
                credential = self.find_credential(ingest_key)

                if credential and credential.get("接入凭证状态") == CREDENTIAL_CONFIRMED:
                    # 已确认的凭证就是落湖结果的权威记录：分区记录之后被归档也不重复写。
                    self._update_credential(ingest_key, {
                        "重复到达次数": int(credential.get("重复到达次数", 0)) + 1,
                        "最近接入请求": request_id,
                        "最近接入时间": datetime.now(timezone.utc).isoformat(),
                        **dedup_update,
                    })
                    return self._ingest_response(
                        "already", self.find_credential(ingest_key) or credential,
                        digest, source_channel_id,
                        message="同源事件已落湖，确认凭证后跳过重复写入",
                    )

                existing = self._find_lake_record(event_type, payload, source_channel_id)
                if existing is not None:
                    # 分区里已有正式记录：本次重试只补确认，不再写第二份。
                    credential = self._confirm_credential(
                        ingest_key, event_type, source_channel_id, request_id,
                        existing["partition"], existing["file_path"],
                        existing.get("record_hash", digest), dedup_update,
                    )
                    return self._ingest_response(
                        "recovered", credential, digest, source_channel_id,
                        message="分区已存在正式记录，补确认接入凭证（未重复写入）",
                    )

                credential = self._register_credential(
                    ingest_key, event_type, source_channel_id, request_id)
                # 到达这里的凭证只可能是“待确认/写入失败”：同一接入身份的重试落到同一份凭证，
                # 而不是各写各的。分区文件按接入身份独占创建，跨进程并发也只会有一次正式写入。
                token = hashlib.sha256(ingest_key.encode()).hexdigest()[:24]
                write_result = self.write_event(
                    token, event_type, payload,
                    source_channel_id=source_channel_id, dedup_token=token,
                )
                if write_result.get("ok"):
                    credential = self._confirm_credential(
                        ingest_key, event_type, source_channel_id, request_id,
                        write_result.get("partition", ""), write_result.get("file_path", ""),
                        write_result.get("record_hash", digest), dedup_update,
                    )
                    return self._ingest_response(
                        "written", credential, digest, source_channel_id,
                        message="首次写入完成并确认",
                    )

                # 写入失败：凭证保持“待确认”并指向实际结果，重试会沿用同一身份续做。
                pending = self._update_credential(ingest_key, {
                    "接入凭证状态": CREDENTIAL_FAILED,
                    "落湖结果": "未写入",
                    "失败原因": write_result.get("message", "写入失败"),
                    "分区": write_result.get("partition", ""),
                    "落湖文件": write_result.get("file_path", ""),
                    "记录哈希": digest,
                    "重试次数": int(credential.get("重试次数", 0)) + 1,
                    "最近接入请求": request_id,
                    "最近接入时间": datetime.now(timezone.utc).isoformat(),
                    **dedup_update,
                }) or credential
                return self._ingest_response(
                    "unconfirmed", pending, digest, source_channel_id,
                    message=write_result.get("message", "写入失败"),
                )

        # 理论上 candidates 至少包含内容哈希候选，这里只是防御性兜底。
        return {"ok": False, "status": "unconfirmed", "message": "无法确定接入身份",
                "credential": None, "event_id": "", "partition": "", "file_path": "",
                "record_hash": digest}

    def _confirm_credential(self, ingest_key: str, event_type: str, source_channel_id: int,
                            request_id: str, partition: str, file_path: str,
                            record_hash: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """把凭证推进到已确认，并把实际落湖位置写回凭证。"""
        credential = self.find_credential(ingest_key) or self._register_credential(
            ingest_key, event_type, source_channel_id, request_id)
        now = datetime.now(timezone.utc).isoformat()
        updated = self._update_credential(ingest_key, {
            "接入凭证状态": CREDENTIAL_CONFIRMED,
            "落湖结果": "已落湖",
            "失败原因": "",
            "分区": partition,
            "落湖文件": file_path,
            "记录哈希": record_hash,
            "确认时间": now,
            "重试次数": int(credential.get("重试次数", 0)) + 1,
            "最近接入请求": request_id,
            "最近接入时间": now,
            **(extra or {}),
        })
        return updated or credential

    def _ingest_response(self, status: str, credential: dict[str, Any], digest: str,
                         source_channel_id: int, message: str) -> dict[str, Any]:
        confirmed = credential.get("接入凭证状态") == CREDENTIAL_CONFIRMED
        return {
            "ok": confirmed,
            "status": status,
            "message": message,
            "credential": credential,
            "event_id": f"{source_channel_id}:{digest}",
            "partition": credential.get("分区", ""),
            "file_path": credential.get("落湖文件", ""),
            "record_hash": credential.get("记录哈希", digest),
        }

    def write_event(self, event_id: str, event_type: str, payload: dict[str, Any],
                    source_channel_id: int = 0,
                    dedup_token: str | None = None) -> dict[str, Any]:
        """将事件数据写入数据湖。

        写入流程：
        1. 确定分区路径（按日期/事件类型）
        2. 序列化数据（默认JSON格式）
        3. 写入分区文件
        4. 更新写入统计

        Args:
            event_id: 事件唯一标识
            event_type: 事件类型
            payload: 事件数据
            source_channel_id: 来源通道ID
            dedup_token: 同源事件的稳定文件名标记。给出时以独占创建写入，
                文件已存在说明另一个接入（可能是别的进程）已经写完，直接复用结果。

        Returns:
            { ok: bool, message: str, partition: str, file_path: str }
        """
        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_partition)
        os.makedirs(partition_path, exist_ok=True)

        # 构建数据记录
        record = {
            "event_id": event_id,
            "event_type": event_type,
            "source_channel_id": source_channel_id,
            "payload": payload,
            "ingested_at": now.isoformat(),
            "record_hash": hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16],
        }

        # 写入分区文件。先写临时文件再 rename：分区里只要出现正式文件，
        # 就说明这份记录已经完整落地，重试才有据可依。
        if dedup_token:
            file_name = f"{dedup_token}.json"
        else:
            file_name = f"{now.strftime('%H%M%S')}_{event_id[:8]}.json"
        file_path = os.path.join(partition_path, file_name)
        tmp_path = file_path + ".tmp"

        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, default=str)
            if dedup_token:
                # 独占链接：同一 token 只允许一个写入者把正式文件放进来。
                try:
                    os.link(tmp_path, file_path)
                except FileExistsError:
                    os.remove(tmp_path)
                    return {
                        "ok": True,
                        "message": f"同源记录已存在，复用分区文件 {date_partition}",
                        "partition": f"events/{event_type}/{date_partition}",
                        "file_path": file_path,
                        "record_hash": record["record_hash"],
                        "existing": True,
                    }
                os.remove(tmp_path)
            else:
                os.replace(tmp_path, file_path)

            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + 1

            return {
                "ok": True,
                "message": f"数据已写入数据湖分区 {date_partition}",
                "partition": f"events/{event_type}/{date_partition}",
                "file_path": file_path,
                "record_hash": record["record_hash"],
            }
        except OSError as e:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            return {"ok": False, "message": f"写入失败: {e}", "partition": "", "file_path": ""}

    def write_batch(self, records: list[dict[str, Any]], event_type: str = "batch",
                    source_channel_id: int = 0) -> dict[str, Any]:
        """批量写入多条记录到数据湖。

        按每小时一个文件聚合写入。
        """
        if not records:
            return {"ok": True, "message": "无数据需要写入", "written": 0}

        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_partition)
        os.makedirs(partition_path, exist_ok=True)

        batch_file = f"batch_{now.strftime('%H%M%S')}.json"
        file_path = os.path.join(partition_path, batch_file)

        batch_records = []
        for i, record in enumerate(records):
            batch_records.append({
                "event_id": f"batch_{now.strftime('%Y%m%d%H%M%S')}_{i:04d}",
                "event_type": event_type,
                "source_channel_id": source_channel_id,
                "payload": record,
                "ingested_at": now.isoformat(),
            })

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(batch_records, f, ensure_ascii=False, default=str)

            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + len(batch_records)

            return {
                "ok": True,
                "message": f"批量 {len(batch_records)} 条写入完成",
                "written": len(batch_records),
                "partition": f"events/{event_type}/{date_partition}",
                "file_path": file_path,
            }
        except OSError as e:
            return {"ok": False, "message": f"批量写入失败: {e}", "written": 0}

    def query_ingest_credential(self, channel_id: int, event_type: str,
                                payload: dict[str, Any]) -> dict[str, Any]:
        """查询同源事件的落湖凭证，供接入方核对确认状态。

        返回命中的第一份凭证（按幂等键优先、内容哈希兜底），没有则 found=False。
        """
        for candidate in self.ingest_candidates(channel_id, event_type, payload):
            credential = self.find_credential(
                self.build_ingest_key(channel_id, event_type, candidate))
            if credential is not None:
                return {
                    "ok": True,
                    "found": True,
                    "confirmed": credential.get("接入凭证状态") == CREDENTIAL_CONFIRMED,
                    "status": credential.get("接入凭证状态"),
                    "credential": credential,
                }
        return {"ok": True, "found": False, "confirmed": False, "status": None, "credential": None}

    def query_lake_data(self, event_type: str, date_str: str | None = None,
                        limit: int = 100) -> dict[str, Any]:
        """查询数据湖中的指定分区数据。"""
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_str)

        if not os.path.exists(partition_path):
            return {"ok": True, "records": [], "total": 0, "partition": f"events/{event_type}/{date_str}"}

        records = []
        for fname in sorted(os.listdir(partition_path))[:limit]:
            if fname.endswith(".json"):
                try:
                    with open(os.path.join(partition_path, fname), "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            records.extend(data)
                        else:
                            records.append(data)
                except (json.JSONDecodeError, OSError):
                    continue

        return {
            "ok": True,
            "records": records[:limit],
            "total": len(records),
            "partition": f"events/{event_type}/{date_str}",
        }

    def get_lake_stats(self) -> dict[str, Any]:
        """获取数据湖存储统计信息。"""
        total_files = 0
        total_size = 0
        by_type: dict[str, int] = {}

        for root, dirs, files in os.walk(self._lake_base):
            for f in files:
                if f.endswith(".json"):
                    total_files += 1
                    filepath = os.path.join(root, f)
                    try:
                        total_size += os.path.getsize(filepath)
                    except OSError:
                        pass

                # 按事件类型统计
                parts = root.split(os.sep)
                if "events" in parts:
                    idx = parts.index("events")
                    if len(parts) > idx + 1:
                        etype = parts[idx + 1]
                        by_type[etype] = by_type.get(etype, 0) + 1

        return {
            "base_dir": self._lake_base,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "by_type": dict(sorted(by_type.items(), key=lambda x: x[1], reverse=True)),
            "write_stats": self._write_stats,
        }

    def get_partition_info(self) -> list[dict[str, Any]]:
        """获取所有分区信息。"""
        partitions = []
        for root, dirs, files in os.walk(self._lake_base):
            json_files = [f for f in files if f.endswith(".json")]
            if json_files:
                rel_path = os.path.relpath(root, self._lake_base)
                total = 0
                for f in json_files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
                partitions.append({
                    "path": rel_path,
                    "file_count": len(json_files),
                    "size_bytes": total,
                })
        return partitions

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据落湖的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        lake_stats = self.get_lake_stats()
        credentials = self.list_credentials()
        credential_status = {
            CREDENTIAL_PENDING: 0, CREDENTIAL_CONFIRMED: 0, CREDENTIAL_FAILED: 0,
        }
        for row in credentials:
            state = row.get("接入凭证状态", CREDENTIAL_PENDING)
            credential_status[state] = credential_status.get(state, 0) + 1
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据落湖数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "lake_files": lake_stats.get("total_files", 0),
            "lake_size_mb": lake_stats.get("total_size_mb", 0),
            "ingest_credentials": len(credentials),
            "ingest_by_credential_status": credential_status,
            "operation": self.OPERATION_TERM,
        }