"""Webhook通道领域服务：事件接收、签名验证、Payload解析与分发编排。

提供完整的Webhook接收流程：签名校验→Payload验证→事件解析→去重→分发路由→落湖。
关联模块：event_schema（结构校验）、dedup_filter（幂等去重）、dispatch_rule（条件路由）、data_lake_sink（落湖存储）。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class WebhookChannelService(BaseModuleService):
    MODULE = "webhook_channel"
    ENTITY = "通道配置"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["通道编号", "通道名称", "接收路径", "签名密钥", "验证方式", "并发上限", "回调地址", "通道版本"]
    REQUIRED_FIELDS = ["通道编号", "通道名称", "接收路径"]
    STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启用通道": "已启用", "停用通道": "已停用"}
    NEGATIVE_ACTIONS = ["停用通道"]

    def __init__(self) -> None:
        super().__init__()
        self._received_events: list[dict[str, Any]] = []

    def receive_webhook(self, channel_id: int, payload: dict[str, Any],
                        signature: str | None = None,
                        headers: dict[str, str] | None = None) -> dict[str, Any]:
        """接收Webhook事件的核心方法。

        完整流程：
        1. 通道状态校验
        2. 签名验证（HMAC-SHA256或RAW）
        3. Payload结构校验
        4. 事件去重判定
        5. 分发规则匹配
        6. 数据落湖

        返回处理结果包含完整的处理轨迹。
        """
        channel = json_store.get_by_id(self.MODULE, channel_id)
        if channel is None:
            return {"ok": False, "message": f"通道 {channel_id} 不存在", "event_id": None, "stage": "通道校验"}

        if channel.get("status") != "已启用":
            return {"ok": False, "message": f"通道 {channel_id} 未启用（当前状态: {channel['status']}）", "event_id": None, "stage": "状态校验"}

        trace: list[dict[str, Any]] = []

        # 阶段1：签名验证
        secret = str(channel.get("签名密钥", ""))
        verify_method = str(channel.get("验证方式", "HMAC-SHA256"))
        sign_ok = self._verify_signature(payload, signature, secret, verify_method)
        trace.append({"stage": "签名验证", "passed": sign_ok, "method": verify_method})

        if not sign_ok and verify_method != "NONE":
            self._record_receive_event(channel_id, payload, "signature_failed", trace)
            return {"ok": False, "message": "签名验证失败", "event_id": None, "stage": "签名验证", "trace": trace}

        # 阶段2：事件结构校验（跨模块：event_schema）
        event_type = payload.get("type") or payload.get("event_type", "unknown")
        schema_validation = self._validate_event_schema(channel_id, event_type, payload)
        trace.append({"stage": "结构校验", "passed": schema_validation["ok"], "detail": schema_validation.get("message")})

        if not schema_validation["ok"]:
            self._record_receive_event(channel_id, payload, "schema_invalid", trace)
            return {"ok": False, "message": f"事件结构校验失败: {schema_validation['message']}", "event_id": None, "stage": "结构校验", "trace": trace}

        # 阶段3：去重判定（跨模块：dedup_filter）
        # 事件ID优先沿用接入方请求头里的 X-Webhook-Id：同一接入请求重发时它是稳定身份的
        # 一部分；缺失时才生成新的，避免重发落到一个全新的身份上。
        event_id = self._resolve_event_id(headers)
        dedup_result = self._check_dedup(channel_id, event_type, payload)
        trace.append({"stage": "去重判定", "passed": dedup_result["ok"], "is_duplicate": dedup_result.get("is_duplicate")})

        if dedup_result.get("is_duplicate"):
            # 去重缓存只是快速路径，重复事件仍要按落湖凭证确认：缓存丢失后重发的事件
            # 也要能认出分区里的正式记录。未确认前不回写给接入方。
            sink_result = self._sink_event(event_id, event_type, payload, channel_id, dedup_hit=True)
            credential = sink_result.get("credential") or {}
            trace.append({"stage": "数据落湖", "passed": sink_result["ok"],
                          "detail": sink_result.get("message"), "dedup": sink_result.get("status")})
            self._record_receive_event(channel_id, payload, "duplicate", trace)
            return {
                # 重复到达本身不算接入失败；确认状态单独用 ingest_confirmed 表达，
                # 未确认时接入方据 ok=False 重试。
                "ok": sink_result["ok"],
                "message": "事件已去重（重复事件）" if sink_result["ok"] else f"事件重复且落湖未确认: {sink_result.get('message')}",
                "event_id": sink_result.get("event_id") or event_id,
                "stage": "去重判定",
                "trace": trace,
                "skipped": True,
                "dedup": True,
                "ingest_status": sink_result.get("status"),
                "ingest_confirmed": sink_result["ok"],
                "credential": credential,
            }

        # 阶段4：分发路由（跨模块：dispatch_rule）
        dispatch_targets = self._match_dispatch_rules(event_type, payload)
        trace.append({"stage": "分发路由", "targets": len(dispatch_targets), "routes": [t.get("目标系统") for t in dispatch_targets]})

        # 阶段5：数据落湖（跨模块：data_lake_sink）
        # 落湖凭证决定是否继续写入：已确认的同源事件不再写分区，只回写确认状态。
        sink_result = self._sink_event(event_id, event_type, payload, channel_id, dedup_hit=False)
        credential = sink_result.get("credential") or {}
        trace.append({"stage": "数据落湖", "passed": sink_result["ok"],
                      "detail": sink_result.get("message"), "ingest_status": sink_result.get("status")})

        if not sink_result["ok"]:
            # 首次写入未确认：不能向接入方返回成功，凭证保持待确认以便重试续做。
            self._record_receive_event(channel_id, payload, "ingest_unconfirmed", trace)
            return {
                "ok": False,
                "message": f"事件落湖未确认，可重试: {sink_result.get('message')}",
                "event_id": sink_result.get("event_id") or event_id,
                "stage": "数据落湖",
                "trace": trace,
                "dispatch_targets": [t.get("目标系统") for t in dispatch_targets],
                "ingest_status": sink_result.get("status"),
                "ingest_confirmed": False,
                "credential": credential,
            }

        # 记录成功接收并累加通道计数。
        self._record_receive_event(channel_id, payload, "success", trace)
        json_store.update(self.MODULE, channel_id, {
            "last_event_at": datetime.now(timezone.utc).isoformat(),
            "total_received": int(channel.get("total_received", 0)) + 1,
        })

        if sink_result.get("status") in ("already", "recovered"):
            message = ("同源事件已落湖，确认凭证后跳过重复写入"
                       if sink_result.get("status") == "already"
                       else "分区已存在正式记录，补确认接入凭证（未重复写入）")
        else:
            message = f"事件接收成功，已路由至 {len(dispatch_targets)} 个目标系统"

        return {
            "ok": True,
            "message": message,
            "event_id": sink_result.get("event_id") or event_id,
            "stage": "完成",
            "trace": trace,
            "dispatch_targets": [t.get("目标系统") for t in dispatch_targets],
            "ingest_status": sink_result.get("status"),
            "ingest_confirmed": True,
            "credential": credential,
        }

    def _resolve_event_id(self, headers: dict[str, str] | None) -> str:
        """解析接入请求身份：优先接入方请求头，缺失时才生成。"""
        for key, value in (headers or {}).items():
            if key.lower() in ("x-webhook-id", "x-event-id", "x-idempotency-key") and value:
                return f"wh-{value}"
        return str(uuid.uuid4())

    def _verify_signature(self, payload: dict[str, Any], signature: str | None,
                          secret: str, method: str) -> bool:
        """验证Webhook签名。

        支持HMAC-SHA256和SHA256两种验证方式。
        """
        if method == "NONE":
            return True
        if not signature:
            return False
        body = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        if method == "HMAC-SHA256":
            expected = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
            return hmac.compare_digest(expected, signature)
        elif method == "SHA256":
            expected = hashlib.sha256(body.encode()).hexdigest()
            return hmac.compare_digest(expected, signature)
        return False

    def _validate_event_schema(self, channel_id: int, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        """跨模块调用event_schema进行事件结构校验。"""
        try:
            from app.services.event_schema import EventSchemaService
            schema_svc = EventSchemaService()
            return schema_svc.validate_event(channel_id, event_type, payload)
        except Exception as e:
            return {"ok": True, "message": f"结构校验跳过（Schema服务不可用: {e}）"}

    def _check_dedup(self, channel_id: int, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        """跨模块调用dedup_filter进行事件去重判定。

        复用服务注册表里的去重服务实例：去重缓存只在进程内有效，每次新建实例
        会让缓存永远为空，重发事件根本判定不出重复。
        """
        try:
            from app.services import service_for
            from app.services.dedup_filter import DedupFilterService
            dedup_svc = service_for("dedup_filter") or DedupFilterService()
            return dedup_svc.check_duplicate(event_type, payload)
        except Exception:
            return {"ok": True, "is_duplicate": False}

    def _match_dispatch_rules(self, event_type: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """跨模块调用dispatch_rule进行分发目标匹配。"""
        try:
            from app.services.dispatch_rule import DispatchRuleService
            dispatch_svc = DispatchRuleService()
            return dispatch_svc.match_targets(event_type, payload)
        except Exception:
            return []

    def _sink_event(self, event_id: str, event_type: str, payload: dict[str, Any],
                    channel_id: int, dedup_hit: bool | None = None) -> dict[str, Any]:
        """跨模块调用data_lake_sink进行事件数据落湖。

        走带幂等凭证的 ingest_event：由凭证决定是否真正写分区，并把确认状态返回给接入方。
        """
        try:
            from app.services.data_lake_sink import DataLakeSinkService
            sink_svc = DataLakeSinkService()
            return sink_svc.ingest_event(event_type, payload, source_channel_id=channel_id,
                                         request_id=event_id, dedup_hit=dedup_hit)
        except Exception as e:
            return {"ok": False, "message": f"落湖失败: {e}", "status": "unconfirmed", "credential": None}

    def _record_receive_event(self, channel_id: int, payload: dict[str, Any],
                              outcome: str, trace: list[dict[str, Any]]) -> None:
        """记录Webhook接收事件的完整轨迹。"""
        record = {
            "channel_id": channel_id,
            "event_type": payload.get("type") or payload.get("event_type", "unknown"),
            "payload_hash": hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16],
            "outcome": outcome,
            "trace": trace,
            "received_at": datetime.now(timezone.utc).isoformat(),
        }
        self._received_events.append(record)

    def get_channel_stats(self, channel_id: int) -> dict[str, Any]:
        """获取通道的接收统计信息。"""
        channel = json_store.get_by_id(self.MODULE, channel_id)
        if channel is None:
            return {"ok": False, "message": f"通道 {channel_id} 不存在"}

        channel_events = [e for e in self._received_events if e["channel_id"] == channel_id]
        return {
            "ok": True,
            "channel": channel.get("通道名称"),
            "total_received": len(channel_events),
            "success_count": sum(1 for e in channel_events if e["outcome"] == "success"),
            "signature_failures": sum(1 for e in channel_events if e["outcome"] == "signature_failed"),
            "schema_failures": sum(1 for e in channel_events if e["outcome"] == "schema_invalid"),
            "duplicates": sum(1 for e in channel_events if e["outcome"] == "duplicate"),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出Webhook通道的数据质量快照，包含跨模块链路健康信息。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "Webhook通道数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_events_received": len(self._received_events),
            "active_channels": sum(1 for r in rows if r.get("status") == "已启用"),
            "operation": self.OPERATION_TERM,
        }