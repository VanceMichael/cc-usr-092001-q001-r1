"""渠道分发：尝试记录、失败退避重试、人工补发与崩溃恢复。"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Protocol

from .store import Store


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ChannelError(Exception):
    """渠道暂时性发送失败，可安排重试。"""


class ChannelAdapter(Protocol):
    """渠道适配器接口：把一条报文送到受控终端引用。"""

    def send(self, endpoint_ref: str, message: dict[str, Any]) -> None: ...


class LoggingAdapter:
    """默认适配器：把报文写到标准输出并成功返回，便于独立运行演示。"""

    def __init__(self, sink: Callable[[str], None] | None = None) -> None:
        self._sink = sink or (lambda line: print(line, flush=True))

    def send(self, endpoint_ref: str, message: dict[str, Any]) -> None:
        self._sink(f"dispatch -> {endpoint_ref}: {message['summary']}")


class ScriptedAdapter:
    """测试用适配器：可按终端引用编排剩余失败次数。"""

    def __init__(self) -> None:
        self.failures: dict[str, int] = {}
        self.sent: list[tuple[str, dict[str, Any]]] = []

    def send(self, endpoint_ref: str, message: dict[str, Any]) -> None:
        if self.failures.get(endpoint_ref, 0) > 0:
            self.failures[endpoint_ref] -= 1
            raise ChannelError(f"模拟发送失败: {endpoint_ref}")
        self.sent.append((endpoint_ref, message))


class Dispatcher:
    """把待派发的送达记录逐条送到渠道，全部尝试留痕。"""

    def __init__(
        self,
        store: Store,
        adapters: dict[str, ChannelAdapter],
        *,
        max_attempts: int = 5,
        base_backoff_seconds: int = 30,
        now_fn: Callable[[], float] | None = None,
    ) -> None:
        self.store = store
        self.adapters = adapters
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds
        self._now_fn = now_fn or time.time

    def recover_inflight(self) -> int:
        """服务重启后回收崩溃时停留在 SENDING 的送达记录。"""
        with self.store.transaction() as conn:
            cursor = conn.execute(
                "UPDATE deliveries SET status='PENDING', updated_at=? WHERE status='SENDING'",
                (_utc_now_iso(),),
            )
            return cursor.rowcount

    def run_once(self, limit: int = 100) -> dict[str, int]:
        """派发一轮到期记录；返回本轮统计，便于测试与巡检。"""
        now = int(self._now_fn())
        due = self.store.all(
            """SELECT * FROM deliveries
               WHERE (status='PENDING'
                      OR (status='FAILED' AND attempt_count < ?))
                 AND next_attempt_epoch <= ?
               ORDER BY created_at, delivery_id LIMIT ?""",
            (self.max_attempts, now, limit),
        )
        sent = failed = 0
        for delivery in due:
            if self._attempt(delivery, trigger_kind="AUTO", now=now) == "SENT":
                sent += 1
            else:
                failed += 1
        return {"due": len(due), "sent": sent, "failed": failed}

    def _attempt(self, delivery: Any, *, trigger_kind: str, now: int) -> str:
        # 占用记录：只有仍处于可派发状态的记录才会被本线程处理，
        # 并发补发/重试因此自然合并，不会重复发送。
        with self.store.transaction() as conn:
            cursor = conn.execute(
                """UPDATE deliveries SET status='SENDING', updated_at=?
                   WHERE delivery_id=? AND status IN ('PENDING','FAILED')""",
                (_utc_now_iso(), delivery["delivery_id"]),
            )
            if cursor.rowcount == 0:
                return "SKIPPED"

        message = self._build_message(delivery)
        adapter = self.adapters.get(delivery["channel"])
        attempt_no = delivery["attempt_count"] + 1
        try:
            if adapter is None:
                raise ChannelError(f"未配置渠道适配器: {delivery['channel']}")
            adapter.send(delivery["endpoint_ref"], message)
        except ChannelError as exc:
            result, detail = "FAILURE", str(exc)
            status = "FAILED" if attempt_no < self.max_attempts else "EXHAUSTED"
            next_attempt = now + self.base_backoff_seconds * attempt_no
        else:
            result, detail = "SUCCESS", None
            status = "SENT"
            next_attempt = 0

        with self.store.transaction() as conn:
            conn.execute(
                """INSERT INTO dispatch_attempts
                   (attempt_id, delivery_id, attempt_no, trigger_kind,
                    result, detail, attempted_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    f"att-{uuid.uuid4().hex[:16]}",
                    delivery["delivery_id"],
                    attempt_no,
                    trigger_kind,
                    result,
                    detail,
                    _utc_now_iso(),
                ),
            )
            conn.execute(
                """UPDATE deliveries
                   SET status=?, attempt_count=?, next_attempt_epoch=?, updated_at=?
                   WHERE delivery_id=?""",
                (status, attempt_no, next_attempt, _utc_now_iso(), delivery["delivery_id"]),
            )
        return status

    def _build_message(self, delivery: Any) -> dict[str, Any]:
        segment = self.store.one(
            "SELECT * FROM region_segments WHERE segment_id=?", (delivery["segment_id"],)
        )
        version = self.store.one(
            "SELECT * FROM alert_versions WHERE version_id=?", (segment["version_id"],)
        )
        if segment["state"] == "CANCELLED":
            summary = (
                f"【解除】{delivery['region_code']} {delivery['subject_ref']} "
                f"预警已解除（第{version['version_seq']}版）"
            )
        else:
            summary = (
                f"【{version['alert_level']}】{delivery['region_code']} "
                f"{delivery['subject_ref']} 第{version['version_seq']}版"
            )
        return {
            "delivery_id": delivery["delivery_id"],
            "subject_ref": delivery["subject_ref"],
            "region_code": delivery["region_code"],
            "channel": delivery["channel"],
            "post_ref": delivery["post_ref"],
            "segment_state": segment["state"],
            "version_seq": version["version_seq"],
            "alert_level": version["alert_level"],
            "rain_range": version["rain_range"],
            "hourly_intensity": version["hourly_intensity"],
            "valid_from": version["valid_from"],
            "valid_until": version["valid_until"],
            "summary": summary,
        }
