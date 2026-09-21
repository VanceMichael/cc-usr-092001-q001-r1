"""分发调度：重试退避、人工补发与确认回执。

所有状态推进都在 SQLite 事务中完成，渠道调用夹在两个短事务之间；
进程崩溃后 ``sending`` 状态的记录会在下次扫描时恢复重试。
自动重试与人工补发共用唯一的 attempts 序列，``reissues.request_id``
和 ``inbound_acks.ack_id`` 在数据库层保证重复请求不产生副作用。
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

from .channels import Channel, ChannelError, default_channels
from .storage import Storage
from .timeutil import Instant, parse_instant

# 自动重试退避（秒），下标为“已失败次数-1”；超过则保持 failed 等待人工补发。
BACKOFF_SCHEDULE = (30, 120, 300, 900, 1800)


class DispatchError(ValueError):
    """分发请求不合法（4xx）。"""


@dataclass
class AttemptReport:
    delivery_id: str
    attempt_id: str
    kind: str
    result: str
    detail: dict[str, Any]


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:20]}"


class Dispatcher:
    def __init__(self, store: Storage, channels: dict[str, Channel] | None = None) -> None:
        self.store = store
        self.channels = channels or default_channels()
        # 串行化“认领→渠道调用→落结果”，防止后台线程与手动补发对同一记录双发。
        self._run_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 接收岗位管理
    # ------------------------------------------------------------------

    def register_recipient(self, data: dict) -> dict:
        for key in ("recipient_id", "area_code", "post", "channel", "address"):
            if not isinstance(data.get(key), str) or not data[key].strip():
                raise DispatchError(f"缺少必填字符串字段：{key}")
        if data["channel"] not in ("sms", "broadcast", "gov_terminal"):
            raise DispatchError("channel 必须是 sms/broadcast/gov_terminal")
        rid = data["recipient_id"].strip()
        now = time.time()
        with self.store.lock:
            self.store.begin()
            try:
                row = self.store.conn.execute(
                    "SELECT recipient_id FROM recipients WHERE channel=? AND address=?",
                    (data["channel"], data["address"]),
                ).fetchone()
                if row is not None and row["recipient_id"] != rid:
                    raise DispatchError(
                        f"地址已绑定接收方 {row['recipient_id']}，不能重复登记"
                    )
                self.store.conn.execute(
                    """INSERT INTO recipients(recipient_id, area_code, post, channel,
                       address, active, created_unix)
                       VALUES(?,?,?,?,?,1,?)
                       ON CONFLICT(recipient_id) DO UPDATE SET
                           area_code=excluded.area_code, post=excluded.post,
                           channel=excluded.channel, address=excluded.address,
                           active=1""",
                    (rid, data["area_code"].strip(), data["post"].strip(),
                     data["channel"], data["address"].strip(), now),
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
        return {"recipient_id": rid, "state": "registered"}

    # ------------------------------------------------------------------
    # 分发扫描
    # ------------------------------------------------------------------

    def run_due(self, now: float | None = None, limit: int = 100) -> list[AttemptReport]:
        """处理所有到期的待发送记录（含崩溃后残留在 sending 的记录）。"""
        now = time.time() if now is None else now
        reports: list[AttemptReport] = []
        with self._run_lock:
            due_ids = self._claim_due(now, limit)
            for delivery_id in due_ids:
                report = self._attempt(delivery_id, now, kind="auto", trigger_ref="scheduler")
                if report is not None:
                    reports.append(report)
        return reports

    def _claim_due(self, now: float, limit: int) -> list[str]:
        with self.store.lock:
            self.store.begin()
            try:
                rows = self.store.conn.execute(
                    """SELECT delivery_id FROM deliveries
                       WHERE state IN ('pending','sending','failed')
                         AND next_attempt_unix IS NOT NULL
                         AND next_attempt_unix <= ?
                         AND attempt_count < max_attempts
                       ORDER BY next_attempt_unix, delivery_id
                       LIMIT ?""",
                    (now, limit),
                ).fetchall()
                ids = [r["delivery_id"] for r in rows]
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
        return ids

    def _attempt(self, delivery_id: str, now: float, *, kind: str,
                 trigger_ref: str) -> AttemptReport | None:
        conn = self.store.conn
        with self.store.lock:
            self.store.begin()
            try:
                d = conn.execute(
                    "SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)
                ).fetchone()
                if d is None:
                    self.store.rollback()
                    return None
                if d["state"] in ("sent", "disabled"):
                    self.store.rollback()
                    return None
                if kind == "auto" and d["attempt_count"] >= d["max_attempts"]:
                    self.store.rollback()
                    return None
                seq_row = conn.execute(
                    "SELECT COALESCE(MAX(seq),0)+1 AS n FROM delivery_attempts WHERE delivery_id=?",
                    (delivery_id,),
                ).fetchone()
                attempt_id = new_id("ATT")
                conn.execute(
                    """INSERT INTO delivery_attempts(attempt_id, delivery_id, seq, kind,
                       trigger_ref, started_unix) VALUES(?,?,?,?,?,?)""",
                    (attempt_id, delivery_id, seq_row["n"], kind, trigger_ref, now),
                )
                conn.execute(
                    """UPDATE deliveries SET state='sending', attempt_count=?,
                       updated_unix=? WHERE delivery_id=?""",
                    (d["attempt_count"] + 1, now, delivery_id),
                )
                recipients = [
                    dict(r)
                    for r in conn.execute(
                        """SELECT recipient_id, channel, address FROM delivery_recipients
                           WHERE delivery_id=? ORDER BY recipient_id""",
                        (delivery_id,),
                    ).fetchall()
                ]
                delivery = dict(d)
                channel_name = d["channel"]
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise

        if not recipients:
            return self._finish_disabled(delivery_id, now, attempt_id, kind, "无在岗接收人")

        channel = self.channels.get(channel_name)
        if channel is None:
            return self._finish_failure(
                delivery_id, now, attempt_id, kind,
                f"未配置渠道适配器：{channel_name}")
        try:
            result = channel.send(delivery=delivery, addresses=recipients)
        except ChannelError as exc:
            return self._finish_failure(delivery_id, now, attempt_id, kind, str(exc))
        except Exception as exc:  # 适配器自身缺陷按可重试失败处理
            return self._finish_failure(
                delivery_id, now, attempt_id, kind, f"渠道异常：{exc}")
        return self._finish_success(delivery_id, now, attempt_id, kind, {
            "provider_ref": result.provider_ref,
            "accepted": result.accepted,
        })

    def _finish_success(self, delivery_id, now, attempt_id, kind, detail) -> AttemptReport:
        with self.store.lock:
            self.store.begin()
            try:
                self.store.conn.execute(
                    """UPDATE deliveries SET state='sent', sent_unix=?, last_error=NULL,
                       next_attempt_unix=NULL, updated_unix=? WHERE delivery_id=?""",
                    (now, now, delivery_id),
                )
                self.store.conn.execute(
                    """UPDATE delivery_attempts SET finished_unix=?, result='success',
                       detail_json=? WHERE attempt_id=?""",
                    (now, json.dumps(detail, ensure_ascii=False), attempt_id),
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
        return AttemptReport(delivery_id, attempt_id, kind, "success", detail)

    def _finish_failure(self, delivery_id, now, attempt_id, kind, error) -> AttemptReport:
        with self.store.lock:
            self.store.begin()
            try:
                row = self.store.conn.execute(
                    "SELECT attempt_count, max_attempts FROM deliveries WHERE delivery_id=?",
                    (delivery_id,),
                ).fetchone()
                failures = row["attempt_count"]
                exhausted = failures >= row["max_attempts"]
                if exhausted:
                    state, next_at = "failed", None
                else:
                    delay = BACKOFF_SCHEDULE[min(failures - 1, len(BACKOFF_SCHEDULE) - 1)]
                    state, next_at = "failed", now + delay
                self.store.conn.execute(
                    """UPDATE deliveries SET state=?, last_error=?, next_attempt_unix=?,
                       updated_unix=? WHERE delivery_id=?""",
                    (state, error, next_at, now, delivery_id),
                )
                self.store.conn.execute(
                    """UPDATE delivery_attempts SET finished_unix=?, result='failure',
                       detail_json=? WHERE attempt_id=?""",
                    (now, json.dumps({"error": error}, ensure_ascii=False), attempt_id),
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
        return AttemptReport(delivery_id, attempt_id, kind, "failure", {"error": error})

    def _finish_disabled(self, delivery_id, now, attempt_id, kind, reason) -> AttemptReport:
        with self.store.lock:
            self.store.begin()
            try:
                self.store.conn.execute(
                    "UPDATE deliveries SET state='disabled', next_attempt_unix=NULL, updated_unix=? WHERE delivery_id=?",
                    (now, delivery_id),
                )
                self.store.conn.execute(
                    "UPDATE delivery_attempts SET finished_unix=?, result='failure', detail_json=? WHERE attempt_id=?",
                    (now, json.dumps({"error": reason}, ensure_ascii=False), attempt_id),
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
        return AttemptReport(delivery_id, attempt_id, kind, "failure", {"error": reason})

    # ------------------------------------------------------------------
    # 人工补发
    # ------------------------------------------------------------------

    def reissue(self, data: dict, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        request_id = data.get("request_id")
        delivery_id = data.get("delivery_id")
        operator = data.get("operator_ref")
        if not all(isinstance(v, str) and v.strip() for v in (request_id, delivery_id, operator)):
            raise DispatchError("request_id / delivery_id / operator_ref 均为必填字符串")
        note = data.get("note")
        if note is not None and not isinstance(note, str):
            raise DispatchError("note 必须是字符串")
        conn = self.store.conn
        with self.store.lock:
            self.store.begin()
            try:
                existing = conn.execute(
                    "SELECT * FROM reissues WHERE request_id=?", (request_id,)
                ).fetchone()
                if existing is not None and existing["attempt_id"] is not None:
                    # 已完成过尝试的补发请求：严格幂等，直接回放原结论。
                    self.store.commit()
                    return {
                        "request_id": request_id,
                        "delivery_id": existing["delivery_id"],
                        "idempotent": True,
                        "attempt_id": existing["attempt_id"],
                        "state": "duplicate",
                    }
                # existing 但 attempt_id 为空：上次在渠道调用前后崩溃，允许重激活。
                d = conn.execute(
                    "SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)
                ).fetchone()
                if d is None:
                    raise DispatchError(f"送达记录不存在：{delivery_id}")
                if existing is not None and existing["delivery_id"] != delivery_id:
                    raise DispatchError(
                        f"request_id={request_id} 已绑定其他送达记录，拒绝复用")
                # 补发即新的一次尝试，立即到期；自动重试上限不再阻止人工补发。
                conn.execute(
                    """UPDATE deliveries SET state='pending', next_attempt_unix=?,
                       max_attempts=MAX(max_attempts, attempt_count+1), updated_unix=?
                       WHERE delivery_id=?""",
                    (now, now, delivery_id),
                )
                conn.execute(
                    """INSERT INTO reissues(request_id, delivery_id, operator_ref, note,
                       request_unix) VALUES(?,?,?,?,?)
                       ON CONFLICT(request_id) DO NOTHING""",
                    (request_id, delivery_id, operator, note, now),
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise

        with self._run_lock:
            report = self._attempt(delivery_id, now, kind="manual", trigger_ref=request_id)
        attempt_id = report.attempt_id if report else None
        with self.store.lock:
            self.store.begin()
            try:
                conn.execute(
                    "UPDATE reissues SET attempt_id=? WHERE request_id=?",
                    (attempt_id, request_id),
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
        d = conn.execute("SELECT state FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
        return {
            "request_id": request_id,
            "delivery_id": delivery_id,
            "idempotent": False,
            "attempt_id": attempt_id,
            "state": d["state"] if report else "unchanged",
        }

    # ------------------------------------------------------------------
    # 确认回执
    # ------------------------------------------------------------------

    def record_ack(self, data: dict, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        ack_id = data.get("ack_id")
        delivery_id = data.get("delivery_id")
        recipient_id = data.get("recipient_id")
        if not all(isinstance(v, str) and v.strip() for v in (ack_id, delivery_id, recipient_id)):
            raise DispatchError("ack_id / delivery_id / recipient_id 均为必填字符串")
        occurred: Instant | None = None
        if data.get("occurred_at"):
            occurred = parse_instant(data["occurred_at"])
        payload_digest = data.get("payload_digest")
        if payload_digest is not None and not isinstance(payload_digest, str):
            raise DispatchError("payload_digest 必须是字符串")
        conn = self.store.conn
        with self.store.lock:
            self.store.begin()
            try:
                dup = conn.execute(
                    "SELECT delivery_id, recipient_id FROM inbound_acks WHERE ack_id=?",
                    (ack_id,),
                ).fetchone()
                if dup is not None:
                    if (dup["delivery_id"], dup["recipient_id"]) != (delivery_id, recipient_id):
                        raise DispatchError("ack_id 已用于其他送达/接收方，拒绝复用")
                    self.store.commit()
                    return {"ack_id": ack_id, "state": "duplicate", "confirmed": True}

                member = conn.execute(
                    """SELECT channel FROM delivery_recipients
                       WHERE delivery_id=? AND recipient_id=?""",
                    (delivery_id, recipient_id),
                ).fetchone()
                if member is None:
                    raise DispatchError("接收方不属于该送达记录，回执无效")
                same_target = conn.execute(
                    "SELECT ack_id FROM inbound_acks WHERE delivery_id=? AND recipient_id=?",
                    (delivery_id, recipient_id),
                ).fetchone()
                if same_target is not None:
                    # 同一送达目标的重复回执（不同 ack_id）天然幂等。
                    self.store.commit()
                    return {"ack_id": ack_id, "state": "duplicate",
                            "confirmed": True, "original_ack_id": same_target["ack_id"]}
                conn.execute(
                    """INSERT INTO inbound_acks(ack_id, delivery_id, recipient_id, channel,
                       occurred_unix, occurred_offset_minutes, received_unix, payload_digest)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (ack_id, delivery_id, recipient_id, member["channel"],
                     occurred.unix_seconds if occurred else None,
                     occurred.tz_offset_minutes if occurred else None,
                     now, payload_digest),
                )
                conn.execute(
                    """UPDATE delivery_recipients SET ack_state='confirmed',
                       confirmed_unix=?, ack_id=?
                       WHERE delivery_id=? AND recipient_id=?""",
                    (now, ack_id, delivery_id, recipient_id),
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
        return {"ack_id": ack_id, "state": "recorded", "confirmed": True}

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def delivery_detail(self, delivery_id: str) -> dict:
        conn = self.store.conn
        with self.store.lock:
            d = conn.execute(
                "SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            if d is None:
                raise DispatchError(f"送达记录不存在：{delivery_id}")
            recipients = [
                dict(r)
                for r in conn.execute(
                    """SELECT recipient_id, channel, address, ack_state, ack_id,
                              added_unix, confirmed_unix
                       FROM delivery_recipients WHERE delivery_id=?
                       ORDER BY ack_state, recipient_id""",
                    (delivery_id,),
                ).fetchall()
            ]
            attempts = [
                dict(r)
                for r in conn.execute(
                    """SELECT attempt_id, seq, kind, trigger_ref, started_unix,
                              finished_unix, result, detail_json
                       FROM delivery_attempts WHERE delivery_id=? ORDER BY seq""",
                    (delivery_id,),
                ).fetchall()
            ]
        result = dict(d)
        result["recipients"] = recipients
        result["attempts"] = attempts
        result["confirmed_count"] = sum(1 for r in recipients if r["ack_state"] == "confirmed")
        result["unconfirmed"] = [
            {"recipient_id": r["recipient_id"], "channel": r["channel"], "address": r["address"]}
            for r in recipients if r["ack_state"] != "confirmed"
        ]
        return result

    def list_pending(self, *, area_code: str | None = None, channel: str | None = None,
                     limit: int = 100) -> list[dict]:
        sql = """SELECT d.*,
                  (SELECT COUNT(*) FROM delivery_recipients dr
                    WHERE dr.delivery_id=d.delivery_id AND dr.ack_state='pending') AS unconfirmed_count
                  FROM deliveries d WHERE d.state != 'disabled'"""
        args: list[Any] = []
        if area_code:
            sql += " AND d.area_code=?"; args.append(area_code)
        if channel:
            sql += " AND d.channel=?"; args.append(channel)
        sql += " ORDER BY d.created_unix DESC, d.delivery_id LIMIT ?"
        args.append(limit)
        with self.store.lock:
            return [dict(r) for r in self.store.conn.execute(sql, args).fetchall()]
