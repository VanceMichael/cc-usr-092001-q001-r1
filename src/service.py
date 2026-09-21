"""预警事件接收、版本链、生效片段与送达记录的核心服务。"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .domain import Moment, ServiceError, canonical_json, parse_moment, require_digest
from .store import Store

EVENT_TYPES = ("ISSUE", "CORRECT", "CANCEL")
CHANNELS = ("sms", "broadcast", "terminal")

STATE_ACTIVE = "ACTIVE"
STATE_CANCELLED = "CANCELLED"

INGEST_APPLIED = "APPLIED"
INGEST_LATE = "LATE"

REASON_EXPLICIT = "EXPLICIT"  # 解除消息显式撤销
REASON_SCOPE_REDUCED = "SCOPE_REDUCED"  # 订正缩小范围而撤销


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class Envelope:
    """通过完整校验的事件信封。"""

    event_id: str
    event_type: str
    subject_ref: str
    source_ref: str
    source_sequence: int
    occurred: Moment
    payload: dict[str, Any]
    payload_digest: str
    regions: tuple[str, ...]
    valid_from: Moment | None
    valid_until: Moment | None


class AlertService:
    """围绕一个 Store 提供接收、查询与送达操作。"""

    def __init__(self, store: Store, now_fn: Callable[[], datetime] | None = None) -> None:
        self.store = store
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    # ------------------------------------------------------------------
    # 事件接收
    # ------------------------------------------------------------------

    def ingest_event(self, message: dict[str, Any]) -> dict[str, Any]:
        """接收一条发布/订正/解除消息。

        幂等：相同 event_id 重复投递返回首次处理结果；
        迟到（来源序号不前进或发生时间早于现行决定）只登记、不应用。
        """
        envelope = self._validate_envelope(message)
        received_at = _utc_now_iso()
        with self.store.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM events WHERE event_id=?", (envelope.event_id,)
            ).fetchone()
            if existing is not None:
                if existing["payload_digest"] != envelope.payload_digest:
                    raise ServiceError(
                        409, "event_conflict", "相同 event_id 的消息摘要与已登记事件不一致"
                    )
                return self._ingest_result(conn, existing, replayed=True)

            status, note = self._decide(conn, envelope)
            conn.execute(
                """INSERT INTO events
                   (event_id, event_type, subject_ref, source_ref, source_sequence,
                    occurred_at, occurred_epoch, occurred_offset_minutes,
                    payload_digest, payload_json, ingest_status, ingest_note, received_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    envelope.event_id,
                    envelope.event_type,
                    envelope.subject_ref,
                    envelope.source_ref,
                    envelope.source_sequence,
                    envelope.occurred.raw,
                    envelope.occurred.epoch,
                    envelope.occurred.offset_minutes,
                    envelope.payload_digest,
                    canonical_json(envelope.payload),
                    status,
                    note,
                    received_at,
                ),
            )
            if status == INGEST_APPLIED:
                self._apply(conn, envelope)
            return self._ingest_result(
                conn,
                conn.execute(
                    "SELECT * FROM events WHERE event_id=?", (envelope.event_id,)
                ).fetchone(),
                replayed=False,
            )

    def _validate_envelope(self, message: dict[str, Any]) -> Envelope:
        if not isinstance(message, dict):
            raise ServiceError(400, "invalid_body", "请求体必须是 JSON 对象")

        def required_text(field: str) -> str:
            value = message.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ServiceError(422, "missing_field", f"缺少字段 {field} 或内容为空")
            return value.strip()

        event_id = required_text("event_id")
        subject_ref = required_text("subject_ref")
        source_ref = required_text("source_ref")
        event_type = required_text("event_type")
        if event_type not in EVENT_TYPES:
            raise ServiceError(
                422, "invalid_event_type", f"event_type 必须是 {'/'.join(EVENT_TYPES)}"
            )
        sequence = message.get("source_sequence")
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
            raise ServiceError(422, "invalid_sequence", "source_sequence 必须是正整数")
        occurred = parse_moment(message.get("occurred_at"), "occurred_at")
        payload = message.get("payload")
        if not isinstance(payload, dict):
            raise ServiceError(422, "invalid_payload", "payload 必须是 JSON 对象")
        digest = require_digest(payload, message.get("payload_digest"))

        raw_regions = payload.get("regions")
        if raw_regions is None:
            regions: tuple[str, ...] = ()
        elif isinstance(raw_regions, list) and all(
            isinstance(r, str) and r.strip() for r in raw_regions
        ):
            regions = tuple(sorted({r.strip() for r in raw_regions}))
        else:
            raise ServiceError(422, "invalid_regions", "payload.regions 必须是非空字符串数组")

        valid_from = valid_until = None
        if event_type in ("ISSUE", "CORRECT"):
            if not regions:
                raise ServiceError(422, "invalid_regions", "发布/订正必须给出至少一个行政区")
            for field in ("alert_level", "rain_range", "hourly_intensity"):
                if not isinstance(payload.get(field), str) or not payload[field].strip():
                    raise ServiceError(422, "missing_field", f"payload.{field} 必须是非空字符串")
            valid_from = parse_moment(payload.get("valid_from"), "payload.valid_from")
            valid_until = parse_moment(payload.get("valid_until"), "payload.valid_until")
            if valid_until.epoch <= valid_from.epoch:
                raise ServiceError(
                    422, "invalid_validity", "payload.valid_until 必须晚于 payload.valid_from"
                )

        return Envelope(
            event_id=event_id,
            event_type=event_type,
            subject_ref=subject_ref,
            source_ref=source_ref,
            source_sequence=sequence,
            occurred=occurred,
            payload=payload,
            payload_digest=digest,
            regions=regions,
            valid_from=valid_from,
            valid_until=valid_until,
        )

    def _decide(self, conn: sqlite3.Connection, envelope: Envelope) -> tuple[str, str | None]:
        """判定事件是否应用；迟到消息登记为 LATE，绝不覆盖较新的决定。"""
        head = conn.execute(
            "SELECT * FROM alert_versions WHERE subject_ref=? ORDER BY version_seq DESC LIMIT 1",
            (envelope.subject_ref,),
        ).fetchone()
        if envelope.event_type == "ISSUE" and head is not None:
            raise ServiceError(409, "subject_exists", "该预警主题已发布，后续调整请使用订正消息")
        if envelope.event_type in ("CORRECT", "CANCEL") and head is None:
            raise ServiceError(404, "subject_not_found", "预警主题不存在，无法订正或解除")

        row = conn.execute(
            "SELECT MAX(source_sequence) AS max_seq FROM events "
            "WHERE source_ref=? AND ingest_status=?",
            (envelope.source_ref, INGEST_APPLIED),
        ).fetchone()
        max_seq = row["max_seq"] or 0
        if envelope.source_sequence <= max_seq:
            return (
                INGEST_LATE,
                f"来源序号 {envelope.source_sequence} 不高于已应用的 {max_seq}，按迟到登记",
            )
        if head is not None and envelope.occurred.epoch < head["decided_epoch"]:
            return (
                INGEST_LATE,
                f"occurred_at 早于现行版本决定时间 {head['decided_at']}，按迟到登记",
            )
        if envelope.event_type == "CORRECT" and head is not None and head["kind"] == "CANCEL":
            raise ServiceError(409, "subject_closed", "预警已解除，主题已关闭，不能订正")
        return INGEST_APPLIED, None

    def _apply(self, conn: sqlite3.Connection, envelope: Envelope) -> None:
        """应用事件：追加一个不可变版本，并按区域差异追加生效片段。"""
        head = conn.execute(
            "SELECT * FROM alert_versions WHERE subject_ref=? ORDER BY version_seq DESC LIMIT 1",
            (envelope.subject_ref,),
        ).fetchone()
        version_seq = 1 if head is None else head["version_seq"] + 1
        version_id = _new_id("ver")
        now_iso = _utc_now_iso()

        active_before = self._active_regions(conn, envelope.subject_ref)
        if envelope.event_type == "CANCEL":
            targets = (
                tuple(sorted(active_before))
                if not envelope.regions
                else tuple(sorted(active_before & set(envelope.regions)))
            )
            self._insert_version(conn, envelope, version_id, version_seq, targets)
            for region in targets:
                self._insert_segment(
                    conn, envelope, version_id, version_seq, region,
                    STATE_CANCELLED, REASON_EXPLICIT, now_iso,
                )
            return

        self._insert_version(conn, envelope, version_id, version_seq, envelope.regions)
        for region in envelope.regions:
            self._insert_segment(
                conn, envelope, version_id, version_seq, region,
                STATE_ACTIVE, None, now_iso,
            )
        for region in sorted(active_before - set(envelope.regions)):
            self._insert_segment(
                conn, envelope, version_id, version_seq, region,
                STATE_CANCELLED, REASON_SCOPE_REDUCED, now_iso,
            )

    def _insert_version(
        self,
        conn: sqlite3.Connection,
        envelope: Envelope,
        version_id: str,
        version_seq: int,
        regions: tuple[str, ...],
    ) -> None:
        payload = envelope.payload
        conn.execute(
            """INSERT INTO alert_versions
               (version_id, subject_ref, version_seq, event_id, kind,
                alert_level, rain_range, hourly_intensity, regions_json,
                valid_from, valid_from_epoch, valid_until, valid_until_epoch,
                decided_at, decided_epoch)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                version_id,
                envelope.subject_ref,
                version_seq,
                envelope.event_id,
                envelope.event_type,
                payload.get("alert_level"),
                payload.get("rain_range"),
                payload.get("hourly_intensity"),
                canonical_json(list(regions)),
                envelope.valid_from.raw if envelope.valid_from else None,
                envelope.valid_from.epoch if envelope.valid_from else None,
                envelope.valid_until.raw if envelope.valid_until else None,
                envelope.valid_until.epoch if envelope.valid_until else None,
                envelope.occurred.raw,
                envelope.occurred.epoch,
            ),
        )

    def _insert_segment(
        self,
        conn: sqlite3.Connection,
        envelope: Envelope,
        version_id: str,
        version_seq: int,
        region: str,
        state: str,
        cancel_reason: str | None,
        now_iso: str,
    ) -> None:
        segment_id = _new_id("seg")
        conn.execute(
            """INSERT INTO region_segments
               (segment_id, subject_ref, region_code, version_id, version_seq,
                state, cancel_reason, effective_from, effective_from_epoch, formed_by_event_id)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                segment_id,
                envelope.subject_ref,
                region,
                version_id,
                version_seq,
                state,
                cancel_reason,
                envelope.occurred.raw,
                envelope.occurred.epoch,
                envelope.event_id,
            ),
        )
        # 每个生效片段 × 接收岗位生成唯一送达记录；解除片段同样需要送达，
        # 避免基层继续转发已撤销的范围。
        recipients = conn.execute(
            "SELECT * FROM recipients WHERE region_code=?", (region,)
        ).fetchall()
        for recipient in recipients:
            conn.execute(
                """INSERT OR IGNORE INTO deliveries
                   (delivery_id, segment_id, subject_ref, region_code, channel,
                    post_ref, recipient_id, endpoint_ref, status,
                    attempt_count, next_attempt_epoch, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?, 'PENDING', 0, 0, ?, ?)""",
                (
                    _new_id("dlv"),
                    segment_id,
                    envelope.subject_ref,
                    region,
                    recipient["channel"],
                    recipient["post_ref"],
                    recipient["recipient_id"],
                    recipient["endpoint_ref"],
                    now_iso,
                    now_iso,
                ),
            )

    def _active_regions(self, conn: sqlite3.Connection, subject_ref: str) -> set[str]:
        rows = conn.execute(
            """SELECT s.region_code, s.state FROM region_segments s
               WHERE s.subject_ref=? AND s.version_seq = (
                 SELECT MAX(version_seq) FROM region_segments t
                 WHERE t.subject_ref=s.subject_ref AND t.region_code=s.region_code)""",
            (subject_ref,),
        ).fetchall()
        return {row["region_code"] for row in rows if row["state"] == STATE_ACTIVE}

    def _ingest_result(
        self, conn: sqlite3.Connection, event: sqlite3.Row, *, replayed: bool
    ) -> dict[str, Any]:
        version = conn.execute(
            "SELECT version_id, version_seq FROM alert_versions WHERE event_id=?",
            (event["event_id"],),
        ).fetchone()
        segments = conn.execute(
            "SELECT COUNT(*) AS n FROM region_segments WHERE formed_by_event_id=?",
            (event["event_id"],),
        ).fetchone()["n"]
        deliveries = conn.execute(
            """SELECT COUNT(*) AS n FROM deliveries d
               JOIN region_segments s ON s.segment_id = d.segment_id
               WHERE s.formed_by_event_id=?""",
            (event["event_id"],),
        ).fetchone()["n"]
        return {
            "event_id": event["event_id"],
            "ingest_status": event["ingest_status"],
            "ingest_note": event["ingest_note"],
            "version_id": version["version_id"] if version else None,
            "version_seq": version["version_seq"] if version else None,
            "segments_created": segments,
            "deliveries_created": deliveries,
            "replayed": replayed,
        }

    # ------------------------------------------------------------------
    # 接收岗位目录
    # ------------------------------------------------------------------

    def register_recipient(self, body: dict[str, Any]) -> dict[str, Any]:
        """登记或更新一个接收岗位；同一 (地区, 渠道, 岗位) 幂等。"""
        region = self._required_text(body, "region_code")
        channel = self._required_text(body, "channel")
        post_ref = self._required_text(body, "post_ref")
        endpoint_ref = self._required_text(body, "endpoint_ref")
        if channel not in CHANNELS:
            raise ServiceError(
                422, "invalid_channel", f"channel 必须是 {'/'.join(CHANNELS)}"
            )
        recipient_id = "rcp-" + uuid.uuid5(
            uuid.NAMESPACE_URL, f"recipient:{region}:{channel}:{post_ref}"
        ).hex[:16]
        with self.store.transaction() as conn:
            conn.execute(
                """INSERT INTO recipients
                   (recipient_id, region_code, channel, post_ref, endpoint_ref, created_at)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(region_code, channel, post_ref)
                   DO UPDATE SET endpoint_ref=excluded.endpoint_ref""",
                (recipient_id, region, channel, post_ref, endpoint_ref, _utc_now_iso()),
            )
        return {
            "recipient_id": recipient_id,
            "region_code": region,
            "channel": channel,
            "post_ref": post_ref,
            "endpoint_ref": endpoint_ref,
        }

    @staticmethod
    def _required_text(body: dict[str, Any], field: str) -> str:
        value = body.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ServiceError(422, "missing_field", f"缺少字段 {field} 或内容为空")
        return value.strip()

    # ------------------------------------------------------------------
    # 主管查询
    # ------------------------------------------------------------------

    def get_event(self, event_id: str) -> dict[str, Any]:
        row = self.store.one("SELECT * FROM events WHERE event_id=?", (event_id,))
        if row is None:
            raise ServiceError(404, "event_not_found", "事件不存在")
        version = self.store.one(
            "SELECT version_id, version_seq FROM alert_versions WHERE event_id=?",
            (event_id,),
        )
        return {
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "subject_ref": row["subject_ref"],
            "source_ref": row["source_ref"],
            "source_sequence": row["source_sequence"],
            "occurred_at": row["occurred_at"],
            "payload_digest": row["payload_digest"],
            "payload": json.loads(row["payload_json"]),
            "ingest_status": row["ingest_status"],
            "ingest_note": row["ingest_note"],
            "received_at": row["received_at"],
            "version_id": version["version_id"] if version else None,
            "version_seq": version["version_seq"] if version else None,
        }

    def subject_versions(self, subject_ref: str) -> dict[str, Any]:
        rows = self.store.all(
            "SELECT * FROM alert_versions WHERE subject_ref=? ORDER BY version_seq",
            (subject_ref,),
        )
        if not rows:
            raise ServiceError(404, "subject_not_found", "预警主题不存在")
        return {
            "subject_ref": subject_ref,
            "versions": [
                {
                    "version_id": row["version_id"],
                    "version_seq": row["version_seq"],
                    "kind": row["kind"],
                    "alert_level": row["alert_level"],
                    "rain_range": row["rain_range"],
                    "hourly_intensity": row["hourly_intensity"],
                    "regions": json.loads(row["regions_json"]),
                    "valid_from": row["valid_from"],
                    "valid_until": row["valid_until"],
                    "decided_at": row["decided_at"],
                    "event_id": row["event_id"],
                }
                for row in rows
            ],
        }

    def region_state(self, region_code: str, at: Moment | None = None) -> dict[str, Any]:
        """任一时刻某行政区的预警状态及其形成依据。"""
        moment = at or parse_moment(self._now_fn().isoformat(), "at")
        subjects = self.store.all(
            "SELECT DISTINCT subject_ref FROM region_segments WHERE region_code=?",
            (region_code,),
        )
        alerts = []
        for row in subjects:
            segment = self.store.one(
                """SELECT * FROM region_segments
                   WHERE region_code=? AND subject_ref=? AND effective_from_epoch<=?
                   ORDER BY version_seq DESC LIMIT 1""",
                (region_code, row["subject_ref"], moment.epoch),
            )
            if segment is None:
                continue
            version = self.store.one(
                "SELECT * FROM alert_versions WHERE version_id=?",
                (segment["version_id"],),
            )
            if segment["state"] == STATE_CANCELLED:
                state = "CANCELLED"
            elif version["valid_from_epoch"] is not None and moment.epoch < version["valid_from_epoch"]:
                state = "SCHEDULED"
            elif version["valid_until_epoch"] is not None and moment.epoch >= version["valid_until_epoch"]:
                state = "EXPIRED"
            else:
                state = "ACTIVE"
            alerts.append(
                {
                    "subject_ref": row["subject_ref"],
                    "state": state,
                    "version_id": version["version_id"],
                    "version_seq": version["version_seq"],
                    "alert_level": version["alert_level"],
                    "rain_range": version["rain_range"],
                    "hourly_intensity": version["hourly_intensity"],
                    "valid_from": version["valid_from"],
                    "valid_until": version["valid_until"],
                    "effective_from": segment["effective_from"],
                    "formed_by_event_id": segment["formed_by_event_id"],
                    "cancel_reason": segment["cancel_reason"],
                }
            )
        alerts.sort(key=lambda item: item["subject_ref"])
        return {"region_code": region_code, "at": moment.raw, "alerts": alerts}

    def unconfirmed(self, region_code: str, channel: str | None = None) -> dict[str, Any]:
        """某地区尚未确认的送达记录（含已发送未回执），按渠道汇总。"""
        sql = (
            """SELECT d.*, s.state AS segment_state, s.version_seq AS version_seq
               FROM deliveries d JOIN region_segments s ON s.segment_id = d.segment_id
               WHERE d.region_code=? AND d.status != 'CONFIRMED'"""
        )
        params: list[Any] = [region_code]
        if channel is not None:
            sql += " AND d.channel=?"
            params.append(channel)
        sql += " ORDER BY d.channel, d.post_ref, d.created_at"
        rows = self.store.all(sql, tuple(params))
        items = [self._delivery_dict(row) for row in rows]
        summary: dict[str, int] = {}
        for item in items:
            summary[item["channel"]] = summary.get(item["channel"], 0) + 1
        return {
            "region_code": region_code,
            "count": len(items),
            "summary": summary,
            "unconfirmed": items,
        }

    def list_deliveries(
        self,
        region: str | None = None,
        channel: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        sql = (
            """SELECT d.*, s.state AS segment_state, s.version_seq AS version_seq
               FROM deliveries d JOIN region_segments s ON s.segment_id = d.segment_id
               WHERE 1=1"""
        )
        params: list[Any] = []
        if region is not None:
            sql += " AND d.region_code=?"
            params.append(region)
        if channel is not None:
            sql += " AND d.channel=?"
            params.append(channel)
        if status is not None:
            sql += " AND d.status=?"
            params.append(status)
        sql += " ORDER BY d.created_at, d.delivery_id"
        rows = self.store.all(sql, tuple(params))
        return {
            "count": len(rows),
            "deliveries": [self._delivery_dict(row) for row in rows],
        }

    def get_delivery(self, delivery_id: str) -> dict[str, Any]:
        row = self.store.one(
            """SELECT d.*, s.state AS segment_state, s.version_seq AS version_seq
               FROM deliveries d JOIN region_segments s ON s.segment_id = d.segment_id
               WHERE d.delivery_id=?""",
            (delivery_id,),
        )
        if row is None:
            raise ServiceError(404, "delivery_not_found", "送达记录不存在")
        attempts = self.store.all(
            "SELECT * FROM dispatch_attempts WHERE delivery_id=? ORDER BY attempt_no",
            (delivery_id,),
        )
        confirmation = self.store.one(
            "SELECT * FROM confirmations WHERE delivery_id=?", (delivery_id,)
        )
        result = self._delivery_dict(row)
        result["endpoint_ref"] = row["endpoint_ref"]
        result["attempts"] = [
            {
                "attempt_no": a["attempt_no"],
                "trigger_kind": a["trigger_kind"],
                "result": a["result"],
                "detail": a["detail"],
                "attempted_at": a["attempted_at"],
            }
            for a in attempts
        ]
        result["confirmation"] = (
            {
                "receipt_id": confirmation["receipt_id"],
                "confirmed_at": confirmation["confirmed_at"],
                "confirmer_ref": confirmation["confirmer_ref"],
            }
            if confirmation
            else None
        )
        return result

    @staticmethod
    def _delivery_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "delivery_id": row["delivery_id"],
            "subject_ref": row["subject_ref"],
            "region_code": row["region_code"],
            "channel": row["channel"],
            "post_ref": row["post_ref"],
            "status": row["status"],
            "attempt_count": row["attempt_count"],
            "segment_id": row["segment_id"],
            "segment_state": row["segment_state"],
            "version_seq": row["version_seq"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    # ------------------------------------------------------------------
    # 确认与人工补发
    # ------------------------------------------------------------------

    def confirm_delivery(self, delivery_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """登记接收确认；按 receipt_id 幂等，重复回执返回首次结果。"""
        receipt_id = self._required_text(body, "receipt_id")
        confirmed = parse_moment(body.get("confirmed_at"), "confirmed_at")
        confirmer = body.get("confirmer_ref")
        if confirmer is not None and not isinstance(confirmer, str):
            raise ServiceError(422, "invalid_field", "confirmer_ref 必须是字符串")
        with self.store.transaction() as conn:
            delivery = conn.execute(
                "SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            if delivery is None:
                raise ServiceError(404, "delivery_not_found", "送达记录不存在")
            existing = conn.execute(
                "SELECT * FROM confirmations WHERE receipt_id=?", (receipt_id,)
            ).fetchone()
            if existing is not None:
                if existing["delivery_id"] != delivery_id:
                    raise ServiceError(
                        409, "receipt_conflict", "receipt_id 已被其他送达记录使用"
                    )
                return self._confirm_result(delivery_id, existing, duplicate=True)
            prior = conn.execute(
                "SELECT * FROM confirmations WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            if prior is not None:
                # 已确认过的送达再次收到不同回执：保留首次确认，幂等返回。
                return self._confirm_result(delivery_id, prior, duplicate=True)
            conn.execute(
                """INSERT INTO confirmations
                   (receipt_id, delivery_id, confirmed_at, confirmed_epoch, confirmer_ref)
                   VALUES (?,?,?,?,?)""",
                (receipt_id, delivery_id, confirmed.raw, confirmed.epoch, confirmer),
            )
            conn.execute(
                "UPDATE deliveries SET status='CONFIRMED', updated_at=? WHERE delivery_id=?",
                (_utc_now_iso(), delivery_id),
            )
            stored = conn.execute(
                "SELECT * FROM confirmations WHERE receipt_id=?", (receipt_id,)
            ).fetchone()
            return self._confirm_result(delivery_id, stored, duplicate=False)

    @staticmethod
    def _confirm_result(
        delivery_id: str, confirmation: sqlite3.Row, *, duplicate: bool
    ) -> dict[str, Any]:
        return {
            "delivery_id": delivery_id,
            "receipt_id": confirmation["receipt_id"],
            "confirmed_at": confirmation["confirmed_at"],
            "status": "CONFIRMED",
            "duplicate": duplicate,
        }

    def resend_delivery(self, delivery_id: str) -> dict[str, Any]:
        """人工补发：把未确认的送达重新排入待派发；已确认的为幂等空操作。"""
        with self.store.transaction() as conn:
            delivery = conn.execute(
                "SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            if delivery is None:
                raise ServiceError(404, "delivery_not_found", "送达记录不存在")
            if delivery["status"] == "CONFIRMED":
                return {
                    "delivery_id": delivery_id,
                    "status": "CONFIRMED",
                    "resend": "ignored",
                    "detail": "已确认的送达记录无需补发",
                }
            conn.execute(
                """UPDATE deliveries
                   SET status='PENDING', next_attempt_epoch=0, updated_at=?
                   WHERE delivery_id=?""",
                (_utc_now_iso(), delivery_id),
            )
            return {"delivery_id": delivery_id, "status": "PENDING", "resend": "scheduled"}
