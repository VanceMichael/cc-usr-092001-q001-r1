"""事件接入与版本投影。

接收三类外部事件（issue 发布 / amend 订正 / lift 解除），完成：

1. 信封与载荷校验，复算 ``payload_digest``（签名摘要不符直接拒收）；
2. 来源序号闸门：``source_sequence`` 只在同一来源内单调前进，迟到的旧序号
   事件以 ``stale`` 状态留痕但**不产生任何投影**，绝不覆盖较新的决定；
3. 把重叠行政区拆成可追溯的生效片段（fragments），每次订正形成新版本，
   记录 ``created_by / basis / replaces / closed_by`` 依据链；
4. 在同一事务内为受影响地区的渠道路由生成送达记录。
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any

from . import canon, messages
from .storage import Storage
from .timeutil import Instant, TimeError, parse_duration_seconds, parse_instant

EVENT_TYPES = ("issue", "amend", "lift")
GENESIS_HASH = "sha256:" + "0" * 64


class IngestionError(ValueError):
    """事件内容不合法（4xx）。"""


class ConflictError(IngestionError):
    """事件编号或来源序号冲突（409）。"""


@dataclass
class IngestResult:
    event_id: str
    event_type: str
    apply_state: str  # applied / stale / duplicate
    stale_reason: str | None
    fragments_opened: list[str]
    fragments_closed: list[str]
    deliveries_created: int
    revision: int | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "apply_state": self.apply_state,
            "stale_reason": self.stale_reason,
            "fragments_opened": self.fragments_opened,
            "fragments_closed": self.fragments_closed,
            "deliveries_created": self.deliveries_created,
            "revision": self.revision,
        }


# ---------------------------------------------------------------------------
# 校验
# ---------------------------------------------------------------------------


def _require(obj: dict, key: str, types: type | tuple[type, ...]) -> Any:
    if key not in obj or obj[key] is None:
        raise IngestionError(f"缺少必填字段：{key}")
    if not isinstance(obj[key], types):
        raise IngestionError(f"字段 {key} 类型不正确")
    return obj[key]


def _validate_envelope(raw: Any) -> dict:
    if not isinstance(raw, dict):
        raise IngestionError("请求体必须是 JSON 对象")
    envelope_keys = {
        "event_id",
        "source_id",
        "subject_ref",
        "event_type",
        "occurred_at",
        "source_sequence",
        "payload_digest",
        "payload",
    }
    missing = envelope_keys - raw.keys()
    if missing:
        raise IngestionError(f"缺少必填字段：{sorted(missing)}")
    event_id = _require(raw, "event_id", str)
    source_id = _require(raw, "source_id", str)
    subject_ref = _require(raw, "subject_ref", str)
    event_type = _require(raw, "event_type", str)
    if event_type not in EVENT_TYPES:
        raise IngestionError(f"event_type 必须是 {EVENT_TYPES} 之一")
    seq = _require(raw, "source_sequence", int)
    if isinstance(seq, bool) or seq < 0:
        raise IngestionError("source_sequence 必须是非负整数")
    digest = _require(raw, "payload_digest", str)
    payload = _require(raw, "payload", dict)
    if not event_id.strip() or not source_id.strip() or not subject_ref.strip():
        raise IngestionError("编号字段不得为空字符串")
    try:
        occurred = parse_instant(_require(raw, "occurred_at", str))
    except TimeError as exc:
        raise IngestionError(str(exc)) from exc
    if not digest.startswith("sha256:"):
        raise IngestionError("payload_digest 必须以 sha256: 开头")
    if not canon.verify_digest(payload, digest):
        raise IngestionError("payload_digest 校验失败：载荷与签名摘要不一致")
    return {
        "event_id": event_id.strip(),
        "source_id": source_id.strip(),
        "subject_ref": subject_ref.strip(),
        "event_type": event_type,
        "source_sequence": seq,
        "occurred": occurred,
        "payload": payload,
        "payload_digest": digest,
    }


def _areas_from_payload(payload: dict, field: str = "areas") -> list[dict]:
    areas = payload.get(field)
    if areas is None:
        return []
    if not isinstance(areas, list):
        raise IngestionError(f"{field} 必须是数组")
    result = []
    seen: set[str] = set()
    for item in areas:
        if not isinstance(item, dict) or not isinstance(item.get("code"), str):
            raise IngestionError(f"{field} 中每项必须包含字符串 code")
        code = item["code"].strip()
        if not code:
            raise IngestionError(f"{field} 中存在空 code")
        if code in seen:
            raise IngestionError(f"{field} 中地区 {code} 重复")
        seen.add(code)
        result.append({"code": code, "name": str(item.get("name") or code)})
    return result


def _detail_from_payload(payload: dict) -> dict:
    detail: dict[str, Any] = {}
    if "warning_level" in payload and payload["warning_level"] is not None:
        if not isinstance(payload["warning_level"], str):
            raise IngestionError("warning_level 必须是字符串")
        detail["level"] = payload["warning_level"]
    rain = payload.get("rainfall_mm")
    if rain is not None:
        if (
            not isinstance(rain, (list, tuple))
            or len(rain) != 2
            or not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in rain)
        ):
            raise IngestionError("rainfall_mm 必须是 [最小, 最大] 两个数值")
        lo, hi = float(rain[0]), float(rain[1])
        if lo < 0 or hi < lo:
            raise IngestionError("rainfall_mm 区间非法（需 0≤最小≤最大）")
        detail["rainfall_min_mm"] = lo
        detail["rainfall_max_mm"] = hi
    hourly = payload.get("hourly_intensity_mm")
    if hourly is not None:
        if not isinstance(hourly, (int, float)) or isinstance(hourly, bool) or hourly < 0:
            raise IngestionError("hourly_intensity_mm 必须是非负数")
        detail["hourly_intensity_mm"] = float(hourly)
    return detail


def _window(payload: dict, occurred: Instant, base_valid_to: float | None,
            base_offset: int | None) -> tuple[float, int, float | None, int | None]:
    if payload.get("valid_from") is not None:
        vf = parse_instant(payload["valid_from"])
        vf_ts, vf_off = vf.unix_seconds, vf.tz_offset_minutes
    else:
        vf_ts, vf_off = occurred.unix_seconds, occurred.tz_offset_minutes
    vt_ts, vt_off = base_valid_to, base_offset
    if payload.get("valid_to") is not None:
        vt = parse_instant(payload["valid_to"])
        vt_ts, vt_off = vt.unix_seconds, vt.tz_offset_minutes
    elif payload.get("valid_for") is not None:
        vt_ts = vf_ts + parse_duration_seconds(payload["valid_for"])
        vt_off = vf_off
    if vt_ts is not None and vt_ts <= vf_ts:
        raise IngestionError("失效时间必须晚于生效时间")
    return vf_ts, vf_off, vt_ts, vt_off


# ---------------------------------------------------------------------------
# 接入
# ---------------------------------------------------------------------------


def ingest(store: Storage, raw: Any, now_ts: float | None = None) -> IngestResult:
    """接入一个外部事件；对同一 event_id 的重复投递安全。"""
    env = _validate_envelope(raw)
    now = time.time() if now_ts is None else now_ts

    with store.lock:
        store.begin()
        try:
            result = _ingest_locked(store, env, raw, now)
            store.commit()
        except Exception:
            store.rollback()
            raise
        return result


def _ingest_locked(store: Storage, env: dict, raw: dict, now: float) -> IngestResult:
    conn = store.conn
    # 幂等重放：同一 event_id 原样返回既有结论；内容对不上则是身份冲突。
    existing = conn.execute(
        """SELECT event_type, apply_state, stale_reason, source_sequence,
                  payload_digest FROM events WHERE event_id=?""",
        (env["event_id"],),
    ).fetchone()
    if existing is not None:
        if (existing["source_sequence"] != env["source_sequence"]
                or existing["payload_digest"] != env["payload_digest"]):
            raise ConflictError(
                f"event_id={env['event_id']} 已存在，但 source_sequence 或载荷摘要不一致"
            )
        return IngestResult(
            event_id=env["event_id"],
            event_type=existing["event_type"],
            apply_state="duplicate:" + existing["apply_state"],
            stale_reason=existing["stale_reason"],
            fragments_opened=[],
            fragments_closed=[],
            deliveries_created=0,
            revision=None,
        )

    # 同一 (来源, 序号) 不允许对应两个不同事件。
    clash = conn.execute(
        "SELECT event_id FROM events WHERE source_id=? AND source_sequence=?",
        (env["source_id"], env["source_sequence"]),
    ).fetchone()
    if clash is not None:
        raise ConflictError(
            f"source_sequence={env['source_sequence']} 已被事件 {clash['event_id']} 占用"
        )

    conn.execute(
        "INSERT OR IGNORE INTO sources(source_id, name, created_unix) VALUES(?,?,?)",
        (env["source_id"], env["source_id"], now),
    )
    cursor = conn.execute(
        "SELECT last_applied_sequence FROM source_cursors WHERE source_id=?",
        (env["source_id"],),
    ).fetchone()
    last_seq = cursor["last_applied_sequence"] if cursor else -1

    envelope_material = canon.canonical_bytes(
        {
            "event_id": env["event_id"],
            "source_id": env["source_id"],
            "source_sequence": env["source_sequence"],
            "subject_ref": env["subject_ref"],
            "event_type": env["event_type"],
            "occurred_at": env["occurred"].iso_with_original_offset(),
            "payload_digest": env["payload_digest"],
        }
    )
    prev_row = conn.execute(
        "SELECT chain_hash FROM events ORDER BY chain_seq DESC LIMIT 1"
    ).fetchone()
    prev_hash = prev_row["chain_hash"] if prev_row else GENESIS_HASH
    chain_seq_row = conn.execute("SELECT COALESCE(MAX(chain_seq), 0) + 1 AS n FROM events").fetchone()
    chain_hash = canon.link_hash(prev_hash, envelope_material)

    payload_json = canon.canonical_json(env["payload"])

    def insert_event(apply_state: str, stale_reason: str | None) -> None:
        conn.execute(
            """INSERT INTO events(event_id, chain_seq, source_id, source_sequence,
               subject_ref, event_type, occurred_unix, occurred_offset_minutes,
               received_unix, payload_json, payload_digest, envelope_canonical,
               chain_hash, apply_state, stale_reason)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                env["event_id"], chain_seq_row["n"], env["source_id"],
                env["source_sequence"], env["subject_ref"], env["event_type"],
                env["occurred"].unix_seconds, env["occurred"].tz_offset_minutes,
                now, payload_json, env["payload_digest"],
                envelope_material.decode("utf-8"), chain_hash, apply_state, stale_reason,
            ),
        )

    if env["source_sequence"] <= last_seq:
        reason = (
            f"迟到事件：source_sequence={env['source_sequence']} "
            f"不新于来源已应用序号 {last_seq}，仅留痕不生效"
        )
        insert_event("stale", reason)
        return IngestResult(
            env["event_id"], env["event_type"], "stale", reason, [], [], 0, None
        )

    # 先落事件台账（同事务，投影失败会一并回滚），片段与送达记录通过外键挂到该事件。
    insert_event("applied", None)
    opened, closed, dcount, revision = _apply_projection(store, env, now)
    conn.execute(
        """INSERT INTO source_cursors(source_id, last_applied_sequence, last_applied_event_id)
           VALUES(?,?,?)
           ON CONFLICT(source_id) DO UPDATE SET
               last_applied_sequence=excluded.last_applied_sequence,
               last_applied_event_id=excluded.last_applied_event_id""",
        (env["source_id"], env["source_sequence"], env["event_id"]),
    )
    return IngestResult(
        env["event_id"], env["event_type"], "applied", None, opened, closed, dcount, revision
    )


# ---------------------------------------------------------------------------
# 投影
# ---------------------------------------------------------------------------


def _fragment_id(subject: str, area: str, version: int) -> str:
    return "FRG-" + uuid.uuid5(
        uuid.NAMESPACE_URL, f"fragment:{subject}:{area}:v{version}"
    ).hex[:20]


def _delivery_id(basis_event_id: str, area: str, channel: str, post: str) -> str:
    return "DLV-" + uuid.uuid5(
        uuid.NAMESPACE_URL, f"delivery:{basis_event_id}:{area}:{channel}:{post}"
    ).hex[:20]


def _upsert_area(conn, area: dict, now: float) -> None:
    conn.execute(
        """INSERT INTO areas(code, name, created_unix) VALUES(?,?,?)
           ON CONFLICT(code) DO UPDATE SET name=excluded.name""",
        (area["code"], area["name"], now),
    )


def _effective_fragments(conn, subject: str):
    rows = conn.execute(
        "SELECT * FROM fragments WHERE subject_ref=? AND state='effective' ORDER BY area_code",
        (subject,),
    ).fetchall()
    return rows


def _next_revision(conn, subject: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(chain_version), 0) + 1 AS n FROM fragments WHERE subject_ref=?",
        (subject,),
    ).fetchone()
    return int(row["n"])


def _insert_fragment(conn, *, subject, area, version, detail, vf_ts, vf_off,
                     vt_ts, vt_off, created_event, basis_event, replaces, now) -> str:
    fid = _fragment_id(subject, area, version)
    conn.execute(
        """INSERT INTO fragments(fragment_id, chain_version, subject_ref, area_code,
           level, rainfall_min_mm, rainfall_max_mm, hourly_intensity_mm, detail_json,
           valid_from_unix, valid_from_offset_minutes, valid_to_unix,
           valid_to_offset_minutes, created_by_event_id, basis_event_id,
           replaces_fragment_id, state, created_unix)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'effective',?)""",
        (
            fid, version, subject, area, detail["level"],
            detail.get("rainfall_min_mm"), detail.get("rainfall_max_mm"),
            detail.get("hourly_intensity_mm"), canon.canonical_json(detail),
            vf_ts, vf_off, vt_ts, vt_off, created_event, basis_event, replaces, now,
        ),
    )
    return fid


def _routes_for_area(conn, area: str) -> list[dict]:
    return [
        dict(r)
        for r in conn.execute(
            """SELECT channel, post FROM recipients
               WHERE active=1 AND area_code=? GROUP BY channel, post ORDER BY channel, post""",
            (area,),
        ).fetchall()
    ]


def _create_deliveries(conn, *, kind, basis_event_id, fragment_id, subject, area,
                       area_name, fragment_row, now, max_attempts) -> int:
    """为一个地区的全部活跃 (渠道, 岗位) 路由生成唯一送达记录。"""
    count = 0
    for route in _routes_for_area(conn, area):
        channel, post = route["channel"], route["post"]
        if kind == "warning":
            title, body = messages.render_warning(fragment_row, area_name)
        else:
            title, body = messages.render_notice(
                kind, fragment_row, area_name, fragment_row.get("close_reason")
            )
        key = f"{basis_event_id}:{area}:{channel}:{post}"
        did = _delivery_id(basis_event_id, area, channel, post)
        cur = conn.execute(
            """INSERT OR IGNORE INTO deliveries(delivery_id, kind, basis_event_id,
               fragment_id, subject_ref, area_code, channel, post, title, body,
               state, max_attempts, next_attempt_unix, idempotency_key,
               created_unix, updated_unix)
               VALUES(?,?,?,?,?,?,?,?,?,?,'pending',?,?,?,?,?)""",
            (did, kind, basis_event_id, fragment_id, subject, area, channel, post,
             title, body, max_attempts, now, key, now, now),
        )
        count += cur.rowcount
        # 为该路由下的接收人生成送达快照（INSERT OR IGNORE 保持幂等）。
        conn.execute(
            """INSERT OR IGNORE INTO delivery_recipients
                   (delivery_id, recipient_id, channel, address, ack_state, added_unix)
               SELECT ?, recipient_id, channel, address, 'pending', ?
               FROM recipients WHERE active=1 AND area_code=? AND channel=? AND post=?""",
            (did, now, area, channel, post),
        )
    return count


def _close_fragment(conn, row, *, event_id, new_state, reason,
                    closed_at_unix, closed_at_offset_minutes) -> None:
    # 旧片段的失效时间与新版本生效时间对齐，保证任一时间点恰好命中一个片段。
    valid_to = row["valid_to_unix"]
    if valid_to is None or closed_at_unix < valid_to:
        valid_to = closed_at_unix
        offset = closed_at_offset_minutes
    else:
        offset = row["valid_to_offset_minutes"]
    conn.execute(
        """UPDATE fragments SET state=?, closed_by_event_id=?, close_reason=?,
           valid_to_unix=?, valid_to_offset_minutes=?
           WHERE fragment_id=?""",
        (new_state, event_id, reason, valid_to, offset, row["fragment_id"]),
    )


def _area_name(conn, code: str) -> str:
    row = conn.execute("SELECT name FROM areas WHERE code=?", (code,)).fetchone()
    return row["name"] if row else code


def _apply_projection(store: Storage, env: dict, now: float) -> tuple[list[str], list[str], int, int | None]:
    conn = store.conn
    payload = env["payload"]
    subject = env["subject_ref"]
    occurred: Instant = env["occurred"]
    etype = env["event_type"]
    max_attempts = int(env_attempts_override(payload))

    opened: list[str] = []
    closed: list[str] = []
    dcount = 0

    if etype == "issue":
        existing = _effective_fragments(conn, subject)
        if existing:
            raise IngestionError(
                f"subject_ref={subject} 已有生效片段，再次发布必须使用 amend"
            )
        areas = _areas_from_payload(payload)
        if not areas:
            raise IngestionError("issue 事件必须包含非空 areas")
        detail = _detail_from_payload(payload)
        if "level" not in detail:
            raise IngestionError("issue 事件必须提供 warning_level")
        vf_ts, vf_off, vt_ts, vt_off = _window(payload, occurred, None, None)
        revision = 1
        for area in areas:
            _upsert_area(conn, area, now)
            fid = _insert_fragment(
                conn, subject=subject, area=area["code"], version=1, detail=detail,
                vf_ts=vf_ts, vf_off=vf_off, vt_ts=vt_ts, vt_off=vt_off,
                created_event=env["event_id"], basis_event=env["event_id"],
                replaces=None, now=now,
            )
            opened.append(fid)
            row = conn.execute("SELECT * FROM fragments WHERE fragment_id=?", (fid,)).fetchone()
            dcount += _create_deliveries(
                conn, kind="warning", basis_event_id=env["event_id"], fragment_id=fid,
                subject=subject, area=area["code"], area_name=area["name"],
                fragment_row=dict(row), now=now, max_attempts=max_attempts,
            )
        return opened, closed, dcount, revision

    if etype == "amend":
        current = {r["area_code"]: r for r in _effective_fragments(conn, subject)}
        if not current:
            raise IngestionError(f"subject_ref={subject} 当前没有生效片段，无法订正")
        new_areas = _areas_from_payload(payload)
        if not new_areas:
            raise IngestionError("amend 事件必须包含非空 areas（订正后的完整范围）")
        new_codes = {a["code"] for a in new_areas}
        revision = _next_revision(conn, subject)
        override = _detail_from_payload(payload)
        any_row = next(iter(current.values()))
        if "level" not in override:
            override["level"] = any_row["level"]
        withdrawn_reason = (
            payload.get("withdrawn_reason")
            or f"订正（{env['event_id']}）后移出预警范围"
        )

        # 1) 退出范围的地区：关闭旧片段，下发撤销通知。
        for code, row in current.items():
            if code in new_codes:
                continue
            _close_fragment(
                conn, row, event_id=env["event_id"], new_state="superseded",
                reason=withdrawn_reason, closed_at_unix=occurred.unix_seconds,
                closed_at_offset_minutes=occurred.tz_offset_minutes,
            )
            closed.append(row["fragment_id"])
            row = conn.execute("SELECT * FROM fragments WHERE fragment_id=?", (row["fragment_id"],)).fetchone()
            _upsert_area(conn, {"code": code, "name": _area_name(conn, code)}, now)
            dcount += _create_deliveries(
                conn, kind="withdrawn", basis_event_id=env["event_id"],
                fragment_id=row["fragment_id"], subject=subject, area=code,
                area_name=_area_name(conn, code), fragment_row=dict(row),
                now=now, max_attempts=max_attempts,
            )

        # 2) 继续受影响 + 新加入的地区：开新版本片段。
        for area in new_areas:
            _upsert_area(conn, area, now)
            old = current.get(area["code"])
            base_vt = old["valid_to_unix"] if old else None
            base_vt_off = old["valid_to_offset_minutes"] if old else None
            vf_ts, vf_off, vt_ts, vt_off = _window(payload, occurred, base_vt, base_vt_off)
            detail = dict(override)
            if old is not None:
                for key in ("rainfall_min_mm", "rainfall_max_mm", "hourly_intensity_mm"):
                    if key not in detail and old[key] is not None:
                        detail[key] = old[key]
            fid = _insert_fragment(
                conn, subject=subject, area=area["code"], version=revision, detail=detail,
                vf_ts=vf_ts, vf_off=vf_off, vt_ts=vt_ts, vt_off=vt_off,
                created_event=env["event_id"], basis_event=env["event_id"],
                replaces=old["fragment_id"] if old else None, now=now,
            )
            opened.append(fid)
            if old is not None:
                _close_fragment(
                    conn, old, event_id=env["event_id"], new_state="superseded",
                    reason=f"被 v{revision}（{env['event_id']}）替代",
                    closed_at_unix=vf_ts, closed_at_offset_minutes=vf_off,
                )
                closed.append(old["fragment_id"])
            row = conn.execute("SELECT * FROM fragments WHERE fragment_id=?", (fid,)).fetchone()
            dcount += _create_deliveries(
                conn, kind="warning", basis_event_id=env["event_id"], fragment_id=fid,
                subject=subject, area=area["code"], area_name=area["name"],
                fragment_row=dict(row), now=now, max_attempts=max_attempts,
            )
        return opened, closed, dcount, revision

    # lift
    lift_areas = _areas_from_payload(payload)
    current = {r["area_code"]: r for r in _effective_fragments(conn, subject)}
    if not current:
        raise IngestionError(f"subject_ref={subject} 当前没有生效片段，无法解除")
    if lift_areas:
        targets = {a["code"] for a in lift_areas}
        unknown = targets - current.keys()
        if unknown:
            raise IngestionError(
                f"以下地区不在生效范围内，不能解除：{sorted(unknown)}"
            )
    else:
        targets = set(current.keys())
    if payload.get("effective_at"):
        effective_instant = parse_instant(payload["effective_at"])
        effective_at = effective_instant.unix_seconds
        effective_off = effective_instant.tz_offset_minutes
    else:
        effective_at = occurred.unix_seconds
        effective_off = occurred.tz_offset_minutes
    reason = payload.get("reason") or f"解除依据：{env['event_id']}"
    revision = None
    for code in sorted(targets):
        row = current[code]
        _close_fragment(
            conn, row, event_id=env["event_id"], new_state="lifted",
            reason=reason, closed_at_unix=effective_at,
            closed_at_offset_minutes=effective_off,
        )
        closed.append(row["fragment_id"])
        row = conn.execute("SELECT * FROM fragments WHERE fragment_id=?", (row["fragment_id"],)).fetchone()
        dcount += _create_deliveries(
            conn, kind="lift", basis_event_id=env["event_id"],
            fragment_id=row["fragment_id"], subject=subject, area=code,
            area_name=_area_name(conn, code), fragment_row=dict(row),
            now=now, max_attempts=max_attempts,
        )
    return opened, closed, dcount, revision


def env_attempts_override(payload: dict) -> int:
    value = payload.get("max_attempts")
    if value is None:
        return 5
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise IngestionError("max_attempts 必须是不小于 1 的整数")
    return value
