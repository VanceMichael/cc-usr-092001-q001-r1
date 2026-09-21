"""值班主管查询与台账完整性校验。

* ``warning_at``：回答“任一时刻某地区处于何种预警、依据哪次订正形成”；
* ``timeline``：给出片段版本链与每次决定的事件依据；
* ``verify_integrity``：重算事件哈希链与载荷摘要，任何对历史的静默改写都会暴露。
"""

from __future__ import annotations

import json
import time
from typing import Any

from . import canon
from .events import GENESIS_HASH
from .storage import Storage
from .timeutil import Instant, parse_instant


def _event_brief(row) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "source_id": row["source_id"],
        "source_sequence": row["source_sequence"],
        "event_type": row["event_type"],
        "apply_state": row["apply_state"],
        "occurred_at": Instant(
            row["occurred_unix"], row["occurred_offset_minutes"]
        ).iso_with_original_offset(),
        "payload_digest": row["payload_digest"],
        "chain_hash": row["chain_hash"],
    }


def _fragment_payload(conn, row) -> dict[str, Any]:
    out = dict(row)
    for key in ("created_unix",):
        out.pop(key, None)
    out["valid_from"] = Instant(
        row["valid_from_unix"], row["valid_from_offset_minutes"]
    ).iso_with_original_offset()
    if row["valid_to_unix"] is not None:
        out["valid_to"] = Instant(
            row["valid_to_unix"], row["valid_to_offset_minutes"]
        ).iso_with_original_offset()
    else:
        out["valid_to"] = None
    basis = conn.execute("SELECT * FROM events WHERE event_id=?", (row["basis_event_id"],)).fetchone()
    out["basis_event"] = _event_brief(basis) if basis else None
    if row["created_by_event_id"] != row["basis_event_id"]:
        cb = conn.execute("SELECT * FROM events WHERE event_id=?", (row["created_by_event_id"],)).fetchone()
        out["created_by_event"] = _event_brief(cb) if cb else None
    if row["closed_by_event_id"]:
        cl = conn.execute("SELECT * FROM events WHERE event_id=?", (row["closed_by_event_id"],)).fetchone()
        out["closed_by_event"] = _event_brief(cl) if cl else None
    prev = row["replaces_fragment_id"]
    if prev:
        prow = conn.execute(
            "SELECT fragment_id, chain_version, basis_event_id FROM fragments WHERE fragment_id=?",
            (prev,),
        ).fetchone()
        out["replaces"] = dict(prow) if prow else None
    out["detail"] = json.loads(row["detail_json"])
    return out


def warning_at(store: Storage, area_code: str, at: str | float | None = None) -> dict[str, Any]:
    """返回某地区在指定时刻（默认现在）生效的预警片段及其完整依据链。"""
    if at is None:
        at_ts = time.time()
    elif isinstance(at, str):
        at_ts = parse_instant(at).unix_seconds
    else:
        at_ts = float(at)
    conn = store.conn
    with store.lock:
        rows = conn.execute(
            """SELECT * FROM fragments
               WHERE area_code=? AND valid_from_unix <= ?
                 AND (valid_to_unix IS NULL OR valid_to_unix > ?)
               ORDER BY chain_version DESC""",
            (area_code, at_ts, at_ts),
        ).fetchall()
        if not rows:
            return {"area_code": area_code, "at_unix": at_ts, "warning": None,
                    "message": "该时刻此地区无生效预警片段"}
        current = _fragment_payload(conn, rows[0])
        # 依据链：沿 replaces 向前追溯到首发版本。
        chain = [current]
        seen = {current["fragment_id"]}
        cursor_id = current.get("replaces_fragment_id")
        while cursor_id and cursor_id not in seen:
            r = conn.execute("SELECT * FROM fragments WHERE fragment_id=?", (cursor_id,)).fetchone()
            if r is None:
                break
            payload = _fragment_payload(conn, r)
            chain.append(payload)
            seen.add(cursor_id)
            cursor_id = r["replaces_fragment_id"]
    return {"area_code": area_code, "at_unix": at_ts, "warning": current,
            "basis_chain": chain}


def timeline(store: Storage, *, subject_ref: str | None = None,
             area_code: str | None = None) -> dict[str, Any]:
    sql = "SELECT * FROM fragments WHERE 1=1"
    args: list[Any] = []
    if subject_ref:
        sql += " AND subject_ref=?"; args.append(subject_ref)
    if area_code:
        sql += " AND area_code=?"; args.append(area_code)
    sql += " ORDER BY valid_from_unix, area_code, chain_version"
    with store.lock:
        rows = store.conn.execute(sql, args).fetchall()
        fragments = [_fragment_payload(store.conn, r) for r in rows]
    return {"count": len(fragments), "fragments": fragments}


def event_log(store: Storage, *, source_id: str | None = None,
              include_stale: bool = True) -> dict[str, Any]:
    sql = "SELECT * FROM events WHERE 1=1"
    args: list[Any] = []
    if source_id:
        sql += " AND source_id=?"; args.append(source_id)
    if not include_stale:
        sql += " AND apply_state='applied'"
    sql += " ORDER BY chain_seq"
    with store.lock:
        rows = [_event_brief(r) for r in store.conn.execute(sql, args).fetchall()]
    return {"count": len(rows), "events": rows}


def verify_integrity(store: Storage) -> dict[str, Any]:
    """重算哈希链与载荷摘要，返回首个断点及校验计数。"""
    problems: list[dict[str, str]] = []
    with store.lock:
        rows = store.conn.execute(
            "SELECT * FROM events ORDER BY chain_seq"
        ).fetchall()
        prev_hash = GENESIS_HASH
        expected_seq = 1
        for row in rows:
            if row["chain_seq"] != expected_seq:
                problems.append({
                    "event_id": row["event_id"],
                    "problem": f"chain_seq 不连续：期望 {expected_seq}，实际 {row['chain_seq']}",
                })
            recomputed = canon.link_hash(
                prev_hash, row["envelope_canonical"].encode("utf-8")
            )
            if recomputed != row["chain_hash"]:
                problems.append({
                    "event_id": row["event_id"],
                    "problem": "哈希链断裂：存储的 chain_hash 与重算结果不一致",
                    "stored": row["chain_hash"],
                    "recomputed": recomputed,
                })
            try:
                payload = json.loads(row["payload_json"])
                if canon.digest_payload(payload) != row["payload_digest"]:
                    problems.append({
                        "event_id": row["event_id"],
                        "problem": "载荷摘要不符：payload_json 与 payload_digest 不一致",
                    })
            except json.JSONDecodeError:
                problems.append({
                    "event_id": row["event_id"], "problem": "payload_json 无法解析",
                })
            prev_hash = row["chain_hash"]
            expected_seq += 1

        # 片段依据必须指向真实事件；当前生效片段在时间轴上不得重叠。
        for fr in store.conn.execute("SELECT * FROM fragments").fetchall():
            for col in ("basis_event_id", "created_by_event_id"):
                ok = store.conn.execute(
                    "SELECT 1 FROM events WHERE event_id=?", (fr[col],)
                ).fetchone()
                if not ok:
                    problems.append({
                        "event_id": fr[col],
                        "problem": f"片段 {fr['fragment_id']} 的依据事件 {col} 缺失",
                    })
        overlaps = store.conn.execute(
            """SELECT a.fragment_id AS a, b.fragment_id AS b, a.area_code AS area
               FROM fragments a JOIN fragments b
                 ON a.subject_ref=b.subject_ref AND a.area_code=b.area_code
                AND a.fragment_id < b.fragment_id
                AND a.valid_from_unix < COALESCE(b.valid_to_unix, 9e18)
                AND b.valid_from_unix < COALESCE(a.valid_to_unix, 9e18)
                AND a.state='effective' AND b.state='effective'"""
        ).fetchall()
        for o in overlaps:
            problems.append({
                "event_id": "",
                "problem": f"地区 {o['area']} 存在时间轴重叠的生效片段 {o['a']} / {o['b']}",
            })
        tip = store.conn.execute(
            "SELECT chain_hash FROM events ORDER BY chain_seq DESC LIMIT 1"
        ).fetchone()
    return {
        "ok": not problems,
        "events_checked": len(rows),
        "chain_tip": tip["chain_hash"] if tip else GENESIS_HASH,
        "problems": problems,
    }
