"""测试辅助：构造临时库、事件信封和接收岗位。"""

from __future__ import annotations

import os
import tempfile

from src import canon
from src.storage import Storage


def make_store() -> Storage:
    fd, path = tempfile.mkstemp(prefix="warntest-", suffix=".sqlite3")
    os.close(fd)
    os.unlink(path)  # Storage 负责创建
    return Storage(path)


def envelope(event_type: str, sequence: int, payload: dict, *,
             event_id: str | None = None,
             source_id: str = "MET-HQ",
             subject_ref: str = "SUBJ-2026-RAIN",
             occurred_at: str = "2026-09-19T22:00:00+08:00") -> dict:
    return {
        "event_id": event_id or f"EVT-{event_type.upper()}-{sequence}",
        "source_id": source_id,
        "subject_ref": subject_ref,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "source_sequence": sequence,
        "payload_digest": canon.digest_payload(payload),
        "payload": payload,
    }


def register_routes(store: Storage, routes: list[tuple[str, str, str, str]]) -> None:
    """routes: [(area_code, channel, post, address), ...]"""
    with store.lock:
        store.begin()
        for i, (code, channel, post, address) in enumerate(routes):
            store.conn.execute(
                """INSERT INTO recipients(recipient_id, area_code, post, channel,
                   address, active, created_unix) VALUES(?,?,?,?,?,1,1000.0)""",
                (f"R-{i:03d}", code, post, channel, address),
            )
        store.commit()
