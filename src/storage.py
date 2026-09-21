"""SQLite 存储层。

设计要点：

* ``events`` 为**追加写台账**：触发器禁止 UPDATE/DELETE，配合哈希链，
  历史版本无法被静默改写。
* ``fragments`` 是事件流对“行政区 × 预警主体”的生效投影，允许状态流转，
  但每次流转都记录依据事件（``created_by_event_id`` / ``closed_by_event_id``）。
* 分发状态全部落库（``deliveries`` / ``delivery_recipients`` /
  ``delivery_attempts`` / ``reissues`` / ``inbound_acks``），进程重启后
  未完成的分发可继续；所有唯一性约束在数据库层兜底幂等。
* 启用 WAL；单连接配合进程级锁串行化写事务，读不阻塞。
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

SCHEMA_VERSION = 2

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS service_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_unix REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS source_cursors (
    source_id TEXT PRIMARY KEY REFERENCES sources(source_id),
    last_applied_sequence INTEGER NOT NULL,
    last_applied_event_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS areas (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    parent_code TEXT REFERENCES areas(code),
    created_unix REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    chain_seq INTEGER NOT NULL UNIQUE,
    source_id TEXT NOT NULL,
    source_sequence INTEGER NOT NULL,
    subject_ref TEXT NOT NULL,
    event_type TEXT NOT NULL
        CHECK (event_type IN ('issue', 'amend', 'lift')),
    occurred_unix REAL NOT NULL,
    occurred_offset_minutes INTEGER NOT NULL,
    received_unix REAL NOT NULL,
    payload_json TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    envelope_canonical TEXT NOT NULL,
    chain_hash TEXT NOT NULL,
    apply_state TEXT NOT NULL
        CHECK (apply_state IN ('applied', 'stale')),
    stale_reason TEXT,
    UNIQUE(source_id, source_sequence)
);

CREATE TRIGGER IF NOT EXISTS events_block_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(FAIL, 'events 为追加写台账，禁止 UPDATE');
END;

CREATE TRIGGER IF NOT EXISTS events_block_delete
BEFORE DELETE ON events
BEGIN
    SELECT RAISE(FAIL, 'events 为追加写台账，禁止 DELETE');
END;

CREATE TABLE IF NOT EXISTS fragments (
    fragment_id TEXT PRIMARY KEY,
    chain_version INTEGER NOT NULL,
    subject_ref TEXT NOT NULL,
    area_code TEXT NOT NULL,
    level TEXT NOT NULL,
    rainfall_min_mm REAL,
    rainfall_max_mm REAL,
    hourly_intensity_mm REAL,
    detail_json TEXT NOT NULL,
    valid_from_unix REAL NOT NULL,
    valid_from_offset_minutes INTEGER NOT NULL,
    valid_to_unix REAL,
    valid_to_offset_minutes INTEGER,
    created_by_event_id TEXT NOT NULL REFERENCES events(event_id),
    basis_event_id TEXT NOT NULL REFERENCES events(event_id),
    replaces_fragment_id TEXT REFERENCES fragments(fragment_id),
    closed_by_event_id TEXT REFERENCES events(event_id),
    state TEXT NOT NULL
        CHECK (state IN ('effective', 'superseded', 'lifted')),
    close_reason TEXT,
    created_unix REAL NOT NULL,
    UNIQUE(subject_ref, area_code, chain_version)
);
CREATE INDEX IF NOT EXISTS idx_fragments_lookup
    ON fragments(subject_ref, area_code, state);
CREATE INDEX IF NOT EXISTS idx_fragments_area_time
    ON fragments(area_code, valid_from_unix);
CREATE INDEX IF NOT EXISTS idx_fragments_state
    ON fragments(state, subject_ref);

CREATE TABLE IF NOT EXISTS recipients (
    recipient_id TEXT PRIMARY KEY,
    area_code TEXT NOT NULL,
    post TEXT NOT NULL,
    channel TEXT NOT NULL
        CHECK (channel IN ('sms', 'broadcast', 'gov_terminal')),
    address TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    created_unix REAL NOT NULL,
    UNIQUE(channel, address)
);
CREATE INDEX IF NOT EXISTS idx_recipients_route
    ON recipients(active, area_code, channel, post);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('warning', 'lift', 'withdrawn')),
    basis_event_id TEXT NOT NULL REFERENCES events(event_id),
    fragment_id TEXT NOT NULL REFERENCES fragments(fragment_id),
    subject_ref TEXT NOT NULL,
    area_code TEXT NOT NULL,
    channel TEXT NOT NULL,
    post TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    state TEXT NOT NULL
        CHECK (state IN ('pending', 'sending', 'sent', 'failed', 'disabled')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    next_attempt_unix REAL,
    last_error TEXT,
    sent_unix REAL,
    idempotency_key TEXT NOT NULL,
    created_unix REAL NOT NULL,
    updated_unix REAL NOT NULL,
    UNIQUE(basis_event_id, area_code, channel, post)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_due
    ON deliveries(state, next_attempt_unix);

CREATE TABLE IF NOT EXISTS delivery_recipients (
    delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
    recipient_id TEXT NOT NULL REFERENCES recipients(recipient_id),
    channel TEXT NOT NULL,
    address TEXT NOT NULL,
    ack_state TEXT NOT NULL
        CHECK (ack_state IN ('pending', 'confirmed')),
    added_unix REAL NOT NULL,
    confirmed_unix REAL,
    ack_id TEXT,
    PRIMARY KEY (delivery_id, recipient_id)
);
CREATE INDEX IF NOT EXISTS idx_dr_ack
    ON delivery_recipients(ack_state, delivery_id);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    attempt_id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('auto', 'manual')),
    trigger_ref TEXT NOT NULL,
    started_unix REAL NOT NULL,
    finished_unix REAL,
    result TEXT CHECK (result IN ('success', 'failure')),
    detail_json TEXT,
    UNIQUE(delivery_id, seq)
);

CREATE TABLE IF NOT EXISTS reissues (
    request_id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
    operator_ref TEXT NOT NULL,
    note TEXT,
    request_unix REAL NOT NULL,
    attempt_id TEXT
);

CREATE TABLE IF NOT EXISTS inbound_acks (
    ack_id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
    recipient_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    occurred_unix REAL,
    occurred_offset_minutes INTEGER,
    received_unix REAL NOT NULL,
    payload_digest TEXT,
    UNIQUE(delivery_id, recipient_id)
);
"""


class StorageError(RuntimeError):
    """存储层异常。"""


class Storage:
    """持有单一 SQLite 连接与进程级写锁。"""

    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(
            self.path,
            check_same_thread=False,
            isolation_level=None,  # 显式事务
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._initialize()

    def _initialize(self) -> None:
        with self.lock:
            self.conn.executescript(SCHEMA_SQL)
            self.conn.execute(
                "INSERT OR IGNORE INTO service_meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            self.conn.execute("PRAGMA user_version=%d" % SCHEMA_VERSION)

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    # -- 基础工具 -------------------------------------------------------

    def begin(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self.conn.execute("COMMIT")

    def rollback(self) -> None:
        self.conn.execute("ROLLBACK")


def initialize_database(path: str | Path) -> None:
    """供迁移脚本调用：建表并关闭连接。"""
    storage = Storage(path)
    storage.close()
