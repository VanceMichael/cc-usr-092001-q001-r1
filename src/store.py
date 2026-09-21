"""SQLite 持久化层：结构定义、连接管理与历史事实的不可变约束。"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS service_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- 接收到的全部事件（含被判迟到的），只追加、不可改写。
CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  source_ref TEXT NOT NULL,
  source_sequence INTEGER NOT NULL,
  occurred_at TEXT NOT NULL,
  occurred_epoch INTEGER NOT NULL,
  occurred_offset_minutes INTEGER NOT NULL,
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  ingest_status TEXT NOT NULL,
  ingest_note TEXT,
  received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_subject ON events(subject_ref);
CREATE INDEX IF NOT EXISTS idx_events_source ON events(source_ref, source_sequence);

-- 预警版本链：每次被应用的发布/订正/解除产生一个不可变版本。
CREATE TABLE IF NOT EXISTS alert_versions (
  version_id TEXT PRIMARY KEY,
  subject_ref TEXT NOT NULL,
  version_seq INTEGER NOT NULL,
  event_id TEXT NOT NULL REFERENCES events(event_id),
  kind TEXT NOT NULL,
  alert_level TEXT,
  rain_range TEXT,
  hourly_intensity TEXT,
  regions_json TEXT NOT NULL,
  valid_from TEXT,
  valid_from_epoch INTEGER,
  valid_until TEXT,
  valid_until_epoch INTEGER,
  decided_at TEXT NOT NULL,
  decided_epoch INTEGER NOT NULL,
  UNIQUE(subject_ref, version_seq)
);

-- 行政区生效片段：同一行政区每次被版本触及就追加一行，可追溯形成依据。
CREATE TABLE IF NOT EXISTS region_segments (
  segment_id TEXT PRIMARY KEY,
  subject_ref TEXT NOT NULL,
  region_code TEXT NOT NULL,
  version_id TEXT NOT NULL REFERENCES alert_versions(version_id),
  version_seq INTEGER NOT NULL,
  state TEXT NOT NULL,
  cancel_reason TEXT,
  effective_from TEXT NOT NULL,
  effective_from_epoch INTEGER NOT NULL,
  formed_by_event_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_segments_region
  ON region_segments(region_code, subject_ref, version_seq);

-- 接收岗位目录（运营数据，允许按 (地区, 渠道, 岗位) 更新终端引用）。
CREATE TABLE IF NOT EXISTS recipients (
  recipient_id TEXT PRIMARY KEY,
  region_code TEXT NOT NULL,
  channel TEXT NOT NULL,
  post_ref TEXT NOT NULL,
  endpoint_ref TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(region_code, channel, post_ref)
);

-- 送达记录：每个生效片段 × 接收岗位唯一一行，重试与补发不产生重复。
CREATE TABLE IF NOT EXISTS deliveries (
  delivery_id TEXT PRIMARY KEY,
  segment_id TEXT NOT NULL REFERENCES region_segments(segment_id),
  subject_ref TEXT NOT NULL,
  region_code TEXT NOT NULL,
  channel TEXT NOT NULL,
  post_ref TEXT NOT NULL,
  recipient_id TEXT NOT NULL,
  endpoint_ref TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_epoch INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(segment_id, recipient_id)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_region ON deliveries(region_code, channel, status);
CREATE INDEX IF NOT EXISTS idx_deliveries_due ON deliveries(status, next_attempt_epoch);

-- 每次渠道尝试一行，只追加。
CREATE TABLE IF NOT EXISTS dispatch_attempts (
  attempt_id TEXT PRIMARY KEY,
  delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
  attempt_no INTEGER NOT NULL,
  trigger_kind TEXT NOT NULL,
  result TEXT NOT NULL,
  detail TEXT,
  attempted_at TEXT NOT NULL,
  UNIQUE(delivery_id, attempt_no)
);

-- 接收确认：按 receipt_id 幂等，只追加。
CREATE TABLE IF NOT EXISTS confirmations (
  receipt_id TEXT PRIMARY KEY,
  delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
  confirmed_at TEXT NOT NULL,
  confirmed_epoch INTEGER NOT NULL,
  confirmer_ref TEXT
);

-- 历史事实不可被静默改写或删除。
CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'events 不可改写'); END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'events 不可删除'); END;
CREATE TRIGGER IF NOT EXISTS versions_no_update BEFORE UPDATE ON alert_versions
BEGIN SELECT RAISE(ABORT, 'alert_versions 不可改写'); END;
CREATE TRIGGER IF NOT EXISTS versions_no_delete BEFORE DELETE ON alert_versions
BEGIN SELECT RAISE(ABORT, 'alert_versions 不可删除'); END;
CREATE TRIGGER IF NOT EXISTS segments_no_update BEFORE UPDATE ON region_segments
BEGIN SELECT RAISE(ABORT, 'region_segments 不可改写'); END;
CREATE TRIGGER IF NOT EXISTS segments_no_delete BEFORE DELETE ON region_segments
BEGIN SELECT RAISE(ABORT, 'region_segments 不可删除'); END;
CREATE TRIGGER IF NOT EXISTS confirmations_no_update BEFORE UPDATE ON confirmations
BEGIN SELECT RAISE(ABORT, 'confirmations 不可改写'); END;
CREATE TRIGGER IF NOT EXISTS confirmations_no_delete BEFORE DELETE ON confirmations
BEGIN SELECT RAISE(ABORT, 'confirmations 不可删除'); END;
"""


class Store:
    """对单文件 SQLite 的线程安全封装；所有写操作经事务串行化。"""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(SCHEMA_SQL)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """串行写事务：BEGIN IMMEDIATE 保证检查与写入之间无并发缝隙。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params).fetchall())

    def close(self) -> None:
        with self._lock:
            self._conn.close()
