"""历史不可静默改写：追加写保护、哈希链与载荷摘要自检。"""

import unittest

from src import events, queries
from tests.helpers import envelope, make_store

HZ = {"code": "330100", "name": "杭州"}


def payload(level="橙色"):
    return {
        "warning_level": level,
        "areas": [HZ],
        "valid_from": "2026-09-19T22:00:00+08:00",
        "valid_for": "PT6H",
    }


class IntegrityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = make_store()

    def tearDown(self) -> None:
        self.store.close()

    def test_clean_chain_verifies(self) -> None:
        events.ingest(self.store, envelope("issue", 1, payload()))
        events.ingest(self.store, envelope(
            "amend", 2, payload("红色"), occurred_at="2026-09-19T23:00:00+08:00"))
        events.ingest(self.store, envelope(
            "lift", 3, {"reason": "雨止"}, occurred_at="2026-09-20T02:00:00+08:00"))
        report = queries.verify_integrity(self.store)
        self.assertTrue(report["ok"], report["problems"])
        self.assertEqual(report["events_checked"], 3)

    def test_payload_tampering_detected(self) -> None:
        events.ingest(self.store, envelope("issue", 1, payload()))
        # 触发器禁止 UPDATE events，直接改载荷只能先关闭触发器（模拟绕过应用的库文件篡改）。
        self.store.conn.execute("DROP TRIGGER events_block_update")
        self.store.conn.execute(
            "UPDATE events SET payload_json=? WHERE event_id='EVT-ISSUE-1'",
            ('{"warning_level":"红色"}',))
        report = queries.verify_integrity(self.store)
        self.assertFalse(report["ok"])
        self.assertTrue(any("载荷摘要不符" in p["problem"] for p in report["problems"]))

    def test_chain_hash_tampering_detected(self) -> None:
        events.ingest(self.store, envelope("issue", 1, payload()))
        events.ingest(self.store, envelope(
            "amend", 2, payload("红色"), occurred_at="2026-09-19T23:00:00+08:00"))
        self.store.conn.execute("DROP TRIGGER events_block_update")
        self.store.conn.execute(
            "UPDATE events SET chain_hash=? WHERE event_id='EVT-ISSUE-1'",
            ("sha256:" + "f" * 64,))
        report = queries.verify_integrity(self.store)
        self.assertFalse(report["ok"])
        self.assertTrue(any("哈希链断裂" in p["problem"] for p in report["problems"]))

    def test_row_deletion_detected_as_sequence_gap(self) -> None:
        events.ingest(self.store, envelope("issue", 1, payload()))
        events.ingest(self.store, envelope(
            "amend", 2, payload("红色"), occurred_at="2026-09-19T23:00:00+08:00"))
        self.store.conn.execute("DROP TRIGGER events_block_delete")
        self.store.conn.execute("PRAGMA foreign_keys=OFF")
        self.store.conn.execute("DELETE FROM events WHERE event_id='EVT-ISSUE-1'")
        report = queries.verify_integrity(self.store)
        self.assertFalse(report["ok"])


if __name__ == "__main__":
    unittest.main()
