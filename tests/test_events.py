"""事件接入与版本投影：摘要校验、迟到保护、片段拆分与依据链、跨午夜。"""

import unittest

from src import events
from src import queries
from tests.helpers import envelope, make_store, register_routes

HZ = {"code": "330100", "name": "杭州"}
NB = {"code": "330200", "name": "宁波"}
WX = {"code": "330500", "name": "湖州"}


def issue_payload(areas, **over):
    p = {
        "warning_level": "橙色",
        "rainfall_mm": [50, 100],
        "hourly_intensity_mm": 40,
        "areas": areas,
        "valid_from": "2026-09-19T22:00:00+08:00",
        "valid_for": "PT8H",
    }
    p.update(over)
    return p


class IngestionSecurityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = make_store()

    def tearDown(self) -> None:
        self.store.close()

    def test_bad_digest_rejected(self) -> None:
        ev = envelope("issue", 1, issue_payload([HZ]))
        ev["payload_digest"] = "sha256:" + "0" * 64
        with self.assertRaises(events.IngestionError):
            events.ingest(self.store, ev)
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"], 0
        )

    def test_naive_occurred_at_rejected(self) -> None:
        ev = envelope("issue", 1, issue_payload([HZ]))
        ev["occurred_at"] = "2026-09-19T22:00:00"
        with self.assertRaises(events.IngestionError):
            events.ingest(self.store, ev)

    def test_same_event_id_different_content_conflict(self) -> None:
        events.ingest(self.store, envelope("issue", 1, issue_payload([HZ])))
        ev2 = envelope("issue", 1, issue_payload([HZ, NB]), event_id="EVT-ISSUE-1")
        with self.assertRaises(events.ConflictError):
            events.ingest(self.store, ev2)

    def test_same_sequence_two_events(self) -> None:
        events.ingest(self.store, envelope("issue", 5, issue_payload([HZ])))
        with self.assertRaises(events.ConflictError):
            events.ingest(self.store, envelope(
                "issue", 5, issue_payload([NB], warning_level="红色"),
                event_id="EVT-OTHER-5"))

    def test_late_lower_sequence_recorded_but_not_applied(self) -> None:
        events.ingest(self.store, envelope("issue", 10, issue_payload([HZ])))
        late = envelope(
            "issue", 9, issue_payload([HZ], warning_level="黄色"),
            event_id="EVT-LATE-9", occurred_at="2026-09-19T21:00:00+08:00",
        )
        result = events.ingest(self.store, late)
        self.assertEqual(result.apply_state, "stale")
        # 当前生效的仍然是较新的 seq=10 版本。
        w = queries.warning_at(self.store, "330100", "2026-09-19T22:30:00+08:00")
        self.assertEqual(w["warning"]["level"], "橙色")
        self.assertEqual(len(self.store.conn.execute(
            "SELECT * FROM events WHERE apply_state='stale'").fetchall()), 1)

    def test_duplicate_event_is_idempotent(self) -> None:
        ev = envelope("issue", 1, issue_payload([HZ]))
        r1 = events.ingest(self.store, ev)
        r2 = events.ingest(self.store, ev)
        self.assertTrue(r2.apply_state.startswith("duplicate"))
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"], 1
        )


class FragmentProjectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = make_store()
        register_routes(self.store, [
            ("330100", "sms", "duty", "13900000001"),
            ("330100", "broadcast", "duty", "RAD-1"),
            ("330200", "sms", "duty", "13900000002"),
        ])

    def tearDown(self) -> None:
        self.store.close()

    def test_issue_creates_per_area_fragments_and_deliveries(self) -> None:
        r = events.ingest(self.store, envelope("issue", 1, issue_payload([HZ, NB])))
        self.assertEqual(r.deliveries_created, 3)
        rows = self.store.conn.execute(
            "SELECT area_code FROM fragments WHERE state='effective'"
        ).fetchall()
        self.assertEqual({row["area_code"] for row in rows}, {"330100", "330200"})

    def test_amend_splits_overlap_and_traces_basis(self) -> None:
        events.ingest(self.store, envelope("issue", 1, issue_payload([HZ, NB])))
        p2 = issue_payload([HZ], warning_level="红色", hourly_intensity_mm=70,
                           withdrawn_reason="雨带北抬，宁波撤销")
        r = events.ingest(self.store, envelope(
            "amend", 2, p2, occurred_at="2026-09-19T23:30:00+08:00"))
        self.assertEqual(r.revision, 2)

        # 宁波旧片段在订正发生时关闭，产生 withdrawn 通知。
        nb = self.store.conn.execute(
            "SELECT * FROM fragments WHERE area_code='330200'").fetchone()
        self.assertEqual(nb["state"], "superseded")
        self.assertEqual(nb["closed_by_event_id"], "EVT-AMEND-2")
        self.assertIn("雨带北抬", nb["close_reason"])
        notice = self.store.conn.execute(
            "SELECT kind FROM deliveries WHERE area_code='330200' AND basis_event_id='EVT-AMEND-2'"
        ).fetchone()
        self.assertEqual(notice["kind"], "withdrawn")

        # 杭州新版本替换旧版本，依据链可追溯。
        w = queries.warning_at(self.store, "330100", "2026-09-20T01:00:00+08:00")
        self.assertEqual(w["warning"]["level"], "红色")
        self.assertEqual(w["warning"]["basis_event"]["event_id"], "EVT-AMEND-2")
        self.assertEqual(len(w["basis_chain"]), 2)
        self.assertEqual(w["basis_chain"][-1]["basis_event"]["event_id"], "EVT-ISSUE-1")

        # 边界时刻 23:30 恰好命中新版本，不重叠。
        w_edge = queries.warning_at(self.store, "330100", "2026-09-19T23:30:00+08:00")
        self.assertEqual(w_edge["warning"]["chain_version"], 2)

    def test_time_window_across_midnight_keeps_original_offset(self) -> None:
        events.ingest(self.store, envelope("issue", 1, issue_payload([HZ])))
        w = queries.warning_at(self.store, "330100", "2026-09-20T05:59:59+08:00")
        self.assertIsNotNone(w["warning"])
        self.assertEqual(w["warning"]["valid_to"], "2026-09-20T06:00:00+08:00")
        w2 = queries.warning_at(self.store, "330100", "2026-09-20T06:00:00+08:00")
        self.assertIsNone(w2["warning"])

    def test_amend_inherits_valid_to_and_partial_updates(self) -> None:
        events.ingest(self.store, envelope("issue", 1, issue_payload([HZ])))
        # 订正只升级等级，未带雨量字段时继承上一版。
        events.ingest(self.store, envelope(
            "amend", 2, {"warning_level": "红色", "areas": [HZ]},
            occurred_at="2026-09-19T23:00:00+08:00"))
        w = queries.warning_at(self.store, "330100", "2026-09-20T01:00:00+08:00")
        self.assertEqual(w["warning"]["rainfall_min_mm"], 50.0)
        self.assertEqual(w["warning"]["hourly_intensity_mm"], 40.0)
        self.assertEqual(w["warning"]["valid_to"], "2026-09-20T06:00:00+08:00")

    def test_lift_partial_areas_and_lookback(self) -> None:
        events.ingest(self.store, envelope("issue", 1, issue_payload([HZ, NB, WX])))
        events.ingest(self.store, envelope(
            "lift", 2, {"areas": [NB], "reason": "宁波雨止"},
            occurred_at="2026-09-20T00:30:00+08:00"))
        self.assertIsNone(
            queries.warning_at(self.store, "330200", "2026-09-20T00:30:00+08:00")["warning"]
        )
        self.assertIsNotNone(
            queries.warning_at(self.store, "330100", "2026-09-20T00:31:00+08:00")["warning"]
        )
        # 解除前的历史时刻仍可回放。
        self.assertIsNotNone(
            queries.warning_at(self.store, "330200", "2026-09-20T00:29:00+08:00")["warning"]
        )

    def test_events_table_is_append_only(self) -> None:
        events.ingest(self.store, envelope("issue", 1, issue_payload([HZ])))
        with self.assertRaises(Exception):
            self.store.conn.execute("UPDATE events SET subject_ref='X' WHERE event_id='EVT-ISSUE-1'")
        with self.assertRaises(Exception):
            self.store.conn.execute("DELETE FROM events WHERE event_id='EVT-ISSUE-1'")


if __name__ == "__main__":
    unittest.main()
