"""分发：唯一送达记录、重试退避、人工补发、ACK 幂等、重启续跑。"""

import time
import unittest

from src.channels import ScriptedChannel
from src.dispatch import BACKOFF_SCHEDULE, DispatchError, Dispatcher
from src import events
from tests.helpers import envelope, make_store, register_routes

HZ = {"code": "330100", "name": "杭州"}


def base_payload(**over):
    p = {
        "warning_level": "橙色",
        "areas": [HZ],
        "valid_from": "2026-09-19T22:00:00+08:00",
        "valid_for": "PT8H",
    }
    p.update(over)
    return p


def did_for(store, channel, post="duty"):
    return store.conn.execute(
        "SELECT delivery_id FROM deliveries WHERE channel=? AND post=?",
        (channel, post)).fetchone()["delivery_id"]


class DispatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = make_store()
        register_routes(self.store, [
            ("330100", "sms", "duty", "13900000001"),
            ("330100", "sms", "chief", "13900000009"),
            ("330100", "broadcast", "duty", "RAD-1"),
        ])
        events.ingest(self.store, envelope("issue", 1, base_payload()))
        self.t0 = time.time()
        self.sms = ScriptedChannel("sms")
        self.broadcast = ScriptedChannel("broadcast")
        self.dispatcher = Dispatcher(self.store, {"sms": self.sms, "broadcast": self.broadcast})

    def tearDown(self) -> None:
        self.store.close()

    def test_unique_deliveries_per_channel_post(self) -> None:
        rows = self.store.conn.execute(
            "SELECT channel, post, COUNT(*) c FROM deliveries GROUP BY channel, post"
        ).fetchall()
        self.assertEqual({(r["channel"], r["post"], r["c"]) for r in rows},
                         {("sms", "chief", 1), ("sms", "duty", 1), ("broadcast", "duty", 1)})
        # 重放同一事件不产生重复送达。
        events.ingest(self.store, envelope("issue", 1, base_payload()))
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) c FROM deliveries").fetchone()["c"], 3)

    def test_dispatch_sends_and_tracks_unconfirmed(self) -> None:
        reports = self.dispatcher.run_due(now=self.t0 + 1)
        self.assertEqual(len(reports), 3)
        detail = self.dispatcher.delivery_detail(did_for(self.store, "sms"))
        self.assertEqual(detail["state"], "sent")
        self.assertEqual(len(detail["unconfirmed"]), 1)
        self.assertEqual(detail["unconfirmed"][0]["recipient_id"], "R-000")

    def test_retry_backoff_then_success(self) -> None:
        failing_sms = ScriptedChannel("sms", failures=2)
        dispatcher = Dispatcher(self.store, {
            "sms": failing_sms, "broadcast": self.broadcast})
        did = did_for(self.store, "sms")

        # 第一轮：两条短信路由各失败一次（共消耗 2 次失败额度），广播成功。
        reports = dispatcher.run_due(now=self.t0 + 1)
        self.assertEqual(sum(r.result == "failure" for r in reports), 2)
        self.assertEqual(sum(r.result == "success" for r in reports), 1)
        row = self.store.conn.execute(
            "SELECT state,next_attempt_unix,attempt_count FROM deliveries WHERE delivery_id=?",
            (did,)).fetchone()
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["attempt_count"], 1)
        self.assertAlmostEqual(row["next_attempt_unix"], self.t0 + 1 + BACKOFF_SCHEDULE[0])

        # 未到退避时间不会重试。
        self.assertEqual(dispatcher.run_due(now=self.t0 + 1 + BACKOFF_SCHEDULE[0] - 1), [])
        # 到点后渠道恢复，短信两条路由都成功。
        second = dispatcher.run_due(now=self.t0 + 1 + BACKOFF_SCHEDULE[0] + 1)
        self.assertEqual({r.result for r in second}, {"success"})
        row = self.store.conn.execute(
            "SELECT state,attempt_count FROM deliveries WHERE delivery_id=?", (did,)).fetchone()
        self.assertEqual(row["state"], "sent")
        self.assertEqual(row["attempt_count"], 2)

    def test_manual_reissue_idempotent_and_bypasses_exhaustion(self) -> None:
        store2 = make_store()
        register_routes(store2, [("330100", "sms", "duty", "13900000001")])
        events.ingest(store2, envelope("issue", 1, base_payload(max_attempts=2)))
        t0 = time.time()
        sms = ScriptedChannel("sms", failures=99)
        dispatcher = Dispatcher(store2, {"sms": sms})
        did = did_for(store2, "sms")
        dispatcher.run_due(now=t0 + 1)
        dispatcher.run_due(now=t0 + 1 + BACKOFF_SCHEDULE[0] + 1)
        row = store2.conn.execute(
            "SELECT state,attempt_count FROM deliveries WHERE delivery_id=?", (did,)).fetchone()
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["attempt_count"], 2)
        # 自动扫描不再捞起已耗尽记录。
        self.assertEqual(dispatcher.run_due(now=t0 + 1e9), [])

        # 人工补发即使耗尽也可执行；同一 request_id 重复提交只生效一次。
        sms.remaining_failures = 0
        rt = t0 + 10000
        r1 = dispatcher.reissue(
            {"request_id": "RE-1", "delivery_id": did, "operator_ref": "OP-LEE",
             "note": "主管补发"}, now=rt)
        r2 = dispatcher.reissue(
            {"request_id": "RE-1", "delivery_id": did, "operator_ref": "OP-LEE"}, now=rt + 1)
        self.assertTrue(r1["attempt_id"])
        self.assertFalse(r1["idempotent"])
        self.assertTrue(r2["idempotent"])
        self.assertEqual(r2["attempt_id"], r1["attempt_id"])
        row = store2.conn.execute(
            "SELECT state FROM deliveries WHERE delivery_id=?", (did,)).fetchone()
        self.assertEqual(row["state"], "sent")
        # 人工补发只产生一次额外尝试。
        kinds = [r["kind"] for r in store2.conn.execute(
            "SELECT kind FROM delivery_attempts WHERE delivery_id=? ORDER BY seq", (did,))]
        self.assertEqual(kinds, ["auto", "auto", "manual"])
        store2.close()

    def test_ack_idempotent_and_scoped(self) -> None:
        self.dispatcher.run_due(now=self.t0 + 1)
        did = did_for(self.store, "sms")
        a1 = self.dispatcher.record_ack(
            {"ack_id": "ACK-1", "delivery_id": did, "recipient_id": "R-000",
             "occurred_at": "2026-09-19T22:05:00+08:00"}, now=self.t0 + 10)
        self.assertEqual(a1["state"], "recorded")
        a2 = self.dispatcher.record_ack(
            {"ack_id": "ACK-1", "delivery_id": did, "recipient_id": "R-000"}, now=self.t0 + 11)
        self.assertEqual(a2["state"], "duplicate")
        # ack_id 不得复用于其他送达/接收方。
        other = did_for(self.store, "sms", "chief")
        with self.assertRaises(DispatchError):
            self.dispatcher.record_ack(
                {"ack_id": "ACK-1", "delivery_id": other, "recipient_id": "R-001"})
        # 不属于该送达记录的接收方无效。
        with self.assertRaises(DispatchError):
            self.dispatcher.record_ack(
                {"ack_id": "ACK-2", "delivery_id": did, "recipient_id": "R-002"})
        detail = self.dispatcher.delivery_detail(did)
        self.assertEqual(detail["confirmed_count"], 1)
        self.assertEqual([u["recipient_id"] for u in detail["unconfirmed"]], [])

    def test_resume_after_restart_picks_up_pending(self) -> None:
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) c FROM deliveries WHERE state='pending'").fetchone()["c"], 3)
        new_dispatcher = Dispatcher(self.store, {
            "sms": ScriptedChannel("sms"), "broadcast": ScriptedChannel("broadcast")})
        reports = new_dispatcher.run_due(now=time.time() + 1)
        self.assertEqual(len(reports), 3)
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) c FROM deliveries WHERE state='sent'").fetchone()["c"], 3)

    def test_resume_recovers_sending_left_by_crash(self) -> None:
        # 直接把一条记录置为 sending（模拟渠道调用中途宕机），重启后仍可续跑。
        did = did_for(self.store, "sms")
        self.store.conn.execute(
            "UPDATE deliveries SET state='sending' WHERE delivery_id=?", (did,))
        reports = self.dispatcher.run_due(now=time.time() + 1)
        self.assertTrue(any(r.delivery_id == did and r.result == "success" for r in reports))


if __name__ == "__main__":
    unittest.main()
