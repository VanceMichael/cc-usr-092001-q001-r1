"""HTTP 端到端：从事件接入到送达确认，以及重启续跑。"""

import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from src import canon
from src.app import build_server
from src.storage import Storage
from tests.helpers import envelope

HZ = {"code": "330100", "name": "杭州"}
NB = {"code": "330200", "name": "宁波"}


class HttpCase(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile, os

        fd, self.db_path = tempfile.mkstemp(prefix="warnhttp-", suffix=".sqlite3")
        os.close(fd)
        os.unlink(self.db_path)
        store = Storage(self.db_path)
        self.httpd: ThreadingHTTPServer = build_server(
            "127.0.0.1", 0, store, auto_dispatch=False)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def tearDown(self) -> None:
        self.httpd.service.shutdown()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)
        self.httpd.service.store.close()

    def call(self, method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_full_lifecycle(self) -> None:
        # 1) 接收岗位
        for rid, code, ch, post, addr in [
            ("R1", "330100", "sms", "duty", "13900000001"),
            ("R2", "330100", "broadcast", "duty", "RAD-1"),
            ("R3", "330200", "gov_terminal", "chief", "GOV-9"),
        ]:
            status, _ = self.call("PUT", "/v1/admin/recipients", {
                "recipient_id": rid, "area_code": code, "post": post,
                "channel": ch, "address": addr})
            self.assertEqual(status, 200)

        # 2) 发布
        payload = {
            "warning_level": "橙色", "rainfall_mm": [50, 100],
            "hourly_intensity_mm": 40, "areas": [HZ, NB],
            "valid_from": "2026-09-19T22:00:00+08:00", "valid_for": "PT8H"}
        status, body = self.call("POST", "/v1/events", envelope("issue", 1, payload))
        self.assertEqual(status, 201)
        self.assertEqual(body["deliveries_created"], 3)

        # 3) 坏摘要拒收
        bad = envelope("issue", 2, payload, event_id="EVT-BAD")
        bad["payload_digest"] = "sha256:" + "0" * 64
        status, body = self.call("POST", "/v1/events", bad)
        self.assertEqual(status, 400)
        self.assertIn("摘要", body["error"])

        # 4) 分发并确认
        status, body = self.call("POST", "/v1/dispatch/run")
        self.assertEqual(body["ran"], 3)
        status, dlist = self.call("GET", "/v1/deliveries?channel=sms")
        self.assertEqual(status, 200)
        did = dlist["deliveries"][0]["delivery_id"]
        status, detail = self.call("GET", f"/v1/deliveries/{did}")
        self.assertEqual(detail["state"], "sent")
        recipient = detail["recipients"][0]["recipient_id"]
        self.assertEqual(len(detail["unconfirmed"]), 1)
        status, ack = self.call("POST", "/v1/acks", {
            "ack_id": "ACK-1", "delivery_id": did, "recipient_id": recipient})
        self.assertEqual(status, 200)
        status, detail = self.call("GET", f"/v1/deliveries/{did}")
        self.assertEqual(detail["confirmed_count"], 1)
        self.assertEqual(detail["unconfirmed"], [])

        # 5) 订正：宁波撤销，杭州升级；值班主管查询依据链
        amend = {"warning_level": "红色", "hourly_intensity_mm": 70,
                 "areas": [HZ], "withdrawn_reason": "雨带北抬"}
        status, body = self.call("POST", "/v1/events", envelope(
            "amend", 2, amend, occurred_at="2026-09-19T23:30:00+08:00"))
        self.assertEqual(body["revision"], 2)
        self.call("POST", "/v1/dispatch/run")
        status, w = self.call("GET", "/v1/warnings/at?area_code=330200&at=2026-09-20T00:00:00%2B08:00")
        self.assertIsNone(w["warning"])
        status, w = self.call("GET", "/v1/warnings/at?area_code=330100&at=2026-09-20T00:00:00%2B08:00")
        self.assertEqual(w["warning"]["level"], "红色")
        self.assertEqual(w["warning"]["basis_event"]["event_id"], "EVT-AMEND-2")

        # 6) 完整性
        status, integrity = self.call("GET", "/v1/integrity")
        self.assertTrue(integrity["ok"], integrity["problems"])

    def test_restart_resumes_undelivered(self) -> None:
        # 登记+发布，但不分发；关闭服务后用同一数据库文件重启，未完成分发继续。
        status, _ = self.call("PUT", "/v1/admin/recipients", {
            "recipient_id": "R1", "area_code": "330100", "post": "duty",
            "channel": "sms", "address": "13900000001"})
        self.assertEqual(status, 200)
        payload = {"warning_level": "黄色", "areas": [HZ],
                   "valid_from": "2026-09-19T22:00:00+08:00", "valid_for": "PT2H"}
        status, _ = self.call("POST", "/v1/events", envelope("issue", 1, payload))
        self.assertEqual(status, 201)

        # 重启
        self.httpd.service.shutdown()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)
        self.httpd.service.store.close()

        store = Storage(self.db_path)
        self.httpd = build_server("127.0.0.1", 0, store, auto_dispatch=False)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"

        status, body = self.call("POST", "/v1/dispatch/run")
        self.assertEqual(status, 200)
        self.assertEqual(body["ran"], 1)
        status, dlist = self.call("GET", "/v1/deliveries")
        self.assertEqual({d["state"] for d in dlist["deliveries"]}, {"sent"})


if __name__ == "__main__":
    unittest.main()
