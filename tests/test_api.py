"""HTTP 接口冒烟测试。"""

import http.client
import json
import threading
from http.server import ThreadingHTTPServer
from urllib.parse import quote

import pytest

from conftest import REGION_A, make_event, make_payload
from src.api import make_handler
from src.dispatch import Dispatcher, ScriptedAdapter
from src.service import CHANNELS, AlertService
from src.store import Store


@pytest.fixture()
def server(tmp_path):
    store = Store(tmp_path / "api.sqlite3")
    service = AlertService(store)
    adapters = {channel: ScriptedAdapter() for channel in CHANNELS}
    dispatcher = Dispatcher(store, adapters, base_backoff_seconds=0)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(service, dispatcher))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield httpd
    httpd.shutdown()
    httpd.server_close()
    store.close()


def _request(server, method, path, body=None):
    connection = http.client.HTTPConnection("127.0.0.1", server.server_port)
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    connection.request(method, path, body=payload, headers=headers)
    response = connection.getresponse()
    data = json.loads(response.read().decode("utf-8"))
    connection.close()
    return response.status, data


def test_full_flow_over_http(server):
    status, health = _request(server, "GET", "/health")
    assert status == 200 and health == {"status": "ok"}

    status, recipient = _request(
        server,
        "POST",
        "/recipients",
        {
            "region_code": REGION_A,
            "channel": "sms",
            "post_ref": "值班长",
            "endpoint_ref": "sms-gateway/360101-lead",
        },
    )
    assert status == 201

    event = make_event(payload=make_payload(regions=[REGION_A]))
    status, ingested = _request(server, "POST", "/events", event)
    assert status == 201
    assert ingested["ingest_status"] == "APPLIED"
    assert ingested["deliveries_created"] == 1

    # 重复投递同一事件：幂等返回首次结果。
    status, replay = _request(server, "POST", "/events", event)
    assert status == 201
    assert replay["replayed"] is True

    at = quote("2026-09-21T00:30:00+08:00")
    status, state = _request(server, "GET", f"/regions/{REGION_A}/state?at={at}")
    assert status == 200
    assert state["alerts"][0]["state"] == "ACTIVE"
    assert state["alerts"][0]["formed_by_event_id"] == "EVT-1"

    status, unconfirmed = _request(server, "GET", f"/regions/{REGION_A}/unconfirmed")
    assert status == 200
    assert unconfirmed["summary"] == {"sms": 1}
    delivery_id = unconfirmed["unconfirmed"][0]["delivery_id"]

    status, confirmed = _request(
        server,
        "POST",
        f"/deliveries/{delivery_id}/confirm",
        {"receipt_id": "RC-1", "confirmed_at": "2026-09-21T00:40:00+08:00"},
    )
    assert status == 200
    assert confirmed["duplicate"] is False

    status, unconfirmed = _request(server, "GET", f"/regions/{REGION_A}/unconfirmed")
    assert unconfirmed["count"] == 0


def test_manual_resend_over_http(server):
    _request(
        server,
        "POST",
        "/recipients",
        {
            "region_code": REGION_A,
            "channel": "terminal",
            "post_ref": "值班员",
            "endpoint_ref": "terminal/360101",
        },
    )
    _request(server, "POST", "/events", make_event(payload=make_payload(regions=[REGION_A])))
    _, unconfirmed = _request(server, "GET", f"/regions/{REGION_A}/unconfirmed")
    delivery_id = unconfirmed["unconfirmed"][0]["delivery_id"]

    status, detail = _request(server, "POST", f"/deliveries/{delivery_id}/resend")
    assert status == 200
    assert detail["status"] == "SENT"  # 补发后立即完成一轮派发
    assert detail["attempt_count"] == 1


def test_error_shapes(server):
    status, body = _request(server, "GET", "/no-such-route")
    assert status == 404
    assert body["error"]["code"] == "not_found"

    bad = make_event(payload=make_payload(regions=[REGION_A]))
    bad["payload"]["alert_level"] = "被篡改"
    status, body = _request(server, "POST", "/events", bad)
    assert status == 422
    assert body["error"]["code"] == "digest_mismatch"

    status, body = _request(server, "GET", "/events/EVT-MISSING")
    assert status == 404
    assert body["error"]["code"] == "event_not_found"
