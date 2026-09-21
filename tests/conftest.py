"""测试公共夹具与构造助手。"""

from __future__ import annotations

import pytest

from src.dispatch import Dispatcher, ScriptedAdapter
from src.domain import compute_digest
from src.service import CHANNELS, AlertService
from src.store import Store

REGION_A = "360101"
REGION_B = "360102"
REGION_C = "360103"


def make_payload(**overrides: object) -> dict:
    payload = {
        "alert_level": "暴雨红色预警",
        "rain_range": "50-80毫米",
        "hourly_intensity": "20-40毫米/小时",
        "regions": [REGION_A, REGION_B],
        "valid_from": "2026-09-20T23:00:00+08:00",
        "valid_until": "2026-09-21T06:00:00+08:00",
    }
    payload.update(overrides)
    return payload


def make_event(
    event_id: str = "EVT-1",
    event_type: str = "ISSUE",
    subject: str = "ALERT-2026-001",
    sequence: int = 1,
    occurred: str = "2026-09-20T22:30:00+08:00",
    source: str = "PROV-MET-01",
    payload: dict | None = None,
) -> dict:
    body = make_payload() if payload is None else payload
    return {
        "schema_version": "1",
        "event_id": event_id,
        "event_type": event_type,
        "subject_ref": subject,
        "source_ref": source,
        "occurred_at": occurred,
        "source_sequence": sequence,
        "payload_digest": compute_digest(body),
        "payload": body,
    }


def add_recipient(
    service: AlertService,
    region: str,
    channel: str,
    post: str,
    endpoint: str | None = None,
) -> dict:
    return service.register_recipient(
        {
            "region_code": region,
            "channel": channel,
            "post_ref": post,
            "endpoint_ref": endpoint or f"ep-{region}-{channel}-{post}",
        }
    )


@pytest.fixture()
def store(tmp_path):
    instance = Store(tmp_path / "test.sqlite3")
    yield instance
    instance.close()


@pytest.fixture()
def service(store):
    return AlertService(store)


@pytest.fixture()
def adapters() -> dict[str, ScriptedAdapter]:
    return {channel: ScriptedAdapter() for channel in CHANNELS}


@pytest.fixture()
def dispatcher(store, adapters):
    return Dispatcher(store, adapters, base_backoff_seconds=0)
