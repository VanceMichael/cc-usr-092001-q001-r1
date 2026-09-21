"""事件接收：校验、幂等与摘要约束。"""

import pytest

from conftest import REGION_A, REGION_B, add_recipient, make_event, make_payload
from src.domain import ServiceError, compute_digest


def test_issue_applies_and_creates_segments_and_deliveries(service):
    add_recipient(service, REGION_A, "sms", "值班长")
    add_recipient(service, REGION_A, "terminal", "值班员")
    add_recipient(service, REGION_B, "broadcast", "广播员")

    result = service.ingest_event(make_event())

    assert result["ingest_status"] == "APPLIED"
    assert result["version_seq"] == 1
    assert result["segments_created"] == 2
    assert result["deliveries_created"] == 3
    assert result["replayed"] is False

    versions = service.subject_versions("ALERT-2026-001")["versions"]
    assert len(versions) == 1
    assert versions[0]["regions"] == [REGION_A, REGION_B]
    assert versions[0]["alert_level"] == "暴雨红色预警"


def test_digest_mismatch_is_rejected_without_storage(service, store):
    event = make_event()
    event["payload"]["alert_level"] = "暴雨橙色预警"  # 篡改内容但保留原摘要

    with pytest.raises(ServiceError) as caught:
        service.ingest_event(event)

    assert caught.value.status == 422
    assert caught.value.code == "digest_mismatch"
    assert store.one("SELECT * FROM events WHERE event_id='EVT-1'") is None


def test_naive_occurred_at_is_rejected(service):
    event = make_event()
    event["occurred_at"] = "2026-09-20T22:30:00"  # 缺时区偏移

    with pytest.raises(ServiceError) as caught:
        service.ingest_event(event)

    assert caught.value.status == 422
    assert caught.value.code == "invalid_time"


def test_duplicate_event_id_replays_first_result(service):
    add_recipient(service, REGION_A, "sms", "值班长")
    first = service.ingest_event(make_event())
    second = service.ingest_event(make_event())

    assert second["replayed"] is True
    assert second["version_id"] == first["version_id"]
    assert second["deliveries_created"] == first["deliveries_created"]
    versions = service.subject_versions("ALERT-2026-001")["versions"]
    assert len(versions) == 1
    deliveries = service.list_deliveries(region=REGION_A)["deliveries"]
    assert len(deliveries) == 1


def test_same_event_id_with_different_digest_conflicts(service):
    service.ingest_event(make_event())
    other = make_event(payload=make_payload(alert_level="暴雨橙色预警"))
    assert other["payload_digest"] != compute_digest(make_event()["payload"])

    with pytest.raises(ServiceError) as caught:
        service.ingest_event(other)

    assert caught.value.status == 409
    assert caught.value.code == "event_conflict"


def test_second_issue_for_same_subject_conflicts(service):
    service.ingest_event(make_event())
    again = make_event(event_id="EVT-2", sequence=2, occurred="2026-09-20T23:10:00+08:00")

    with pytest.raises(ServiceError) as caught:
        service.ingest_event(again)

    assert caught.value.status == 409
    assert caught.value.code == "subject_exists"


def test_correct_for_unknown_subject_is_not_found(service):
    event = make_event(event_type="CORRECT")

    with pytest.raises(ServiceError) as caught:
        service.ingest_event(event)

    assert caught.value.status == 404
    assert caught.value.code == "subject_not_found"


def test_invalid_validity_window_is_rejected(service):
    payload = make_payload(
        valid_from="2026-09-21T06:00:00+08:00",
        valid_until="2026-09-20T23:00:00+08:00",
    )
    with pytest.raises(ServiceError) as caught:
        service.ingest_event(make_event(payload=payload))

    assert caught.value.status == 422
    assert caught.value.code == "invalid_validity"
