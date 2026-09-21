"""送达记录：唯一性、渠道重试、人工补发与确认幂等。"""

from conftest import REGION_A, REGION_B, add_recipient, make_event, make_payload


def _only_delivery(service, region=REGION_A):
    deliveries = service.list_deliveries(region=region)["deliveries"]
    assert len(deliveries) == 1
    return deliveries[0]


def test_channel_retry_succeeds_after_transient_failure(service, dispatcher, adapters):
    add_recipient(service, REGION_A, "sms", "值班长")
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A])))
    delivery = _only_delivery(service)
    endpoint = service.get_delivery(delivery["delivery_id"])["endpoint_ref"]
    adapters["sms"].failures[endpoint] = 2

    assert dispatcher.run_once()["failed"] == 1
    assert dispatcher.run_once()["failed"] == 1
    assert dispatcher.run_once()["sent"] == 1

    detail = service.get_delivery(delivery["delivery_id"])
    assert detail["status"] == "SENT"
    assert detail["attempt_count"] == 3
    assert [a["result"] for a in detail["attempts"]] == ["FAILURE", "FAILURE", "SUCCESS"]


def test_exhausted_delivery_can_be_manually_resent(store, service, adapters):
    from src.dispatch import Dispatcher

    dispatcher = Dispatcher(store, adapters, max_attempts=2, base_backoff_seconds=0)
    add_recipient(service, REGION_A, "sms", "值班长")
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A])))
    delivery = _only_delivery(service)
    endpoint = service.get_delivery(delivery["delivery_id"])["endpoint_ref"]
    adapters["sms"].failures[endpoint] = 2  # 自动重试耗尽后，补发时渠道已恢复

    dispatcher.run_once()
    dispatcher.run_once()
    assert service.get_delivery(delivery["delivery_id"])["status"] == "EXHAUSTED"
    assert dispatcher.run_once()["due"] == 0  # 不再自动重试

    outcome = service.resend_delivery(delivery["delivery_id"])
    assert outcome["resend"] == "scheduled"
    dispatcher.run_once()
    assert service.get_delivery(delivery["delivery_id"])["status"] == "SENT"


def test_resend_is_idempotent_and_never_duplicates_delivery(service, dispatcher):
    add_recipient(service, REGION_A, "sms", "值班长")
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A])))
    delivery = _only_delivery(service)

    dispatcher.run_once()
    service.resend_delivery(delivery["delivery_id"])
    service.resend_delivery(delivery["delivery_id"])
    dispatcher.run_once()

    # 补发不产生新的送达记录，只追加尝试。
    assert len(service.list_deliveries(region=REGION_A)["deliveries"]) == 1
    detail = service.get_delivery(delivery["delivery_id"])
    assert detail["status"] == "SENT"
    assert detail["attempt_count"] == 2

    service.confirm_delivery(
        delivery["delivery_id"],
        {"receipt_id": "RC-1", "confirmed_at": "2026-09-21T00:10:00+08:00"},
    )
    outcome = service.resend_delivery(delivery["delivery_id"])
    assert outcome["resend"] == "ignored"
    assert service.get_delivery(delivery["delivery_id"])["attempt_count"] == 2


def test_confirm_is_idempotent_by_receipt(service, dispatcher):
    add_recipient(service, REGION_A, "sms", "值班长")
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A])))
    delivery = _only_delivery(service)
    dispatcher.run_once()

    first = service.confirm_delivery(
        delivery["delivery_id"],
        {"receipt_id": "RC-1", "confirmed_at": "2026-09-21T00:10:00+08:00"},
    )
    assert first["duplicate"] is False

    again = service.confirm_delivery(
        delivery["delivery_id"],
        {"receipt_id": "RC-1", "confirmed_at": "2026-09-21T00:11:00+08:00"},
    )
    assert again["duplicate"] is True
    assert again["confirmed_at"] == "2026-09-21T00:10:00+08:00"

    other = service.confirm_delivery(
        delivery["delivery_id"],
        {"receipt_id": "RC-2", "confirmed_at": "2026-09-21T00:12:00+08:00"},
    )
    assert other["duplicate"] is True
    assert other["receipt_id"] == "RC-1"  # 保留首次确认

    assert service.get_delivery(delivery["delivery_id"])["status"] == "CONFIRMED"


def test_unconfirmed_query_groups_by_channel(service, dispatcher):
    add_recipient(service, REGION_A, "sms", "值班长")
    add_recipient(service, REGION_A, "broadcast", "广播员")
    add_recipient(service, REGION_A, "terminal", "值班员")
    add_recipient(service, REGION_B, "sms", "值班长")
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A, REGION_B])))
    dispatcher.run_once()

    pending = service.unconfirmed(REGION_A)
    assert pending["count"] == 3
    assert pending["summary"] == {"sms": 1, "broadcast": 1, "terminal": 1}

    sms_delivery = service.list_deliveries(region=REGION_A, channel="sms")["deliveries"][0]
    service.confirm_delivery(
        sms_delivery["delivery_id"],
        {"receipt_id": "RC-9", "confirmed_at": "2026-09-21T00:20:00+08:00"},
    )

    remaining = service.unconfirmed(REGION_A)
    assert remaining["count"] == 2
    assert "sms" not in remaining["summary"]
    sms_only = service.unconfirmed(REGION_A, channel="sms")
    assert sms_only["count"] == 0
    # 已发送未回执的仍属于未确认。
    statuses = {item["status"] for item in remaining["unconfirmed"]}
    assert statuses == {"SENT"}


def test_cancel_notice_is_delivered_to_scope_reduced_region(service, dispatcher):
    add_recipient(service, REGION_B, "sms", "值班长")
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A, REGION_B])))
    service.ingest_event(
        make_event(
            event_id="EVT-2",
            event_type="CORRECT",
            sequence=2,
            occurred="2026-09-20T23:40:00+08:00",
            payload=make_payload(regions=[REGION_A]),
        )
    )
    dispatcher.run_once()

    deliveries = service.list_deliveries(region=REGION_B)["deliveries"]
    assert len(deliveries) == 2
    states = {item["segment_state"] for item in deliveries}
    assert states == {"ACTIVE", "CANCELLED"}
    assert all(item["status"] == "SENT" for item in deliveries)
