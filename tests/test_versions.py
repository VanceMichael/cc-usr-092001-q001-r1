"""版本链与生效片段：订正拆分、迟到不覆盖、跨午夜时效。"""

import pytest

from conftest import REGION_A, REGION_B, REGION_C, add_recipient, make_event, make_payload
from src.domain import ServiceError, parse_moment


def _state(service, region, at):
    return service.region_state(region, parse_moment(at, "at"))["alerts"]


def test_correction_splits_regions_into_traceable_segments(service):
    for region in (REGION_A, REGION_B, REGION_C):
        add_recipient(service, region, "sms", "值班长")
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A, REGION_B])))
    correction = make_event(
        event_id="EVT-2",
        event_type="CORRECT",
        sequence=2,
        occurred="2026-09-20T23:40:00+08:00",
        payload=make_payload(alert_level="暴雨橙色预警", regions=[REGION_B, REGION_C]),
    )
    result = service.ingest_event(correction)

    assert result["ingest_status"] == "APPLIED"
    assert result["version_seq"] == 2
    # B 继续、C 新增、A 被撤销，共 3 个片段。
    assert result["segments_created"] == 3

    at = "2026-09-21T00:30:00+08:00"
    dropped = _state(service, REGION_A, at)
    assert dropped[0]["state"] == "CANCELLED"
    assert dropped[0]["cancel_reason"] == "SCOPE_REDUCED"
    assert dropped[0]["formed_by_event_id"] == "EVT-2"

    continued = _state(service, REGION_B, at)
    assert continued[0]["state"] == "ACTIVE"
    assert continued[0]["version_seq"] == 2
    assert continued[0]["alert_level"] == "暴雨橙色预警"
    assert continued[0]["formed_by_event_id"] == "EVT-2"

    added = _state(service, REGION_C, at)
    assert added[0]["state"] == "ACTIVE"
    assert added[0]["version_seq"] == 2

    # 每个片段都生成送达：A 收到解除通知，B/C 收到新版内容。
    assert len(service.list_deliveries(region=REGION_A)["deliveries"]) == 2
    assert len(service.list_deliveries(region=REGION_B)["deliveries"]) == 2
    assert len(service.list_deliveries(region=REGION_C)["deliveries"]) == 1


def test_late_sequence_does_not_override_newer_decision(service):
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A])))
    service.ingest_event(
        make_event(
            event_id="EVT-3",
            event_type="CORRECT",
            sequence=3,
            occurred="2026-09-20T23:50:00+08:00",
            payload=make_payload(alert_level="暴雨橙色预警", regions=[REGION_A]),
        )
    )
    late = service.ingest_event(
        make_event(
            event_id="EVT-2",
            event_type="CORRECT",
            sequence=2,
            occurred="2026-09-20T23:40:00+08:00",
            payload=make_payload(alert_level="暴雨黄色预警", regions=[REGION_A, REGION_C]),
        )
    )

    assert late["ingest_status"] == "LATE"
    assert late["version_id"] is None
    state = _state(service, REGION_A, "2026-09-21T00:30:00+08:00")
    assert state[0]["alert_level"] == "暴雨橙色预警"
    assert state[0]["version_seq"] == 2
    # 迟到消息声称覆盖的 C 区不应出现任何片段。
    assert _state(service, REGION_C, "2026-09-21T00:30:00+08:00") == []
    # 迟到事件本身留痕可查。
    stored = service.get_event("EVT-2")
    assert stored["ingest_status"] == "LATE"


def test_older_occurred_at_does_not_override(service):
    service.ingest_event(make_event(occurred="2026-09-20T23:30:00+08:00",
                                    payload=make_payload(regions=[REGION_A])))
    late = service.ingest_event(
        make_event(
            event_id="EVT-2",
            event_type="CORRECT",
            sequence=2,
            occurred="2026-09-20T23:10:00+08:00",
            payload=make_payload(alert_level="暴雨黄色预警", regions=[REGION_A]),
        )
    )

    assert late["ingest_status"] == "LATE"
    state = _state(service, REGION_A, "2026-09-21T00:30:00+08:00")
    assert state[0]["alert_level"] == "暴雨红色预警"


def test_cancel_closes_regions_and_blocks_correction(service):
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A, REGION_B])))
    cancel = service.ingest_event(
        make_event(
            event_id="EVT-2",
            event_type="CANCEL",
            sequence=2,
            occurred="2026-09-21T01:00:00+08:00",
            payload={"regions": [REGION_A]},
        )
    )
    assert cancel["ingest_status"] == "APPLIED"

    at = "2026-09-21T01:30:00+08:00"
    assert _state(service, REGION_A, at)[0]["state"] == "CANCELLED"
    assert _state(service, REGION_A, at)[0]["cancel_reason"] == "EXPLICIT"
    assert _state(service, REGION_B, at)[0]["state"] == "ACTIVE"

    with pytest.raises(ServiceError) as caught:
        service.ingest_event(
            make_event(
                event_id="EVT-3",
                event_type="CORRECT",
                sequence=3,
                occurred="2026-09-21T01:10:00+08:00",
                payload=make_payload(regions=[REGION_A]),
            )
        )
    assert caught.value.status == 409
    assert caught.value.code == "subject_closed"

    # 空 regions 的解除作用于全部仍在生效的区域。
    service.ingest_event(
        make_event(
            event_id="EVT-4",
            event_type="CANCEL",
            sequence=4,
            occurred="2026-09-21T02:00:00+08:00",
            payload={},
        )
    )
    assert _state(service, REGION_B, "2026-09-21T02:30:00+08:00")[0]["state"] == "CANCELLED"


def test_late_correction_after_cancel_does_not_revive(service):
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A])))
    service.ingest_event(
        make_event(
            event_id="EVT-2",
            event_type="CANCEL",
            sequence=2,
            occurred="2026-09-21T01:00:00+08:00",
            payload={"regions": [REGION_A]},
        )
    )
    late = service.ingest_event(
        make_event(
            event_id="EVT-3",
            event_type="CORRECT",
            sequence=3,
            occurred="2026-09-20T23:55:00+08:00",
            payload=make_payload(regions=[REGION_A]),
        )
    )

    assert late["ingest_status"] == "LATE"
    assert _state(service, REGION_A, "2026-09-21T01:30:00+08:00")[0]["state"] == "CANCELLED"


def test_cross_midnight_validity_uses_original_timezone(service):
    service.ingest_event(
        make_event(
            occurred="2026-09-20T22:30:00+08:00",
            payload=make_payload(
                regions=[REGION_A],
                valid_from="2026-09-20T23:00:00+08:00",
                valid_until="2026-09-21T06:00:00+08:00",
            ),
        )
    )

    # 生效前、跨午夜生效中、到期后，全部按 +08:00 原始时区判定。
    assert _state(service, REGION_A, "2026-09-20T22:59:59+08:00")[0]["state"] == "SCHEDULED"
    assert _state(service, REGION_A, "2026-09-21T05:59:59+08:00")[0]["state"] == "ACTIVE"
    assert _state(service, REGION_A, "2026-09-21T06:00:00+08:00")[0]["state"] == "EXPIRED"
    # 同一时刻用 UTC 表达，结论一致。
    assert _state(service, REGION_A, "2026-09-20T21:59:59Z")[0]["state"] == "ACTIVE"
    assert _state(service, REGION_A, "2026-09-20T22:00:00Z")[0]["state"] == "EXPIRED"


def test_state_query_before_first_segment_is_empty(service):
    service.ingest_event(
        make_event(
            occurred="2026-09-20T22:30:00+08:00",
            payload=make_payload(regions=[REGION_A]),
        )
    )
    assert _state(service, REGION_A, "2026-09-20T22:00:00+08:00") == []
