"""重启恢复与历史不可变约束。"""

import sqlite3

import pytest

from conftest import REGION_A, add_recipient, make_event, make_payload
from src.dispatch import Dispatcher, ScriptedAdapter
from src.service import AlertService
from src.store import Store


def test_restart_resumes_pending_dispatch(tmp_path):
    path = tmp_path / "restart.sqlite3"
    store1 = Store(path)
    service1 = AlertService(store1)
    add_recipient(service1, REGION_A, "sms", "值班长")
    add_recipient(service1, REGION_A, "terminal", "值班员")
    service1.ingest_event(make_event(payload=make_payload(regions=[REGION_A])))
    assert service1.list_deliveries(status="PENDING")["count"] == 2
    store1.close()

    # 模拟服务重启：新连接、新分发器，未完成的派发继续进行。
    store2 = Store(path)
    adapters = {channel: ScriptedAdapter() for channel in ("sms", "terminal")}
    dispatcher = Dispatcher(store2, adapters, base_backoff_seconds=0)
    dispatcher.recover_inflight()
    assert dispatcher.run_once()["sent"] == 2

    service2 = AlertService(store2)
    assert service2.list_deliveries(status="SENT")["count"] == 2
    assert len(adapters["sms"].sent) == 1
    assert len(adapters["terminal"].sent) == 1
    store2.close()


def test_inflight_sending_is_recovered_on_restart(tmp_path):
    path = tmp_path / "inflight.sqlite3"
    store1 = Store(path)
    service1 = AlertService(store1)
    add_recipient(service1, REGION_A, "sms", "值班长")
    service1.ingest_event(make_event(payload=make_payload(regions=[REGION_A])))
    # 模拟崩溃：记录停留在 SENDING。
    with store1.transaction() as conn:
        conn.execute("UPDATE deliveries SET status='SENDING'")
    store1.close()

    store2 = Store(path)
    dispatcher = Dispatcher(store2, {"sms": ScriptedAdapter()}, base_backoff_seconds=0)
    assert dispatcher.recover_inflight() == 1
    assert dispatcher.run_once()["sent"] == 1
    store2.close()


def test_history_tables_reject_update_and_delete(service, store):
    add_recipient(service, REGION_A, "sms", "值班长")
    service.ingest_event(make_event(payload=make_payload(regions=[REGION_A])))

    for statement in (
        "UPDATE events SET ingest_status='LATE'",
        "DELETE FROM events",
        "UPDATE alert_versions SET alert_level='暴雨蓝色预警'",
        "DELETE FROM alert_versions",
        "UPDATE region_segments SET state='CANCELLED'",
        "DELETE FROM region_segments",
    ):
        with pytest.raises(sqlite3.IntegrityError):
            with store.transaction() as conn:
                conn.execute(statement)

    # 历史版本内容保持原样。
    versions = service.subject_versions("ALERT-2026-001")["versions"]
    assert versions[0]["alert_level"] == "暴雨红色预警"
    event = service.get_event("EVT-1")
    assert event["ingest_status"] == "APPLIED"
