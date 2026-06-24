"""Integration tests for notif-service via FastAPI TestClient.

A bare TestClient is used so the startup event (which spins up the real Kafka
consumer thread) never fires; the in-memory notification log is exercised
directly.
"""

import pytest
from fastapi.testclient import TestClient

from _harness import import_isolated

notif_main = import_isolated("services/notif-service", module="main")


@pytest.fixture
def client():
    # No `with` block => startup events (Kafka consumer thread) do not run.
    tc = TestClient(notif_main.app)
    with notif_main.notifications_lock:
        notif_main.notifications_log.clear()
    yield tc
    with notif_main.notifications_lock:
        notif_main.notifications_log.clear()


def test_notifications_empty_by_default(client):
    resp = client.get("/notifications")
    assert resp.status_code == 200
    assert resp.json() == []


def test_notifications_returns_logged_events(client):
    with notif_main.notifications_lock:
        notif_main.notifications_log.append({"order_id": 1, "product": "widget"})
    resp = client.get("/notifications")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["order_id"] == 1


def test_health_unhealthy_without_consumer(client):
    # consumer_instance is None until the (unstarted) loop initializes it.
    resp = client.get("/health")
    assert resp.status_code == 503
    assert resp.json()["components"]["kafka_consumer"] == "disconnected"
