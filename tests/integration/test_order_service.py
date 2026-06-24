"""Integration tests for order-service via FastAPI TestClient.

Postgres is replaced with in-memory SQLite and the Kafka producer with a fake,
so order creation and event publishing are verified without a broker.
"""

import json

import pytest
from fastapi.testclient import TestClient

from _harness import import_isolated, FakeProducer

order_main = import_isolated("services/order-service", module="main")


@pytest.fixture
def client(sqlite_session_factory):
    engine, factory = sqlite_session_factory
    fake_producer = FakeProducer()
    order_main.producer = fake_producer
    order_main.app.dependency_overrides[order_main.get_session] = factory
    yield TestClient(order_main.app), fake_producer
    order_main.app.dependency_overrides.clear()


def _new_order(user_id=1, product="widget"):
    return {"user_id": user_id, "product": product, "quantity": 2, "price": 9.99}


def test_create_order_persists_and_returns(client):
    tc, _ = client
    resp = tc.post("/orders", json=_new_order())
    assert resp.status_code == 201
    body = resp.json()
    assert body["id"] is not None
    assert body["product"] == "widget"
    assert body["status"] == "pending"


def test_create_order_publishes_kafka_event(client):
    tc, producer = client
    body = tc.post("/orders", json=_new_order(product="gizmo")).json()
    assert len(producer.produced) == 1
    event = producer.produced[0]
    assert event["topic"] == "order_created"
    assert event["key"] == str(body["id"])
    payload = json.loads(event["value"])
    assert payload["product"] == "gizmo"
    assert payload["order_id"] == body["id"]


def test_create_order_succeeds_even_without_producer(client):
    tc, _ = client
    order_main.producer = None  # simulate a broker that never initialized
    try:
        resp = tc.post("/orders", json=_new_order())
        assert resp.status_code == 201, "order creation must not depend on Kafka"
    finally:
        order_main.producer = FakeProducer()


def test_get_order(client):
    tc, _ = client
    created = tc.post("/orders", json=_new_order()).json()
    resp = tc.get(f"/orders/{created['id']}")
    assert resp.status_code == 200
    assert resp.json()["id"] == created["id"]


def test_get_missing_order_404(client):
    tc, _ = client
    assert tc.get("/orders/99999").status_code == 404


def test_list_orders(client):
    tc, _ = client
    tc.post("/orders", json=_new_order(product="a"))
    tc.post("/orders", json=_new_order(product="b"))
    resp = tc.get("/orders")
    assert resp.status_code == 200
    assert len(resp.json()) == 2
