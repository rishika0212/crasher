"""Integration tests for user-service via FastAPI TestClient.

Postgres is replaced with in-memory SQLite and Redis with a fake, so the full
request path (validation, persistence, cache read/write, eviction) runs without
external infrastructure.
"""

import pytest
from fastapi.testclient import TestClient

from _harness import import_isolated, FakeRedis

# Imported once at module load to avoid SQLModel metadata table re-definition.
user_main = import_isolated("services/user-service", module="main")


@pytest.fixture
def client(sqlite_session_factory):
    engine, factory = sqlite_session_factory
    fake_redis = FakeRedis()
    user_main.redis_client = fake_redis
    user_main.app.dependency_overrides[user_main.get_session] = factory
    yield TestClient(user_main.app), fake_redis
    user_main.app.dependency_overrides.clear()


def _new_user(username="alice"):
    return {
        "username": username,
        "email": f"{username}@example.com",
        "full_name": username.title(),
    }


def test_create_user(client):
    tc, _ = client
    resp = tc.post("/users", json=_new_user())
    assert resp.status_code == 201
    body = resp.json()
    assert body["id"] is not None
    assert body["username"] == "alice"
    assert body["is_active"] is True


def test_create_caches_user(client):
    tc, fake_redis = client
    body = tc.post("/users", json=_new_user("bob")).json()
    assert f"user:{body['id']}" in fake_redis.store


def test_get_user_cache_hit_skips_db(client):
    tc, fake_redis = client
    created = tc.post("/users", json=_new_user("carol")).json()
    uid = created["id"]
    # Poison the cache so a hit returns a value distinct from the DB row.
    fake_redis.store[f"user:{uid}"] = '{"id": %d, "username": "cached", "email": "c@e.com", "full_name": "Cached", "is_active": true}' % uid
    resp = tc.get(f"/users/{uid}")
    assert resp.status_code == 200
    assert resp.json()["username"] == "cached", "served value should come from cache"


def test_get_user_cache_miss_falls_back_to_db(client):
    tc, fake_redis = client
    created = tc.post("/users", json=_new_user("dave")).json()
    uid = created["id"]
    fake_redis.store.clear()  # force a miss
    resp = tc.get(f"/users/{uid}")
    assert resp.status_code == 200
    assert resp.json()["username"] == "dave"
    # Cache should be re-populated after the miss.
    assert f"user:{uid}" in fake_redis.store


def test_get_user_survives_redis_outage(client):
    tc, fake_redis = client
    created = tc.post("/users", json=_new_user("erin")).json()
    uid = created["id"]
    fake_redis.up = False  # every redis call now raises
    resp = tc.get(f"/users/{uid}")
    assert resp.status_code == 200, "user lookup must degrade gracefully when Redis is down"
    assert resp.json()["username"] == "erin"


def test_get_missing_user_404(client):
    tc, _ = client
    assert tc.get("/users/99999").status_code == 404


def test_list_users(client):
    tc, _ = client
    tc.post("/users", json=_new_user("fred"))
    tc.post("/users", json=_new_user("gina"))
    resp = tc.get("/users")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


def test_delete_user_evicts_cache(client):
    tc, fake_redis = client
    created = tc.post("/users", json=_new_user("hank")).json()
    uid = created["id"]
    assert f"user:{uid}" in fake_redis.store
    resp = tc.delete(f"/users/{uid}")
    assert resp.status_code == 204
    assert f"user:{uid}" not in fake_redis.store
    assert tc.get(f"/users/{uid}").status_code == 404


def test_delete_missing_user_404(client):
    tc, _ = client
    assert tc.delete("/users/99999").status_code == 404
