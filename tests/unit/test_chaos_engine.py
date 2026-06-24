"""Unit tests for the chaos-engine fault routing and HTTP API.

Docker is mocked so the scenarios exercise routing/validation/state logic
without touching a real Docker daemon.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from _harness import import_isolated


@pytest.fixture
def chaos():
    """Import the chaos-engine app with a mocked Docker client."""
    mod = import_isolated("chaos-engine", module="main")

    fake_client = MagicMock(name="docker_client")
    # exec_run must report success so network_delay does not raise.
    container = MagicMock(name="container")
    container.exec_run.return_value = SimpleNamespace(exit_code=0, output=b"")
    container.status = "running"
    fake_client.containers.get.return_value = container

    mod.docker_client = fake_client
    # Reset shared in-memory state between tests.
    with mod.scenarios_lock:
        mod.active_scenarios.clear()
    return mod, fake_client, container


def test_kill_container_requires_target(chaos):
    mod, _, _ = chaos
    with pytest.raises(HTTPException) as exc:
        mod.trigger_chaos("kill_container", target=None)
    assert exc.value.status_code == 400


def test_kill_container_invokes_docker_kill(chaos):
    mod, _, container = chaos
    mod.trigger_chaos("kill_container", target="order-service")
    container.kill.assert_called_once()


def test_cpu_spike_records_active_scenario(chaos):
    mod, _, container = chaos
    mod.trigger_chaos("cpu_spike", target="order-service", duration=30)
    container.exec_run.assert_called_once()
    assert "cpu_spike" in mod.active_scenarios["order-service"]


def test_network_delay_applies_and_records(chaos):
    mod, _, container = chaos
    # Long duration so the self-heal thread does not remove the record mid-assert.
    mod.trigger_chaos("network_delay", target="user-service-1", duration=60)
    # tc qdisc add must have been issued.
    issued = [c.args[0] for c in container.exec_run.call_args_list]
    assert any("netem delay 500ms" in cmd for cmd in issued)
    assert "network_delay" in mod.active_scenarios["user-service-1"]


def test_network_delay_raises_on_tc_failure(chaos):
    mod, _, container = chaos
    container.exec_run.return_value = SimpleNamespace(exit_code=2, output=b"no NET_ADMIN")
    with pytest.raises(HTTPException) as exc:
        mod.trigger_chaos("network_delay", target="user-service-1", duration=0)
    assert exc.value.status_code == 500


def test_db_corrupt_pauses_postgres(chaos):
    mod, _, container = chaos
    mod.trigger_chaos("db_corrupt", duration=60)
    container.pause.assert_called_once()
    assert "db_corrupt" in mod.active_scenarios["postgres"]


def test_unknown_scenario_rejected(chaos):
    mod, _, _ = chaos
    with pytest.raises(HTTPException) as exc:
        mod.trigger_chaos("meltdown")
    assert exc.value.status_code == 400


def test_get_container_404_when_missing(chaos):
    mod, fake_client, _ = chaos
    import docker

    fake_client.containers.get.side_effect = docker.errors.NotFound("nope")
    with pytest.raises(HTTPException) as exc:
        mod.get_container("ghost")
    assert exc.value.status_code == 404


def test_get_container_503_when_docker_unavailable(chaos):
    mod, _, _ = chaos
    mod.docker_client = None
    with pytest.raises(HTTPException) as exc:
        mod.get_container("anything")
    assert exc.value.status_code == 503


# --- HTTP API tests -------------------------------------------------------

@pytest.fixture
def client(chaos):
    mod, _, _ = chaos
    # No lifespan/startup events on this app, so a bare TestClient is fine.
    return TestClient(mod.app), mod


def test_api_trigger_endpoint(client):
    tc, _ = client
    resp = tc.post("/chaos/trigger", json={"scenario": "kill_container", "target": "order-service"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "triggered"


def test_api_active_filters_expired(client):
    tc, mod = client
    # Inject one expired and one live record directly.
    import time

    with mod.scenarios_lock:
        mod.active_scenarios["svc"] = {
            "cpu_spike": {"expires_at": time.time() - 5},
            "network_delay": {"expires_at": time.time() + 60},
        }
    body = tc.get("/chaos/active").json()
    assert "cpu_spike" not in body.get("svc", {})
    assert "network_delay" in body["svc"]


def test_api_schedule_rejects_short_interval(client):
    tc, _ = client
    resp = tc.post("/chaos/schedule", json={"scenario": "cpu_spike", "target": "a", "interval": 1})
    assert resp.status_code == 400


def test_api_schedule_accepts_valid_interval(client):
    tc, mod = client
    resp = tc.post("/chaos/schedule", json={"scenario": "cpu_spike", "target": "a", "interval": 10})
    assert resp.status_code == 200
    mod.scheduler.clear_jobs()


def test_health_reports_docker_state(client):
    tc, _ = client
    body = tc.get("/health").json()
    assert body["service"] == "chaos-engine"
    assert body["docker_socket"] in {"connected", "disconnected"}
