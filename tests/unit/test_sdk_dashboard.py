"""Unit tests for the crashboard SDK Grafana dashboard generator."""

import json

from crashboard import dashboard


def _titles(db):
    return {p["title"] for p in db["panels"]}


def test_core_panels_always_present():
    db = dashboard.generate_dashboard_dict([], has_redis=False, has_kafka=False)
    titles = _titles(db)
    assert "HTTP Request Latency (ms)" in titles
    assert "HTTP Error Rates (%)" in titles
    assert "Container CPU Usage (%)" in titles
    assert "Container Statuses (Online/Offline)" in titles


def test_redis_panel_is_conditional():
    with_redis = dashboard.generate_dashboard_dict([], has_redis=True, has_kafka=False)
    without_redis = dashboard.generate_dashboard_dict([], has_redis=False, has_kafka=False)
    assert "Redis Cache Hit Ratio" in _titles(with_redis)
    assert "Redis Cache Hit Ratio" not in _titles(without_redis)


def test_kafka_panel_is_conditional():
    with_kafka = dashboard.generate_dashboard_dict([], has_redis=False, has_kafka=True)
    without_kafka = dashboard.generate_dashboard_dict([], has_redis=False, has_kafka=False)
    assert "Kafka Consumer Lag" in _titles(with_kafka)
    assert "Kafka Consumer Lag" not in _titles(without_kafka)


def test_panel_ids_are_unique():
    db = dashboard.generate_dashboard_dict([], has_redis=True, has_kafka=True)
    ids = [p["id"] for p in db["panels"]]
    assert len(ids) == len(set(ids)), "duplicate panel ids would collide in Grafana"


def test_dashboard_has_stable_uid_and_tags():
    db = dashboard.generate_dashboard_dict([], has_redis=True, has_kafka=True)
    assert db["uid"] == "crashboard-chaos"
    assert "chaos" in db["tags"]


def test_write_dashboard_json_emits_valid_file(tmp_path):
    out = tmp_path / "nested" / "crashboard.json"
    returned = dashboard.write_dashboard_json(
        ["user-service"], has_redis=True, has_kafka=True, output_path=str(out)
    )
    assert out.exists()
    on_disk = json.loads(out.read_text())
    # Returned dict and the serialized file must agree.
    assert on_disk["uid"] == returned["uid"]
    assert len(on_disk["panels"]) == len(returned["panels"])
