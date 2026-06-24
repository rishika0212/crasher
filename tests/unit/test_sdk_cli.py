"""Unit tests for the crashboard SDK CLI helper functions."""

import textwrap

from crashboard import cli


def test_get_job_name_strips_replica_suffix():
    assert cli.get_job_name("user-service-1") == "user-service"
    assert cli.get_job_name("user-service-2") == "user-service"
    assert cli.get_job_name("order_service_3") == "order_service"


def test_get_job_name_leaves_plain_names():
    assert cli.get_job_name("order-service") == "order-service"
    assert cli.get_job_name("notif-service") == "notif-service"


def test_discover_services_missing_file(tmp_path):
    app_services, has_redis, has_kafka = cli.discover_services(str(tmp_path / "nope.yml"))
    assert app_services == []
    assert has_redis is False
    assert has_kafka is False


def test_discover_services_classifies_app_and_infra(tmp_path):
    compose = tmp_path / "docker-compose.yml"
    compose.write_text(textwrap.dedent("""
        services:
          user-service:
            image: x
          order-service:
            image: x
          redis:
            image: redis
          kafka:
            image: kafka
          prometheus:
            image: prom
    """))
    app_services, has_redis, has_kafka = cli.discover_services(str(compose))
    assert set(app_services) == {"user-service", "order-service"}
    assert has_redis is True
    assert has_kafka is True
    # Reserved infra services must never become scrape targets.
    assert "prometheus" not in app_services


def test_discover_services_handles_malformed_yaml(tmp_path):
    compose = tmp_path / "docker-compose.yml"
    compose.write_text("services: [this is : not valid")
    app_services, has_redis, has_kafka = cli.discover_services(str(compose))
    assert app_services == []


def test_generate_prometheus_config_collapses_replicas():
    cfg = cli.generate_prometheus_config(["user-service-1", "user-service-2", "order-service"])
    jobs = {j["job_name"]: j for j in cfg["scrape_configs"]}

    # Two user-service replicas collapse into one job with two targets.
    assert "user-service" in jobs
    targets = jobs["user-service"]["static_configs"][0]["targets"]
    assert set(targets) == {"user-service-1:8000", "user-service-2:8000"}

    # Bundled backends are always scraped.
    assert "chaos-engine" in jobs
    assert "metric-collector" in jobs
    # Prometheus scrapes itself.
    assert "prometheus" in jobs


def test_generate_prometheus_config_scrape_interval():
    cfg = cli.generate_prometheus_config([])
    assert cfg["global"]["scrape_interval"] == "1s"
