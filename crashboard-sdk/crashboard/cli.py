import os
import re
import sys
import shutil
import click
import yaml
import requests
import subprocess
from crashboard.dashboard import write_dashboard_json

# Paths to assets shipped inside the installed package.
PKG_DIR = os.path.dirname(__file__)
TEMPLATE_DIR = os.path.join(PKG_DIR, "templates")
INFRA_DIR = os.path.join(PKG_DIR, "infra")

CRASHBOARD_DIR = ".crashboard"
OVERLAY_FILE = os.path.join(CRASHBOARD_DIR, "docker-compose.crashboard.yml")

# Chaos engine is published on this host port by the overlay compose.
CHAOS_ENGINE_PORT = 8080

# Infra/observability service names that should never be treated as app scrape targets.
RESERVED_SERVICES = {
    "postgres", "prometheus", "grafana", "loki", "nginx",
    "chaos-engine", "metric-collector", "dashboard-backend", "dashboard-frontend",
}


def get_job_name(service_name):
    # Strip trailing numbers (e.g. user-service-1 -> user-service) so replicas
    # of the same service collapse into one Prometheus job.
    return re.sub(r'[-_]\d+$', '', service_name)


def discover_services(compose_file):
    """Parse a docker-compose file and split it into app services + infra flags.

    Returns (app_services, has_redis, has_kafka).
    """
    has_redis = False
    has_kafka = False
    app_services = []

    if not os.path.exists(compose_file):
        click.echo("[WARN] No docker-compose file found. Generating an infra-only stack;")
        click.echo("       add scrape targets later by re-running `crashboard init` from your repo root.")
        return [], False, False

    click.echo(f"Found compose file: {compose_file}. Parsing services...")
    try:
        with open(compose_file, "r") as f:
            compose_data = yaml.safe_load(f) or {}
    except Exception as e:
        click.echo(f"[WARN] Error parsing {compose_file}: {e}. Continuing with infra-only stack.")
        return [], False, False

    for svc_name in (compose_data.get("services") or {}):
        lname = svc_name.lower()
        if "redis" in lname:
            has_redis = True
        elif "kafka" in lname or "zookeeper" in lname:
            has_kafka = True
        elif lname in RESERVED_SERVICES:
            continue
        else:
            app_services.append(svc_name)

    click.echo(f"Detected application services: {', '.join(app_services) or '(none)'}")
    click.echo(f"Detected infrastructure: Redis={has_redis}, Kafka={has_kafka}")
    return app_services, has_redis, has_kafka


def scaffold_dirs():
    """Create the .crashboard/ output tree and return the paths we write into."""
    infra_dir = os.path.join(CRASHBOARD_DIR, "infrastructure")
    paths = {
        "loki": os.path.join(infra_dir, "loki"),
        "datasources": os.path.join(infra_dir, "grafana", "provisioning", "datasources"),
        "prov_dashboards": os.path.join(infra_dir, "grafana", "provisioning", "dashboards"),
        "dashboards": os.path.join(infra_dir, "grafana", "dashboards"),
    }
    for d in paths.values():
        os.makedirs(d, exist_ok=True)
    return paths


def bundle_infra():
    """Copy the bundled chaos-engine + metric-collector source into .crashboard/.

    This is what makes the SDK droppable into any repo: the overlay compose builds
    these local copies rather than referencing source that only exists in the demo.
    """
    mapping = {
        os.path.join(INFRA_DIR, "chaos-engine"): os.path.join(CRASHBOARD_DIR, "engine"),
        os.path.join(INFRA_DIR, "metric-collector"): os.path.join(CRASHBOARD_DIR, "collector"),
    }
    for src, dst in mapping.items():
        if not os.path.isdir(src):
            raise click.ClickException(
                f"Bundled infra missing at {src}. Reinstall crashboard-sdk."
            )
        shutil.copytree(src, dst, dirs_exist_ok=True)


def generate_prometheus_config(app_services):
    """Build a Prometheus scrape config for the discovered app services + infra."""
    job_groups = {}
    for svc in app_services:
        job_groups.setdefault(get_job_name(svc), []).append(svc)

    scrape_configs = [
        {"job_name": "prometheus", "static_configs": [{"targets": ["localhost:9090"]}]}
    ]
    for job, targets in job_groups.items():
        scrape_configs.append({
            "job_name": job,
            "metrics_path": "/metrics",
            "static_configs": [{"targets": [f"{t}:8000" for t in targets]}],
        })
    # Always scrape the bundled backends.
    for job in ("chaos-engine", "metric-collector"):
        scrape_configs.append({
            "job_name": job,
            "metrics_path": "/metrics",
            "static_configs": [{"targets": [f"{job}:8000"]}],
        })

    return {
        "global": {"scrape_interval": "1s", "evaluation_interval": "1s"},
        "scrape_configs": scrape_configs,
    }


@click.group()
def main():
    """CrashBoard - Chaos-as-a-Service SDK."""
    pass


@main.command()
@click.option("--compose-file", default="docker-compose.yml",
              help="Path to your primary docker-compose.yml file")
def init(compose_file):
    """Scan your project and generate the chaos + observability overlay in .crashboard/."""
    click.echo("[INFO] Initializing CrashBoard...")

    app_services, has_redis, has_kafka = discover_services(compose_file)

    paths = scaffold_dirs()

    # Static Grafana/Loki provisioning templates.
    shutil.copy(os.path.join(TEMPLATE_DIR, "loki-config.yaml"),
                os.path.join(paths["loki"], "loki-config.yaml"))
    shutil.copy(os.path.join(TEMPLATE_DIR, "datasource.yaml"),
                os.path.join(paths["datasources"], "datasource.yaml"))
    shutil.copy(os.path.join(TEMPLATE_DIR, "dashboard.yaml"),
                os.path.join(paths["prov_dashboards"], "dashboard.yaml"))

    # Bundled infra source + the overlay compose that builds it (copied verbatim
    # so its comments survive; it no longer needs per-repo service injection).
    bundle_infra()
    shutil.copy(os.path.join(TEMPLATE_DIR, "docker-compose.tmpl.yml"), OVERLAY_FILE)

    # Dynamic Prometheus scrape config.
    with open(os.path.join(CRASHBOARD_DIR, "prometheus.yml"), "w") as f:
        yaml.safe_dump(generate_prometheus_config(app_services), f, default_flow_style=False)

    # Programmatically generated Grafana chaos dashboard.
    write_dashboard_json(
        app_services, has_redis=has_redis, has_kafka=has_kafka,
        output_path=os.path.join(paths["dashboards"], "crashboard.json"),
    )

    click.echo("[SUCCESS] CrashBoard overlay generated in .crashboard/")
    click.echo("   Next: add `crashboard.init(app, \"<service-name>\")` to your services, then run `crashboard up`.")


def _compose_command(*extra):
    cmd = ["docker", "compose"]
    if os.path.exists("docker-compose.yml"):
        cmd.extend(["-f", "docker-compose.yml"])
    cmd.extend(["-f", OVERLAY_FILE])
    cmd.extend(extra)
    return cmd


@main.command()
def up():
    """Launch the chaos + observability stack (merged with your compose if present)."""
    if not os.path.exists(OVERLAY_FILE):
        raise click.ClickException("No .crashboard overlay found. Run `crashboard init` first.")

    click.echo("[INFO] Starting CrashBoard stack...")
    cmd = _compose_command("up", "-d", "--build")
    click.echo(f"Executing: {' '.join(cmd)}")
    res = subprocess.run(cmd)
    if res.returncode == 0:
        click.echo("\n[SUCCESS] CrashBoard is online!")
        click.echo("   - Grafana (chaos dashboard): http://localhost:3000  (anonymous admin)")
        click.echo("   - Prometheus:                http://localhost:9090")
        click.echo("   - Loki:                      http://localhost:3100")
        click.echo(f"   - Chaos Engine API:          http://localhost:{CHAOS_ENGINE_PORT}")
    else:
        click.echo("[ERROR] Failed to start container stack. Check Docker logs.", err=True)
        sys.exit(res.returncode)


@main.command()
def down():
    """Shut down the chaos + observability stack."""
    if not os.path.exists(OVERLAY_FILE):
        raise click.ClickException("No .crashboard overlay found. Run `crashboard init` first.")
    click.echo("[INFO] Stopping CrashBoard stack...")
    subprocess.run(_compose_command("down"))
    click.echo("[SUCCESS] Stack stopped.")


@main.command()
@click.argument("scenario")
@click.option("--target", default=None, help="Name of target container (e.g. user-service-1)")
@click.option("--duration", default=30, type=int, help="Duration of scenario in seconds")
def trigger(scenario, target, duration):
    """Inject a chaos scenario: kill_container, cpu_spike, network_delay, kafka_flood, db_corrupt."""
    click.echo(f"[INFO] Injecting chaos: scenario={scenario}, target={target}, duration={duration}s...")

    payload = {"scenario": scenario, "target": target, "duration": duration}
    url = f"http://localhost:{CHAOS_ENGINE_PORT}/chaos/trigger"
    try:
        r = requests.post(url, json=payload, timeout=5.0)
        if r.status_code == 200:
            click.echo(f"[SUCCESS] {r.json()}")
        else:
            click.echo(f"[ERROR] Chaos engine returned {r.status_code}: {r.text}", err=True)
            sys.exit(1)
    except Exception:
        click.echo(
            "[ERROR] Could not reach the Chaos Engine API at "
            f"{url}. Is the CrashBoard stack running (`crashboard up`)?", err=True
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
