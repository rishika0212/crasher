import os
import re
import sys
import shutil
import click
import yaml
import requests
import subprocess
from crashboard.dashboard import write_dashboard_json

# Paths
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

def get_job_name(service_name):
    # Strip trailing numbers (e.g. user-service-1 -> user-service)
    return re.sub(r'[-_]\d+$', '', service_name)

@click.group()
def main():
    """CrashBoard - Chaos-as-a-Service CLI"""
    pass

@main.command()
@click.option("--compose-file", default="docker-compose.yml", help="Path to your primary docker-compose.yml file")
def init(compose_file):
    """Scan project structure and generate telemetry/dashboard configs."""
    click.echo("[INFO] Initializing CrashBoard CaaS SDK config...")
    
    # 1. Discover services from compose file
    has_redis = False
    has_kafka = False
    app_services = []
    
    if os.path.exists(compose_file):
        click.echo(f"Found primary compose file: {compose_file}. Parsing services...")
        try:
            with open(compose_file, "r") as f:
                compose_data = yaml.safe_load(f)
                
            services = compose_data.get("services", {})
            for svc_name, svc_conf in services.items():
                svc_name_lower = svc_name.lower()
                
                # Exclude storage, queue and observability services from scrape lists
                if "redis" in svc_name_lower:
                    has_redis = True
                elif "kafka" in svc_name_lower or "zookeeper" in svc_name_lower:
                    has_kafka = True
                elif svc_name_lower in ["postgres", "prometheus", "grafana", "loki", "nginx", "chaos-engine", "metric-collector", "dashboard-backend", "dashboard-frontend"]:
                    continue
                else:
                    # Treat it as a microservice
                    app_services.append(svc_name)
                    
            click.echo(f"Detected Application Services: {', '.join(app_services)}")
            click.echo(f"Detected Infrastructure Components: Redis={has_redis}, Kafka={has_kafka}")
        except Exception as e:
            click.echo(f"[WARN] Error parsing {compose_file}: {e}. Initializing with defaults.")
    else:
        click.echo("[WARN] Primary docker-compose.yml not found. Initializing with default app templates.")
        app_services = ["user-service", "order-service", "notif-service"]
        has_redis = True
        has_kafka = True

    # 2. Setup directory structure
    crashboard_dir = ".crashboard"
    infra_dir = os.path.join(crashboard_dir, "infrastructure")
    loki_dir = os.path.join(infra_dir, "loki")
    grafana_dir = os.path.join(infra_dir, "grafana")
    prov_dir = os.path.join(grafana_dir, "provisioning")
    ds_dir = os.path.join(prov_dir, "datasources")
    p_db_dir = os.path.join(prov_dir, "dashboards")
    db_dir = os.path.join(grafana_dir, "dashboards")

    for d in [loki_dir, ds_dir, p_db_dir, db_dir]:
        os.makedirs(d, exist_ok=True)

    # 3. Copy static templates
    shutil.copy(os.path.join(TEMPLATE_DIR, "loki-config.yaml"), os.path.join(loki_dir, "loki-config.yaml"))
    shutil.copy(os.path.join(TEMPLATE_DIR, "datasource.yaml"), os.path.join(ds_dir, "datasource.yaml"))
    shutil.copy(os.path.join(TEMPLATE_DIR, "dashboard.yaml"), os.path.join(p_db_dir, "dashboard.yaml"))

    # Load compose template and overlay service volume mounts + entrypoints
    with open(os.path.join(TEMPLATE_DIR, "docker-compose.tmpl.yml"), "r") as f:
        compose_template = yaml.safe_load(f) or {}
        
    if "services" not in compose_template:
        compose_template["services"] = {}
        
    for svc in app_services:
        compose_template["services"][svc] = {
            "volumes": [
                "../crashboard-sdk:/crashboard-sdk"
            ],
            "entrypoint": [
                "sh", "-c",
                "pip install -e /crashboard-sdk && uvicorn main:app --host 0.0.0.0 --port 8000"
            ]
        }
        
    with open(os.path.join(crashboard_dir, "docker-compose.crashboard.yml"), "w") as f:
        yaml.safe_dump(compose_template, f, default_flow_style=False)

    # 4. Generate prometheus.yml scrape config dynamically
    job_groups = {}
    for svc in app_services:
        job = get_job_name(svc)
        if job not in job_groups:
            job_groups[job] = []
        job_groups[job].append(svc)

    prom_config = {
        "global": {
            "scrape_interval": "1s",
            "evaluation_interval": "1s"
        },
        "scrape_configs": [
            {
                "job_name": "prometheus",
                "static_configs": [{"targets": ["localhost:9090"]}]
            }
        ]
    }

    for job, targets in job_groups.items():
        prom_config["scrape_configs"].append({
            "job_name": job,
            "metrics_path": "/metrics",
            "static_configs": [{"targets": [f"{t}:8000" for t in targets]}]
        })

    # Add default backend integrations
    prom_config["scrape_configs"].append({
        "job_name": "chaos-engine",
        "metrics_path": "/metrics",
        "static_configs": [{"targets": ["chaos-engine:8000"]}]
    })
    prom_config["scrape_configs"].append({
        "job_name": "metric-collector",
        "metrics_path": "/metrics",
        "static_configs": [{"targets": ["metric-collector:8000"]}]
    })

    with open(os.path.join(crashboard_dir, "prometheus.yml"), "w") as f:
        yaml.safe_dump(prom_config, f, default_flow_style=False)

    # 5. Programmatically write Grafana Chaos Dashboard JSON
    dashboard_path = os.path.join(db_dir, "crashboard.json")
    write_dashboard_json(app_services, has_redis=has_redis, has_kafka=has_kafka, output_path=dashboard_path)

    click.echo("[SUCCESS] CrashBoard configuration and infrastructure sidecar templates successfully generated!")
    click.echo(f"   Configs stored in: {crashboard_dir}/")

@main.command()
def up():
    """Launch the Chaos Observability Infrastructure overlay."""
    click.echo("[INFO] Starting CrashBoard Sidecar Infrastructure...")
    
    # We overlay the crashboard docker-compose onto the primary docker-compose
    cmd = ["docker", "compose"]
    if os.path.exists("docker-compose.yml"):
        cmd.extend(["-f", "docker-compose.yml"])
    cmd.extend(["-f", ".crashboard/docker-compose.crashboard.yml", "up", "-d", "--build"])

    click.echo(f"Executing: {' '.join(cmd)}")
    res = subprocess.run(cmd)
    if res.returncode == 0:
        click.echo("\n[SUCCESS] CrashBoard Chaos & Observability Control Room is Online!")
        click.echo("   - Grafana Dashboard:     http://localhost:3000  (Anonymous Admin mode)")
        click.echo("   - Prometheus Target:     http://localhost:9090")
        click.echo("   - Loki Log Endpoint:      http://localhost:3100")
        click.echo("   - CrashBoard UI Control: http://localhost       (via Nginx Gateway)")
    else:
        click.echo("[ERROR] Failed to start container stack. Check Docker logs.", err=True)
        sys.exit(res.returncode)

@main.command()
def down():
    """Shutdown the Chaos Observability Infrastructure."""
    click.echo("[INFO] Stopping CrashBoard Sidecar Infrastructure...")
    
    cmd = ["docker", "compose"]
    if os.path.exists("docker-compose.yml"):
        cmd.extend(["-f", "docker-compose.yml"])
    cmd.extend(["-f", ".crashboard/docker-compose.crashboard.yml", "down"])

    subprocess.run(cmd)
    click.echo("[SUCCESS] Infrastructure stopped.")

@main.command()
@click.argument("scenario")
@click.option("--target", default=None, help="Name of target container (e.g. user-service-1)")
@click.option("--duration", default=30, type=int, help="Duration of scenario in seconds")
def trigger(scenario, target, duration):
    """Trigger a chaos scenario (e.g. kill_container, cpu_spike, network_delay, kafka_flood, db_corrupt)"""
    click.echo(f"[INFO] Injecting chaos: scenario={scenario}, target={target}, duration={duration}s...")
    
    # Send POST request to chaos engine
    payload = {
        "scenario": scenario,
        "target": target,
        "duration": duration
    }
    
    # Try through proxy first, then direct
    urls = [
        "http://localhost/api/chaos/trigger",
        "http://localhost:8000/chaos/trigger"
    ]
    
    success = False
    for url in urls:
        try:
            r = requests.post(url, json=payload, timeout=5.0)
            if r.status_code == 200:
                click.echo(f"[SUCCESS] Success: {r.json()}")
                success = True
                break
        except Exception:
            pass
            
    if not success:
        click.echo("[ERROR] Could not connect to Chaos Engine API. Make sure CrashBoard stack is running.", err=True)

if __name__ == "__main__":
    main()
