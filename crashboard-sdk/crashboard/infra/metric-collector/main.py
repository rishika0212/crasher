import os
import time
import logging
import threading
from fastapi import FastAPI, Response
from prometheus_client import Gauge, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
import docker

# Logging
logger = logging.getLogger("metric-collector")
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Container Metric Collector",
    description="Exposes Docker container metrics to Prometheus",
    version="1.0.0"
)

# Custom isolated Prometheus registry
registry = CollectorRegistry()
cpu_gauge = Gauge("container_cpu_usage_percent", "CPU usage percent of the container", ["container_name"], registry=registry)
mem_gauge = Gauge("container_memory_usage_bytes", "Memory usage in bytes of the container", ["container_name"], registry=registry)
limit_gauge = Gauge("container_memory_limit_bytes", "Memory limit in bytes of the container", ["container_name"], registry=registry)
status_gauge = Gauge("container_status", "Container running status (1 = running, 0 = stopped)", ["container_name"], registry=registry)

metrics_cache = {}
metrics_lock = threading.Lock()

def calculate_cpu_percent(stats):
    """Calculates CPU usage percentage from Docker stats JSON."""
    try:
        cpu_stats = stats.get('cpu_stats', {})
        precpu_stats = stats.get('precpu_stats', {})
        
        cpu_usage = cpu_stats.get('cpu_usage', {}).get('total_usage', 0)
        precpu_usage = precpu_stats.get('cpu_usage', {}).get('total_usage', 0)
        
        system_cpu = cpu_stats.get('system_cpu_usage', 0)
        system_precpu = precpu_stats.get('system_cpu_usage', 0)
        
        cpu_delta = cpu_usage - precpu_usage
        system_delta = system_cpu - system_precpu
        
        online_cpus = cpu_stats.get('online_cpus', 1)
        
        if system_delta > 0.0 and cpu_delta > 0.0:
            return (cpu_delta / system_delta) * online_cpus * 100.0
    except Exception as e:
        logger.warning(f"Error calculating CPU percent: {e}")
    return 0.0

def stats_collector_thread():
    logger.info("Docker stats collector thread started.")
    client = None
    
    # Establish connection with Docker socket
    while not client:
        try:
            client = docker.from_env()
            client.ping()
        except Exception as e:
            logger.error(f"Failed to connect to Docker socket: {e}. Retrying in 3s...")
            time.sleep(3)
            
    while True:
        try:
            containers = client.containers.list(all=True)
            new_cache = {}
            for container in containers:
                name = container.name
                # Ignore random containers that are not part of our compose system if needed,
                # but tracking all is simple and generic.
                status = container.status
                is_running = 1 if status == "running" else 0
                
                cpu_pct = 0.0
                mem_bytes = 0.0
                mem_limit = 0.0
                
                if is_running:
                    try:
                        stats = container.stats(stream=False)
                        cpu_pct = calculate_cpu_percent(stats)
                        mem_bytes = stats.get('memory_stats', {}).get('usage', 0.0)
                        mem_limit = stats.get('memory_stats', {}).get('limit', 0.0)
                    except Exception as stats_err:
                        logger.debug(f"Could not read stats for container {name}: {stats_err}")
                
                new_cache[name] = {
                    "running": is_running,
                    "cpu_pct": cpu_pct,
                    "mem_bytes": mem_bytes,
                    "mem_limit": mem_limit
                }
                
            with metrics_lock:
                # Update current status
                metrics_cache.clear()
                metrics_cache.update(new_cache)
                
        except Exception as e:
            logger.error(f"Error in stats collector loop: {e}")
            
        time.sleep(2)

@app.on_event("startup")
def on_startup():
    t = threading.Thread(target=stats_collector_thread, daemon=True)
    t.start()

@app.get("/metrics")
def get_metrics():
    # Update Prometheus gauges
    with metrics_lock:
        for name, stats in metrics_cache.items():
            cpu_gauge.labels(container_name=name).set(stats["cpu_pct"])
            mem_gauge.labels(container_name=name).set(stats["mem_bytes"])
            limit_gauge.labels(container_name=name).set(stats["mem_limit"])
            status_gauge.labels(container_name=name).set(stats["running"])
            
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "metric-collector"}
