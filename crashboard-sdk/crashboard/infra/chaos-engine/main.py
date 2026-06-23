import os
import time
import json
import logging
import threading
from typing import Dict, Optional
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator
import docker
from confluent_kafka import Producer

from loki_logger import setup_loki_logger
from scheduler import ChaosScheduler

# Initialize Loki Logger
setup_loki_logger("chaos-engine")

logger = logging.getLogger("chaos-engine")
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Chaos Engine",
    description="Orchestrates fault injection scenarios across CrashBoard microservices",
    version="1.0.0"
)

# Instrument the app for Prometheus metrics
Instrumentator().instrument(app).expose(app)

# Track active scenarios in memory
# e.g., active_scenarios = {"user-service-1": {"network_delay": True, "cpu_spike": True}}
active_scenarios: Dict[str, Dict] = {}
scenarios_lock = threading.Lock()

class ChaosRequest(BaseModel):
    scenario: str
    target: Optional[str] = None
    duration: Optional[int] = 30  # Default 30 seconds for transient chaos

# Establish Docker Client connection
try:
    docker_client = docker.from_env()
except Exception as de:
    logger.error(f"Failed to connect to Docker socket: {de}")
    docker_client = None

def get_container(name: str):
    if not docker_client:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Docker socket connection unavailable"
        )
    try:
        return docker_client.containers.get(name)
    except docker.errors.NotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Target container '{name}' not found"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Docker error: {e}"
        )

# Scenario functions
def execute_kill_container(target: str):
    container = get_container(target)
    logger.warning(f"CHAOS: Killing container {target}")
    container.kill()
    logger.info(f"CHAOS: Container {target} killed.")

def execute_cpu_spike(target: str, duration: int):
    container = get_container(target)
    logger.warning(f"CHAOS: Spiking CPU on container {target} for {duration}s")
    # Execute stress-ng command inside container
    # Runs 2 CPU stressors in parallel for specified timeout
    exec_res = container.exec_run(f"stress-ng --cpu 2 --timeout {duration}", detach=True)
    
    with scenarios_lock:
        if target not in active_scenarios:
            active_scenarios[target] = {}
        active_scenarios[target]["cpu_spike"] = {
            "expires_at": time.time() + duration
        }

def execute_network_delay(target: str, duration: int):
    container = get_container(target)
    logger.warning(f"CHAOS: Adding 500ms network delay to {target} for {duration}s")
    
    # Clean up existing qdisc rules first, ignoring failure
    container.exec_run("tc qdisc del dev eth0 root", privilege=True)
    # Add network delay rule
    res = container.exec_run("tc qdisc add dev eth0 root netem delay 500ms", privilege=True)
    if res.exit_code != 0:
        logger.error(f"Failed to set traffic control delay: {res.output.decode('utf-8')}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Traffic shaping failed (NET_ADMIN cap active?): {res.output.decode('utf-8')}"
        )
        
    with scenarios_lock:
        if target not in active_scenarios:
            active_scenarios[target] = {}
        active_scenarios[target]["network_delay"] = {
            "expires_at": time.time() + duration
        }
        
    # Schedule self-healing recovery thread
    def recover_net():
        time.sleep(duration)
        try:
            c = docker_client.containers.get(target)
            c.exec_run("tc qdisc del dev eth0 root", privilege=True)
            logger.info(f"Self-healed: Removed network delay on {target}")
            with scenarios_lock:
                if target in active_scenarios and "network_delay" in active_scenarios[target]:
                    del active_scenarios[target]["network_delay"]
        except Exception as err:
            logger.error(f"Failed to self-heal network delay on {target}: {err}")

    threading.Thread(target=recover_net, daemon=True).start()

def execute_kafka_flood():
    # Kafka coordinates are configurable so the SDK works against any user's broker/topic.
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("CHAOS_KAFKA_TOPIC", "order_created")
    logger.warning(f"CHAOS: Flooding Kafka topic '{topic}' on {bootstrap} with 10k messages")

    def producer_thread():
        try:
            p = Producer({'bootstrap.servers': bootstrap, 'message.timeout.ms': 1000})
            for i in range(10000):
                p.produce(
                    topic,
                    key=f"flood-{i}", 
                    value=json.dumps({
                        "order_id": 999000 + i,
                        "user_id": 8888,
                        "product": "Chaos Flood Load Test Item " * 5,
                        "quantity": 2,
                        "price": 49.99,
                        "status": "flood"
                    })
                )
                if i % 1000 == 0:
                    p.poll(0)
            p.flush(timeout=2.0)
            logger.info("CHAOS: Finished flooding 10k Kafka messages.")
        except Exception as e:
            logger.error(f"Failed in Kafka flood thread: {e}")

    threading.Thread(target=producer_thread, daemon=True).start()
    
    with scenarios_lock:
        active_scenarios["kafka"] = {
            "kafka_flood": {
                "expires_at": time.time() + 15  # Assume flood spikes consumer lag for ~15 seconds
            }
        }

def execute_db_corrupt(duration: int):
    logger.warning(f"CHAOS: Pausing primary PostgreSQL database container for {duration}s")
    postgres = get_container("postgres")
    try:
        postgres.pause()
    except Exception as e:
        logger.error(f"Failed to pause Postgres container: {e}")
        # If already paused, ignore or raise
    
    with scenarios_lock:
        active_scenarios["postgres"] = {
            "db_corrupt": {
                "expires_at": time.time() + duration
            }
        }
        
    def recover_db():
        time.sleep(duration)
        try:
            c = docker_client.containers.get("postgres")
            if c.status == "paused":
                c.unpause()
                logger.info("Self-healed: Unpaused PostgreSQL database container.")
            with scenarios_lock:
                if "postgres" in active_scenarios and "db_corrupt" in active_scenarios["postgres"]:
                    del active_scenarios["postgres"]["db_corrupt"]
        except Exception as err:
            logger.error(f"Failed to unpause PostgreSQL container during self-healing: {err}")

    threading.Thread(target=recover_db, daemon=True).start()

def trigger_chaos(scenario: str, target: Optional[str] = None, duration: int = 30):
    if scenario == "kill_container":
        if not target:
            raise HTTPException(status_code=400, detail="Target container required for kill_container")
        execute_kill_container(target)
    elif scenario == "cpu_spike":
        if not target:
            raise HTTPException(status_code=400, detail="Target container required for cpu_spike")
        execute_cpu_spike(target, duration)
    elif scenario == "network_delay":
        if not target:
            raise HTTPException(status_code=400, detail="Target container required for network_delay")
        execute_network_delay(target, duration)
    elif scenario == "kafka_flood":
        execute_kafka_flood()
    elif scenario == "db_corrupt":
        execute_db_corrupt(duration)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown chaos scenario: {scenario}")

# Scheduler Callback Adapter
def scheduler_trigger(scenario: str, target: str):
    # Scheduled chaos runs with a 15-second default duration
    trigger_chaos(scenario=scenario, target=target if target else None, duration=15)

# Initialize Scheduler
scheduler = ChaosScheduler(trigger_func=scheduler_trigger)

@app.post("/chaos/trigger")
def api_trigger_chaos(req: ChaosRequest):
    trigger_chaos(req.scenario, req.target, req.duration)
    return {"status": "triggered", "scenario": req.scenario, "target": req.target}

@app.post("/chaos/clear")
def api_clear_chaos():
    logger.info("CHAOS: Cleared all chaos and triggering heals.")
    healed = []
    
    # 1. Unpause DB
    try:
        pg = docker_client.containers.get("postgres")
        if pg.status == "paused":
            pg.unpause()
            healed.append("postgres")
    except Exception as e:
        logger.debug(f"Clear postgres pause: {e}")
        
    # 2. Clear network shaping on every running container we can reach. We discover
    #    targets dynamically so this works against any user's stack, not a fixed list.
    try:
        running = docker_client.containers.list()
    except Exception as e:
        logger.debug(f"Clear: could not list containers: {e}")
        running = []
    for c in running:
        try:
            res = c.exec_run("tc qdisc del dev eth0 root", privilege=True)
            if res.exit_code == 0:
                healed.append(c.name)
        except Exception:
            pass
            
    with scenarios_lock:
        active_scenarios.clear()
        
    return {"status": "cleared", "healed_targets": healed}

@app.get("/chaos/active")
def api_get_active_chaos():
    # Filter expired records
    now = time.time()
    valid_active = {}
    with scenarios_lock:
        for target, scens in list(active_scenarios.items()):
            valid_scens = {}
            for name, details in scens.items():
                if details["expires_at"] > now:
                    valid_scens[name] = {
                        "remaining_seconds": max(0, int(details["expires_at"] - now))
                    }
            if valid_scens:
                valid_active[target] = valid_scens
            else:
                active_scenarios.pop(target, None)
                
    return valid_active

# Scheduler endpoints
@app.get("/chaos/schedule")
def api_get_schedule():
    return scheduler.get_jobs()

class ScheduleRequest(BaseModel):
    scenario: str
    target: Optional[str] = ""
    interval: int

@app.post("/chaos/schedule")
def api_add_schedule(req: ScheduleRequest):
    if req.interval < 5:
        raise HTTPException(status_code=400, detail="Scheduler interval must be at least 5 seconds")
    scheduler.add_job(req.scenario, req.target, req.interval)
    return {"status": "scheduled", "jobs": scheduler.get_jobs()}

@app.post("/chaos/schedule/clear")
def api_clear_schedule():
    scheduler.clear_jobs()
    return {"status": "schedule_cleared"}

@app.get("/health")
def health_check():
    docker_ok = False
    if docker_client:
        try:
            docker_client.ping()
            docker_ok = True
        except Exception:
            pass
            
    return {
        "status": "healthy",
        "service": "chaos-engine",
        "docker_socket": "connected" if docker_ok else "disconnected"
    }
