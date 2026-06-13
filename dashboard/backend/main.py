import os
import json
import logging
import asyncio
import time
from typing import List, Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import httpx

from loki_logger import setup_loki_logger

# Initialize Loki Logger
setup_loki_logger("dashboard-backend")

logger = logging.getLogger("dashboard-backend")
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Dashboard Backend",
    description="WebSocket metrics aggregator for CrashBoard dashboard",
    version="1.0.0"
)

# Enable CORS for easy local development testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_headers=["*"],
    allow_methods=["*"]
)

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
CHAOS_ENGINE_URL = os.getenv("CHAOS_ENGINE_URL", "http://chaos-engine:8000")

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self.lock:
            self.active_connections.append(websocket)
            logger.info(f"WebSocket client connected. Total clients: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        async with self.lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
                logger.info(f"WebSocket client disconnected. Total clients: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        async with self.lock:
            disconnected = []
            for connection in self.active_connections:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    logger.warning(f"Failed to send message to client, marking for removal: {e}")
                    disconnected.append(connection)
            
            for connection in disconnected:
                if connection in self.active_connections:
                    self.active_connections.remove(connection)

manager = ConnectionManager()

# Prometheus query helper
async def query_prometheus_instant(client: httpx.AsyncClient, query: str) -> List[dict]:
    try:
        r = await client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=1.0)
        if r.status_code == 200:
            res = r.json()
            if res.get("status") == "success":
                return res.get("data", {}).get("result", [])
    except Exception as e:
        logger.warning(f"Prometheus query error for query '{query}': {e}")
    return []

# Fetch service health check info directly
async def ping_service_health(client: httpx.AsyncClient, service_url: str, name: str) -> Dict:
    try:
        # FastAPI instances respond on /health
        # Use short timeout (0.5s) to avoid blocking broadcast cycle
        r = await client.get(f"http://{service_url}/health", timeout=0.5)
        if r.status_code == 200:
            data = r.json()
            return {
                "status": "online",
                "health_details": data
            }
        else:
            return {
                "status": "degraded",
                "health_details": r.json() if r.headers.get("content-type") == "application/json" else {}
            }
    except Exception as e:
        logger.debug(f"Ping to {name} failed: {e}")
        return {
            "status": "offline",
            "health_details": {"error": str(e)}
        }

async def fetch_metrics_payload() -> dict:
    async with httpx.AsyncClient() as client:
        # 1. Ping health endpoints concurrently
        services_to_ping = {
            "user-service-1": "user-service-1:8000",
            "user-service-2": "user-service-2:8000",
            "order-service": "order-service:8000",
            "notif-service": "notif-service:8000",
            "chaos-engine": "chaos-engine:8000"
        }
        
        ping_tasks = [
            ping_service_health(client, url, name) 
            for name, url in services_to_ping.items()
        ]
        ping_results = await asyncio.gather(*ping_tasks)
        health_status = {
            name: result for name, result in zip(services_to_ping.keys(), ping_results)
        }
        
        # 2. Query active chaos scenarios
        chaos_active = {}
        try:
            r = await client.get(f"{CHAOS_ENGINE_URL}/chaos/active", timeout=1.0)
            if r.status_code == 200:
                chaos_active = r.json()
        except Exception as e:
            logger.warning(f"Failed to fetch active chaos scenarios: {e}")
            
        # 3. Query Prometheus for metrics
        # CPU
        cpu_results = await query_prometheus_instant(client, "container_cpu_usage_percent")
        container_cpu = {
            res["metric"].get("container_name"): float(res["value"][1])
            for res in cpu_results if "container_name" in res["metric"]
        }
        
        # Memory
        mem_results = await query_prometheus_instant(client, "container_memory_usage_bytes")
        container_mem = {
            res["metric"].get("container_name"): float(res["value"][1]) / (1024 * 1024) # to MB
            for res in mem_results if "container_name" in res["metric"]
        }

        # Consumer Lag
        lag_results = await query_prometheus_instant(client, "kafka_consumer_lag")
        kafka_lag = 0
        if lag_results:
            try:
                kafka_lag = int(float(lag_results[0]["value"][1]))
            except Exception:
                pass
                
        # Cache hits and misses
        cache_hits_res = await query_prometheus_instant(client, "sum(rate(cache_hits_total[15s]))")
        cache_misses_res = await query_prometheus_instant(client, "sum(rate(cache_misses_total[15s]))")
        
        hits_rate = float(cache_hits_res[0]["value"][1]) if cache_hits_res else 0.0
        misses_rate = float(cache_misses_res[0]["value"][1]) if cache_misses_res else 0.0
        
        total_cache_queries = hits_rate + misses_rate
        cache_hit_ratio = 100.0
        if total_cache_queries > 0:
            cache_hit_ratio = (hits_rate / total_cache_queries) * 100.0
            
        # Latencies
        latency_results = await query_prometheus_instant(
            client, 
            "sum(rate(http_request_duration_seconds_sum[15s])) by (job) / sum(rate(http_request_duration_seconds_count[15s])) by (job) * 1000"
        )
        job_latencies = {
            res["metric"].get("job"): float(res["value"][1])
            for res in latency_results if "job" in res["metric"]
        }
        
        # Error Rates (percentage of non-2xx codes)
        error_results = await query_prometheus_instant(
            client,
            "sum(rate(http_requests_total{status!~'2..'}[15s])) by (job) / sum(rate(http_requests_total[15s])) by (job) * 100"
        )
        job_errors = {
            res["metric"].get("job"): float(res["value"][1])
            for res in error_results if "job" in res["metric"]
        }

        # 4. Construct unified payload
        services_data = {}
        for name in services_to_ping.keys():
            status_info = health_status[name]["status"]
            # map internal docker container names to prometheus jobs if they differ
            prom_job_map = {
                "user-service-1": "user-service",
                "user-service-2": "user-service",
                "order-service": "order-service",
                "notif-service": "notif-service",
                "chaos-engine": "chaos-engine"
            }
            prom_job = prom_job_map.get(name, name)
            
            # Extract latency and errors
            latency = job_latencies.get(prom_job, 0.0)
            errors = job_errors.get(prom_job, 0.0)
            
            # CPU/Mem
            cpu = container_cpu.get(name, 0.0)
            mem = container_mem.get(name, 0.0)
            
            # Customize cache hit reporting for user services
            cache_ratio = None
            if "user-service" in name:
                cache_ratio = cache_hit_ratio
                
            services_data[name] = {
                "name": name,
                "status": status_info,
                "latency_ms": round(latency, 2),
                "error_rate": round(errors, 2),
                "cpu_pct": round(cpu, 1),
                "memory_mb": round(mem, 1),
                "cache_hit_ratio": round(cache_ratio, 1) if cache_ratio is not None else None,
                "health_details": health_status[name]["health_details"]
            }

        # 3.5. Query Loki for logs
        logs = []
        try:
            now_ns = int(time.time() * 1e9)
            start_ns = now_ns - int(15 * 1e9)
            r = await client.get(
                "http://loki:3100/loki/api/v1/query_range",
                params={
                    "query": '{service=~".+"}',
                    "start": str(start_ns),
                    "limit": 30
                },
                timeout=1.0
            )
            if r.status_code == 200:
                res = r.json()
                if res.get("status") == "success":
                    streams = res.get("data", {}).get("result", [])
                    for stream in streams:
                        service = stream.get("stream", {}).get("service", "unknown")
                        level = stream.get("stream", {}).get("level", "INFO")
                        for val in stream.get("values", []):
                            timestamp = float(val[0]) / 1e9
                            msg = val[1]
                            logs.append({
                                "timestamp": timestamp,
                                "service": service,
                                "level": level,
                                "message": msg
                            })
                    logs.sort(key=lambda x: x["timestamp"])
                    logs = logs[-30:]
        except Exception as e:
            logger.warning(f"Failed to query Loki logs: {e}")

        return {
            "timestamp": time.time(),
            "services": services_data,
            "kafka_lag": kafka_lag,
            "chaos": {
                "active": chaos_active
            },
            "logs": logs
        }

# Background scraping/broadcasting task
async def metrics_broadcaster_loop():
    logger.info("Starting background metrics broadcaster loop...")
    while True:
        # Only query and send if there are active dashboard users listening
        if len(manager.active_connections) > 0:
            try:
                payload = await fetch_metrics_payload()
                await manager.broadcast(payload)
            except Exception as e:
                logger.error(f"Error in broadcast lifecycle: {e}")
        await asyncio.sleep(1.0)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(metrics_broadcaster_loop())

@app.websocket("/ws/metrics")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Keep connection open and read messages if client sends any
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as e:
        logger.warning(f"WebSocket exception: {e}")
        await manager.disconnect(websocket)

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "dashboard-backend"}
