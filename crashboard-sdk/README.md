# CrashBoard SDK — Chaos-as-a-Service

Drop chaos engineering **and** a complete observability stack into any microservice
repo with one line of code. CrashBoard scans your `docker-compose.yml`, auto-generates
a Grafana chaos dashboard tailored to *your* services, and ships a fault-injection
engine so you can break things on purpose and watch the blast radius in real time.

```
pip install crashboard-sdk
```

There are two halves:

| Half | What it does | How you use it |
|------|--------------|----------------|
| **In-process SDK** | Exposes Prometheus metrics, instruments Redis/Kafka, ships logs to Loki | One line in your FastAPI app: `crashboard.init(app, "my-service")` |
| **`crashboard` CLI** | Generates + runs the chaos & observability overlay (Prometheus, Grafana, Loki, metric-collector, chaos-engine) | `crashboard init` → `crashboard up` → `crashboard trigger ...` |

The infra (chaos-engine + metric-collector) is **bundled inside the package** — there's
nothing to clone. `crashboard init` copies it into `.crashboard/` in your repo, so the
generated stack builds and runs anywhere.

---

## Quickstart

### 1. Instrument your service (the one line)

```python
from fastapi import FastAPI
import crashboard

app = FastAPI()

# Exposes GET /metrics, wires up logging, and (optionally) instruments Redis/Kafka.
crashboard.init(app, "user-service")
```

Optional integrations — pass the clients you already have:

```python
crashboard.init(
    app,
    "order-service",
    redis_client=redis_client,         # tracks cache hit/miss ratio
    kafka_producer=producer,           # counts produced messages per topic
    kafka_consumer=consumer,           # tracks consumer lag in a background thread
    kafka_topic="order_created",
)
```

Add the SDK to your service's image (e.g. `pip install crashboard-sdk` in its Dockerfile).

### 2. Generate the overlay

From your repo root (where your `docker-compose.yml` lives):

```
crashboard init
```

This scans your compose file, detects your app services and whether you run Redis/Kafka,
and writes everything to `.crashboard/` (see [What gets generated](#what-gets-generated)).

### 3. Launch

```
crashboard up
```

CrashBoard merges its overlay with your `docker-compose.yml` and starts the stack:

- **Grafana** (chaos dashboard) — http://localhost:3000 *(anonymous admin)*
- **Prometheus** — http://localhost:9090
- **Loki** — http://localhost:3100
- **Chaos Engine API** — http://localhost:8080

### 4. Break something

```
crashboard trigger cpu_spike --target user-service-1 --duration 30
crashboard trigger network_delay --target order-service --duration 20
crashboard trigger kill_container --target notif-service
```

Watch latency, error rates, CPU/memory, and container status react live in Grafana.

```
crashboard down     # tear it all down
```

---

## Chaos scenarios

| Scenario | Needs `--target` | Effect | Self-heals |
|----------|:---------------:|--------|:----------:|
| `kill_container` | ✅ | Hard-kills the target container | — (use Docker `restart: always`) |
| `cpu_spike` | ✅ | Runs `stress-ng` CPU load inside the target for `--duration` | ✅ |
| `network_delay` | ✅ | Adds 500ms latency on the target's `eth0` via `tc netem` | ✅ after `--duration` |
| `kafka_flood` | — | Floods the configured Kafka topic with 10k messages | ✅ (lag drains) |
| `db_corrupt` | — | Pauses the `postgres` container for `--duration` | ✅ after `--duration` |

> `network_delay` and `cpu_spike` require the target container to have `NET_ADMIN`
> capability / `stress-ng` available. The chaos-engine reaches targets over the Docker
> socket, so it works against any container in your stack by name.

The chaos engine also exposes a small HTTP API directly (http://localhost:8080):
`POST /chaos/trigger`, `POST /chaos/clear`, `GET /chaos/active`, and a scheduler
(`GET/POST /chaos/schedule`) for recurring fault injection.

---

## Python API reference

All exported from the top-level `crashboard` package:

- **`init(app, service_name, redis_client=None, kafka_producer=None, kafka_consumer=None, kafka_topic="order_created")`**
  One-line entrypoint. Exposes `/metrics`, and instruments any clients you pass.
- **`instrument_redis(client)`** — wraps `client.get` to emit `cache_hits_total` / `cache_misses_total`.
- **`instrument_kafka_producer(producer)`** — wraps `producer.produce` to emit `kafka_messages_produced_total{topic}`.
- **`setup_loki_logger(service_name)`** — attaches a non-blocking Loki log handler (no-op unless `LOKI_URL` is set).

### Metrics emitted

| Metric | Source | Dashboard panel |
|--------|--------|-----------------|
| `http_request_duration_seconds_*` | auto (instrumentator) | HTTP Request Latency |
| `http_requests_total` | auto (instrumentator) | HTTP Error Rates |
| `cache_hits_total` / `cache_misses_total` | `instrument_redis` | Redis Cache Hit Ratio |
| `kafka_messages_produced_total` / `kafka_consumer_lag` | Kafka instrumentation | Kafka Consumer Lag |
| `container_cpu_usage_percent` / `container_memory_usage_bytes` / `container_status` | bundled metric-collector | CPU, Memory, Container Statuses |

---

## CLI reference

| Command | Description |
|---------|-------------|
| `crashboard init [--compose-file docker-compose.yml]` | Scan the project and generate the `.crashboard/` overlay |
| `crashboard up` | Build + start the stack (merged with your compose if present) |
| `crashboard down` | Stop the stack |
| `crashboard trigger <scenario> [--target NAME] [--duration N]` | Inject a chaos scenario |

---

## What gets generated

`crashboard init` writes a self-contained overlay to `.crashboard/`:

```
.crashboard/
├── docker-compose.crashboard.yml      # the observability + chaos overlay
├── prometheus.yml                     # scrape config built from YOUR services
├── engine/                            # bundled chaos-engine (built locally)
├── collector/                         # bundled metric-collector (built locally)
└── infrastructure/
    ├── loki/loki-config.yaml
    └── grafana/
        ├── provisioning/datasources/datasource.yaml
        ├── provisioning/dashboards/dashboard.yaml
        └── dashboards/crashboard.json   # the auto-generated chaos dashboard
```

**Dashboard auto-generation:** the Grafana dashboard JSON is built programmatically
from your detected services — latency/error/CPU/memory panels are always included, and
Redis (cache hit ratio) and Kafka (consumer lag) panels are added only if those
components are detected in your compose file.

`.crashboard/` is a generated artifact and is safe to commit or `.gitignore`.

---

## How it stays droppable

Earlier iterations built the infra from paths that only existed in the original demo
repo (`build: ../chaos-engine`). This version ships that source **inside the wheel** and
copies it into `.crashboard/` on `init`, so `crashboard up` works in any repo with no
external checkout. Prometheus discovers your services on the shared compose network at
`<service>:8000/metrics`, which is exactly where `crashboard.init` exposes them.

## License

MIT — see [LICENSE](LICENSE).
