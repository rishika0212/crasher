<div align="center">

# CrashBoard

**Break your microservices on purpose — and watch exactly what happens.**

A full chaos-engineering playground (Kafka · Redis · Postgres · Prometheus · Grafana · Loki),
packaged so you can drop the same chaos + observability stack into *your* repo with one line.

`fault injection` · `live dashboards` · `drop-in SDK`

</div>

---

## What's inside

CrashBoard is two things in one repo:

| | |
|---|---|
| **The platform** | A realistic microservice system — user, order & notification services behind an Nginx gateway, talking over Kafka and caching in Redis — wired to Prometheus, Grafana and Loki, with a live React control room. |
| **The SDK** | [`crashboard-sdk/`](crashboard-sdk/) — `pip install` it, add one line to any FastAPI service, and `crashboard init` scaffolds the whole chaos + observability stack into *your* project. |

A **chaos engine** sits in the middle: it injects faults — killing containers, spiking CPU, delaying the network, flooding Kafka, pausing the database — while every service streams metrics and logs you can watch in real time.

```
          ┌─ user-service ─┐
 Nginx ───┤  order-service  ├── Kafka / Redis / Postgres
          └─ notif-service ─┘            │
                  │                       │
            metrics + logs ──► Prometheus · Loki ──► Grafana
                  ▲
            Chaos Engine ──► inject faults on demand
```

## Quickstart — run the platform

```bash
docker compose up -d --build
```

| Surface | URL |
|---|---|
| Control room (Nginx) | http://localhost |
| Grafana dashboards | http://localhost:3000 |
| Prometheus | http://localhost:9090 |

## Quickstart — use the SDK in your own repo

```bash
pip install crashboard-sdk
```

```python
from fastapi import FastAPI
import crashboard

app = FastAPI()
crashboard.init(app, "my-service")   # the one line
```

```bash
crashboard init      # scaffold the chaos + observability overlay
crashboard up        # launch it alongside your docker-compose
```

Full SDK docs → [`crashboard-sdk/README.md`](crashboard-sdk/README.md)

## Chaos scenarios

| Scenario | What it does |
|---|---|
| `kill_container` | Hard-kills a target service |
| `cpu_spike` | Pins CPU inside the target for a set duration |
| `network_delay` | Adds 500ms latency on the target's interface |
| `kafka_flood` | Floods the topic with 10k messages to spike consumer lag |
| `db_corrupt` | Pauses Postgres to simulate a database stall |

```bash
crashboard trigger cpu_spike --target order-service --duration 30
```

Every scenario self-heals after its duration, so the system always returns to green.

## Layout

```
services/        user · order · notif · metric-collector
chaos-engine/    fault-injection API + scheduler
dashboard/       React control room (backend + frontend)
infrastructure/  Prometheus · Grafana · Loki · Nginx · Postgres config
crashboard-sdk/  the installable Chaos-as-a-Service SDK
```

<div align="center">
<sub>Built to make resilience testing something you actually run — not a doc you mean to read.</sub>
</div>
