# CrashBoard test suite

Fast, hermetic tests — no Docker, Postgres, Redis or Kafka required. Postgres is
replaced with in-memory SQLite, and Redis/Kafka with in-process fakes, so the
full request path of each service runs in the test process.

## Running

```bash
pip install -r requirements-dev.txt   # from the repo root
pytest                                 # run everything
pytest tests/unit                      # unit tests only
pytest -v -k order                     # filter by name
```

## Layout

| Path | Covers |
|---|---|
| `tests/unit/test_scheduler.py` | chaos-engine `ChaosScheduler` job lifecycle + firing loop |
| `tests/unit/test_chaos_engine.py` | fault routing, validation, active-scenario state, HTTP API (Docker mocked) |
| `tests/unit/test_sdk_cli.py` | service discovery + Prometheus config generation |
| `tests/unit/test_sdk_dashboard.py` | Grafana dashboard JSON generation |
| `tests/unit/test_sdk_instrumentator.py` | Redis/Kafka metric monkeypatches + read-only-producer proxy |
| `tests/integration/test_user_service.py` | user CRUD, Redis cache hit/miss/eviction, Redis-outage degradation |
| `tests/integration/test_order_service.py` | order CRUD + Kafka event publishing |
| `tests/integration/test_notif_service.py` | notification log endpoint + health |

## How service isolation works

Each service ships top-level modules named `main`/`database`/`models`, which
would collide in `sys.modules`. `tests/_harness.py::import_isolated` loads one
service's `main` with that service's directory at the front of `sys.path` and
the colliding names evicted first, so every app imports cleanly in one process.
