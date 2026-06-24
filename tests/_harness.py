"""Test harness helpers for CrashBoard.

The repo ships several independent FastAPI apps (user-service, order-service,
notif-service, chaos-engine) that each contain top-level modules named ``main``,
``database`` and ``models``.  Because they share module names, importing more
than one of them in the same interpreter would clash in ``sys.modules``.

``import_isolated`` loads one service's ``main`` module with that service's
directory placed at the front of ``sys.path`` and the colliding module names
evicted from the import cache first, so each app can be imported cleanly for
testing without a running Docker/Kafka/Postgres stack.
"""

import os
import sys
import importlib
from types import SimpleNamespace

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Module names that several services define and therefore collide on.
_COLLIDING = ("main", "database", "models", "scheduler", "loki_logger")


def import_isolated(rel_dir, module="main", env=None):
    """Import ``module`` from ``rel_dir`` (relative to the repo root) in isolation.

    ``env`` is a mapping of environment overrides applied for the duration of the
    import (e.g. unset LOKI_URL so the logger stays offline).
    """
    target = os.path.join(REPO_ROOT, rel_dir)
    saved_env = {}
    env = env or {}
    # Always keep the Loki logger offline during import.
    env.setdefault("LOKI_URL", "")

    for key, value in env.items():
        saved_env[key] = os.environ.get(key)
        if value == "":
            os.environ.pop(key, None)
        else:
            os.environ[key] = value

    for name in _COLLIDING:
        sys.modules.pop(name, None)

    sys.path.insert(0, target)
    try:
        mod = importlib.import_module(module)
        importlib.reload(mod)  # ensure a fresh module bound to this dir
        return mod
    finally:
        # Restore sys.path and env so we don't leak state between services.
        try:
            sys.path.remove(target)
        except ValueError:
            pass
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class FakeRedis:
    """Minimal in-memory stand-in for the redis client used by user-service."""

    def __init__(self):
        self.store = {}
        self.up = True

    def _check(self):
        if not self.up:
            raise ConnectionError("fake redis down")

    def get(self, key):
        self._check()
        return self.store.get(key)

    def setex(self, key, ttl, value):
        self._check()
        self.store[key] = value
        return True

    def delete(self, *keys):
        self._check()
        for key in keys:
            self.store.pop(key, None)
        return True

    def ping(self):
        self._check()
        return True


class _FakeMsg:
    def __init__(self, topic):
        self._topic = topic

    def topic(self):
        return self._topic

    def partition(self):
        return 0


class FakeProducer:
    """In-memory stand-in for the confluent_kafka Producer used by order-service."""

    def __init__(self):
        self.produced = []

    def produce(self, topic, key=None, value=None, callback=None, **kwargs):
        self.produced.append({"topic": topic, "key": key, "value": value})
        if callback is not None:
            callback(None, _FakeMsg(topic))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        return 0

    def list_topics(self, timeout=None):
        return SimpleNamespace(topics={"order_created": object()})
