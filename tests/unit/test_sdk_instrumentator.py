"""Unit tests for the crashboard SDK metric instrumentation monkeypatches."""

from prometheus_client import REGISTRY

from crashboard import instrumentator


def _counter(name, labels=None):
    return REGISTRY.get_sample_value(name, labels or {})


class DummyRedis:
    def __init__(self, store):
        self._store = store

    def get(self, key):
        return self._store.get(key)


class DummyProducer:
    def __init__(self):
        self.calls = []

    def produce(self, topic, **kwargs):
        self.calls.append((topic, kwargs))


def test_instrument_redis_counts_hits_and_misses():
    client = DummyRedis({"user:1": "cached"})
    instrumentator.instrument_redis(client)

    before_hits = _counter("cache_hits_total") or 0.0
    before_misses = _counter("cache_misses_total") or 0.0

    assert client.get("user:1") == "cached"   # hit
    assert client.get("user:404") is None      # miss

    assert (_counter("cache_hits_total") or 0.0) == before_hits + 1
    assert (_counter("cache_misses_total") or 0.0) == before_misses + 1


def test_instrument_redis_is_idempotent():
    client = DummyRedis({})
    instrumentator.instrument_redis(client)
    patched = client.get
    instrumentator.instrument_redis(client)
    assert client.get is patched, "second instrumentation must not re-wrap"
    assert client._crashboard_instrumented is True


def test_instrument_kafka_producer_counts_and_forwards():
    producer = DummyProducer()
    instrumentator.instrument_kafka_producer(producer)

    before = _counter("kafka_messages_produced_total", {"topic": "order_created"}) or 0.0
    producer.produce("order_created", key="1", value="{}")

    assert producer.calls, "original produce must still be invoked"
    after = _counter("kafka_messages_produced_total", {"topic": "order_created"}) or 0.0
    assert after == before + 1


def test_instrument_kafka_producer_is_idempotent():
    producer = DummyProducer()
    instrumentator.instrument_kafka_producer(producer)
    patched = producer.produce
    instrumentator.instrument_kafka_producer(producer)
    assert producer.produce is patched


class ReadOnlyProduceProducer:
    """Mimics confluent_kafka's C producer whose `produce` cannot be replaced."""

    def __init__(self):
        self.calls = []

    def produce(self, topic, **kwargs):
        self.calls.append((topic, kwargs))

    def __setattr__(self, name, value):
        if name == "produce":
            raise AttributeError("'cimpl.Producer' object attribute 'produce' is read-only")
        object.__setattr__(self, name, value)

    def flush(self, timeout=None):
        return 0


def test_read_only_producer_falls_back_to_proxy():
    """Regression: a real confluent_kafka.Producer has a read-only `produce`.

    Patching it in place raises AttributeError, which previously crashed
    order-service at startup. The SDK must instead return a counting proxy.
    """
    producer = ReadOnlyProduceProducer()
    instrumented = instrumentator.instrument_kafka_producer(producer)

    assert instrumented is not producer, "should return a proxy, not the raw producer"

    before = _counter("kafka_messages_produced_total", {"topic": "order_created"}) or 0.0
    instrumented.produce("order_created", key="1", value="{}")

    # Metric incremented and the call reached the underlying producer.
    after = _counter("kafka_messages_produced_total", {"topic": "order_created"}) or 0.0
    assert after == before + 1
    assert producer.calls, "proxy must forward produce() to the wrapped producer"
    # Non-intercepted attributes transparently forward.
    assert instrumented.flush(timeout=1.0) == 0
