import time
import logging
import threading
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import REGISTRY, Counter, Gauge

logger = logging.getLogger("crashboard-instrumentator")

def get_or_create_counter(name, documentation, labelnames=()):
    if name in REGISTRY._names_to_collectors:
        collector = REGISTRY._names_to_collectors[name]
        if isinstance(collector, Counter):
            return collector
    return Counter(name, documentation, labelnames)

def get_or_create_gauge(name, documentation, labelnames=()):
    if name in REGISTRY._names_to_collectors:
        collector = REGISTRY._names_to_collectors[name]
        if isinstance(collector, Gauge):
            return collector
    return Gauge(name, documentation, labelnames)

# Metrics definitions
CACHE_HIT_COUNTER = get_or_create_counter("cache_hits_total", "Total Redis cache hits")
CACHE_MISS_COUNTER = get_or_create_counter("cache_misses_total", "Total Redis cache misses")
KAFKA_PRODUCED_COUNTER = get_or_create_counter("kafka_messages_produced_total", "Total Kafka messages produced", ["topic"])
KAFKA_LAG_GAUGE = get_or_create_gauge("kafka_consumer_lag", "Current consumer lag in message count", ["topic"])

def instrument_redis(client):
    """Monkeypatch Redis client to collect cache hits & misses metrics."""
    if hasattr(client, "_crashboard_instrumented"):
        return
        
    logger.info("Instrumenting Redis client for CrashBoard cache metrics.")
    original_get = client.get
    
    def wrapped_get(name, *args, **kwargs):
        try:
            res = original_get(name, *args, **kwargs)
            if res is not None:
                CACHE_HIT_COUNTER.inc()
            else:
                CACHE_MISS_COUNTER.inc()
            return res
        except Exception as e:
            CACHE_MISS_COUNTER.inc()
            raise e
            
    client.get = wrapped_get
    client._crashboard_instrumented = True

class _InstrumentedProducer:
    """Transparent proxy that counts produced messages.

    The real ``confluent_kafka.Producer`` is a C extension whose ``produce``
    attribute is read-only, so it cannot be monkeypatched in place. When that
    is the case we wrap the producer in this proxy, which forwards every other
    attribute to the underlying producer and only intercepts ``produce``.
    """

    def __init__(self, producer):
        object.__setattr__(self, "_producer", producer)
        object.__setattr__(self, "_crashboard_instrumented", True)

    def produce(self, topic, *args, **kwargs):
        KAFKA_PRODUCED_COUNTER.labels(topic=topic).inc()
        return self._producer.produce(topic, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._producer, name)


def instrument_kafka_producer(producer):
    """Instrument a Kafka producer to track outbound message count.

    Returns the producer to use: the same object when ``produce`` can be
    patched in place, otherwise a counting proxy (see ``_InstrumentedProducer``).
    Callers should adopt the returned object.
    """
    if getattr(producer, "_crashboard_instrumented", False):
        return producer

    logger.info("Instrumenting Kafka Producer for CrashBoard traffic metrics.")
    original_produce = producer.produce

    def wrapped_produce(topic, *args, **kwargs):
        KAFKA_PRODUCED_COUNTER.labels(topic=topic).inc()
        return original_produce(topic, *args, **kwargs)

    try:
        producer.produce = wrapped_produce
        producer._crashboard_instrumented = True
        return producer
    except (AttributeError, TypeError):
        # Read-only C-extension producer: fall back to a transparent proxy so
        # the host service still boots and the metric still increments.
        logger.info("Producer.produce is read-only; wrapping in a counting proxy.")
        return _InstrumentedProducer(producer)

def calculate_lag(consumer):
    """Calculates partition lag by checking committed vs watermark offsets."""
    try:
        partitions = consumer.assignment()
        if not partitions:
            return 0
            
        total_lag = 0
        for p in partitions:
            committed = consumer.committed([p], timeout=0.5)
            if committed and committed[0].offset >= 0:
                last_committed = committed[0].offset
                low, high = consumer.get_watermark_offsets(p, timeout=0.5)
                lag = high - last_committed
                if lag > 0:
                    total_lag += lag
            else:
                low, high = consumer.get_watermark_offsets(p, timeout=0.5)
                total_lag += high
        return total_lag
    except Exception as e:
        logger.debug(f"Error calculating consumer lag: {e}")
        return 0

def start_kafka_lag_thread(consumer, topic="order_created"):
    """Spins up a daemon thread to periodically track consumer lag."""
    logger.info(f"Starting consumer lag tracking thread for topic '{topic}'.")
    def loop():
        while True:
            try:
                # Calculate lag
                lag = calculate_lag(consumer)
                KAFKA_LAG_GAUGE.labels(topic=topic).set(lag)
            except Exception as e:
                logger.error(f"Error in lag collection thread: {e}")
            time.sleep(1.0)
            
    t = threading.Thread(target=loop, daemon=True)
    t.start()

def init_instrumentation(app: FastAPI, service_name: str, redis_client=None, kafka_producer=None, kafka_consumer=None, kafka_topic="order_created"):
    """
    One-line entry point to instrument a FastAPI application.
    Enables prometheus endpoint, monkeypatches Redis and Kafka, and initializes logging.
    """
    logger.info(f"Initializing CrashBoard instrumentation for service: {service_name}")

    # 1. Expose metrics endpoint
    Instrumentator().instrument(app).expose(app)

    # 2. Redis integration
    if redis_client is not None:
        instrument_redis(redis_client)

    # 3. Kafka integration
    instrumented_producer = None
    if kafka_producer is not None:
        instrumented_producer = instrument_kafka_producer(kafka_producer)

    if kafka_consumer is not None:
        start_kafka_lag_thread(kafka_consumer, topic=kafka_topic)

    # Return the instrumented producer so callers can adopt the proxy used when
    # the underlying producer's `produce` cannot be patched in place.
    return instrumented_producer
