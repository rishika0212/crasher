from crashboard.instrumentator import init_instrumentation as init
from crashboard.instrumentator import instrument_redis, instrument_kafka_producer
from crashboard.logger import setup_loki_logger

__all__ = [
    "init",
    "instrument_redis",
    "instrument_kafka_producer",
    "setup_loki_logger"
]
