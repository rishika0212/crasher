import os
import json
import time
import logging
import socket
import threading
from typing import List
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Gauge
from confluent_kafka import Consumer, KafkaError
from loki_logger import setup_loki_logger

# Initialize Loki Logger
setup_loki_logger(os.getenv("SERVICE_NAME", "notif-service"))

# Logging
logger = logging.getLogger("notif-service")
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Notification Service",
    description="Consumes order events from Kafka and tracks lag",
    version="1.0.0"
)

# Custom Prometheus gauge for Kafka Consumer Lag
KAFKA_LAG_GAUGE = Gauge("kafka_consumer_lag", "Current consumer lag in message count for order_created topic")

# In-memory notifications log (sliding window of last 100)
notifications_log = []
notifications_lock = threading.Lock()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'notif-service-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'max.poll.interval.ms': 10000, # Set low for demo lag responsiveness
}

consumer_instance = None
consumer_active = False

def calculate_lag(consumer):
    """Calculates partition lag by checking committed vs watermark offsets."""
    try:
        partitions = consumer.assignment()
        if not partitions:
            return 0
            
        total_lag = 0
        for p in partitions:
            # Get committed offset
            committed = consumer.committed([p], timeout=0.5)
            if committed and committed[0].offset >= 0:
                last_committed = committed[0].offset
                # Get watermarks (low, high)
                low, high = consumer.get_watermark_offsets(p, timeout=0.5)
                lag = high - last_committed
                if lag > 0:
                    total_lag += lag
            else:
                # If offset is not committed yet, lag is high watermark
                low, high = consumer.get_watermark_offsets(p, timeout=0.5)
                total_lag += high
        return total_lag
    except Exception as e:
        logger.warning(f"Error calculating consumer lag: {e}")
        return 0

def kafka_consumer_loop():
    global consumer_instance, consumer_active
    logger.info("Starting Kafka Consumer thread...")
    
    # Retry initialization in case Kafka is booting
    while not consumer_active:
        try:
            consumer_instance = Consumer(kafka_conf)
            consumer_instance.subscribe(['order_created'])
            consumer_active = True
            logger.info("Kafka Consumer subscribed to 'order_created'.")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Consumer: {e}. Retrying in 3s...")
            time.sleep(3)
            
    last_lag_check = 0
    while True:
        try:
            # Poll for new messages
            msg = consumer_instance.poll(0.5)
            
            # Periodically calculate and update lag gauge (every 1 second)
            now = time.time()
            if now - last_lag_check > 1.0:
                lag = calculate_lag(consumer_instance)
                KAFKA_LAG_GAUGE.set(lag)
                last_lag_check = now

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    time.sleep(1)
                    continue

            # Process valid message
            try:
                payload = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Notification generated for Order {payload.get('order_id')}")
                
                with notifications_lock:
                    notifications_log.append({
                        "order_id": payload.get("order_id"),
                        "user_id": payload.get("user_id"),
                        "product": payload.get("product"),
                        "quantity": payload.get("quantity"),
                        "price": payload.get("price"),
                        "processed_at": time.strftime("%Y-%m-%d %H:%M:%S")
                    })
                    if len(notifications_log) > 100:
                        notifications_log.pop(0)
                        
            except Exception as pe:
                logger.error(f"Failed to parse Kafka message: {pe}")
                
        except Exception as e:
            logger.error(f"Exception in consumer loop: {e}")
            time.sleep(1)

@app.on_event("startup")
def on_startup():
    # Start the consumer daemon thread
    t = threading.Thread(target=kafka_consumer_loop, daemon=True)
    t.start()

# Instrument FastAPI with Prometheus exporter
Instrumentator().instrument(app).expose(app)

@app.get("/health")
def health_check():
    # Check if consumer is initialized and connected to metadata
    kafka_connected = False
    if consumer_instance:
        try:
            consumer_instance.list_topics(timeout=1.0)
            kafka_connected = True
        except Exception as e:
            logger.error(f"Kafka health check query failed: {e}")
            
    is_healthy = kafka_connected
    status_code = status.HTTP_200_OK if is_healthy else status.HTTP_503_SERVICE_UNAVAILABLE
    
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "healthy" if is_healthy else "unhealthy",
            "service": "notification-service",
            "components": {
                "kafka_consumer": "connected" if kafka_connected else "disconnected"
            }
        }
    )

@app.get("/notifications")
def get_notifications():
    with notifications_lock:
        return list(notifications_log)
