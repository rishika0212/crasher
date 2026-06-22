import os
import json
import logging
import socket
from typing import List
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlmodel import Session, select
from confluent_kafka import Producer
import crashboard

from database import init_db, get_session, check_db_health
from models import Order

# Initialize Loki Logger via CrashBoard SDK
crashboard.setup_loki_logger(os.getenv("SERVICE_NAME", "order-service"))

# Logging
logger = logging.getLogger("order-service")
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Order Service",
    description="Manages orders and publishes creation events to Kafka",
    version="1.0.0"
)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': socket.gethostname(),
    'message.timeout.ms': 3000,  # 3s timeout for fast failure
}

producer = None
try:
    producer = Producer(kafka_conf)
    logger.info("Kafka Producer initialized.")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Producer: {e}")

# Initialize telemetry and instrument Kafka Producer
crashboard.init(app, os.getenv("SERVICE_NAME", "order-service"), kafka_producer=producer)

# Startup
@app.on_event("startup")
def on_startup():
    init_db()

def check_kafka_health():
    if not producer:
        return False
    try:
        # Request metadata to verify connection
        producer.list_topics(timeout=1.0)
        return True
    except Exception as e:
        logger.error(f"Kafka connection check failed: {e}")
        return False

def delivery_report(err, msg):
    """Callback called once message delivered or failed."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@app.get("/health")
def health_check():
    db_ok = check_db_health()
    kafka_ok = check_kafka_health()
    
    is_healthy = db_ok  # Service is functionally up if DB is up, Kafka failure degrades notification flow
    
    status_code = status.HTTP_200_OK if is_healthy else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "healthy" if is_healthy else "unhealthy",
            "service": "order-service",
            "components": {
                "database": "connected" if db_ok else "disconnected",
                "kafka": "connected" if kafka_ok else "disconnected"
            }
        }
    )

@app.post("/orders", response_model=Order, status_code=status.HTTP_201_CREATED)
def create_order(order: Order, session: Session = Depends(get_session)):
    try:
        session.add(order)
        session.commit()
        session.refresh(order)
        
        # Publish event to Kafka
        event_payload = {
            "order_id": order.id,
            "user_id": order.user_id,
            "product": order.product,
            "quantity": order.quantity,
            "price": order.price,
            "status": order.status
        }
        
        if producer:
            try:
                # The produce call is monkeypatched by CrashBoard to log produced count
                producer.produce(
                    "order_created",
                    key=str(order.id),
                    value=json.dumps(event_payload),
                    callback=delivery_report
                )
                # Flush with short timeout to push it synchronously for this demo, or rely on background thread
                producer.poll(0)
            except Exception as ke:
                logger.error(f"Failed to publish to Kafka: {ke}")
        else:
            logger.warning("Kafka producer not initialized. Skipping event dispatch.")
            
        return order
    except Exception as e:
        logger.error(f"Error creating order: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )

@app.get("/orders/{order_id}", response_model=Order)
def get_order(order_id: int, session: Session = Depends(get_session)):
    try:
        order = session.get(Order, order_id)
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} not found"
            )
        return order
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Database error on fetching order {order_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database query failed"
        )

@app.get("/orders", response_model=List[Order])
def get_all_orders(session: Session = Depends(get_session)):
    try:
        orders = session.exec(select(Order)).all()
        return orders
    except Exception as e:
        logger.error(f"Database error on listing orders: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database list query failed"
        )
