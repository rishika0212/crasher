import os
import json
import logging
from typing import List
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlmodel import Session, select
from prometheus_fastapi_instrumentator import Instrumentator
import redis

from database import init_db, get_session, check_db_health
from models import User
from loki_logger import setup_loki_logger

# Initialize Loki Logger
setup_loki_logger(os.getenv("SERVICE_NAME", "user-service"))

# Logging
logger = logging.getLogger("user-service")
logging.basicConfig(level=logging.INFO)

from prometheus_client import Counter
CACHE_HIT_COUNTER = Counter("cache_hits_total", "Total Redis cache hits")
CACHE_MISS_COUNTER = Counter("cache_misses_total", "Total Redis cache misses")

app = FastAPI(
    title="User Service",
    description="Manages users and profiles with Redis caching",
    version="1.0.0"
)

# Initialize Redis client with a quick timeout for fail-soft resilience
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    db=0,
    decode_responses=True,
    socket_timeout=1.0,
    socket_connect_timeout=1.0
)

# Startup event
@app.on_event("startup")
def on_startup():
    init_db()

# Instrument the app for Prometheus metrics
Instrumentator().instrument(app).expose(app)

def check_redis_health():
    try:
        redis_client.ping()
        return True
    except Exception as e:
        logger.error(f"Redis Health check failed: {e}")
        return False

@app.get("/health")
def health_check():
    db_ok = check_db_health()
    redis_ok = check_redis_health()
    
    is_healthy = db_ok # User service needs DB to be healthy. Redis failure degrades but doesn't halt.
    
    status_code = status.HTTP_200_OK if is_healthy else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "healthy" if is_healthy else "unhealthy",
            "service": "user-service",
            "components": {
                "database": "connected" if db_ok else "disconnected",
                "redis": "connected" if redis_ok else "disconnected"
            }
        }
    )

@app.post("/users", response_model=User, status_code=status.HTTP_201_CREATED)
def create_user(user: User, session: Session = Depends(get_session)):
    try:
        session.add(user)
        session.commit()
        session.refresh(user)
        
        # Cache the user profile
        try:
            redis_client.setex(f"user:{user.id}", 60, user.model_dump_json())
        except Exception as e:
            logger.warning(f"Failed to cache user in Redis: {e}")
            
        return user
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )

@app.get("/users/{user_id}", response_model=User)
def get_user(user_id: int, session: Session = Depends(get_session)):
    # 1. Try Redis cache first
    try:
        cached_user = redis_client.get(f"user:{user_id}")
        if cached_user:
            logger.info(f"Cache HIT for user {user_id}")
            CACHE_HIT_COUNTER.inc()
            return User(**json.loads(cached_user))
    except Exception as e:
        logger.warning(f"Redis connection error on get user: {e}")
    
    # 2. Cache miss or Redis down: query PostgreSQL
    logger.info(f"Cache MISS/BYPASS for user {user_id}")
    CACHE_MISS_COUNTER.inc()
    try:
        user = session.get(User, user_id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User {user_id} not found"
            )
        
        # Write back to cache
        try:
            redis_client.setex(f"user:{user_id}", 60, user.model_dump_json())
        except Exception as e:
            logger.warning(f"Failed to populate Redis cache after miss: {e}")
            
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Database error on fetching user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database query failed"
        )

@app.get("/users", response_model=List[User])
def get_all_users(session: Session = Depends(get_session)):
    try:
        users = session.exec(select(User)).all()
        return users
    except Exception as e:
        logger.error(f"Database error on listing users: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database list query failed"
        )

@app.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(user_id: int, session: Session = Depends(get_session)):
    try:
        user = session.get(User, user_id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User {user_id} not found"
            )
        session.delete(user)
        session.commit()
        
        # Evict cache
        try:
            redis_client.delete(f"user:{user_id}")
        except Exception as e:
            logger.warning(f"Failed to evict Redis cache: {e}")
            
        return None
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Database error on deleting user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database delete failed"
        )
