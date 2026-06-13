import os
import time
import logging
from sqlmodel import create_engine, SQLModel, Session
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/user_db")

logger = logging.getLogger("user-service")
logging.basicConfig(level=logging.INFO)

# Pool configuration for robustness
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_timeout=3,
    pool_recycle=1800
)

def init_db():
    """Attempt to initialize the database tables with retry logic."""
    retries = 5
    while retries > 0:
        try:
            logger.info(f"Connecting to database at {DATABASE_URL}...")
            SQLModel.metadata.create_all(engine)
            logger.info("Database tables initialized successfully.")
            return True
        except OperationalError as e:
            retries -= 1
            logger.warning(f"Database connection failed. Retries left: {retries}. Error: {e}")
            if retries == 0:
                logger.error("Could not connect to database on startup. Continuing boot in degraded state.")
                return False
            time.sleep(2)

def get_session():
    with Session(engine) as session:
        yield session

def check_db_health():
    """Verify database connection is alive."""
    try:
        with Session(engine) as session:
            # Simple low-cost query
            session.execute(text("SELECT 1"))
            return True
    except Exception as e:
        logger.error(f"DB Health check failed: {e}")
        return False
