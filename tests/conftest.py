"""Shared pytest fixtures for CrashBoard.

Provides in-memory SQLite session factories so the service apps can be exercised
through FastAPI's TestClient without a live Postgres/Redis/Kafka stack.
"""

import os
import sys

import pytest
from sqlmodel import SQLModel, Session, create_engine
from sqlalchemy.pool import StaticPool

# Make the test harness importable as a top-level module.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def sqlite_session_factory():
    """Return (engine, session_factory) backed by a shared in-memory SQLite db.

    All models registered on the global SQLModel metadata are created; using a
    StaticPool keeps the same connection across the TestClient's threads.
    """
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)

    def factory():
        with Session(engine) as session:
            yield session

    yield engine, factory
    SQLModel.metadata.drop_all(engine)
    engine.dispose()
