"""
Database connection and session configuration for University Idempotency Guard.
Uses SQLite with SQLAlchemy ORM.
Implements SQLite WAL mode, foreign keys, busy_timeout, safe migrations, and retry backoff.
"""

import os
import time
import random
import logging
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError
from models import Base

logger = logging.getLogger("university_guard.database")

DB_PATH = os.environ.get("DB_PATH", "university.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

# Create SQLite engine with connection timeout of 30 seconds for concurrent requests
engine = create_engine(
    DATABASE_URL,
    connect_args={
        "check_same_thread": False,
        "timeout": 30.0
    }
)

# Enable foreign keys, WAL mode, and busy timeout on SQLite connections
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout = 30000")  # 30-second busy timeout
    cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db(target_engine=engine):
    """
    Create all database tables and perform non-destructive schema migrations.
    Safely adds new columns (e.g. event_version) without dropping or altering existing records.
    """
    Base.metadata.create_all(bind=target_engine)
    
    # Safe schema migration: add event_version column if missing in existing tables
    with target_engine.connect() as conn:
        for table_name in ["transactions", "processed_events", "baseline_transactions"]:
            try:
                res = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
                existing_cols = [row[1] for row in res]
                if existing_cols and "event_version" not in existing_cols:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN event_version INTEGER DEFAULT 1 NOT NULL"))
                    conn.commit()
            except Exception as e:
                logger.warning(f"Safe migration note on {table_name}: {e}")


def with_db_retry(operation, max_retries=5, base_delay=0.05, max_delay=1.0):
    """
    Executes a callable with exponential backoff and jitter to gracefully handle
    transient SQLite 'database is locked' operational errors under high concurrency.
    """
    last_err = None
    for attempt in range(max_retries):
        try:
            return operation()
        except OperationalError as oe:
            err_msg = str(oe).lower()
            if "locked" in err_msg or "busy" in err_msg:
                last_err = oe
                sleep_time = min(max_delay, base_delay * (2 ** attempt)) + random.uniform(0.01, 0.05)
                logger.warning(f"Database locked, retrying in {sleep_time:.3f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(sleep_time)
            else:
                raise
        except Exception:
            raise
    if last_err:
        raise last_err


def get_db():
    """FastAPI dependency for yielding transactional db session."""
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()

