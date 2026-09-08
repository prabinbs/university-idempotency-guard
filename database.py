"""
Database connection and session configuration for University Idempotency Guard.
Uses SQLite with SQLAlchemy ORM.
"""

import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from models import Base

DB_PATH = os.environ.get("DB_PATH", "university.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

# Create SQLite engine. 'check_same_thread=False' allows multi-threaded requests in FastAPI
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}
)

# Enable foreign keys and WAL mode on SQLite connections
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db(target_engine=engine):
    """Create all database tables."""
    Base.metadata.create_all(bind=target_engine)


def get_db():
    """FastAPI dependency for yielding transactional db session."""
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()
