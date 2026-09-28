"""
Data Models and Database Schemas for University Idempotency Guard
Contains Pydantic schemas for API validation and SQLAlchemy ORM models.
"""

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, String, Float, DateTime, Index
from sqlalchemy.orm import declarative_base

Base = declarative_base()


# =====================================================================
# Pydantic Schemas (API Data Transfer Objects)
# =====================================================================

class EventPayload(BaseModel):
    """
    Synthetic Event Schema representing business events across
    university subsystems (Admissions, Academics, Finance, Alumni).
    Strictly free of personally identifiable information (PII).
    """
    message_id: str = Field(..., description="Transport delivery message identifier (e.g. MSG-000001)")
    transaction_id: str = Field(..., description="Domain business transaction identifier (e.g. TXN-000001)")
    event_type: str = Field(..., description="Type of event: FEE_PAYMENT, ADMISSION_CREATED, etc.")
    source_system: str = Field(..., description="Originating university system: FINANCE, ADMISSIONS, ACADEMICS, ALUMNI")
    amount: Optional[float] = Field(None, description="Monetary transaction amount (applicable for FEE_PAYMENT)")
    event_timestamp: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc), description="Timestamp event was generated")
    event_version: int = Field(default=1, description="Payload schema version")

    model_config = {
        "json_schema_extra": {
            "example": {
                "message_id": "MSG-000001",
                "transaction_id": "TXN-000001",
                "event_type": "FEE_PAYMENT",
                "source_system": "FINANCE",
                "amount": 5000.0,
                "event_timestamp": "2026-09-07T10:00:00Z",
                "event_version": 1
            }
        }
    }


class EventResponse(BaseModel):
    """
    Structured acknowledgement response returned to event queue / broker.
    """
    status: str = Field(..., description="Processing outcome: 'processed', 'duplicate', 'out_of_order', 'stale', 'expired', 'failed', or 'dlq'")
    transaction_id: str
    message_id: str
    message: str
    event_version: Optional[int] = Field(default=1, description="Event version tracked")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class MetricsSummary(BaseModel):
    """
    System metrics model for operational monitoring.
    """
    total_events_received: int
    unique_transactions_processed: int
    duplicate_events_detected: int
    failed_events: int
    out_of_order_events: int = 0
    expired_events: int = 0
    dlq_events: int = 0
    prevention_rate_percent: float


# =====================================================================
# SQLAlchemy ORM Entities (Database Tables)
# =====================================================================

class ProcessedEventRecord(Base):
    """
    Idempotency Guard tracking table (Layer 1).
    Tracks every incoming message delivery and associates it with the
    underlying business transaction ID and processing lifecycle state.
    """
    __tablename__ = "processed_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(String(64), nullable=False, index=True)
    transaction_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(64), nullable=False)
    source_system = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, default="RECEIVED")  # RECEIVED, PROCESSING, PROCESSED, DUPLICATE, OUT_OF_ORDER, STALE, EXPIRED, FAILED, DLQ
    event_version = Column(Integer, default=1, nullable=False)
    first_seen_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    processed_at = Column(DateTime, nullable=True)
    retry_count = Column(Integer, default=0)

    __table_args__ = (
        Index("idx_processed_event_txn", "transaction_id"),
        Index("idx_processed_event_msg", "message_id"),
    )


class TransactionRecord(Base):
    """
    Target business ledger table (Layer 2).
    Represents actual committed business outcomes.
    Enforces a relational UNIQUE constraint on transaction_id as an ACID
    safety-net preventing duplicate financial/academic records.
    """
    __tablename__ = "transactions"

    transaction_id = Column(String(64), primary_key=True, unique=True, nullable=False)
    event_type = Column(String(64), nullable=False)
    source_system = Column(String(64), nullable=False)
    amount = Column(Float, nullable=True)
    event_version = Column(Integer, default=1, nullable=False)
    processed_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    status = Column(String(32), default="COMPLETED", nullable=False)


class BaselineTransactionRecord(Base):
    """
    Naive target table used for Baseline Demonstration.
    Intentionally omits unique constraints to demonstrate how duplicate
    messages cause duplicate business transactions and financial discrepancies.
    """
    __tablename__ = "baseline_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(String(64), nullable=False)
    transaction_id = Column(String(64), nullable=False)  # NO UNIQUE CONSTRAINT!
    event_type = Column(String(64), nullable=False)
    source_system = Column(String(64), nullable=False)
    amount = Column(Float, nullable=True)
    event_version = Column(Integer, default=1, nullable=False)
    processed_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    status = Column(String(32), default="COMPLETED", nullable=False)


class DeadLetterEventRecord(Base):
    """
    Dead Letter Queue (DLQ) persistent storage (Improvement 5).
    Safely captures poisoned or repeatedly failing events after MAX_RETRIES.
    """
    __tablename__ = "dead_letter_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(String(64), nullable=False, index=True)
    transaction_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(64), nullable=False)
    source_system = Column(String(64), nullable=True)
    failure_reason = Column(String(256), nullable=False)
    retry_count = Column(Integer, default=0, nullable=False)
    failed_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    payload = Column(String(1024), nullable=True)

    __table_args__ = (
        Index("idx_dlq_txn", "transaction_id"),
        Index("idx_dlq_msg", "message_id"),
    )

