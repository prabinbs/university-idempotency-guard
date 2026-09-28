"""
Automated Test Suite for University Idempotent Event Guard.
Expanded for Review 1 improvements:
- Normal event processing
- Exact duplicate handling
- Producer retry with new message ID
- Multi-transaction and multi-subsystem independence
- Dynamic Next ID generation
- Concurrent lock contention and race condition handling (threading)
- Out-of-order event version progression and stale protection (v1->v2, v2->v1, v2->v2, v3->v1)
- Freshness and idempotency window expiration (boundary, expired, clock skew)
- Failure simulation, retry limit, and Dead Letter Queue (DLQ)
- Controlled 100-event Retry Storm simulation
- Message broker simulator integration
"""

import os
import time
import pytest
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from models import Base, ProcessedEventRecord, TransactionRecord, DeadLetterEventRecord
from database import get_db
from app import app
from message_broker import MessageBroker
from idempotency_guard import IdempotencyGuard


# Set up isolated SQLite database with WAL mode for concurrency test compatibility
TEST_DB_FILE = "test_isolated.db"
TEST_DATABASE_URL = f"sqlite:///{TEST_DB_FILE}"

test_engine = create_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30.0}
)

@event.listens_for(test_engine, "connect")
def set_sqlite_test_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout = 30000")
    cursor.close()

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(autouse=True)
def setup_database():
    """Create fresh tables for every test."""
    Base.metadata.create_all(bind=test_engine)
    yield
    Base.metadata.drop_all(bind=test_engine)
    db = TestingSessionLocal()
    db.close()
    test_engine.dispose()



@pytest.fixture
def client():
    return TestClient(app)


# =====================================================================
# Core Baseline & Deduplication Tests
# =====================================================================

def test_1_normal_event(client):
    """
    Test 1 – Normal Event
    Send one event with fresh timestamp.
    Expected:
    - Status: 'processed'
    - Exactly 1 target record created in the database.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "message_id": "MSG-000001",
        "transaction_id": "TXN-000001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 5000.0,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    }

    response = client.post("/events", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "processed"
    assert data["transaction_id"] == "TXN-000001"
    assert data["message_id"] == "MSG-000001"

    # Verify target record in DB
    db = TestingSessionLocal()
    target_records = db.query(TransactionRecord).filter(
        TransactionRecord.transaction_id == "TXN-000001"
    ).all()
    assert len(target_records) == 1
    assert target_records[0].amount == 5000.0
    assert target_records[0].status == "COMPLETED"
    assert target_records[0].event_version == 1

    # Verify idempotency store status
    event_records = db.query(ProcessedEventRecord).filter(
        ProcessedEventRecord.transaction_id == "TXN-000001"
    ).all()
    assert len(event_records) == 1
    assert event_records[0].status == "PROCESSED"
    db.close()


def test_2_exact_duplicate(client):
    """
    Test 2 – Exact Duplicate
    Send exactly the same message twice.
    Expected:
    - 1st response: 'processed'
    - 2nd response: 'duplicate'
    - Only 1 target record in the business ledger.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "message_id": "MSG-000002",
        "transaction_id": "TXN-000002",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 12000.0,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    }

    # First attempt
    res1 = client.post("/events", json=payload)
    assert res1.status_code == 200
    assert res1.json()["status"] == "processed"

    # Second attempt (exact duplicate)
    res2 = client.post("/events", json=payload)
    assert res2.status_code == 200
    assert res2.json()["status"] == "duplicate"
    assert res2.json()["transaction_id"] == "TXN-000002"

    # Verify only 1 target record exists
    db = TestingSessionLocal()
    target_count = db.query(TransactionRecord).filter(
        TransactionRecord.transaction_id == "TXN-000002"
    ).count()
    assert target_count == 1
    db.close()


def test_3_retry_with_new_message_id(client):
    """
    Test 3 – Retry With New Message ID (CRITICAL TEST)
    Send:
    MSG-001 -> TXN-001
    Then:
    MSG-002 -> TXN-001
    Expected:
    - 1st response: 'processed'
    - 2nd response: 'duplicate'
    - Only 1 business transaction record created in the target table.
    - Proves the guard does NOT rely solely on message_id!
    """
    now = datetime.now(timezone.utc)
    first_attempt = {
        "message_id": "MSG-100001",
        "transaction_id": "TXN-300001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 25000.0,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    }

    # First message delivery
    res1 = client.post("/events", json=first_attempt)
    assert res1.status_code == 200
    assert res1.json()["status"] == "processed"

    # Producer retries due to lost ACK, generating a fresh message envelope
    retry_attempt = {
        "message_id": "MSG-100002",  # New message_id!
        "transaction_id": "TXN-300001",  # Same canonical transaction_id
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 25000.0,
        "event_timestamp": (now + timedelta(seconds=10)).isoformat(),
        "event_version": 1
    }

    res2 = client.post("/events", json=retry_attempt)
    assert res2.status_code == 200
    assert res2.json()["status"] == "duplicate"
    assert "Duplicate business transaction detected" in res2.json()["message"]

    # Target business table MUST have exactly 1 record for TXN-300001
    db = TestingSessionLocal()
    targets = db.query(TransactionRecord).filter(
        TransactionRecord.transaction_id == "TXN-300001"
    ).all()
    assert len(targets) == 1
    assert targets[0].amount == 25000.0

    # Idempotency store should record the duplicate message and increment retry_count
    audit_records = db.query(ProcessedEventRecord).filter(
        ProcessedEventRecord.transaction_id == "TXN-300001"
    ).all()
    assert len(audit_records) == 2  # Original PROCESSED + Retry DUPLICATE
    assert audit_records[1].retry_count >= 1
    db.close()


def test_4_different_transactions(client):
    """
    Test 4 – Different Transactions
    Send:
    MSG-001 -> TXN-001
    MSG-002 -> TXN-002
    Expected:
    - Both responses: 'processed'
    - Exactly 2 distinct target records created in the target table.
    """
    now = datetime.now(timezone.utc)
    event_a = {
        "message_id": "MSG-000004A",
        "transaction_id": "TXN-000004A",
        "event_type": "COURSE_ENROLLED",
        "source_system": "ACADEMICS",
        "amount": None,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    }

    event_b = {
        "message_id": "MSG-000004B",
        "transaction_id": "TXN-000004B",
        "event_type": "ADMISSION_CREATED",
        "source_system": "ADMISSIONS",
        "amount": None,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    }

    res_a = client.post("/events", json=event_a)
    assert res_a.status_code == 200
    assert res_a.json()["status"] == "processed"

    res_b = client.post("/events", json=event_b)
    assert res_b.status_code == 200
    assert res_b.json()["status"] == "processed"

    db = TestingSessionLocal()
    assert db.query(TransactionRecord).count() == 2
    assert db.query(TransactionRecord).filter(
        TransactionRecord.transaction_id == "TXN-000004A"
    ).count() == 1
    assert db.query(TransactionRecord).filter(
        TransactionRecord.transaction_id == "TXN-000004B"
    ).count() == 1
    db.close()


def test_5_multiple_subsystems(client):
    """
    Test event processing across Admissions, Academics, Finance, and Alumni.
    """
    now = datetime.now(timezone.utc)
    subsystems = [
        ("ADMISSIONS", "ADMISSION_CREATED", None),
        ("ACADEMICS", "COURSE_ENROLLED", None),
        ("ACADEMICS", "MARK_UPDATED", None),
        ("ALUMNI", "ALUMNI_UPDATED", None),
        ("FINANCE", "FEE_PAYMENT", 15000.0)
    ]

    for idx, (source, etype, amt) in enumerate(subsystems):
        payload = {
            "message_id": f"MSG-SUB-{idx}",
            "transaction_id": f"TXN-SUB-{idx}",
            "event_type": etype,
            "source_system": source,
            "amount": amt,
            "event_timestamp": now.isoformat(),
            "event_version": 1
        }
        res = client.post("/events", json=payload)
        assert res.status_code == 200
        assert res.json()["status"] == "processed"

    db = TestingSessionLocal()
    assert db.query(TransactionRecord).count() == len(subsystems)
    db.close()


def test_6_next_id_endpoint(client):
    """
    Test 6 - Dynamic Next ID Generation
    Verify /next-id returns consecutive IDs based on DB state.
    """
    res1 = client.get("/next-id")
    assert res1.status_code == 200
    data1 = res1.json()
    assert data1["next_transaction_id"] == "TXN-200001"
    assert data1["next_message_id"] == "MSG-100001"

    # Insert an event
    payload = {
        "message_id": "MSG-100001",
        "transaction_id": "TXN-200001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 5000.0,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_version": 1
    }
    client.post("/events", json=payload)

    # Next ID should now be incremented
    res2 = client.get("/next-id")
    assert res2.status_code == 200
    data2 = res2.json()
    assert data2["next_transaction_id"] == "TXN-200002"
    assert data2["next_message_id"] == "MSG-100002"


# =====================================================================
# Improvement 1: Concurrency & Lock Contention Tests
# =====================================================================

def test_7_concurrent_duplicate_requests(client):
    """
    Improvement 1 – Concurrent Race Condition Handling
    Send the exact same transaction (TXN-999999) concurrently from 10 threads.
    Expected:
    - Target records created = 1
    - Duplicate business outcomes = 0
    - All other 9 requests return status: 'duplicate'
    """
    now = datetime.now(timezone.utc)
    txn_id = "TXN-999999"

    def send_attempt(thread_idx):
        payload = {
            "message_id": f"MSG-CONCURRENT-{thread_idx}",
            "transaction_id": txn_id,
            "event_type": "FEE_PAYMENT",
            "source_system": "FINANCE",
            "amount": 50000.0,
            "event_timestamp": now.isoformat(),
            "event_version": 1
        }
        res = client.post("/events", json=payload)
        return res.json()

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(send_attempt, range(10)))

    statuses = [r["status"] for r in results]
    processed_count = statuses.count("processed")
    duplicate_count = statuses.count("duplicate")

    assert processed_count == 1, f"Expected exactly 1 processed event, got {processed_count}"
    assert duplicate_count == 9, f"Expected 9 duplicates intercepted, got {duplicate_count}"

    # Verify database level
    db = TestingSessionLocal()
    targets = db.query(TransactionRecord).filter(
        TransactionRecord.transaction_id == txn_id
    ).all()
    assert len(targets) == 1, "Relational unique constraint violated in target records!"
    assert targets[0].amount == 50000.0
    db.close()


# =====================================================================
# Improvement 2: Out-of-Order Event Handling Tests
# =====================================================================

def test_8_out_of_order_version_1_then_version_2(client):
    """
    Improvement 2 Test 1: Version 1 -> Version 2 (In-order progression)
    Expected:
    - Both processed in correct order. Target reflects version 2.
    """
    now = datetime.now(timezone.utc)
    txn_id = "TXN-ORDER-001"

    # Step 1: Version 1 (CREATED)
    v1_payload = {
        "message_id": "MSG-ORD-01A",
        "transaction_id": txn_id,
        "event_type": "FEE_PAYMENT_CREATED",
        "source_system": "FINANCE",
        "amount": 10000.0,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    }
    r1 = client.post("/events", json=v1_payload)
    assert r1.status_code == 200
    assert r1.json()["status"] == "processed"

    # Step 2: Version 2 (CONFIRMED)
    v2_payload = {
        "message_id": "MSG-ORD-01B",
        "transaction_id": txn_id,
        "event_type": "FEE_PAYMENT_CONFIRMED",
        "source_system": "FINANCE",
        "amount": 10000.0,
        "event_timestamp": (now + timedelta(seconds=5)).isoformat(),
        "event_version": 2
    }
    r2 = client.post("/events", json=v2_payload)
    assert r2.status_code == 200
    assert r2.json()["status"] == "processed"

    db = TestingSessionLocal()
    target = db.query(TransactionRecord).filter(TransactionRecord.transaction_id == txn_id).one()
    assert target.event_version == 2
    assert target.event_type == "FEE_PAYMENT_CONFIRMED"
    db.close()


def test_9_out_of_order_version_2_first_then_version_1(client):
    """
    Improvement 2 Test 2: Version 2 -> Version 1 (Out-of-order arrival)
    Expected:
    - Version 2 arrives first: processed.
    - Version 1 arrives later: rejected as OUT_OF_ORDER / STALE.
    - Version 2 remains the latest state; Version 1 does NOT overwrite it.
    """
    now = datetime.now(timezone.utc)
    txn_id = "TXN-ORDER-002"

    # Version 2 arrives FIRST
    v2_payload = {
        "message_id": "MSG-ORD-02A",
        "transaction_id": txn_id,
        "event_type": "FEE_PAYMENT_CONFIRMED",
        "source_system": "FINANCE",
        "amount": 15000.0,
        "event_timestamp": now.isoformat(),
        "event_version": 2
    }
    r2 = client.post("/events", json=v2_payload)
    assert r2.status_code == 200
    assert r2.json()["status"] == "processed"

    # Version 1 arrives LATER
    v1_payload = {
        "message_id": "MSG-ORD-02B",
        "transaction_id": txn_id,
        "event_type": "FEE_PAYMENT_CREATED",
        "source_system": "FINANCE",
        "amount": 15000.0,
        "event_timestamp": (now - timedelta(seconds=10)).isoformat(),
        "event_version": 1
    }
    r1 = client.post("/events", json=v1_payload)
    assert r1.status_code == 200
    assert r1.json()["status"] == "out_of_order"
    assert "older than stored version 2" in r1.json()["message"]

    # Target record MUST remain at Version 2!
    db = TestingSessionLocal()
    target = db.query(TransactionRecord).filter(TransactionRecord.transaction_id == txn_id).one()
    assert target.event_version == 2
    assert target.event_type == "FEE_PAYMENT_CONFIRMED"
    db.close()


def test_10_out_of_order_version_2_duplicate(client):
    """
    Improvement 2 Test 3: Version 2 -> Version 2 (Same version duplicate)
    Expected:
    - 1st response: 'processed'
    - 2nd response: 'duplicate'
    """
    now = datetime.now(timezone.utc)
    txn_id = "TXN-ORDER-003"
    payload = {
        "message_id": "MSG-ORD-03A",
        "transaction_id": txn_id,
        "event_type": "FEE_PAYMENT_CONFIRMED",
        "source_system": "FINANCE",
        "amount": 8000.0,
        "event_timestamp": now.isoformat(),
        "event_version": 2
    }
    r1 = client.post("/events", json=payload)
    assert r1.json()["status"] == "processed"

    # Send exact same version again
    payload["message_id"] = "MSG-ORD-03B"
    r2 = client.post("/events", json=payload)
    assert r2.json()["status"] == "duplicate"


def test_11_out_of_order_version_3_then_version_1(client):
    """
    Improvement 2 Test 4: Version 3 -> Version 1 (Multi-version stale jump)
    Expected:
    - Version 3: processed.
    - Version 1: rejected / classified as out_of_order.
    """
    now = datetime.now(timezone.utc)
    txn_id = "TXN-ORDER-004"

    # Version 3 arrives
    v3 = {
        "message_id": "MSG-ORD-04A",
        "transaction_id": txn_id,
        "event_type": "FEE_PAYMENT_SETTLED",
        "source_system": "FINANCE",
        "amount": 30000.0,
        "event_timestamp": now.isoformat(),
        "event_version": 3
    }
    r3 = client.post("/events", json=v3)
    assert r3.json()["status"] == "processed"

    # Old Version 1 arrives late
    v1 = {
        "message_id": "MSG-ORD-04B",
        "transaction_id": txn_id,
        "event_type": "FEE_PAYMENT_CREATED",
        "source_system": "FINANCE",
        "amount": 30000.0,
        "event_timestamp": (now - timedelta(seconds=60)).isoformat(),
        "event_version": 1
    }
    r1 = client.post("/events", json=v1)
    assert r1.json()["status"] == "out_of_order"

    # State preserved at Version 3
    db = TestingSessionLocal()
    target = db.query(TransactionRecord).filter(TransactionRecord.transaction_id == txn_id).one()
    assert target.event_version == 3
    assert target.event_type == "FEE_PAYMENT_SETTLED"
    db.close()


# =====================================================================
# Improvement 3: Idempotency Window & Expiration Tests
# =====================================================================

def test_12_event_expiration_fresh_vs_old(client):
    """
    Improvement 3:
    - Event 5 minutes old (< 30 min window): accepted as processed.
    - Event 45 minutes old (> 30 min window): rejected as expired.
    - Historical records not deleted.
    """
    now = datetime.now(timezone.utc)

    # 1. Fresh event: 5 minutes old
    fresh_payload = {
        "message_id": "MSG-FRESH-001",
        "transaction_id": "TXN-FRESH-001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 5000.0,
        "event_timestamp": (now - timedelta(minutes=5)).isoformat(),
        "event_version": 1
    }
    rf = client.post("/events", json=fresh_payload)
    assert rf.json()["status"] == "processed"

    # 2. Old event: 45 minutes old
    old_payload = {
        "message_id": "MSG-OLD-001",
        "transaction_id": "TXN-OLD-001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 5000.0,
        "event_timestamp": (now - timedelta(minutes=45)).isoformat(),
        "event_version": 1
    }
    ro = client.post("/events", json=old_payload)
    assert ro.json()["status"] == "expired"
    assert "older than configured idempotency window" in ro.json()["message"]

    # Target table MUST NOT contain TXN-OLD-001
    db = TestingSessionLocal()
    assert db.query(TransactionRecord).filter(TransactionRecord.transaction_id == "TXN-OLD-001").count() == 0
    # But TXN-FRESH-001 must exist intact
    assert db.query(TransactionRecord).filter(TransactionRecord.transaction_id == "TXN-FRESH-001").count() == 1
    db.close()


def test_13_event_boundary_window(client):
    """
    Improvement 3: Event exactly within the 30-minute boundary (e.g. 29.5 minutes old).
    Accepted.
    """
    now = datetime.now(timezone.utc)
    boundary_payload = {
        "message_id": "MSG-BOUND-001",
        "transaction_id": "TXN-BOUND-001",
        "event_type": "COURSE_ENROLLED",
        "source_system": "ACADEMICS",
        "amount": None,
        "event_timestamp": (now - timedelta(minutes=29, seconds=30)).isoformat(),
        "event_version": 1
    }
    rb = client.post("/events", json=boundary_payload)
    assert rb.json()["status"] == "processed"


def test_14_future_timestamp_clock_skew(client):
    """
    Improvement 3: Clock-skew protection.
    Event timestamp 30 minutes in the future (> 5 min tolerance) rejected safely.
    """
    now = datetime.now(timezone.utc)
    skewed_payload = {
        "message_id": "MSG-SKEW-001",
        "transaction_id": "TXN-SKEW-001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 2000.0,
        "event_timestamp": (now + timedelta(minutes=30)).isoformat(),
        "event_version": 1
    }
    rs = client.post("/events", json=skewed_payload)
    assert rs.json()["status"] == "failed"
    assert "Clock-skew rejected" in rs.json()["message"]


# =====================================================================
# Improvement 5: Dead Letter Queue (DLQ) & Failure Tests
# =====================================================================

def test_15_failure_and_dlq_routing():
    """
    Improvement 5:
    Simulate event failure. Verify retry increment, and upon exceeding MAX_RETRIES (3),
    event is routed to dead_letter_events table and retries cease.
    """
    db = TestingSessionLocal()
    guard = IdempotencyGuard(db=db, max_retries=3)
    now = datetime.now(timezone.utc)

    event_payload = {
        "message_id": "MSG-POISON-001",
        "transaction_id": "TXN-POISON-001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 5000.0,
        "event_timestamp": now.isoformat(),
        "event_version": 1,
        "simulate_failure": True,
        "failure_reason": "Banking gateway HTTP 500 timeout"
    }

    # Attempt 1: Failed (retry_count = 1)
    r1 = guard.process_event(event_payload)
    assert r1["status"] == "failed"
    assert r1["retry_count"] == 1

    # Attempt 2: Failed (retry_count = 2)
    event_payload["retry_count"] = 1
    r2 = guard.process_event(event_payload)
    assert r2["status"] == "failed"
    assert r2["retry_count"] == 2

    # Attempt 3: Exceeds max retries -> moves to DLQ
    event_payload["retry_count"] = 2
    r3 = guard.process_event(event_payload)
    assert r3["status"] == "dlq"
    assert "Moved to Dead Letter Queue" in r3["message"]

    # Verify DLQ table records
    dlq_records = guard.get_dlq_events()
    assert len(dlq_records) >= 1
    assert dlq_records[0]["transaction_id"] == "TXN-POISON-001"
    assert "Exceeded max retries" in dlq_records[0]["failure_reason"]
    db.close()


# =====================================================================
# Improvement 6: Retry Storm Simulation Test
# =====================================================================

def test_16_retry_storm_simulation(client):
    """
    Improvement 6:
    Controlled retry-storm: 100 duplicate delivery attempts for same transaction (TXN-500001).
    Expected:
    - Events received = 100
    - Unique business transactions = 1
    - Target records = 1
    - Duplicate business outcomes = 0
    - Duplicates prevented = 99
    - Duplicate Prevention Rate = 100.0%
    """
    now = datetime.now(timezone.utc)
    storm_txn = "TXN-500001"

    results = []
    for i in range(1, 101):
        payload = {
            "message_id": f"MSG-STORM-{i:06d}",
            "transaction_id": storm_txn,
            "event_type": "FEE_PAYMENT",
            "source_system": "FINANCE",
            "amount": 25000.0,
            "event_timestamp": now.isoformat(),
            "event_version": 1
        }
        res = client.post("/events", json=payload)
        results.append(res.json())

    processed_count = sum(1 for r in results if r["status"] == "processed")
    duplicate_count = sum(1 for r in results if r["status"] == "duplicate")

    assert processed_count == 1, "Expected exactly 1 target record created during retry storm!"
    assert duplicate_count == 99, "Expected 99 duplicate attempts prevented!"

    prevention_rate = (duplicate_count / 99) * 100.0
    assert prevention_rate == 100.0

    db = TestingSessionLocal()
    target_count = db.query(TransactionRecord).filter(
        TransactionRecord.transaction_id == storm_txn
    ).count()
    assert target_count == 1
    db.close()


# =====================================================================
# Improvement 4: Message Broker Simulation Test
# =====================================================================

def test_17_message_broker_simulation():
    """
    Improvement 4:
    Verify message broker simulation pipeline:
    Producer -> Broker.enqueue -> Broker.consume -> Guard.process_event -> Target DB -> ACK
    """
    db = TestingSessionLocal()
    guard = IdempotencyGuard(db=db)
    broker = MessageBroker(max_retries=3)
    now = datetime.now(timezone.utc)

    # Enqueue 3 events: 2 distinct transactions, 1 duplicate
    broker.enqueue({
        "message_id": "MSG-B-001",
        "transaction_id": "TXN-B-001",
        "event_type": "ADMISSION_CREATED",
        "source_system": "ADMISSIONS",
        "amount": None,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    })
    broker.enqueue({
        "message_id": "MSG-B-002",
        "transaction_id": "TXN-B-002",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 7500.0,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    })
    # Duplicate envelope for TXN-B-001
    broker.enqueue({
        "message_id": "MSG-B-003",
        "transaction_id": "TXN-B-001",
        "event_type": "ADMISSION_CREATED",
        "source_system": "ADMISSIONS",
        "amount": None,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    })

    # Process all via broker
    results = broker.process_all(guard)
    assert len(results) == 3
    assert results[0]["status"] == "processed"
    assert results[0]["broker_ack"] is True
    assert results[1]["status"] == "processed"
    assert results[1]["broker_ack"] is True
    assert results[2]["status"] == "duplicate"
    assert results[2]["broker_ack"] is True

    # Broker acknowledged ledger
    assert len(broker.acknowledged) == 3
    assert len(broker.queue) == 0

    # Target records in DB must be exactly 2
    assert db.query(TransactionRecord).count() == 2
    db.close()


# =====================================================================
# Review 2 – Additional Targeted Tests
# =====================================================================

def test_18_twenty_concurrent_requests(client):
    """
    Review 2 – Phase 2: 20 Concurrent Requests Race Condition
    Send TXN-CONCURRENT-001 from 20 threads simultaneously.
    Expected:
    - Exactly 1 target record created.
    - 0 duplicate business outcomes.
    - All 19 extras return 'duplicate'.
    """
    txn_id = "TXN-CONCURRENT-001"
    now = datetime.now(timezone.utc)

    def send_attempt(idx):
        payload = {
            "message_id": f"MSG-C{idx:03d}",
            "transaction_id": txn_id,
            "event_type": "FEE_PAYMENT",
            "source_system": "FINANCE",
            "amount": 50000.0,
            "event_timestamp": now.isoformat(),
            "event_version": 1
        }
        return client.post("/events", json=payload).json()

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(send_attempt, range(1, 21)))

    statuses = [r["status"] for r in results]
    processed_count = statuses.count("processed")
    duplicate_count = statuses.count("duplicate")

    assert processed_count == 1, f"Expected exactly 1 processed; got {processed_count}"
    assert duplicate_count == 19, f"Expected 19 duplicates; got {duplicate_count}"

    db = TestingSessionLocal()
    target_count = db.query(TransactionRecord).filter(
        TransactionRecord.transaction_id == txn_id
    ).count()
    assert target_count == 1, "UNIQUE constraint violated: more than 1 target record created!"
    db.close()


def test_19_ack_loss_simulation_endpoint(client):
    """
    Review 2 – Phase 5: ACK Loss Simulation via /simulate/ack-loss
    The endpoint simulates:
      1. Original event committed to DB.
      2. ACK dropped/lost in transit.
      3. Producer retries with new message ID.
      4. Guard intercepts retry, returns duplicate.
      5. DB still holds exactly 1 target record.
    """
    response = client.post("/simulate/ack-loss")
    assert response.status_code == 200
    data = response.json()

    # Step 1: original event processed
    assert data["step_1_original"]["status"] == "processed"

    # Step 2: ACK explicitly flagged as not delivered
    assert data["step_2_ack_simulation"]["ack_delivered_to_source"] is False

    # Step 3: retry intercepted as duplicate
    assert data["step_3_source_retry"]["status"] == "duplicate"

    # Verification: idempotency preserved
    assert data["verification"]["target_records_in_db"] == 1
    assert data["verification"]["duplicate_business_outcomes"] == 0
    assert data["verification"]["idempotency_preserved"] is True


def test_20_simulate_failure_and_dlq(client):
    """
    Review 2 – Phase 7 & 8: Failure simulation → 3 attempts → DLQ
    Endpoint /simulate/failure runs 3 attempts with simulate_failure=True.
    Expected: final_status = 'dlq', moved_to_dlq = True, target_records_created = 0.
    """
    response = client.post("/simulate/failure")
    assert response.status_code == 200
    data = response.json()

    # At least one attempt logged
    assert len(data["attempts"]) >= 1

    # Final disposition should be DLQ after exhausting retries
    assert data["final_status"] == "dlq", f"Expected 'dlq', got '{data['final_status']}'"
    assert data["moved_to_dlq"] is True

    # No business transaction should be committed when event always fails
    assert data["target_records_created"] == 0


def test_21_dlq_replay_endpoint(client):
    """
    Review 2 – Phase 8: DLQ Replay via /dlq/{id}/retry
    1. Create a DLQ entry using /simulate/failure.
    2. Fetch DLQ listing to get the id.
    3. Replay via /dlq/{id}/retry.
    4. DLQ record is removed after successful replay.
    """
    # Create DLQ entry
    fail_res = client.post("/simulate/failure")
    assert fail_res.status_code == 200
    fail_data = fail_res.json()
    assert fail_data["moved_to_dlq"] is True

    # Fetch DLQ listing
    dlq_res = client.get("/dlq")
    assert dlq_res.status_code == 200
    dlq_items = dlq_res.json()
    assert len(dlq_items) >= 1

    # Get first DLQ item's id
    dlq_id = dlq_items[0]["id"]

    # Replay from DLQ
    replay_res = client.post(f"/dlq/{dlq_id}/retry")
    assert replay_res.status_code == 200
    replay_data = replay_res.json()

    # Replay should succeed (processed or duplicate)
    assert replay_data["status"] in ["processed", "duplicate"], \
        f"Expected replay to succeed, got status: {replay_data['status']}"
    assert replay_data.get("replayed_from_dlq") is True

    # DLQ listing should be empty (or shorter) after replay
    dlq_after = client.get("/dlq").json()
    assert len(dlq_after) < len(dlq_items), \
        "DLQ entry was not removed after successful replay"


def test_22_metrics_endpoint_retries_count(client):
    """
    Review 2: /metrics endpoint must return 'retries_count' field.
    This field counts events with retry_count > 0.
    """
    now = datetime.now(timezone.utc)
    # First event: original
    client.post("/events", json={
        "message_id": "MSG-METRICS-001",
        "transaction_id": "TXN-METRICS-001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 1000.0,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    })
    # Second event: retry (new msg_id, same txn → duplicate, increments retry_count)
    client.post("/events", json={
        "message_id": "MSG-METRICS-002",
        "transaction_id": "TXN-METRICS-001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 1000.0,
        "event_timestamp": now.isoformat(),
        "event_version": 1
    })

    metrics_res = client.get("/metrics")
    assert metrics_res.status_code == 200
    metrics = metrics_res.json()

    # retries_count field must exist
    assert "retries_count" in metrics, "Missing 'retries_count' in /metrics response"
    # At least 1 retry was recorded
    assert metrics["retries_count"] >= 1, \
        f"Expected retries_count >= 1, got {metrics['retries_count']}"
    assert metrics["duplicates_detected"] >= 1
