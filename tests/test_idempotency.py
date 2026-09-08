"""
Automated Test Suite for University Idempotent Event Guard.
Verifies normal event processing, exact duplicate handling, producer retries
with new message IDs, multi-transaction independence, and database constraints.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from models import Base, ProcessedEventRecord, TransactionRecord
from database import get_db
from app import app


# Set up isolated in-memory SQLite database for testing
TEST_DATABASE_URL = "sqlite:///:memory:"

test_engine = create_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool
)
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


@pytest.fixture
def client():
    return TestClient(app)


def test_1_normal_event(client):
    """
    Test 1 – Normal Event
    Send one event.
    Expected:
    - Status: 'processed'
    - Exactly 1 target record created in the database.
    """
    payload = {
        "message_id": "MSG-000001",
        "transaction_id": "TXN-000001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 5000.0,
        "event_timestamp": "2026-09-07T10:00:00Z",
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
    payload = {
        "message_id": "MSG-000002",
        "transaction_id": "TXN-000002",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 12000.0,
        "event_timestamp": "2026-09-07T10:05:00Z",
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
    first_attempt = {
        "message_id": "MSG-100001",
        "transaction_id": "TXN-300001",
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 25000.0,
        "event_timestamp": "2026-09-07T10:10:00Z",
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
        "event_timestamp": "2026-09-07T10:12:00Z",
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
    assert audit_records[0].retry_count >= 1
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
    event_a = {
        "message_id": "MSG-000004A",
        "transaction_id": "TXN-000004A",
        "event_type": "COURSE_ENROLLED",
        "source_system": "ACADEMICS",
        "amount": None,
        "event_timestamp": "2026-09-07T10:20:00Z",
        "event_version": 1
    }

    event_b = {
        "message_id": "MSG-000004B",
        "transaction_id": "TXN-000004B",
        "event_type": "ADMISSION_CREATED",
        "source_system": "ADMISSIONS",
        "amount": None,
        "event_timestamp": "2026-09-07T10:25:00Z",
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
            "event_timestamp": "2026-09-07T11:00:00Z",
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
    # Initially in fresh DB
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
        "event_timestamp": "2026-09-07T10:00:00Z",
        "event_version": 1
    }
    client.post("/events", json=payload)

    # Next ID should now be incremented
    res2 = client.get("/next-id")
    assert res2.status_code == 200
    data2 = res2.json()
    assert data2["next_transaction_id"] == "TXN-200002"
    assert data2["next_message_id"] == "MSG-100002"

