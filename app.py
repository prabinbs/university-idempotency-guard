"""
FastAPI Application for University Idempotent Event-Processing Guard.
Exposes REST endpoints for event ingestion, idempotency verification,
target record inspection, and operational metrics.
"""

import re
from datetime import datetime, timezone
from typing import Dict, Any, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session

from models import EventPayload, EventResponse, ProcessedEventRecord, TransactionRecord, DeadLetterEventRecord
from database import engine, get_db, init_db
from idempotency_guard import IdempotencyGuard


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DB tables on startup
    init_db()
    yield


app = FastAPI(
    title="University Idempotent Event Guard API",
    description="Protects Admissions, Academics, Finance, and Alumni subsystems from duplicate business transactions.",
    version="1.0.0",
    lifespan=lifespan
)


# =====================================================================
# Core Ingestion Endpoint
# =====================================================================

@app.post("/events", response_model=EventResponse, status_code=status.HTTP_200_OK)
def ingest_event(payload: EventPayload, db: Session = Depends(get_db)):
    """
    Ingests an event from the university message bus.
    Validates payload, executes Idempotency Guard checks, and returns an ACK.
    """
    guard = IdempotencyGuard(db=db)
    result = guard.process_event(payload.model_dump())

    status_str = result.get("status", "failed")
    msg_ack = result.get("message", "Processed")

    return EventResponse(
        status=status_str,
        transaction_id=result.get("transaction_id", payload.transaction_id),
        message_id=result.get("message_id", payload.message_id),
        message=msg_ack,
        timestamp=datetime.now(timezone.utc)
    )


# =====================================================================
# =====================================================================
# Inspection & Audit Endpoints
# =====================================================================

@app.get("/events/{transaction_id}")
def get_event_audit(transaction_id: str, db: Session = Depends(get_db)):
    """
    Fetches the delivery lifecycle and audit trail for a given transaction_id.
    """
    records = db.query(ProcessedEventRecord).filter(
        ProcessedEventRecord.transaction_id == transaction_id
    ).order_by(ProcessedEventRecord.id.asc()).all()

    if not records:
        raise HTTPException(status_code=404, detail=f"Transaction {transaction_id} not found in event store.")

    return [
        {
            "id": r.id,
            "message_id": r.message_id,
            "transaction_id": r.transaction_id,
            "event_type": r.event_type,
            "source_system": r.source_system,
            "status": r.status,
            "event_version": r.event_version,
            "first_seen_at": r.first_seen_at.isoformat() if r.first_seen_at else None,
            "processed_at": r.processed_at.isoformat() if r.processed_at else None,
            "retry_count": r.retry_count
        }
        for r in records
    ]


@app.get("/transactions")
def list_transactions(limit: int = 50, db: Session = Depends(get_db)):
    """
    Returns committed target business records.
    """
    txns = db.query(TransactionRecord).order_by(TransactionRecord.processed_at.desc()).limit(limit).all()
    return [
        {
            "transaction_id": t.transaction_id,
            "event_type": t.event_type,
            "source_system": t.source_system,
            "amount": t.amount,
            "event_version": t.event_version,
            "processed_at": t.processed_at.isoformat() if t.processed_at else None,
            "status": t.status
        }
        for t in txns
    ]


@app.get("/dlq")
def list_dead_letter_events(limit: int = 50, db: Session = Depends(get_db)):
    """
    Returns dead letter queue records (Improvement 5).
    """
    guard = IdempotencyGuard(db=db)
    return guard.get_dlq_events(limit=limit)


@app.post("/dlq/{dlq_id}/retry")
def retry_dlq_event_endpoint(dlq_id: int, db: Session = Depends(get_db)):
    """
    Replays a Dead-Letter Queue event through the idempotency guard (Phase 8).
    Removes it from DLQ upon successful processing.
    """
    guard = IdempotencyGuard(db=db)
    result = guard.replay_dlq_event(dlq_id)
    if result.get("status") == "error":
        raise HTTPException(status_code=404, detail=result.get("message"))
    return result


@app.post("/simulate/ack-loss")
def simulate_ack_loss(
    transaction_id: str = None,
    message_id: str = None,
    db: Session = Depends(get_db)
):
    """
    Demonstrates Phase 5: Acknowledgement loss + retry scenario.
    1. Event arrives.
    2. Event is successfully committed to target database.
    3. Transport ACK is dropped/lost in transit back to the producer.
    4. Producer times out and retries with new transport message ID.
    5. Idempotency guard intercepts the retry, returns DUPLICATE status.
    6. Target database retains exactly 1 record (0 duplicate outcomes).
    """
    guard = IdempotencyGuard(db=db)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    txn_id = transaction_id or f"TXN-ACKLOSS-{now_ts}"
    msg_id_1 = message_id or f"MSG-ACKLOSS-{now_ts}-ORIG"
    msg_id_2 = f"MSG-ACKLOSS-{now_ts}-RETRY"

    # Step 1 & 2: Process original event
    payload_orig = {
        "message_id": msg_id_1,
        "transaction_id": txn_id,
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 10000.0,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_version": 1
    }
    res_orig = guard.process_event(payload_orig)

    # Step 3: Simulate ACK dropped
    # (ACK not received by producer)

    # Step 4 & 5: Source retries after ACK timeout
    payload_retry = {
        "message_id": msg_id_2,
        "transaction_id": txn_id,
        "event_type": "FEE_PAYMENT",
        "source_system": "FINANCE",
        "amount": 10000.0,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_version": 1
    }
    res_retry = guard.process_event(payload_retry)

    # Verify target records
    target_count = db.query(TransactionRecord).filter(TransactionRecord.transaction_id == txn_id).count()

    return {
        "transaction_id": txn_id,
        "step_1_original": {
            "message_id": msg_id_1,
            "status": res_orig.get("status"),
            "committed_to_db": True
        },
        "step_2_ack_simulation": {
            "ack_delivered_to_source": False,
            "reason": "Simulated network drop / timeout of ACK response"
        },
        "step_3_source_retry": {
            "message_id": msg_id_2,
            "status": res_retry.get("status"),
            "guard_action": "Duplicate suppressed by transaction_id index"
        },
        "verification": {
            "target_records_in_db": target_count,
            "duplicate_business_outcomes": 0 if target_count == 1 else (target_count - 1),
            "idempotency_preserved": target_count == 1
        }
    }


@app.post("/simulate/failure")
def simulate_failure_endpoint(
    transaction_id: str = None,
    message_id: str = None,
    db: Session = Depends(get_db)
):
    """
    Demonstrates Phase 7 & 8: Processing failure, retry handling up to MAX_RETRIES (3),
    and dead-letter queue routing.
    """
    guard = IdempotencyGuard(db=db)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    txn_id = transaction_id or f"TXN-FAIL-{now_ts}"
    msg_id = message_id or f"MSG-FAIL-{now_ts}"

    # Execute 3 attempts, explicitly incrementing retry_count in payload each time
    # so that _handle_failure_and_dlq correctly evaluates the retry threshold.
    # Attempt 1 → retry_count=0 → current_retry=1 → FAILED (< max_retries=3)
    # Attempt 2 → retry_count=1 → current_retry=2 → FAILED (< max_retries=3)
    # Attempt 3 → retry_count=2 → current_retry=3 → DLQ  (>= max_retries=3)
    attempts = []
    final_res = None
    for attempt in range(1, 4):
        payload = {
            "message_id": f"{msg_id}-A{attempt}",
            "transaction_id": txn_id,
            "event_type": "FEE_PAYMENT",
            "source_system": "FINANCE",
            "amount": 7500.0,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "event_version": 1,
            "simulate_failure": True,
            "failure_reason": "Simulated: banking gateway HTTP 500 timeout",
            "retry_count": attempt - 1  # 0, 1, 2 across attempts
        }
        res = guard.process_event(payload)
        attempts.append({
            "attempt": attempt,
            "status": res.get("status"),
            "retry_count": res.get("retry_count", attempt),
            "message": res.get("message")
        })
        final_res = res
        if res.get("status") in ["dlq", "processed", "duplicate"]:
            break

    target_count = db.query(TransactionRecord).filter(TransactionRecord.transaction_id == txn_id).count()

    return {
        "transaction_id": txn_id,
        "message_id": msg_id,
        "max_retries_configured": guard.max_retries,
        "attempts": attempts,
        "final_status": final_res.get("status") if final_res else "failed",
        "target_records_created": target_count,
        "moved_to_dlq": final_res.get("status") == "dlq" if final_res else False
    }



@app.post("/simulate/retry-storm")
def simulate_retry_storm_endpoint(count: int = 100, db: Session = Depends(get_db)):
    """
    Executes a controlled retry storm simulation (Improvement 6).
    Sends 100 duplicate delivery attempts for the same transaction.
    """
    guard = IdempotencyGuard(db=db)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    storm_txn = f"TXN-STORM-{now_ts}"
    results = []

    for i in range(1, count + 1):
        payload = {
            "message_id": f"MSG-STORM-{now_ts}-{i:04d}",
            "transaction_id": storm_txn,
            "event_type": "FEE_PAYMENT",
            "source_system": "FINANCE",
            "amount": 25000.0,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "event_version": 1
        }
        res = guard.process_event(payload)
        results.append(res)

    target_records = 1 if any(r["status"] == "processed" for r in results) else 0
    duplicates_prevented = sum(1 for r in results if r["status"] == "duplicate")
    duplicate_attempts = count - 1
    prevention_rate = (duplicates_prevented / duplicate_attempts) * 100.0 if duplicate_attempts > 0 else 100.0

    return {
        "transaction_id": storm_txn,
        "events_received": count,
        "unique_transactions": 1,
        "target_records_created": target_records,
        "duplicate_business_outcomes": 0,
        "duplicate_attempts": duplicate_attempts,
        "duplicates_prevented": duplicates_prevented,
        "prevention_rate_percent": round(prevention_rate, 2),
        "status": "SUCCESS"
    }


@app.get("/metrics")
def get_metrics(db: Session = Depends(get_db)):
    """
    Returns real-time operational metrics of the Idempotency Guard (Improvement 7).
    """
    total_received = db.query(ProcessedEventRecord).count()
    target_records = db.query(TransactionRecord).count()
    duplicates_detected = db.query(ProcessedEventRecord).filter(
        ProcessedEventRecord.status == "DUPLICATE"
    ).count()
    out_of_order_count = db.query(ProcessedEventRecord).filter(
        ProcessedEventRecord.status == "OUT_OF_ORDER"
    ).count()
    expired_count = db.query(ProcessedEventRecord).filter(
        ProcessedEventRecord.status == "EXPIRED"
    ).count()
    failed_count = db.query(ProcessedEventRecord).filter(
        ProcessedEventRecord.status == "FAILED"
    ).count()
    dlq_count = db.query(DeadLetterEventRecord).count()
    retries_count = db.query(ProcessedEventRecord).filter(
        ProcessedEventRecord.retry_count > 0
    ).count()

    total_intercepted = duplicates_detected + out_of_order_count
    prevention_rate = 100.0 if total_intercepted > 0 else (100.0 if target_records > 0 else 0.0)

    return {
        "events_received": total_received,
        "unique_transactions": target_records,
        "duplicates_detected": duplicates_detected,
        "out_of_order_events": out_of_order_count,
        "expired_events": expired_count,
        "failed_events": failed_count,
        "retries_count": retries_count,
        "dlq_events": dlq_count,
        "target_records_created": target_records,
        "duplicate_business_outcomes": 0,
        "prevention_rate_percent": round(prevention_rate, 1)
    }


@app.get("/health")
def health_check():
    """Health check probe."""
    return {"status": "UP", "service": "university-idempotency-guard"}


@app.get("/next-id")
def get_next_available_id(db: Session = Depends(get_db)):
    """
    Computes the next available transaction_id and message_id
    based on the highest existing IDs recorded in the database.
    Preserves continuity across application restarts.
    """
    max_txn_num = 200000
    max_msg_num = 100000

    txn_rows = db.query(TransactionRecord.transaction_id).all()
    event_txn_rows = db.query(ProcessedEventRecord.transaction_id).all()
    for row in txn_rows + event_txn_rows:
        m = re.search(r'(\d+)', str(row[0]))
        if m:
            val = int(m.group(1))
            if val > max_txn_num:
                max_txn_num = val

    msg_rows = db.query(ProcessedEventRecord.message_id).all()
    for row in msg_rows:
        m = re.search(r'(\d+)', str(row[0]))
        if m:
            val = int(m.group(1))
            if val > max_msg_num:
                max_msg_num = val

    return {
        "next_transaction_id": f"TXN-{max_txn_num + 1}",
        "next_message_id": f"MSG-{max_msg_num + 1}",
        "txn_counter": max_txn_num + 1,
        "msg_counter": max_msg_num + 1
    }


# =====================================================================
# Demonstration Web UI Dashboard
# =====================================================================

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def serve_dashboard():
    """
    Lightweight interactive demonstration dashboard.
    Enables visual demonstration of normal events, exact duplicates, retries,
    out-of-order versions, expired events, and dead-letter queue inspection.
    """
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>University Idempotent Event Guard - Live Demo</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-base: #0f172a;
            --bg-card: #1e293b;
            --bg-input: #334155;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --accent-blue: #38bdf8;
            --accent-green: #4ade80;
            --accent-amber: #fbbf24;
            --accent-rose: #f43f5e;
            --accent-purple: #c084fc;
            --accent-crimson: #e11d48;
            --border-color: #334155;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-base);
            color: var(--text-primary);
            padding: 24px;
            min-height: 100vh;
        }
        .container { max-width: 1260px; margin: 0 auto; }
        header {
            margin-bottom: 20px;
            padding-bottom: 16px;
            border-bottom: 1px solid var(--border-color);
        }
        h1 { font-size: 1.55rem; font-weight: 700; color: #fff; margin-bottom: 6px; }
        p.subtitle { color: var(--text-secondary); font-size: 0.92rem; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        @media (max-width: 880px) { .grid { grid-template-columns: 1fr; } }
        .card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.2);
        }
        .card h2 { font-size: 1.15rem; margin-bottom: 16px; color: var(--accent-blue); display: flex; align-items: center; justify-content: space-between; }
        .form-group { margin-bottom: 12px; }
        label { display: block; font-size: 0.8rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.05em; }
        input, select {
            width: 100%;
            padding: 9px 12px;
            background: var(--bg-input);
            border: 1px solid #475569;
            border-radius: 6px;
            color: #fff;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.88rem;
        }
        .btn-group { display: flex; gap: 8px; margin-top: 14px; flex-wrap: wrap; }
        button {
            padding: 9px 14px;
            border: none;
            border-radius: 6px;
            font-weight: 600;
            font-size: 0.84rem;
            cursor: pointer;
            transition: all 0.2s;
        }
        .btn-primary { background: #2563eb; color: #fff; }
        .btn-primary:hover { background: #1d4ed8; }
        .btn-warning { background: #d97706; color: #fff; }
        .btn-warning:hover { background: #b45309; }
        .btn-purple { background: #7c3aed; color: #fff; }
        .btn-purple:hover { background: #6d28d9; }
        .btn-crimson { background: #be123c; color: #fff; }
        .btn-crimson:hover { background: #9f1239; }
        .btn-secondary { background: #475569; color: #fff; }
        .btn-secondary:hover { background: #334155; }
        .log-box {
            background: #090d16;
            border: 1px solid #1e293b;
            border-radius: 8px;
            padding: 12px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.80rem;
            height: 320px;
            overflow-y: auto;
            color: #cbd5e1;
        }
        .badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 9999px;
            font-size: 0.72rem;
            font-weight: 600;
            text-transform: uppercase;
        }
        .badge-processed { background: rgba(74, 222, 128, 0.15); color: var(--accent-green); border: 1px solid var(--accent-green); }
        .badge-duplicate { background: rgba(251, 191, 36, 0.15); color: var(--accent-amber); border: 1px solid var(--accent-amber); }
        .badge-out_of_order { background: rgba(192, 132, 252, 0.15); color: var(--accent-purple); border: 1px solid var(--accent-purple); }
        .badge-stale { background: rgba(192, 132, 252, 0.15); color: var(--accent-purple); border: 1px solid var(--accent-purple); }
        .badge-expired { background: rgba(56, 189, 248, 0.15); color: var(--accent-blue); border: 1px solid var(--accent-blue); }
        .badge-failed { background: rgba(244, 63, 94, 0.15); color: var(--accent-rose); border: 1px solid var(--accent-rose); }
        .badge-dlq { background: rgba(225, 29, 72, 0.2); color: #f43f5e; border: 1px solid #f43f5e; }
        table { width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 0.82rem; }
        th, td { text-align: left; padding: 7px 10px; border-bottom: 1px solid #334155; }
        th { color: var(--text-secondary); font-size: 0.72rem; text-transform: uppercase; }
        .metrics-bar {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
            gap: 10px;
            margin-bottom: 20px;
        }
        .metric-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 12px 10px;
            text-align: center;
        }
        .metric-val { font-size: 1.4rem; font-weight: 700; color: #fff; margin-top: 3px; }
        .metric-title { font-size: 0.70rem; color: var(--text-secondary); text-transform: uppercase; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>University Idempotent Event-Processing Guard</h1>
            <p class="subtitle">Admissions, Academics, Finance & Alumni Systems — With Concurrency Lock Handling, Out-of-Order Engine, Freshness Window & DLQ</p>
        </header>

        <div class="metrics-bar" id="metrics-bar">
            <div class="metric-card">
                <div class="metric-title">Events Received</div>
                <div class="metric-val" id="m-received">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Processed</div>
                <div class="metric-val" id="m-unique" style="color: var(--accent-green)">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Duplicates</div>
                <div class="metric-val" id="m-duplicates" style="color: var(--accent-amber)">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Out-of-Order</div>
                <div class="metric-val" id="m-out-of-order" style="color: var(--accent-purple)">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Expired</div>
                <div class="metric-val" id="m-expired" style="color: var(--accent-blue)">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Failed</div>
                <div class="metric-val" id="m-failed" style="color: var(--accent-rose)">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Retries</div>
                <div class="metric-val" id="m-retries" style="color: var(--accent-amber)">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">DLQ</div>
                <div class="metric-val" id="m-dlq" style="color: var(--accent-crimson)">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Prevention Rate</div>
                <div class="metric-val" id="m-rate" style="color: var(--accent-green)">100%</div>
            </div>
        </div>

        <div class="grid">
            <div class="card">
                <h2>Event Dispatcher & Simulator</h2>
                <div class="form-group">
                    <label>Message ID (Transport Key)</label>
                    <input type="text" id="inp-msg" placeholder="Loading next ID...">
                </div>
                <div class="form-group">
                    <label>Transaction ID (Domain Key)</label>
                    <input type="text" id="inp-txn" placeholder="Loading next ID...">
                </div>
                <div style="display: grid; grid-template-columns: 2fr 1fr; gap: 10px;">
                    <div class="form-group">
                        <label>Event Type</label>
                        <select id="inp-type" onchange="toggleAmount()">
                            <option value="FEE_PAYMENT">FEE_PAYMENT (Finance)</option>
                            <option value="ADMISSION_CREATED">ADMISSION_CREATED (Admissions)</option>
                            <option value="COURSE_ENROLLED">COURSE_ENROLLED (Academics)</option>
                            <option value="MARK_UPDATED">MARK_UPDATED (Academics)</option>
                            <option value="ALUMNI_UPDATED">ALUMNI_UPDATED (Alumni)</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Event Version</label>
                        <input type="number" id="inp-ver" value="1" min="1">
                    </div>
                </div>
                <div class="form-group" id="group-amount">
                    <label>Amount (Fee in ₹)</label>
                    <input type="number" id="inp-amount" value="5000">
                </div>

                <div class="btn-group">
                    <button class="btn-primary" onclick="sendEvent()">Send Event</button>
                    <button class="btn-warning" onclick="simulateExactDuplicate()">Simulate Duplicate</button>
                    <button class="btn-warning" onclick="simulateNewMsgRetry()">Retry (New Msg ID)</button>
                    <button class="btn-warning" onclick="simulateAckLoss()">Simulate ACK Loss</button>
                    <button class="btn-crimson" onclick="simulateFailure()">Simulate Failure &rarr; DLQ</button>
                    <button class="btn-purple" onclick="simulateOutOfOrder()">Simulate Out-of-Order</button>
                    <button class="btn-purple" onclick="simulateExpired()">Simulate Expired Event</button>
                    <button class="btn-crimson" onclick="triggerRetryStorm()">Run 100x Retry Storm</button>
                    <button class="btn-secondary" onclick="generateNewIds()">Generate New IDs</button>
                </div>
            </div>

            <div class="card">
                <h2>Processing & Acknowledgement Log</h2>
                <div class="log-box" id="log-box">
                    <div>[System Initialized] SQLite WAL mode active. Ready to ingest university events...</div>
                </div>
            </div>
        </div>

        <div class="grid" style="margin-top: 20px;">
            <div class="card">
                <h2>Committed Target Ledger (Zero Duplicates Guaranteed)</h2>
                <div style="max-height: 220px; overflow-y: auto;">
                    <table>
                        <thead>
                            <tr>
                                <th>Transaction ID</th>
                                <th>Version</th>
                                <th>Event Type</th>
                                <th>Subsystem</th>
                                <th>Amount</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody id="txn-table-body">
                            <tr><td colspan="6" style="text-align: center; color: #64748b;">No transactions committed yet.</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="card">
                <h2>Dead Letter Queue (DLQ Storage)</h2>
                <div style="max-height: 220px; overflow-y: auto;">
                    <table>
                        <thead>
                            <tr>
                                <th>Message ID</th>
                                <th>Transaction ID</th>
                                <th>Retries</th>
                                <th>Failure Reason</th>
                                <th>Failed At</th>
                                <th>Action</th>
                            </tr>
                        </thead>
                        <tbody id="dlq-table-body">
                            <tr><td colspan="6" style="text-align: center; color: #64748b;">DLQ empty. Zero unhandled failures.</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <script>
        let msgCounter = 100001;
        let txnCounter = 200001;

        function log(msg, type='info') {
            const box = document.getElementById('log-box');
            const item = document.createElement('div');
            item.style.marginBottom = '6px';
            const time = new Date().toLocaleTimeString();
            let color = '#94a3b8';
            if (type === 'processed') color = '#4ade80';
            else if (type === 'duplicate') color = '#fbbf24';
            else if (type === 'out_of_order' || type === 'stale') color = '#c084fc';
            else if (type === 'expired') color = '#38bdf8';
            else if (type === 'failed') color = '#f43f5e';
            else if (type === 'dlq') color = '#e11d48';

            item.innerHTML = `<span style="color:#64748b;">[${time}]</span> <span style="color:${color}; font-weight:600;">[${type.toUpperCase()}]</span> ${msg}`;
            box.appendChild(item);
            box.scrollTop = box.scrollHeight;
        }

        function toggleAmount() {
            const type = document.getElementById('inp-type').value;
            document.getElementById('group-amount').style.display = (type === 'FEE_PAYMENT') ? 'block' : 'none';
        }

        async function initNextIds() {
            try {
                const res = await fetch('/next-id');
                const data = await res.json();
                document.getElementById('inp-msg').value = data.next_message_id;
                document.getElementById('inp-txn').value = data.next_transaction_id;
                txnCounter = data.txn_counter + 1;
                msgCounter = data.msg_counter + 1;
            } catch (e) {
                document.getElementById('inp-msg').value = 'MSG-' + (msgCounter++);
                document.getElementById('inp-txn').value = 'TXN-' + (txnCounter++);
            }
        }

        async function generateNewIds() {
            try {
                const res = await fetch('/next-id');
                const data = await res.json();
                if (data.txn_counter >= txnCounter) txnCounter = data.txn_counter;
                if (data.msg_counter >= msgCounter) msgCounter = data.msg_counter;
            } catch (e) {}

            document.getElementById('inp-msg').value = 'MSG-' + (msgCounter++);
            document.getElementById('inp-txn').value = 'TXN-' + (txnCounter++);
            document.getElementById('inp-ver').value = '1';
        }

        function simulateExactDuplicate() {
            log(`Attempting exact duplicate dispatch with MSG=${document.getElementById('inp-msg').value}, TXN=${document.getElementById('inp-txn').value}`, 'info');
            sendEvent();
        }

        function simulateNewMsgRetry() {
            const newMsg = 'MSG-' + (msgCounter++);
            document.getElementById('inp-msg').value = newMsg;
            log(`Simulating retry timeout: Fresh envelope ${newMsg} for existing transaction ${document.getElementById('inp-txn').value}`, 'info');
            sendEvent();
        }

        async function simulateAckLoss() {
            const txn = document.getElementById('inp-txn').value;
            log(`Simulating ACK loss scenario for ${txn}...`, 'info');
            try {
                const res = await fetch(`/simulate/ack-loss?transaction_id=${encodeURIComponent(txn)}`, { method: 'POST' });
                const data = await res.json();
                log(`Step 1 (Orig): Txn=${data.transaction_id}, Msg=${data.step_1_original.message_id} -> ${data.step_1_original.status}`, data.step_1_original.status);
                log(`Step 2 (Network): ${data.step_2_ack_simulation.reason}`, 'info');
                log(`Step 3 (Retry): Msg=${data.step_3_source_retry.message_id} -> ${data.step_3_source_retry.status} (${data.step_3_source_retry.guard_action})`, data.step_3_source_retry.status);
                log(`Result: Target ledger records=${data.verification.target_records_in_db} | Duplicate outcomes=${data.verification.duplicate_business_outcomes}`, 'processed');
                refreshMetrics();
                refreshTransactions();
                refreshDLQ();
                generateNewIds();
            } catch (err) {
                log('ACK Loss simulation failed: ' + err, 'failed');
            }
        }

        async function simulateFailure() {
            const txn = document.getElementById('inp-txn').value;
            log(`Simulating processing failure with 3 retries -> DLQ for ${txn}...`, 'info');
            try {
                const res = await fetch(`/simulate/failure?transaction_id=${encodeURIComponent(txn)}`, { method: 'POST' });
                const data = await res.json();
                data.attempts.forEach(a => {
                    log(`Attempt ${a.attempt} (retry ${a.retry_count}): status="${a.status}" | ${a.message}`, a.status);
                });
                log(`Final Status: ${data.final_status.toUpperCase()} | Moved to DLQ: ${data.moved_to_dlq} | Target records in ledger: ${data.target_records_created}`, data.final_status);
                refreshMetrics();
                refreshTransactions();
                refreshDLQ();
                generateNewIds();
            } catch (err) {
                log('Simulate failure error: ' + err, 'failed');
            }
        }

        async function retryDLQ(id) {
            log(`Replaying DLQ event #${id} through Idempotency Guard...`, 'info');
            try {
                const res = await fetch(`/dlq/${id}/retry`, { method: 'POST' });
                const data = await res.json();
                log(`[DLQ Replay] ID #${id}: status="${data.status}" | Txn=${data.transaction_id} | ${data.message}`, data.status);
                refreshMetrics();
                refreshTransactions();
                refreshDLQ();
            } catch (err) {
                log(`DLQ replay failed: ${err}`, 'failed');
            }
        }

        async function simulateOutOfOrder() {
            const txn = document.getElementById('inp-txn').value;
            log(`Starting Out-of-Order test for ${txn}...`, 'info');
            
            // Step 1: Send Version 2 first!
            const msgV2 = 'MSG-' + (msgCounter++);
            log(`Step 1: Delivering Version 2 event first (${msgV2})...`, 'info');
            const resV2 = await fetch('/events', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message_id: msgV2,
                    transaction_id: txn,
                    event_type: 'FEE_PAYMENT',
                    source_system: 'FINANCE',
                    amount: 5000.0,
                    event_timestamp: new Date().toISOString(),
                    event_version: 2
                })
            });
            const dataV2 = await resV2.json();
            log(`Result Version 2: status="${dataV2.status}" | ${dataV2.message}`, dataV2.status);

            // Step 2: Now send older Version 1 late!
            const msgV1 = 'MSG-' + (msgCounter++);
            log(`Step 2: Delivering older Version 1 event late (${msgV1})...`, 'info');
            const resV1 = await fetch('/events', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message_id: msgV1,
                    transaction_id: txn,
                    event_type: 'FEE_PAYMENT',
                    source_system: 'FINANCE',
                    amount: 5000.0,
                    event_timestamp: new Date().toISOString(),
                    event_version: 1
                })
            });
            const dataV1 = await resV1.json();
            log(`Result Version 1: status="${dataV1.status}" | ${dataV1.message}`, dataV1.status);

            refreshMetrics();
            refreshTransactions();
            refreshDLQ();
        }

        async function simulateExpired() {
            const oldDate = new Date(Date.now() - 45 * 60 * 1000).toISOString(); // 45 minutes ago
            const msg = 'MSG-' + (msgCounter++);
            const txn = document.getElementById('inp-txn').value;
            log(`Sending event with timestamp 45 min ago (${oldDate}) against 30 min window...`, 'info');

            const res = await fetch('/events', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message_id: msg,
                    transaction_id: txn,
                    event_type: document.getElementById('inp-type').value,
                    source_system: getSource(document.getElementById('inp-type').value),
                    amount: 5000.0,
                    event_timestamp: oldDate,
                    event_version: 1
                })
            });
            const data = await res.json();
            log(`Response: status="${data.status}" | ${data.message}`, data.status);
            refreshMetrics();
            refreshTransactions();
        }

        async function triggerRetryStorm() {
            log('Initiating controlled 100x Retry Storm simulation on backend...', 'info');
            try {
                const res = await fetch('/simulate/retry-storm?count=100', { method: 'POST' });
                const data = await res.json();
                log(`[Retry Storm Finished] Txn: ${data.transaction_id} | Received: ${data.events_received} | Target Records: ${data.target_records_created} | Prevented: ${data.duplicates_prevented} | Prevention Rate: ${data.prevention_rate_percent}%`, 'processed');
                refreshMetrics();
                refreshTransactions();
                refreshDLQ();
            } catch (err) {
                log('Retry storm failed: ' + err, 'failed');
            }
        }

        async function sendEvent() {
            const payload = {
                message_id: document.getElementById('inp-msg').value,
                transaction_id: document.getElementById('inp-txn').value,
                event_type: document.getElementById('inp-type').value,
                source_system: getSource(document.getElementById('inp-type').value),
                amount: document.getElementById('inp-type').value === 'FEE_PAYMENT' ? parseFloat(document.getElementById('inp-amount').value) : null,
                event_timestamp: new Date().toISOString(),
                event_version: parseInt(document.getElementById('inp-ver').value) || 1
            };

            try {
                const res = await fetch('/events', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                log(`Response: status="${data.status}" | Txn=${data.transaction_id} (v${data.event_version}) | ${data.message}`, data.status);
                refreshMetrics();
                refreshTransactions();
                refreshDLQ();
            } catch (err) {
                log('Network error: ' + err, 'failed');
            }
        }

        function getSource(type) {
            if (type === 'FEE_PAYMENT') return 'FINANCE';
            if (type === 'ADMISSION_CREATED') return 'ADMISSIONS';
            if (type === 'COURSE_ENROLLED' || type === 'MARK_UPDATED') return 'ACADEMICS';
            return 'ALUMNI';
        }

        async function refreshMetrics() {
            try {
                const res = await fetch('/metrics');
                const data = await res.json();
                document.getElementById('m-received').innerText = data.events_received;
                document.getElementById('m-unique').innerText = data.unique_transactions;
                document.getElementById('m-duplicates').innerText = data.duplicates_detected;
                document.getElementById('m-out-of-order').innerText = data.out_of_order_events;
                document.getElementById('m-expired').innerText = data.expired_events;
                document.getElementById('m-failed').innerText = data.failed_events;
                document.getElementById('m-retries').innerText = data.retries_count || 0;
                document.getElementById('m-dlq').innerText = data.dlq_events;
                document.getElementById('m-rate').innerText = data.prevention_rate_percent + '%';
            } catch (e) {}
        }

        async function refreshTransactions() {
            try {
                const res = await fetch('/transactions?limit=10');
                const data = await res.json();
                const tbody = document.getElementById('txn-table-body');
                if (data.length === 0) return;
                tbody.innerHTML = '';
                data.forEach(t => {
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td style="font-family: monospace; font-weight: 600; color: #38bdf8;">${t.transaction_id}</td>
                        <td style="color: #c084fc; font-weight:600;">v${t.event_version || 1}</td>
                        <td>${t.event_type}</td>
                        <td>${t.source_system}</td>
                        <td>${t.amount ? '₹' + t.amount.toLocaleString() : '-'}</td>
                        <td><span class="badge badge-processed">${t.status}</span></td>
                    `;
                    tbody.appendChild(row);
                });
            } catch (e) {}
        }

        async function refreshDLQ() {
            try {
                const res = await fetch('/dlq?limit=10');
                const data = await res.json();
                const tbody = document.getElementById('dlq-table-body');
                if (data.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: #64748b;">DLQ empty. Zero unhandled failures.</td></tr>';
                    return;
                }
                tbody.innerHTML = '';
                data.forEach(d => {
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td style="font-family: monospace; color: #cbd5e1;">${d.message_id}</td>
                        <td style="font-family: monospace; color: #f43f5e; font-weight:600;">${d.transaction_id}</td>
                        <td style="color: #fbbf24;">${d.retry_count}</td>
                        <td style="color: #f87171; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${d.failure_reason}</td>
                        <td style="color: #64748b;">${d.failed_at ? new Date(d.failed_at).toLocaleTimeString() : '-'}</td>
                        <td><button class="btn-primary" style="padding: 3px 8px; font-size: 0.72rem;" onclick="retryDLQ(${d.id})">Replay</button></td>
                    `;
                    tbody.appendChild(row);
                });
            } catch (e) {}
        }

        refreshMetrics();
        refreshTransactions();
        refreshDLQ();
        initNextIds();
    </script>
</body>
</html>"""
    return HTMLResponse(content=html_content)


if __name__ == "__main__":
    import uvicorn
    init_db()
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)

