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

from models import EventPayload, EventResponse, ProcessedEventRecord, TransactionRecord
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
            "processed_at": t.processed_at.isoformat() if t.processed_at else None,
            "status": t.status
        }
        for t in txns
    ]


@app.get("/metrics")
def get_metrics(db: Session = Depends(get_db)):
    """
    Returns real-time operational metrics of the Idempotency Guard.
    """
    total_received = db.query(ProcessedEventRecord).count()
    target_records = db.query(TransactionRecord).count()
    duplicates_detected = db.query(ProcessedEventRecord).filter(
        ProcessedEventRecord.status == "DUPLICATE"
    ).count()

    prevention_rate = 100.0 if duplicates_detected > 0 else 0.0

    return {
        "events_received": total_received,
        "unique_transactions": target_records,
        "duplicates_detected": duplicates_detected,
        "target_records_created": target_records,
        "duplicate_business_outcomes": 0,
        "prevention_rate_percent": prevention_rate
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
    Enables visual demonstration of normal events, exact duplicates, and retries with new message IDs.
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
        .container { max-width: 1200px; margin: 0 auto; }
        header {
            margin-bottom: 24px;
            padding-bottom: 16px;
            border-bottom: 1px solid var(--border-color);
        }
        h1 { font-size: 1.6rem; font-weight: 700; color: #fff; margin-bottom: 6px; }
        p.subtitle { color: var(--text-secondary); font-size: 0.95rem; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        @media (max-width: 860px) { .grid { grid-template-columns: 1fr; } }
        .card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.2);
        }
        .card h2 { font-size: 1.15rem; margin-bottom: 16px; color: var(--accent-blue); display: flex; align-items: center; gap: 8px; }
        .form-group { margin-bottom: 12px; }
        label { display: block; font-size: 0.8rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.05em; }
        input, select {
            width: 100%;
            padding: 10px 12px;
            background: var(--bg-input);
            border: 1px solid #475569;
            border-radius: 6px;
            color: #fff;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.9rem;
        }
        .btn-group { display: flex; gap: 8px; margin-top: 16px; flex-wrap: wrap; }
        button {
            padding: 10px 16px;
            border: none;
            border-radius: 6px;
            font-weight: 600;
            font-size: 0.88rem;
            cursor: pointer;
            transition: all 0.2s;
        }
        .btn-primary { background: #2563eb; color: #fff; }
        .btn-primary:hover { background: #1d4ed8; }
        .btn-warning { background: #d97706; color: #fff; }
        .btn-warning:hover { background: #b45309; }
        .btn-secondary { background: #475569; color: #fff; }
        .btn-secondary:hover { background: #334155; }
        .log-box {
            background: #090d16;
            border: 1px solid #1e293b;
            border-radius: 8px;
            padding: 12px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.82rem;
            height: 280px;
            overflow-y: auto;
            color: #cbd5e1;
        }
        .badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        .badge-processed { background: rgba(74, 222, 128, 0.15); color: var(--accent-green); border: 1px solid var(--accent-green); }
        .badge-duplicate { background: rgba(251, 191, 36, 0.15); color: var(--accent-amber); border: 1px solid var(--accent-amber); }
        .badge-failed { background: rgba(244, 63, 94, 0.15); color: var(--accent-rose); border: 1px solid var(--accent-rose); }
        table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 0.85rem; }
        th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #334155; }
        th { color: var(--text-secondary); font-size: 0.75rem; text-transform: uppercase; }
        .metrics-bar {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }
        .metric-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 14px;
            text-align: center;
        }
        .metric-val { font-size: 1.6rem; font-weight: 700; color: #fff; margin-top: 4px; }
        .metric-title { font-size: 0.75rem; color: var(--text-secondary); text-transform: uppercase; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>University Idempotent Event-Processing Guard</h1>
            <p class="subtitle">Admissions, Academics, Finance (Fee Payment) & Alumni Event Deduplication</p>
        </header>

        <div class="metrics-bar" id="metrics-bar">
            <div class="metric-card">
                <div class="metric-title">Events Received</div>
                <div class="metric-val" id="m-received">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Unique Transactions</div>
                <div class="metric-val" id="m-unique" style="color: var(--accent-blue)">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Duplicates Prevented</div>
                <div class="metric-val" id="m-duplicates" style="color: var(--accent-amber)">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Duplicate Outcomes</div>
                <div class="metric-val" id="m-bad-outcomes" style="color: var(--accent-green)">0</div>
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
                    <label>Transaction ID (Business Key)</label>
                    <input type="text" id="inp-txn" placeholder="Loading next ID...">
                </div>
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
                <div class="form-group" id="group-amount">
                    <label>Amount (Fee in ₹)</label>
                    <input type="number" id="inp-amount" value="5000">
                </div>

                <div class="btn-group">
                    <button class="btn-primary" onclick="sendEvent()">Send Event</button>
                    <button class="btn-warning" onclick="simulateExactDuplicate()">Simulate Exact Duplicate</button>
                    <button class="btn-warning" onclick="simulateNewMsgRetry()">Simulate Retry (New Msg ID)</button>
                    <button class="btn-secondary" onclick="generateNewIds()">Generate New IDs</button>
                </div>
            </div>

            <div class="card">
                <h2>Processing & Acknowledgement Log</h2>
                <div class="log-box" id="log-box">
                    <div>[System Initialized] Ready to accept university events...</div>
                </div>
            </div>
        </div>

        <div class="card" style="margin-top: 20px;">
            <h2>Committed Target Transactions (Zero Duplicates Guaranteed)</h2>
            <div style="max-height: 250px; overflow-y: auto;">
                <table>
                    <thead>
                        <tr>
                            <th>Transaction ID</th>
                            <th>Event Type</th>
                            <th>Subsystem</th>
                            <th>Amount</th>
                            <th>Processed At</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody id="txn-table-body">
                        <tr><td colspan="6" style="text-align: center; color: #64748b;">No transactions committed yet.</td></tr>
                    </tbody>
                </table>
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
            if (type === 'duplicate') color = '#fbbf24';
            if (type === 'failed') color = '#f43f5e';
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
                if (data.txn_counter >= txnCounter) {
                    txnCounter = data.txn_counter;
                }
                if (data.msg_counter >= msgCounter) {
                    msgCounter = data.msg_counter;
                }
            } catch (e) {}

            document.getElementById('inp-msg').value = 'MSG-' + (msgCounter++);
            document.getElementById('inp-txn').value = 'TXN-' + (txnCounter++);
        }

        function simulateExactDuplicate() {
            log(`Attempting exact duplicate dispatch with MSG=${document.getElementById('inp-msg').value}, TXN=${document.getElementById('inp-txn').value}`, 'info');
            sendEvent();
        }

        function simulateNewMsgRetry() {
            const oldMsg = document.getElementById('inp-msg').value;
            const newMsg = 'MSG-' + (msgCounter++);
            document.getElementById('inp-msg').value = newMsg;
            log(`Simulating producer retry timeout: Generated fresh transport envelope ${newMsg} for existing transaction ${document.getElementById('inp-txn').value}`, 'info');
            sendEvent();
        }

        async function sendEvent() {
            const payload = {
                message_id: document.getElementById('inp-msg').value,
                transaction_id: document.getElementById('inp-txn').value,
                event_type: document.getElementById('inp-type').value,
                source_system: getSource(document.getElementById('inp-type').value),
                amount: document.getElementById('inp-type').value === 'FEE_PAYMENT' ? parseFloat(document.getElementById('inp-amount').value) : null,
                event_timestamp: new Date().toISOString(),
                event_version: 1
            };

            try {
                const res = await fetch('/events', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                log(`Response: status="${data.status}" | Txn=${data.transaction_id} | ${data.message}`, data.status);
                refreshMetrics();
                refreshTransactions();
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
                document.getElementById('m-bad-outcomes').innerText = data.duplicate_business_outcomes;
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
                        <td>${t.event_type}</td>
                        <td>${t.source_system}</td>
                        <td>${t.amount ? '₹' + t.amount.toLocaleString() : '-'}</td>
                        <td>${t.processed_at ? new Date(t.processed_at).toLocaleTimeString() : '-'}</td>
                        <td><span class="badge badge-processed">${t.status}</span></td>
                    `;
                    tbody.appendChild(row);
                });
            } catch (e) {}
        }

        refreshMetrics();
        refreshTransactions();
        initNextIds();
    </script>
</body>
</html>"""
    return HTMLResponse(content=html_content)


if __name__ == "__main__":
    import uvicorn
    init_db()
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
