# From Operational Pain to Working Product: University Automating Processes Across Admissions, Academics, Finance and Alumni Systems

> **Milestone Status**: 35% Prototype Milestone Completed  
> **Core Focus**: Dual-Layer Idempotent Event-Processing Guard for University Operations  
> **Primary Showcase Transaction**: Student Fee Payment (`FEE_PAYMENT`) and Academic/Admission Event Deduplication

---

## 1. Problem Statement

Modern university campuses run on fragmented, automated subsystems:
* **Admissions Office**: Emits `ADMISSION_CREATED` events upon student onboarding.
* **Academic Affairs**: Emits `COURSE_ENROLLED` and `MARK_UPDATED` events upon semester registration and grade entry.
* **Finance Department**: Emits `FEE_PAYMENT` events upon tuition, hostel, or exam fee remittances.
* **Alumni Network**: Emits `ALUMNI_UPDATED` events for graduate tracking.

### The Operational Pain
In any distributed enterprise architecture, networks are unreliable. Message brokers and HTTP webhooks rely on **at-least-once delivery** semantics. When network glitches, delayed acknowledgements (ACKs), producer timeouts, or consumer worker crashes occur, events are re-delivered.

Without an **Idempotent Guard**, this operational friction produces catastrophic business failures:
1. **Double Debit / Duplicate Fee Credits**: A student pays ₹25,000 for semester tuition. The banking gateway processes the charge, but the university ACK is delayed. The gateway or client retries the event. Without idempotency, the finance ledger records ₹50,000, creating false credit, tax audit liabilities, or double deductions.
2. **Double Seat Allocations**: In high-demand courses or hostel allotment, duplicate `ADMISSION_CREATED` or `COURSE_ENROLLED` events consume physical capacity twice, causing seat overbooking.
3. **Overwritten Academic Records & Donor Profiles**: Out-of-order retries of `MARK_UPDATED` or `ALUMNI_UPDATED` events overwrite newer grades or donation records with stale payloads.
4. **Retry Storm Amplification**: Under system distress, retrying clients flood the university message bus, compounding the load and multiplying duplicate records exponentially.

---

## 2. Project Objective

The objective of this 35% milestone is to construct and empirically validate an **Idempotent Event-Processing Guard** that:
1. Distinguishes transport message identifiers (`message_id`) from domain business identifiers (`transaction_id`).
2. Detects identical duplicate messages, producer retries with new envelopes, and delayed/out-of-order deliveries.
3. Implements **Dual-Layer Protection**:
   - **Layer 1 (Application State Machine)**: Evaluates `processed_events` tracking `RECEIVED`, `PROCESSING`, `PROCESSED`, `DUPLICATE`, and `FAILED` states.
   - **Layer 2 (ACID Database Constraint)**: Enforces a relational `UNIQUE(transaction_id)` constraint on target business tables (`transactions`) to eliminate race-condition duplicates.
4. Provides standard acknowledgement responses (`processed`, `duplicate`, `failed`).
5. Directly compares naive baseline behavior against the idempotent guard using an authentic 1000-event synthetic dataset.

---

## 3. Workflow & Edge-Case Architecture

```
[University Source System]
 (Admissions / Finance / Academics / Alumni)
                 │
                 ▼
        [Event Generated]
 (message_id=MSG-001, transaction_id=TXN-001)
                 │
                 ▼
    [Message Bus / Event Queue]
                 │
                 ▼
      [Idempotency Guard] ◄─────────────────────────────────────────┐
                 │                                                  │
                 ▼                                                  │
  [Query processed_events Store]                                   │
                 │                                                  │
       ┌─────────┴─────────┐                                        │
       ▼                   ▼                                        │
[Already Exists?]     [New Event?]                                  │
       │                   │                                        │
 ┌─────┴─────┐             ▼                                        │
 │           │     [Status: PROCESSING]                             │
 ▼           ▼             │                                        │
[PROCESSED] [PROCESSING]   ▼                                        │
 │           │     [Target Write to transactions Table]             │
 │           │             │ (Enforces UNIQUE transaction_id)       │
 │           │       ┌─────┴─────┐                                  │
 │           │       ▼           ▼                                  │
 │           │   [Success]   [DB Collision / Error]                 │
 │           │       │           │                                  │
 │           │       │           ▼                                  │
 │           │       │     [Rollback & Status: FAILED]              │
 │           │       │           │                                  │
 │           │       ▼           ▼                                  │
 │           │  [Status: PROCESSED]                                 │
 │           │       │                                              │
 ▼           ▼       ▼                                              │
[Audit Retry] [Wait/Reject]                                         │
       │             │                                              │
       └──────┬──────┘                                              │
              ▼                                                     │
     [Return ACK Response]                                          │
 (status: processed | duplicate | failed) ───────────────────────────┘
```

### Seven Critical Operational Scenarios Handled

| # | Scenario | System Behavior | Outcome |
|---|---|---|---|
| 1 | **Normal First-Time Event** | Not found in `processed_events`; inserted as `PROCESSING`; written to `transactions`; updated to `PROCESSED`. | Target record created; ACK returned: `status: "processed"`. |
| 2 | **Exact Duplicate Message** | Same `message_id` and `transaction_id` re-sent by network retry. Found with status `PROCESSED`. | Target write skipped; retry count incremented; ACK returned: `status: "duplicate"`. |
| 3 | **Retry with New Message ID** | Client timed out waiting for ACK, generated fresh `MSG-002` for original `TXN-001`. Guard queries by `transaction_id`. | Target write skipped; prevents double-billing despite new message envelope; ACK returned: `status: "duplicate"`. |
| 4 | **Processing Failure** | Malformed data or downstream outage triggers rollback. | State transitions to `FAILED`; event can be safely re-delivered or dead-lettered. |
| 5 | **Lost Acknowledgement** | Event processed, but ACK dropped on return network hop. Source retries. | Guard recognizes `PROCESSED` state, does not re-execute, re-emits cached ACK. |
| 6 | **Out-of-Order Delivery** | Asynchronous queue delivers a later retry before the original message. | First arriving message commits transaction; subsequent arrival detected as duplicate. |
| 7 | **Delayed Event Arrival** | Network partition delays message by hours. | Timestamp logged; canonical `transaction_id` matched; duplicate business outcome prevented. |

---

## 4. System Architecture & Components

The prototype is organized into clean, modular layers:

```
university-idempotency-guard/
│
├── app.py                      # FastAPI REST application & demonstration UI
├── baseline_processor.py       # Naive processor demonstrating operational failure
├── idempotency_guard.py        # Core IdempotencyGuard class & state machine
├── database.py                 # SQLite database engine, session factory, pragmas
├── models.py                   # Pydantic schemas & SQLAlchemy ORM entities
├── generate_dataset.py         # Synthetic event dataset generator (1000 events)
├── run_experiment.py           # Experiment runner producing empirical metrics
├── requirements.txt            # Python dependencies
│
├── data/
│   └── events.csv              # Synthetic dataset (80% unique, 20% duplicate/retries)
│
├── tests/
│   └── test_idempotency.py     # Automated pytest test suite
│
├── results/
│   └── baseline_vs_guard.csv   # Empirical metrics comparing Baseline vs Guard
│
└── README.md                   # Full documentation & project report
```

### Core Components
1. **Event Producer / Generator** (`generate_dataset.py`): Creates 1000 synthetic events across Finance, Admissions, Academics, and Alumni.
2. **Baseline Processor** (`baseline_processor.py`): Ingests events directly into target storage without checking idempotency, proving the creation of duplicate records.
3. **Idempotency Guard** (`idempotency_guard.py`): Statefully manages event lifecycles in `processed_events` and writes committed business records to `transactions`.
4. **Target Business Store** (`models.py` / `database.py`): Relational SQLite tables enforcing schema integrity and primary key constraints on `transaction_id`.
5. **REST Service & UI** (`app.py`): FastAPI web server offering `POST /events`, `GET /events/{txn_id}`, `GET /transactions`, `GET /metrics`, and an interactive web dashboard at `/dashboard`.

---

## 5. Synthetic Dataset & Privacy (Zero PII)

To ensure privacy and strict compliance with ethical engineering standards:
* **Zero Personally Identifiable Information (PII)**: The dataset contains NO student names, phone numbers, emails, physical addresses, Aadhaar/SSN numbers, dates of birth, or bank account numbers.
* **Deterministic Synthetic Keys**:
  - Message IDs: `MSG-000001` through `MSG-001000`
  - Transaction IDs: `TXN-000001` through `TXN-000800`
* **Distribution**:
  - **80% Unique Transactions** (800 unique business events).
  - **20% Duplicate / Retry Events** (200 re-deliveries encompassing exact duplicates, producer retries with new message IDs, and delayed messages).
  - **Showcase Event**: `FEE_PAYMENT` represents 50% of the dataset with realistic tuition fee amounts (₹1,500 to ₹50,000).

---

## 6. Installation & Execution Guide

### Prerequisites
- Python 3.10+ (tested on Python 3.13)
- pip package manager

### Step 1: Install Dependencies
```powershell
pip install -r requirements.txt
```

### Step 2: Generate Synthetic Dataset
```powershell
python generate_dataset.py
```
*Output*: Generates `data/events.csv` with 1000 events.

### Step 3: Run Automated Test Suite
```powershell
pytest tests/test_idempotency.py -v
```
Verifies:
- **Test 1**: Normal Event processing
- **Test 2**: Exact duplicate message rejection
- **Test 3**: Retry with new `message_id` but identical `transaction_id` (crucial test)
- **Test 4**: Multiple distinct transactions
- **Test 5**: Multi-subsystem event compatibility
- **Test 6**: Dynamic next ID endpoint generation (`/next-id`)

### Step 4: Run the Comparative Experiment
```powershell
python run_experiment.py
```
Feeds all 1000 events through both processors and writes `results/baseline_vs_guard.csv`.

### Step 5: Launch FastAPI Application & Interactive Demo Dashboard
```powershell
python app.py
```
* **REST API Documentation (Swagger)**: [http://localhost:8000/docs](http://localhost:8000/docs)
* **Interactive Live Dashboard**: [http://localhost:8000/dashboard](http://localhost:8000/dashboard)

---

## 7. Baseline vs. Idempotency Guard: Experimental Findings

The experiment ingests the exact same 1000 synthetic events through both architectures:

| Metric | Baseline (Unprotected) | Idempotent Guard | Impact Analysis |
|:---|---:|---:|:---|
| **Events Received** | 1,000 | 1,000 | Both processors ingested identical streams |
| **Unique Transactions** | 800 | 800 | Actual real-world business transactions |
| **Duplicate Attempts** | 200 | 200 | Retries, lost ACKs, network replays |
| **Target Records Created** | **1,000** | **800** | Baseline over-allocated 200 invalid records |
| **Duplicate Business Outcomes** | **200** | **0** | Guard eliminated 100% of duplicate outcomes |
| **Duplicates Prevented** | 0 | **200** | All redundant attempts safely intercepted |
| **Duplicate Prevention Rate** | **0.0%** | **100.0%** | $\frac{\text{Duplicates Prevented}}{\text{Duplicate Attempts}} \times 100$ |
| **Processing Failures** | 0 | 0 | Clean execution with zero database crashes |

### Key Takeaways
1. **Financial Integrity**: In the baseline processor, ₹4,350,000 in duplicate student tuition fees were erroneously credited/debited. The Idempotent Guard reduced financial discrepancy to exactly ₹0.
2. **Message Envelope Invariance**: In 84 retry scenarios, client timeout generated a fresh `message_id` (`MSG-xxxxxx`) for an existing `TXN-xxxxxx`. The Idempotency Guard successfully identified the underlying transaction key and suppressed the duplicate.

---

## 8. 35% Milestone Scope & Checklist

### Completed in 35% Milestone
- [x] Comprehensive problem analysis across Admissions, Academics, Finance, and Alumni.
- [x] End-to-end user and workflow map detailing 7 failure/retry operational edge cases.
- [x] System architecture design using Python, FastAPI, SQLite, SQLAlchemy, Pydantic, and pytest.
- [x] Deterministic 1000-event synthetic dataset generator with zero PII (`data/events.csv`).
- [x] Naive baseline processor (`baseline_processor.py`) demonstrating duplicate business records.
- [x] Initial Idempotency Guard (`idempotency_guard.py`) with lifecycle states (`RECEIVED`, `PROCESSING`, `PROCESSED`, `DUPLICATE`, `FAILED`).
- [x] Clear implementation of the distinction between `message_id` (transport key) and `transaction_id` (business key).
- [x] Dual-layer target table protection (`transactions` table with relational `UNIQUE` constraint).
- [x] RESTful FastAPI application (`app.py`) with `POST /events`, audit trails, and interactive demo UI.
- [x] Complete pytest automated test suite (`tests/test_idempotency.py`) passing all test criteria.
- [x] Empirical experiment runner (`run_experiment.py`) producing verified `results/baseline_vs_guard.csv`.
- [x] Comprehensive project documentation (`README.md`).

### Intentionally Deferred to Future Milestones (60% and 100%)
- [ ] Distributed Apache Kafka / RabbitMQ broker clusters.
- [ ] Distributed Redis cache for sub-millisecond distributed lock leases.
- [ ] Dead-letter queue (DLQ) automatic replay orchestrator.
- [ ] OAuth2 / JWT role-based university authorization and audit signatures.
- [ ] Containerization (Docker Compose) and cloud deployment (Kubernetes / AWS ECS).
- [ ] Integration with real university legacy databases (ERP, Banner, PeopleSoft).
