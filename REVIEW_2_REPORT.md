# REVIEW 2 REPORT: University Idempotency Guard

## 1. Review 1 Summary
In Review 1, the baseline prototype was built to demonstrate the danger of unprotected event ingestion. The initial version proved that standard consumers under at-least-once delivery networks produce duplicate business outcomes (e.g., double fee payments) when retries occur due to dropped ACKs or network timeouts. The foundation for separating transport keys (`message_id`) from domain business keys (`transaction_id`) was established, and a basic `processed_events` audit table was implemented.

## 2. Review 2 Completed Features
For Review 2, the system was upgraded into an Enterprise-grade Idempotency Guard. The following features were successfully implemented and empirically tested:
- **Concurrent event handling:** Integrated SQLite WAL mode, relational `UNIQUE` constraints on the target ledger, and application-level retry backoff (`with_db_retry`) to safely handle lock contention.
- **Out-of-order event handling:** Version-progression engine that accepts newer events, flags identical versions as duplicates, and strictly rejects older versions as `OUT_OF_ORDER`.
- **Delayed/expired events:** Introduced a freshness window (`IDEMPOTENCY_WINDOW_MINUTES = 30`). Events older than 30 minutes are safely rejected as `EXPIRED` to prevent stale historical mutations.
- **Acknowledgement-loss simulation:** Built into the API (`/simulate/ack-loss`) to demonstrate successful deduplication when transport ACKs are dropped and retries occur with new message IDs.
- **Local message broker simulator:** A memory-based broker (`message_broker.py`) simulating queueing, in-flight tracking, consumer consumption, ACKs, retries, and dead-letter routing.
- **Failure and retry handling:** Configurable maximum retries (`MAX_RETRIES = 3`).
- **Dead Letter Queue (DLQ):** Poison-pill events that exceed the retry limit are safely stored in `dead_letter_events` with detailed failure telemetry.
- **Retry storm experiment:** End-to-end automated simulation injecting 100 duplicate deliveries for a single transaction.
- **Baseline comparison:** Fully automated script (`run_experiment.py`) comparing unprotected baseline against the Idempotency Guard using exactly 1,000 empirical events.
- **UI/Dashboard:** An interactive frontend showcasing the guard's behavior under duplication, failure, out-of-order, and DLQ routing scenarios.
- **Error analysis:** Documented in `results/error_analysis.md`.

## 3. Architecture Overview
The current architecture routes events via a simulated Local Message Broker into the Idempotency Guard. The guard executes a multi-layer check:
1. **Time/Expiration Check** (Stale/Expired protection)
2. **Application State Check** (Duplicate transport envelope check)
3. **Version Check** (Out-of-order protection)
4. **ACID Transaction Commit** (Database UNIQUE constraint safety-net)
If successful, target records are safely persisted. If failures exceed thresholds, the payload drops into the Dead Letter Queue.

## 4. Concurrency Handling
Lock contention is handled via SQLite WAL (`PRAGMA journal_mode=WAL`), a 30s busy timeout, and a Python retry decorator with exponential backoff + jitter. If two concurrent threads attempt the same transaction, Layer 2 (ACID `UNIQUE(transaction_id)`) triggers an `IntegrityError` on the trailing thread, cleanly catching and routing it as a `DUPLICATE` without crashing.

## 5. Out-of-Order Handling
The `event_version` schema property guarantees chronological state integrity. If $v_{\text{incoming}} < v_{\text{stored}}$, the write is rejected to prevent stale overwrites.

## 6. Delayed / Expired Events
Through `IDEMPOTENCY_WINDOW_MINUTES`, network-partitioned events arriving extremely late are logged but not executed. Crucially, historical ledger entries are preserved.

## 7. ACK-Loss Simulation
Demonstrates the primary cause of idempotency failures: an event commits successfully, but the network drops the transport ACK. The upstream producer retries with a fresh `message_id`. The guard identifies the matching `transaction_id`, prevents the duplicate write, and returns a successful response to silence the producer.

## 8. Retry Handling & Broker Simulation
`message_broker.py` simulates network queuing. When processing fails, the broker increments the retry count and re-enqueues the payload, simulating typical consumer recovery strategies.

## 9. Dead Letter Queue (DLQ)
When retry attempts exceed `MAX_RETRIES = 3`, the broker permanently routes the payload to the DLQ (`dead_letter_events`). This prevents poison-pill payloads from creating infinite loops and stalling consumer throughput.

## 10. Retry Storm Experiment
A controlled stress test was executed: 100 delivery attempts of `FEE_PAYMENT` for `TXN-500001`.
- **Target created:** 1
- **Duplicates prevented:** 99
- **Duplicate business outcomes:** 0
- **Prevention Rate:** 100%

## 11. Baseline Comparison & Actual Measured Results
The empirical dataset (`data/events.csv`) of 1,000 synthetic events (800 unique, 200 duplicates) was run through both architectures:
- **Baseline:** Created 1,000 target records, resulting in 200 duplicate business outcomes (e.g., ₹4,350,000 in duplicate fee payments).
- **Idempotency Guard:** Created exactly 800 target records, successfully identifying and preventing all 200 redundant operations.
- **DLQ, Out-of-Order, and Expired Events:** Each successfully demonstrated and verified empirically (1 recorded in each bucket during phase 4 testing).

## 12. Error Analysis & Limitations
See `results/error_analysis.md`. Current limitations include SQLite write serialization bottlenecks and the ephemeral nature of the in-memory broker, both of which require migration for a true production rollout.

## 13. Remaining Work
- **Production Infrastructure Migration:** Replace SQLite with PostgreSQL/Redis. Replace `message_broker.py` with Apache Kafka or RabbitMQ.
- **Cloud Deployment:** Containerization (Docker/Kubernetes).
- **Authentication/Security:** Implementing OAuth2/JWT for endpoint protection.
