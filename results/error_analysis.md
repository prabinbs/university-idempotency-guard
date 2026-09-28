# Error Analysis & Edge Cases Report
**Project:** University Idempotency Guard (Review 2)

## 1. Dead Letter Queue (DLQ) Routing
**Scenario:** Events that repeatedly fail processing (e.g., simulated HTTP 500 errors or logic failures) up to the maximum retry count (`MAX_RETRIES` = 3).
**Resolution:** After 3 failed attempts, the Idempotency Guard automatically intercepts the failure, abandons further retries (preventing infinite retry storms), and securely writes the event payload and failure reason to the `dead_letter_events` table for manual review.

## 2. Out-of-Order Event Delivery
**Scenario:** A newer version of an event (e.g., Version 2 `FEE_PAYMENT_CONFIRMED`) arrives *before* the older version (e.g., Version 1 `FEE_PAYMENT_CREATED`).
**Resolution:** The guard utilizes a version progression engine. It accepts Version 2. When Version 1 later arrives, it detects `event_version` (1) < `stored_version` (2) and rejects Version 1 as `OUT_OF_ORDER` (or `STALE`), strictly preserving the newer business state.

## 3. Expired & Delayed Events
**Scenario:** Network partitions cause events to arrive far later than their generation time (e.g., 45 minutes old).
**Resolution:** Controlled by `IDEMPOTENCY_WINDOW_MINUTES` (default 30 mins). If an event timestamp is older than the server time minus the window, the guard safely rejects the write (`EXPIRED`) to prevent ancient events from mutating long-settled accounts.

## 4. Concurrent Duplicates (Lock Contention)
**Scenario:** 10 worker threads attempt to write the exact same domain transaction simultaneously.
**Resolution:** The application relies on a composite `UNIQUE(transaction_id)` constraint on the target ledger. The database (SQLite in WAL mode) catches the first successful write. Subsequent concurrent threads encounter an `IntegrityError`, which the guard cleanly catches, rolling back the transaction and logging a `DUPLICATE` prevention attempt without crashing the application.

## 5. Acknowledgement (ACK) Loss
**Scenario:** An event is fully processed and written, but the network drops the transport ACK to the producer, causing the producer to retry with a new message envelope ID.
**Resolution:** Since the underlying `transaction_id` remains constant, the guard intercepts the retry at the domain layer, suppressing duplicate ledger writes and immediately returning a successful deduplicated ACK to quiet the producer.

## 6. Known Limitations
- **SQLite Concurrency Limits:** Despite WAL mode and busy timeouts, SQLite operates with a single writer lock. In high-throughput distributed systems, lock contention could starve requests.
- **In-Memory Broker:** The `MessageBroker` class is in-memory and ephemerally tracks queues. If the pod crashes, in-flight non-persistent messages are lost (requires Apache Kafka or RabbitMQ for production).
- **Time Drift:** Expiration checks rely on server time (`datetime.now(timezone.utc)`). Server clock drift vs. producer clock drift > `CLOCK_SKEW_TOLERANCE_MINUTES` could cause legitimate events to be improperly rejected.
