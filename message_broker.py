"""
Lightweight Message Broker Simulator for University Idempotency Guard.
Simulates asynchronous message queue behavior (Producer -> Broker -> Consumer -> Guard -> DB -> ACK)
with built-in retry backoff and Dead Letter Queue (DLQ) routing.

NOTE: This is an in-memory local prototype simulator.
For distributed production deployment, this interface can be migrated to Apache Kafka or RabbitMQ.
"""

import time
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from idempotency_guard import IdempotencyGuard, DEFAULT_MAX_RETRIES

logger = logging.getLogger("university_guard.broker")


class MessageBroker:
    """
    In-memory message broker simulator supporting enqueue, consume,
    acknowledgement (ACK), retry scheduling, and dead-lettering.
    """

    def __init__(self, max_retries: int = DEFAULT_MAX_RETRIES):
        self.max_retries = max_retries
        self.queue: deque = deque()
        self.in_flight: Dict[str, Dict[str, Any]] = {}
        self.acknowledged: List[Dict[str, Any]] = []
        self.dead_letter_queue: List[Dict[str, Any]] = []
        self.retry_counts: Dict[str, int] = {}

    def enqueue(self, event_data: Dict[str, Any]) -> str:
        """
        Enqueues an incoming event payload from a university producer.
        Returns the transport message_id.
        """
        msg_id = str(event_data.get("message_id", f"MSG-SIM-{int(time.time()*1000)}"))
        event_copy = dict(event_data)
        event_copy["message_id"] = msg_id
        if "enqueued_at" not in event_copy:
            event_copy["enqueued_at"] = datetime.now(timezone.utc).isoformat()

        self.queue.append(event_copy)
        logger.debug(f"[Broker] Enqueued message {msg_id} (Queue depth: {len(self.queue)})")
        return msg_id

    def consume(self) -> Optional[Dict[str, Any]]:
        """
        Consumes the next available event from the queue.
        Places event in 'in-flight' tracking until acknowledged or failed.
        """
        if not self.queue:
            return None
        event = self.queue.popleft()
        msg_id = event["message_id"]
        self.in_flight[msg_id] = event
        return event

    def acknowledge(self, message_id: str, outcome: str = "PROCESSED") -> bool:
        """
        Acknowledges successful receipt/handling of an in-flight message (ACK).
        Removes message from in-flight and moves to acknowledged ledger.
        """
        if message_id in self.in_flight:
            event = self.in_flight.pop(message_id)
            event["ack_status"] = outcome
            event["acked_at"] = datetime.now(timezone.utc).isoformat()
            self.acknowledged.append(event)
            return True
        return False

    def retry(self, event_data: Dict[str, Any], reason: str = "Processing error") -> Dict[str, Any]:
        """
        Handles consumer failure:
        Increments retry count. If retries <= max_retries, re-enqueues for another attempt.
        If retry count exceeds max_retries, moves event to Dead Letter Queue (DLQ).
        """
        msg_id = event_data["message_id"]
        # Clear from in_flight
        self.in_flight.pop(msg_id, None)

        current_retries = self.retry_counts.get(msg_id, 0) + 1
        self.retry_counts[msg_id] = current_retries

        if current_retries >= self.max_retries:
            return self.dead_letter(event_data, reason=f"Max retries ({self.max_retries}) exceeded: {reason}")
        else:
            event_retry = dict(event_data)
            event_retry["retry_count"] = current_retries
            event_retry["last_error"] = reason
            self.queue.append(event_retry)
            logger.info(f"[Broker] Scheduled retry {current_retries}/{self.max_retries} for message {msg_id}")
            return {
                "status": "retrying",
                "message_id": msg_id,
                "retry_count": current_retries,
                "message": f"Event scheduled for retry attempt {current_retries}"
            }

    def dead_letter(self, event_data: Dict[str, Any], reason: str) -> Dict[str, Any]:
        """
        Permanently routes an unprocessable or poisoned event to the Dead Letter Queue.
        """
        msg_id = event_data["message_id"]
        self.in_flight.pop(msg_id, None)

        dlq_entry = dict(event_data)
        dlq_entry["failure_reason"] = reason
        dlq_entry["failed_at"] = datetime.now(timezone.utc).isoformat()
        dlq_entry["retry_count"] = self.retry_counts.get(msg_id, self.max_retries)
        self.dead_letter_queue.append(dlq_entry)
        logger.warning(f"[Broker] Message {msg_id} routed to DLQ: {reason}")

        return {
            "status": "dlq",
            "message_id": msg_id,
            "reason": reason,
            "retry_count": dlq_entry["retry_count"]
        }

    def process_next(self, guard: IdempotencyGuard) -> Optional[Dict[str, Any]]:
        """
        End-to-end execution of a single queued event through the Idempotency Guard:
        Broker -> Consumer -> IdempotencyGuard -> Target DB -> ACK or Retry/DLQ.
        """
        event = self.consume()
        if not event:
            return None

        msg_id = event["message_id"]
        try:
            result = guard.process_event(event)
            status_val = result.get("status")

            if status_val in ["processed", "duplicate", "out_of_order", "stale", "expired"]:
                # Normal terminal states from the guard perspective: send ACK
                self.acknowledge(msg_id, outcome=status_val)
                result["broker_ack"] = True
                return result
            elif status_val == "dlq":
                # Already sent to DLQ by guard
                self.dead_letter(event, reason=result.get("message", "Guard routed to DLQ"))
                result["broker_ack"] = False
                return result
            else:
                # Failure: trigger broker retry
                retry_res = self.retry(event, reason=result.get("error", "Event processing failed"))
                result["retry_info"] = retry_res
                result["broker_ack"] = False
                return result

        except Exception as e:
            retry_res = self.retry(event, reason=str(e))
            return {
                "status": "failed",
                "message_id": msg_id,
                "error": str(e),
                "retry_info": retry_res,
                "broker_ack": False
            }

    def process_all(self, guard: IdempotencyGuard) -> List[Dict[str, Any]]:
        """Drains and processes all currently enqueued messages."""
        results = []
        while len(self.queue) > 0:
            res = self.process_next(guard)
            if res:
                results.append(res)
        return results

    def get_stats(self) -> Dict[str, int]:
        """Returns operational broker metrics."""
        return {
            "enqueued_pending": len(self.queue),
            "in_flight": len(self.in_flight),
            "acknowledged": len(self.acknowledged),
            "dead_letter": len(self.dead_letter_queue)
        }
