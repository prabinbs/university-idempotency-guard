"""
Baseline Event Processor (WITHOUT Idempotency Guard).
Demonstrates what happens in naive university event architectures:
Every received event is inserted directly into the target business records,
causing duplicate financial charges, duplicate seat allocations, and inconsistent state.
"""

import csv
from datetime import datetime, timezone
from typing import Dict, Any, List
from sqlalchemy.orm import Session
from database import engine, SessionLocal, init_db
from models import BaselineTransactionRecord


class BaselineProcessor:
    """
    Naive processor that performs no deduplication or idempotency checks.
    Directly writes every incoming message into the business table.
    """

    def __init__(self, db: Session = None):
        self.db = db if db is not None else SessionLocal()
        self.metrics = {
            "events_received": 0,
            "unique_transactions": set(),
            "duplicate_attempts": 0,
            "target_records_created": 0,
            "duplicate_business_outcomes": 0,
            "duplicates_prevented": 0,
            "processing_failures": 0
        }

    def process_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes a single event without any idempotency check.
        Inserts a new record every time, even if the transaction_id was already processed.
        """
        self.metrics["events_received"] += 1
        txn_id = event_data["transaction_id"]
        msg_id = event_data["message_id"]

        if txn_id in self.metrics["unique_transactions"]:
            self.metrics["duplicate_attempts"] += 1
        else:
            self.metrics["unique_transactions"].add(txn_id)

        try:
            amount_val = float(event_data["amount"]) if event_data.get("amount") not in [None, ""] else None
            record = BaselineTransactionRecord(
                message_id=msg_id,
                transaction_id=txn_id,
                event_type=event_data["event_type"],
                source_system=event_data["source_system"],
                amount=amount_val,
                processed_at=datetime.now(timezone.utc),
                status="COMPLETED"
            )
            self.db.add(record)
            self.db.commit()
            self.metrics["target_records_created"] += 1

            return {
                "status": "processed",
                "transaction_id": txn_id,
                "message_id": msg_id,
                "target_record_id": record.id,
                "message": "Baseline: Event written directly without deduplication"
            }
        except Exception as e:
            self.db.rollback()
            self.metrics["processing_failures"] += 1
            return {
                "status": "failed",
                "transaction_id": txn_id,
                "message_id": msg_id,
                "error": str(e)
            }

    def process_csv(self, csv_path: str = "data/events.csv") -> Dict[str, Any]:
        """
        Ingests all events from CSV using baseline processor.
        """
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.process_event(row)

        total_unique = len(self.metrics["unique_transactions"])
        dup_outcomes = self.metrics["target_records_created"] - total_unique

        summary = {
            "processor": "Baseline (Unprotected)",
            "events_received": self.metrics["events_received"],
            "unique_transactions": total_unique,
            "duplicate_attempts": self.metrics["duplicate_attempts"],
            "target_records_created": self.metrics["target_records_created"],
            "duplicate_business_outcomes": dup_outcomes,
            "duplicates_prevented": 0,
            "prevention_rate": 0.0,
            "processing_failures": self.metrics["processing_failures"]
        }
        return summary


def run_baseline_demo():
    """CLI runner to demo baseline behavior."""
    init_db()
    processor = BaselineProcessor()
    results = processor.process_csv("data/events.csv")
    print("\n--- BASELINE PROCESSOR RESULTS ---")
    for k, v in results.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    run_baseline_demo()
