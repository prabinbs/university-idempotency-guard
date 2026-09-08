"""
Synthetic Event Dataset Generator for University Idempotency Guard.
Generates exactly 1000 events without collecting personal student information (Zero PII).
Simulates 80% unique business events and 20% duplicate/retry/delayed scenarios.
"""

import os
import random
import csv
from datetime import datetime, timedelta, timezone

EVENT_TYPES = [
    ("FEE_PAYMENT", "FINANCE", True),          # Primary showcase (50% weight)
    ("ADMISSION_CREATED", "ADMISSIONS", False), # 20%
    ("COURSE_ENROLLED", "ACADEMICS", False),    # 15%
    ("MARK_UPDATED", "ACADEMICS", False),       # 10%
    ("ALUMNI_UPDATED", "ALUMNI", False),        # 5%
]

EVENT_WEIGHTS = [50, 20, 15, 10, 5]
FEE_AMOUNTS = [1500.0, 3000.0, 5000.0, 10000.0, 15000.0, 25000.0, 50000.0]


def generate_dataset(output_path: str = "data/events.csv", total_events: int = 1000, seed: int = 42):
    random.seed(seed)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    base_time = datetime(2026, 9, 1, 9, 0, 0, tzinfo=timezone.utc)

    # 80% unique events (800 transactions), 20% duplicates/retries (200 events)
    num_unique = int(total_events * 0.80)  # 800
    num_duplicates = total_events - num_unique  # 200

    unique_events = []
    message_counter = 1

    for i in range(1, num_unique + 1):
        txn_id = f"TXN-{i:06d}"
        msg_id = f"MSG-{message_counter:06d}"
        message_counter += 1

        chosen = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        event_type, source_system, has_amount = chosen
        amount = random.choice(FEE_AMOUNTS) if has_amount else ""

        # Stagger timestamps across 5 days
        offset_seconds = (i * 450) + random.randint(0, 60)
        timestamp = base_time + timedelta(seconds=offset_seconds)

        event = {
            "message_id": msg_id,
            "transaction_id": txn_id,
            "event_type": event_type,
            "source_system": source_system,
            "event_timestamp": timestamp.isoformat(),
            "amount": amount,
            "event_version": 1,
            "processing_status": "NEW"
        }
        unique_events.append(event)

    # Generate 200 duplicate/retry events from existing transactions
    duplicate_events = []
    # Pick 200 candidates from unique events to duplicate
    sample_targets = random.choices(unique_events, k=num_duplicates)

    for target in sample_targets:
        dup_type = random.choice(["EXACT_DUPLICATE", "RETRY_NEW_MSG_ID", "DELAYED_RETRY"])

        orig_time = datetime.fromisoformat(target["event_timestamp"])

        if dup_type == "EXACT_DUPLICATE":
            # Scenario: Network retry delivers identical message packet
            dup_msg_id = target["message_id"]
            dup_time = orig_time + timedelta(seconds=random.randint(5, 45))
        elif dup_type == "RETRY_NEW_MSG_ID":
            # Scenario: Source client timeout generates a new message envelope for same business transaction
            dup_msg_id = f"MSG-{message_counter:06d}"
            message_counter += 1
            dup_time = orig_time + timedelta(seconds=random.randint(60, 300))
        else: # DELAYED_RETRY / OUT_OF_ORDER
            # Scenario: Network partition resolves late or delayed delivery
            dup_msg_id = f"MSG-{message_counter:06d}"
            message_counter += 1
            dup_time = orig_time + timedelta(hours=random.randint(12, 48))

        dup_event = {
            "message_id": dup_msg_id,
            "transaction_id": target["transaction_id"],
            "event_type": target["event_type"],
            "source_system": target["source_system"],
            "event_timestamp": dup_time.isoformat(),
            "amount": target["amount"],
            "event_version": 1,
            "processing_status": "NEW"
        }
        duplicate_events.append(dup_event)

    # Combine events and simulate realistic interleaving
    all_events = unique_events + duplicate_events
    # Sort primarily by timestamp to mimic arrival stream, but preserve realistic out-of-order jitter
    random.shuffle(all_events)  # slight jitter before timestamp sort
    all_events.sort(key=lambda e: e["event_timestamp"])

    # Write to CSV
    fieldnames = [
        "message_id",
        "transaction_id",
        "event_type",
        "source_system",
        "event_timestamp",
        "amount",
        "event_version",
        "processing_status"
    ]

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_events)

    print(f"Dataset generated successfully at: {output_path}")
    print(f"Total events: {len(all_events)} | Unique Transactions: {num_unique} | Duplicates/Retries: {num_duplicates}")
    return len(all_events)


if __name__ == "__main__":
    generate_dataset()
