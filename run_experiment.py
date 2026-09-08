"""
Empirical Experiment Runner: Baseline vs. Idempotent Guard.
Feeds data/events.csv through both architectures, calculates empirical metrics,
and writes results/baseline_vs_guard.csv with ZERO fabricated data.
"""

import os
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base
from baseline_processor import BaselineProcessor
from idempotency_guard import IdempotencyGuard


def run_experiment(
    dataset_path: str = "data/events.csv",
    output_csv_path: str = "results/baseline_vs_guard.csv"
):
    print("=" * 70)
    print("STARTING EXPERIMENT: BASELINE vs. IDEMPOTENT EVENT GUARD")
    print(f"Dataset: {dataset_path}")
    print("=" * 70)

    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

    # 1. RUN BASELINE EXPERIMENT IN DEDICATED DB
    baseline_db_url = "sqlite:///baseline_experiment.db"
    if os.path.exists("baseline_experiment.db"):
        os.remove("baseline_experiment.db")
    b_engine = create_engine(baseline_db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=b_engine)
    BSession = sessionmaker(autocommit=False, autoflush=False, bind=b_engine)
    b_session = BSession()

    print("\n[Phase 1/2] Processing 1000 events via Baseline Processor (Unprotected)...")
    b_processor = BaselineProcessor(db=b_session)
    b_results = b_processor.process_csv(dataset_path)
    b_session.close()

    # 2. RUN IDEMPOTENT GUARD EXPERIMENT IN DEDICATED DB
    guard_db_url = "sqlite:///guard_experiment.db"
    if os.path.exists("guard_experiment.db"):
        os.remove("guard_experiment.db")
    g_engine = create_engine(guard_db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=g_engine)
    GSession = sessionmaker(autocommit=False, autoflush=False, bind=g_engine)
    g_session = GSession()

    print("[Phase 2/2] Processing 1000 events via Idempotent Guard (Dual-Layer Guard)...")
    g_guard = IdempotencyGuard(db=g_session)
    g_results = g_guard.process_csv(dataset_path)
    g_session.close()

    # 3. BUILD COMPARATIVE METRICS TABLE
    metrics_rows = [
        ("Events received", b_results["events_received"], g_results["events_received"]),
        ("Unique transactions", b_results["unique_transactions"], g_results["unique_transactions"]),
        ("Duplicate attempts", b_results["duplicate_attempts"], g_results["duplicate_attempts"]),
        ("Target records created", b_results["target_records_created"], g_results["target_records_created"]),
        ("Duplicate business outcomes", b_results["duplicate_business_outcomes"], g_results["duplicate_business_outcomes"]),
        ("Duplicates prevented", b_results["duplicates_prevented"], g_results["duplicates_prevented"]),
        ("Duplicate Prevention Rate", f"{b_results['prevention_rate']:.1f}%", f"{g_results['prevention_rate']:.1f}%"),
        ("Processing failures", b_results["processing_failures"], g_results["processing_failures"])
    ]

    # Save to CSV
    with open(output_csv_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "Baseline", "Idempotent Guard"])
        for row in metrics_rows:
            writer.writerow(row)

    print(f"\nResults successfully saved to: {output_csv_path}\n")

    # Display comparison table
    print("=" * 66)
    print(f"{'Metric':<32} | {'Baseline':<12} | {'Idempotent Guard':<16}")
    print("-" * 66)
    for m, b_val, g_val in metrics_rows:
        print(f"{m:<32} | {str(b_val):>12} | {str(g_val):>16}")
    print("=" * 66)

    return metrics_rows


if __name__ == "__main__":
    run_experiment()
