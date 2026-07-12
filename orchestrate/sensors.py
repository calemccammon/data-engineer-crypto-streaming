"""
Dagster sensors for the crypto streaming pipeline.

Sensors allow event-driven triggering — instead of running on a fixed schedule,
we react to things that *happen* (new data arriving in the Bronze layer).

Key sensor: `bronze_parquet_sensor`
- Watches data/bronze/ for new Parquet files dropped by the Kafka consumer
- When a new file appears, triggers the Silver → Gold pipeline
- Tracks which files it has already processed (cursor-based, persists across restarts)

This demonstrates the difference between:
  - Schedule: "run every 5 minutes no matter what"
  - Sensor:   "run when new Bronze data actually arrives"
"""

import os
from pathlib import Path

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from orchestrate.jobs import silver_to_gold_job

BRONZE_DIR = Path(os.getenv("DATA_DIR", "data")) / "bronze"


@sensor(
    job=silver_to_gold_job,
    minimum_interval_seconds=30,
    description=(
        "Watches data/bronze/ for new Parquet files. "
        "Triggers Silver→Gold pipeline when new files arrive."
    ),
)
def bronze_parquet_sensor(context: SensorEvaluationContext) -> SensorResult:
    """
    Cursor-based file sensor.

    The cursor stores the set of Parquet filenames we've already processed.
    On each evaluation we compare the current files to the cursor and trigger
    a run if anything is new.
    """
    if not BRONZE_DIR.exists():
        return SensorResult(skip_reason=SkipReason("Bronze directory does not exist yet"))

    current_files = {f.name for f in BRONZE_DIR.glob("*.parquet")}
    already_seen = set(context.cursor.split(",")) if context.cursor else set()
    new_files = current_files - already_seen

    if not new_files:
        return SensorResult(skip_reason=SkipReason(f"No new Parquet files (seen {len(already_seen)})"))

    context.log.info(f"New Bronze files detected: {new_files}")

    # Trigger one run per sensor evaluation (processes all new files together)
    run_request = RunRequest(
        run_key=",".join(sorted(new_files)),
        run_config={},
        tags={"triggered_by": "bronze_sensor", "new_files": str(len(new_files))},
    )

    # Update cursor to include newly seen files
    updated_cursor = ",".join(sorted(current_files))

    return SensorResult(
        run_requests=[run_request],
        cursor=updated_cursor,
    )
