"""
Dagster definitions entry point.

Usage:
    uv run dagster dev -m orchestrate -p 3001
"""

from dagster import Definitions, ScheduleDefinition

from orchestrate.assets import bronze_raw_trades, silver_candles, gold_models
from orchestrate.jobs import silver_to_gold_job, full_pipeline_job
from orchestrate.sensors import bronze_parquet_sensor

# Also run dbt Gold refresh on a 5-minute schedule as a fallback
gold_refresh_schedule = ScheduleDefinition(
    job=silver_to_gold_job,
    cron_schedule="*/5 * * * *",
    name="gold_refresh_5min",
    description="Refresh Gold layer every 5 minutes (fallback to sensor-driven runs)",
)

defs = Definitions(
    assets=[bronze_raw_trades, silver_candles, gold_models],
    jobs=[silver_to_gold_job, full_pipeline_job],
    sensors=[bronze_parquet_sensor],
    schedules=[gold_refresh_schedule],
)
