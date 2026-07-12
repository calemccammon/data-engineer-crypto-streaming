"""Dagster job definitions."""

from dagster import define_asset_job, AssetSelection

# Materialise Silver + Gold in sequence
silver_to_gold_job = define_asset_job(
    name="silver_to_gold_job",
    selection=AssetSelection.keys("silver_candles", "gold_models"),
    description="Process Bronze trades into Silver candles, then run dbt Gold models",
)

# Full pipeline (rarely needed manually — normally triggered by sensor)
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
    description="End-to-end: Bronze (external) → Silver → Gold",
)
