"""
Dagster software-defined assets for the crypto streaming pipeline.

Asset dependency graph:
    bronze_raw_trades → silver_candles → [gold_vwap, gold_volatility, gold_trade_flow, gold_summary]

The Bronze asset is "external" (managed by the running Kafka consumer).
The Silver and Gold assets are computed on demand / by schedule.
"""

import os
import subprocess
from pathlib import Path

import duckdb
import structlog
from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MetadataValue,
    Output,
    asset,
)
from dotenv import load_dotenv

from consume.silver_processor import run_processor

load_dotenv()

log = structlog.get_logger()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/crypto.duckdb")
TRANSFORM_DIR = Path(__file__).parent.parent / "transform"


# --- External asset: Bronze is written by the always-on Kafka consumer ---
# An AssetSpec with no compute function is an external asset: Dagster shows it in
# the graph and never tries to materialize it.
bronze_raw_trades = AssetSpec(
    key="bronze_raw_trades",
    description="Raw trades written by the Kafka bronze consumer. Always running.",
    metadata={"duckdb_table": "raw.raw_trades", "parquet_dir": "data/bronze"},
)


@asset(
    deps=[bronze_raw_trades],
    description="Run the Silver micro-batch processor: Bronze → OHLCV candles",
    metadata={"duckdb_table": "silver.ohlcv_1m"},
)
def silver_candles(context: AssetExecutionContext) -> Output[dict]:
    run_processor(once=True)

    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    row_count = conn.execute("SELECT COUNT(*) FROM silver.ohlcv_1m").fetchone()[0]
    latest = conn.execute(
        "SELECT MAX(candle_open) FROM silver.ohlcv_1m"
    ).fetchone()[0]
    conn.close()

    context.log.info(f"Silver candles: {row_count} rows, latest candle at {latest}")

    return Output(
        value={"row_count": row_count, "latest_candle": str(latest)},
        metadata={
            "row_count": MetadataValue.int(row_count),
            "latest_candle": MetadataValue.text(str(latest)),
        },
    )


def _run_dbt(command: list[str]) -> None:
    """Run a dbt command inside the transform directory."""
    # dbt runs with transform/ as its working directory, so a relative DUCKDB_PATH
    # would point at a different file there. Pass the path this process uses.
    env = {**os.environ, "DUCKDB_PATH": str(Path(DUCKDB_PATH).resolve())}
    result = subprocess.run(
        ["dbt"] + command + ["--profiles-dir", "."],
        cwd=str(TRANSFORM_DIR),
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt failed:\n{result.stdout}\n{result.stderr}")
    log.info("dbt_success", command=command, output=result.stdout[-500:])


@asset(
    deps=[silver_candles],
    description="Run dbt incremental models to build the Gold layer",
)
def gold_models(context: AssetExecutionContext) -> Output[dict]:
    _run_dbt(["run", "--select", "gold"])
    _run_dbt(["test", "--select", "gold"])

    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    vwap_rows = conn.execute("SELECT COUNT(*) FROM gold.gold_vwap").fetchone()[0]
    summary_rows = conn.execute(
        "SELECT COUNT(*) FROM gold.gold_symbol_summary"
    ).fetchone()[0]
    conn.close()

    context.log.info(f"Gold models complete: {vwap_rows} VWAP rows, {summary_rows} symbols")

    return Output(
        value={"vwap_rows": vwap_rows, "symbol_count": summary_rows},
        metadata={
            "vwap_rows": MetadataValue.int(vwap_rows),
            "symbol_count": MetadataValue.int(summary_rows),
        },
    )
