# Crypto Streaming Pipeline

A local streaming pipeline that reads live trades for BTC, ETH and SOL from the Binance.US public WebSocket, publishes them to a Kafka-compatible broker (Redpanda), and builds bronze, silver and gold layers in DuckDB. A Streamlit dashboard reads the result. Everything runs on one machine. The only external service is the public Binance.US market data stream, which needs no API key.

![Dashboard](docs/dashboard.png)

## Running it

Prerequisites: Docker with Compose, Python 3.11 or later, and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/calemccammon/data-engineer-crypto-streaming.git
cd data-engineer-crypto-streaming

uv sync --extra dev
cp .env.example .env
docker compose up -d
```

`docker compose up -d` starts a single Redpanda node with its Kafka listener on `localhost:19092`, creates the `raw-trades` topic with 3 partitions, and starts the Redpanda Console on http://localhost:8080.

Start each long-running process in its own terminal, from the repository root:

```bash
# 1. Binance WebSocket -> Kafka
uv run python -m ingest.binance_producer

# 2. Kafka -> data/bronze.duckdb and data/bronze/*.parquet
uv run python -m consume.bronze_consumer

# 3. Parquet -> silver tables in data/crypto.duckdb, every 30 seconds
uv run python -m consume.silver_processor

# 4. Dashboard on http://localhost:8501
uv run streamlit run dashboard/app.py
```

The gold tables are built by dbt, and none of the four processes above runs it. Once the silver processor has logged its first `batch_processed`, build gold from a fifth terminal and repeat the command whenever the gold panels should catch up:

```bash
cd transform
uv run dbt run --profiles-dir .
uv run dbt test --profiles-dir .
```

Until the first `dbt run`, the dashboard shows candles (which it reads from silver) and a "No Gold data yet" warning for the other panels.

### Configuration

All settings are environment variables, loaded from `.env` where present.

| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | producer, bronze consumer |
| `KAFKA_TOPIC_RAW_TRADES` | `raw-trades` | producer, bronze consumer |
| `KAFKA_CONSUMER_GROUP` | `crypto-pipeline` | bronze consumer |
| `SYMBOLS` | `btcusdt,ethusdt,solusdt` | producer, dashboard |
| `DATA_DIR` | `data` | bronze consumer, silver processor, Dagster sensor |
| `BRONZE_DUCKDB_PATH` | `data/bronze.duckdb` | bronze consumer |
| `DUCKDB_PATH` | `data/crypto.duckdb` | silver processor, dbt, dashboard |
| `PARQUET_FLUSH_ROWS` | `5000` | bronze consumer |
| `PARQUET_FLUSH_INTERVAL_SECONDS` | `30` | bronze consumer |
| `LATE_ARRIVAL_WINDOW_SECONDS` | `90` | silver processor |
| `SILVER_POLL_INTERVAL_SECONDS` | `30` | silver processor |
| `DASHBOARD_REFRESH_SECONDS` | `5` | dashboard |

`.env.example` also lists `CANDLE_INTERVAL_SECONDS`. Nothing reads it. The candle width is fixed at one minute in the silver SQL.

## How it works

```
Binance.US WebSocket (wss://stream.binance.us:9443, combined aggTrade stream)
  |
  v
ingest/binance_producer.py    parse into TradeEvent, produce to Kafka topic raw-trades
  |                           (key = symbol, 3 partitions)
  v
consume/bronze_consumer.py    data/bronze.duckdb  (raw.raw_trades)
  |                           data/bronze/trades_<symbols>_<timestamp>.parquet
  v  reads the Parquet files only
consume/silver_processor.py   data/crypto.duckdb  (silver.ohlcv_1m, silver.trades_clean)
  |
  v  dbt run
transform/models/gold/        data/crypto.duckdb  (gold.gold_vwap, gold.gold_rolling_volatility,
  |                                                gold.gold_trade_flow, gold.gold_symbol_summary)
  v
dashboard/app.py              Streamlit, re-queries DuckDB every 5 seconds
```

| Component | Tool |
|---|---|
| Broker | Redpanda in dev-container mode, via Docker Compose |
| Ingestion | `websockets`, `confluent-kafka`, Pydantic v2, `tenacity` for reconnects |
| Storage | DuckDB files and Parquet snapshots under `data/` |
| Transformation | dbt-core with dbt-duckdb |
| Orchestration | Dagster definitions in `orchestrate/` (see [Limitations](#limitations)) |
| Dashboard | Streamlit and Plotly |

### Ingestion

The producer opens one combined WebSocket connection for all configured symbols and reconnects with exponential back-off (1 to 30 seconds) when the connection fails. Each `aggTrade` message is converted to a `TradeEvent` Pydantic model (`ingest/schemas.py`) before it is produced. A message with a missing field or a non-numeric price or quantity raises during conversion, is logged as `event_parse_error`, and is not sent to Kafka. The model checks types only. It has no range checks, so a zero or negative price would pass.

Messages are keyed by symbol, so all trades for one symbol land on one partition and keep their order. The producer uses `acks=1`.

### Bronze

The bronze consumer has auto-commit disabled and commits the offset after each row is inserted. It writes every trade to two places:

- `raw.raw_trades` in `data/bronze.duckdb`, using `INSERT OR IGNORE` on the `trade_id` primary key.
- An in-memory buffer that is flushed to a new Snappy-compressed Parquet file in `data/bronze/` every 5,000 rows or every 30 seconds, whichever comes first, and on shutdown.

| Column | Type | Description |
|---|---|---|
| `trade_id` | BIGINT, primary key | Binance aggregate trade ID |
| `symbol` | VARCHAR | For example `BTCUSDT` |
| `price` | DOUBLE | Trade price |
| `quantity` | DOUBLE | Trade quantity |
| `trade_time` | TIMESTAMPTZ | When the trade executed on Binance |
| `received_at` | TIMESTAMPTZ | When the producer parsed the message |
| `is_buyer_maker` | BOOLEAN | True means the trade was sell-initiated |

### Silver

Every 30 seconds the silver processor opens `data/crypto.duckdb`, reads `data/bronze/*.parquet` with `read_parquet`, writes two tables, and closes the connection.

- `silver.ohlcv_1m`: one row per symbol and minute with open, high, low, close, volume, buy volume, sell volume, trade count, VWAP and `updated_at`. The primary key is `(symbol, candle_open)`.
- `silver.trades_clean`: one row per `trade_id` with `side` (`buy` or `sell`, derived from `is_buyer_maker`), `notional_value` (price times quantity) and `candle_open`.

A single-row table, `silver._watermark`, stores the largest `trade_time` seen by the previous batch.

### Gold

The gold layer is a dbt project in `transform/`. Two staging views (`stg_candles`, `stg_trades`) select from the silver tables.

| Model | Materialization | Contents |
|---|---|---|
| `gold_vwap` | incremental | VWAP, OHLC and candle change per symbol and minute |
| `gold_rolling_volatility` | incremental | Log returns and annualized realized volatility over 5, 15 and 60 candles |
| `gold_trade_flow` | incremental | Order flow imbalance `(buy - sell) / volume`, buy ratio and a 5-candle sum of the imbalance |
| `gold_symbol_summary` | table | Latest price and all-history high, low, volume, VWAP and change per symbol |

Volatility is the standard deviation of one-minute log returns multiplied by `sqrt(525600)`, the number of minutes in a year.

### Dashboard

The dashboard opens a read-only DuckDB connection for each query. The price tiles read `gold.gold_symbol_summary`, the candlestick chart reads `silver.ohlcv_1m` directly, and the volatility and order flow charts read `gold.gold_rolling_volatility` and `gold.gold_trade_flow`. The sidebar selects the symbol and a lookback of 10 to 240 minutes.

### Dagster

`orchestrate/` defines a Dagster code location intended to replace the manual silver and dbt steps:

- `silver_candles` runs one silver batch. `gold_models` runs `dbt run --select gold` and then `dbt test --select gold` in a subprocess.
- `bronze_parquet_sensor` lists `data/bronze/*.parquet` every 30 seconds, compares the file names with the set stored in its cursor, and requests one `silver_to_gold_job` run when new files have appeared.
- `gold_refresh_5min` runs the same job on a five-minute cron schedule as a fallback.

This code location does not load with the dependency versions in `uv.lock`. See [Limitations](#limitations).

## Design decisions

**Candles are bucketed by event time.** The silver SQL groups on `date_trunc('minute', trade_time)`, where `trade_time` is the execution time reported by Binance. `received_at` is stored but not used for bucketing. A trade executed at 10:00:59 and received at 10:01:05 belongs to the 10:00 candle.

**Late trades are handled by re-reading a trailing window.** Each silver batch selects trades whose `trade_time` is no more than 90 seconds older than the previous batch's watermark and writes the resulting candles with `INSERT OR REPLACE`. A trade that arrives within that window is folded into its candle on the next batch. A trade that arrives later than that is ignored.

**Processes exchange data through Parquet files, because DuckDB allows one writing process per database file.** The bronze consumer holds `data/bronze.duckdb` open for as long as it runs, so no other process can open that file. The silver processor therefore reads the Parquet snapshots instead. The second database file, `data/crypto.duckdb`, is shared by the silver processor, dbt and the dashboard. The silver processor and the dashboard each open it only for the duration of a batch or a query, which keeps the periods in which one blocks another short. There is no retry when they do collide (see Limitations).

**Gold models are incremental with `delete+insert`.** Silver rewrites a candle's `updated_at` each time it recomputes the candle. On an incremental run each gold model selects the candles whose `updated_at` is later than the newest value already in the gold table minus a buffer (2 minutes, or 2 hours for the volatility model), deletes the rows with the same `symbol` and `candle_open`, and inserts the recomputed rows. `gold_symbol_summary` is rebuilt in full on every run.

**The sensor triggers on files, with a schedule as a fallback.** Runs are requested when the bronze consumer has written new Parquet files, so an idle feed causes no silver or dbt runs from the sensor.

## Testing

```bash
uv run pytest tests/ -v
```

There are 11 pytest cases, and GitHub Actions runs them on every push and pull request to `main`.

| File | Cases | Covers |
|---|---|---|
| `tests/test_producer.py` | 6 | `TradeEvent` parsing from an `aggTrade` message, symbol upper-casing, JSON serialization, and rejection of a non-numeric price and a missing field |
| `tests/test_consumer.py` | 5 | The silver batch SQL against a temporary DuckDB file: OHLC values, volume, buy and sell split, VWAP, minute boundaries, and a repeated batch producing no duplicate candles |

The dbt project defines 19 schema tests (`not_null`, `unique`, `accepted_values`) and one singular test, `assert_valid_ohlc`, which fails if a candle has a high below its low or an open or close outside the high-low range. They need a populated `data/crypto.duckdb` and are not run in CI.

The producer loop, the bronze consumer, the Dagster definitions, the late-arrival window and the dashboard have no automated tests.

## Limitations

- **The candle that straddles the late-arrival cutoff is recomputed from part of its trades.** The silver batch filters trades by `trade_time >= watermark - 90s` and then replaces whole candles. For the minute that contains the cutoff, only the trades after the cutoff are aggregated, and that partial result overwrites the earlier, complete candle. Aligning the cutoff to a minute boundary would correct it.
- **Incremental gold runs truncate the window functions.** The rolling volatility and 5-candle order flow sums are computed over only the candles selected for the incremental run, so the earliest rows of each run are calculated from fewer candles than their window calls for and replace values from earlier runs. `dbt run --full-refresh` recomputes them over the full history.
- **The Dagster code location does not load with the locked dependencies.** `orchestrate/assets.py` imports `external_asset_from_spec`, which is not present in Dagster 1.13.13, the version in `uv.lock`. `orchestrate/__init__.py` also does not export the `Definitions` object, so the module to pass to `dagster dev -m` is `orchestrate.definitions`.
- **Lock contention on `data/crypto.duckdb` is not handled.** If the silver processor or dbt opens the file while another process has it open, the connection fails. The silver processor does not catch that error and exits. The dashboard catches every query error and shows its empty state for that refresh.
- **Bronze Parquet can lag or miss rows after a crash.** Kafka offsets are committed as each row is inserted into `bronze.duckdb`, before the buffered rows are written to Parquet. If the consumer is killed without running its shutdown handler, the buffered rows (up to 30 seconds or 5,000 trades) exist in `bronze.duckdb` but never reach silver. Nothing reads `bronze.duckdb` back.
- **`trade_id` is treated as globally unique.** Binance aggregate trade IDs are assigned per symbol, so two symbols can share an ID. Keying on `(symbol, trade_id)` would avoid dropped rows in `raw.raw_trades` and overwritten rows in `silver.trades_clean`. The candles are not affected, because they are aggregated from Parquet without that key.
- **Nothing is compacted or expired.** Every silver batch scans all Parquet files in `data/bronze/`, the sensor cursor stores every file name it has seen, and "session" statistics in `gold_symbol_summary` cover all data in the database.
- **Rolling windows count rows, not minutes.** A minute with no trades produces no candle, so a 5-row window can span more than five minutes for a thinly traded symbol.
- **The broker is a single development node.** Redpanda runs as one broker with replication factor 1, and the images in `docker-compose.yml` are tagged `latest`.

## Data source

Trade data comes from the Binance.US public market data WebSocket (`aggTrade` streams). The repository stores none of it. The `data/` directory is created at runtime and is ignored by git.
