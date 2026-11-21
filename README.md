# Crypto Streaming Pipeline

A local streaming pipeline that reads live trades for BTC, ETH and SOL from the Binance.US public WebSocket, publishes them to a Kafka-compatible broker (Redpanda), and builds bronze, silver and gold layers in DuckDB. A Streamlit dashboard reads the result. Everything runs on one machine. The only external service is the public Binance.US market data stream, which needs no API key.

![Dashboard](docs/dashboard.png)

## Running it

Prerequisites: Docker with Compose and [uv](https://docs.astral.sh/uv/). uv uses Python 3.12, the version in `.python-version`, and downloads it if it is missing. dbt does not yet import on Python 3.14.

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

### Running silver and gold from Dagster

Dagster can take over step 3 and the dbt commands. Stop the silver processor if it is running, then start Dagster from the repository root:

```bash
uv run dagster dev -m orchestrate -p 3001
```

Open http://localhost:3001 and turn on `bronze_parquet_sensor`. Each time the bronze consumer writes a Parquet file, the sensor starts a run that processes one silver batch and then builds the dbt project, tests included.

Without `DAGSTER_HOME`, `dagster dev` keeps its state in a temporary directory and the sensor cursor is lost on restart. To keep it, export `DAGSTER_HOME` as the absolute path of an existing directory before starting Dagster.

### Upgrading an existing `data/` directory

The primary key of `raw.raw_trades` and `silver.trades_clean` is `(symbol, trade_id)`. Databases created before that change keep their old key on `trade_id` alone, because the tables are only created when missing. Delete `data/` to recreate them.

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

The candle width is fixed at one minute in the silver SQL.

## How it works

```
Binance.US WebSocket (wss://stream.binance.us:9443, combined aggTrade stream)
  |
  v
ingest/binance_producer.py    parse into TradeEvent, produce to Kafka topic raw-trades
  |                           (key = symbol, 3 partitions)
  v
consume/bronze_consumer.py    data/bronze.duckdb  (raw.raw_trades)
  |                           data/bronze/trades_<symbols>_<timestamp>_<id>.parquet
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
| Orchestration | Dagster assets, a file sensor and a schedule in `orchestrate/` |
| Dashboard | Streamlit and Plotly |

### Ingestion

The producer opens one combined WebSocket connection for all configured symbols and reconnects with exponential back-off (1 to 30 seconds) when the connection fails. Each `aggTrade` message is converted to a `TradeEvent` Pydantic model (`ingest/schemas.py`) before it is produced. A message with a missing field or a non-numeric price or quantity raises during conversion, is logged as `event_parse_error`, and is not sent to Kafka. The model checks types only. It has no range checks, so a zero or negative price would pass.

Messages are keyed by symbol, so all trades for one symbol land on one partition and keep their order. The producer uses `acks=1`.

### Bronze

The bronze consumer checks each message against the same `TradeEvent` model the producer uses. A message that is not valid JSON, lacks a field or has a value of the wrong type is logged as `message_skipped` and passed over. It writes every trade to two places:

- `raw.raw_trades` in `data/bronze.duckdb`, using `INSERT OR IGNORE` on the `(symbol, trade_id)` primary key. Binance assigns aggregate trade IDs per symbol, so the ID alone is not unique.
- An in-memory buffer that is flushed to a new Snappy-compressed Parquet file in `data/bronze/` every 5,000 rows or every 30 seconds, whichever comes first, and on shutdown. Each file is written under a temporary name and renamed when it is complete, so the silver processor never reads part of one.

Auto-commit is disabled. Offsets are committed after each flush, once the trades they cover are in a Parquet file. A consumer that is killed therefore reads its unflushed trades again when it restarts, and a file can repeat trades that an earlier file already holds. Delivery into bronze is at-least-once; silver removes the repeats.

| Column | Type | Description |
|---|---|---|
| `trade_id` | BIGINT | Binance aggregate trade ID. Primary key together with `symbol` |
| `symbol` | VARCHAR | For example `BTCUSDT` |
| `price` | DOUBLE | Trade price |
| `quantity` | DOUBLE | Trade quantity |
| `trade_time` | TIMESTAMPTZ | When the trade executed on Binance |
| `received_at` | TIMESTAMPTZ | When the producer parsed the message |
| `is_buyer_maker` | BOOLEAN | True means the trade was sell-initiated |

The Parquet files have the same columns. A trade that Kafka delivers twice is ignored by the DuckDB table but is written to Parquet again.

### Silver

Every 30 seconds the silver processor opens `data/crypto.duckdb`, reads `data/bronze/*.parquet` with `read_parquet`, keeps one row per `(symbol, trade_id)`, writes two tables, and closes the connection.

- `silver.ohlcv_1m`: one row per symbol and minute with open, high, low, close, volume, buy volume, sell volume, trade count, VWAP and `updated_at`. The primary key is `(symbol, candle_open)`.
- `silver.trades_clean`: one row per `(symbol, trade_id)` with `side` (`buy` or `sell`, derived from `is_buyer_maker`), `notional_value` (price times quantity) and `candle_open`.

`silver._symbol_watermark` stores, for each symbol, the largest `trade_time` the previous batch saw. Each batch writes both tables and the watermarks in one transaction.

### Gold

The gold layer is a dbt project in `transform/`. Two staging views (`stg_candles`, `stg_trades`) select from the silver tables.

| Model | Materialization | Contents |
|---|---|---|
| `gold_vwap` | incremental | VWAP, OHLC and candle change per symbol and minute |
| `gold_rolling_volatility` | incremental | Log returns and annualized realized volatility over 5, 15 and 60 candles |
| `gold_trade_flow` | incremental | Order flow imbalance `(buy - sell) / volume`, buy ratio and a 5-candle sum of the imbalance |
| `gold_symbol_summary` | table | Latest price and all-history high, low, volume, VWAP and change from the first candle's open, per symbol |

Volatility is the standard deviation of one-minute log returns multiplied by `sqrt(525600)`, the number of minutes in a year.

### Dashboard

The dashboard opens a read-only DuckDB connection for each query. The price tiles read `gold.gold_symbol_summary`, the candlestick chart reads `silver.ohlcv_1m` directly, and the volatility and order flow charts read `gold.gold_rolling_volatility` and `gold.gold_trade_flow`. The sidebar selects the symbol and a lookback of 10 to 240 minutes.

### Dagster

`orchestrate/` defines a Dagster code location that replaces the manual silver and dbt steps:

- `bronze_raw_trades` is an external asset that stands for the bronze consumer's output. `silver_candles` runs one silver batch. `gold_models` runs `dbt build` in a subprocess, passing the absolute database path because dbt runs from `transform/`. `dbt build` creates the staging views before the gold models that read them, so the job works on a database dbt has never touched, and it runs every dbt test.
- `bronze_parquet_sensor` lists `data/bronze/*.parquet` every 30 seconds, compares the file names with the set stored in its cursor, and requests one `silver_to_gold_job` run when new files have appeared.
- `gold_refresh_5min` runs the same job on a five-minute cron schedule as a fallback.

## Design decisions

**Candles are bucketed by event time.** The silver SQL groups on `date_trunc('minute', trade_time)`, where `trade_time` is the execution time reported by Binance. `received_at` is stored but not used for bucketing. A trade executed at 10:00:59 and received at 10:01:05 belongs to the 10:00 candle.

**Late trades are handled by re-reading a trailing window.** For each symbol, a silver batch takes that symbol's watermark, subtracts 90 seconds, and rounds down to the start of that minute. It re-aggregates the symbol's trades from that point on and writes the resulting candles with `INSERT OR REPLACE`. The rounding matters because candles are replaced whole: a batch that started mid-minute would rebuild that candle from only part of its trades. A trade that arrives within the window is folded into its candle on the next batch. A trade that arrives later than that is ignored.

**The watermark is per symbol.** After a consumer restart the symbols are read back unevenly, and one batch can see trades for one symbol that are minutes newer than the trades the next batch brings for another. A single watermark would be set by the symbol furthest ahead and would pass over the others' backlog. Kafka keeps each symbol's trades in order, so a symbol's own watermark never runs ahead of trades that are still to come for it, beyond the late window.

**Open and close are settled by trade ID when times tie.** Trade times are in milliseconds, and one order filled at several prices produces several trades with the same time. The candle's open and close order trades by `trade_time` and then `trade_id`, so they do not change from one run to the next.

**Processes exchange data through Parquet files, because DuckDB allows one writing process per database file.** The bronze consumer holds `data/bronze.duckdb` open for as long as it runs, so no other process can open that file. The silver processor therefore reads the Parquet snapshots instead. The second database file, `data/crypto.duckdb`, is shared by the silver processor, dbt and the dashboard. The silver processor and the dashboard each open it only for the duration of a batch or a query, which keeps the periods in which one blocks another short. When the silver processor finds the file locked it skips that batch and tries again on the next poll. When the dashboard finds it locked it shows a notice and tries again on the next refresh.

**Gold models are incremental with `delete+insert`.** Silver rewrites a candle's `updated_at` each time it recomputes the candle. On an incremental run `gold_vwap` selects the candles whose `updated_at` is later than the newest value already in the gold table minus 2 minutes, deletes the rows with the same `symbol` and `candle_open`, and inserts the recomputed rows. The two models with rolling windows compute their windows over all silver candles first and then re-emit only the newest rows by `candle_open` (10 minutes for `gold_trade_flow`, 2 hours for `gold_rolling_volatility`). Filtering before the window functions would give the earliest rows of each run a shortened window. `gold_symbol_summary` is rebuilt in full on every run.

**The sensor triggers on files, with a schedule as a fallback.** Runs are requested when the bronze consumer has written new Parquet files, so an idle feed causes no silver or dbt runs from the sensor.

## Testing

```bash
uv run pytest tests/ -v
```

There are 35 pytest cases, and GitHub Actions runs them on every push and pull request to `main`.

| File | Cases | Covers |
|---|---|---|
| `tests/test_producer.py` | 6 | `TradeEvent` parsing from an `aggTrade` message, symbol upper-casing, JSON serialization, and a `ValidationError` for a non-numeric price and for a missing field |
| `tests/test_bronze_consumer.py` | 15 | Messages that are not trades are skipped (six kinds); a redelivered trade is stored once; the Parquet writer's column values and timestamp types; two flushes in one second make two files; a file is not visible under `*.parquet` until it is complete; offsets are committed after the file exists, and not at all when the write fails |
| `tests/test_silver_processor.py` | 12 | The silver batch SQL against a temporary DuckDB file: OHLC values, volume, buy and sell split, VWAP, minute boundaries, a repeated batch producing no duplicate candles, a candle that straddles the late-arrival cutoff, one trade ID shared by two symbols, a trade repeated in bronze, a symbol whose trades arrive behind another's, trades at the same instant, and a failed batch leaving nothing behind |
| `tests/test_pipeline.py` | 2 | The Dagster definitions load. Starting from Parquet files and no database, the Dagster assets build silver and then run `dbt build`, twice, and the silver and gold row counts are checked |

The dbt project defines 18 schema tests (`not_null`, `unique`, `accepted_values`) and one singular test, `assert_valid_ohlc`, which fails if a candle has a high below its low or an open or close outside the high-low range. `tests/test_pipeline.py` runs them all, against the tables it builds.

The producer loop, the bronze consumer's Kafka loop and the dashboard have no automated tests.

## Limitations

- **dbt does not wait for a lock on `data/crypto.duckdb`.** A `dbt run` that starts while the silver processor or a dashboard query has the file open fails and has to be run again. The reverse case is handled: the silver processor and the dashboard skip and retry.
- **Bronze Parquet lags the feed by up to 30 seconds**, the flush interval. Nothing reads `bronze.duckdb` back, so silver sees a trade only once it has been flushed.
- **A skipped message is only logged.** There is no dead-letter topic and no count of skipped messages.
- **Nothing is compacted or expired.** Every silver batch scans all Parquet files in `data/bronze/`, the windowed gold models read every silver candle on each run, the sensor cursor stores every file name it has seen, and "session" statistics in `gold_symbol_summary` cover all data in the database.
- **Rolling windows count rows, not minutes.** A minute with no trades produces no candle, so a 5-row window can span more than five minutes for a thinly traded symbol.
- **The broker is a single development node.** Redpanda runs as one broker with replication factor 1, and the images in `docker-compose.yml` are tagged `latest`.

## Data source

Trade data comes from the Binance.US public market data WebSocket (`aggTrade` streams). The repository stores none of it. The `data/` directory is created at runtime and is ignored by git.
