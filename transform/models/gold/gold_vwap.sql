-- gold_vwap.sql
--
-- Volume-Weighted Average Price per symbol per minute.
--
-- Incremental with delete+insert. Each run reprocesses the candles Silver has
-- rewritten since the last run, which is how late-arriving trades reach Gold.

{{
  config(
    materialized = 'incremental',
    unique_key    = 'surrogate_key',
    incremental_strategy = 'delete+insert',
  )
}}

WITH candles AS (
    SELECT *
    FROM {{ ref('stg_candles') }}

    {% if is_incremental() %}
    -- Only process candles updated since the last dbt run (+ 2-minute buffer).
    -- The COALESCE covers a Gold table that exists but is still empty.
    WHERE updated_at > (
        SELECT COALESCE(MAX(updated_at), TIMESTAMPTZ '1970-01-01 00:00:00+00') - INTERVAL '2 minutes'
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    symbol || '_' || CAST(candle_open AS VARCHAR)      AS surrogate_key,
    symbol,
    candle_open,
    vwap,
    open_price,
    close_price,
    high_price,
    low_price,
    volume,
    trade_count,
    -- Price change within the candle
    close_price - open_price                           AS candle_change,
    (close_price - open_price) / NULLIF(open_price, 0) AS candle_change_pct,
    updated_at
FROM candles
