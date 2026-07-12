-- gold_vwap.sql
--
-- Volume-Weighted Average Price per symbol per minute.
--
-- Demonstrates: dbt incremental model with delete+insert strategy.
-- On each run, we only reprocess candles that were updated in the last
-- LATE_ARRIVAL_WINDOW_SECONDS to handle late-arriving events.

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
    -- Only process candles updated since the last dbt run (+ 2-minute buffer)
    WHERE updated_at > (SELECT MAX(updated_at) - INTERVAL '2 minutes' FROM {{ this }})
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
