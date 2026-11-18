-- gold_rolling_volatility.sql
--
-- Realized volatility over rolling 5m, 15m, and 1h windows.
-- Uses log returns computed from consecutive candle close prices.
--
-- The windows are computed over every candle in Silver, and the incremental
-- filter is applied afterwards. Filtering first would cut the windows short for
-- the earliest rows of each run.

{{
  config(
    materialized = 'incremental',
    unique_key    = 'surrogate_key',
    incremental_strategy = 'delete+insert',
  )
}}

WITH with_log_return AS (
    SELECT
        symbol,
        candle_open,
        close_price,
        LN(close_price / NULLIF(LAG(close_price) OVER (PARTITION BY symbol ORDER BY candle_open), 0))
            AS log_return,
        updated_at
    FROM {{ ref('stg_candles') }}
),

volatility AS (
SELECT
    symbol || '_' || CAST(candle_open AS VARCHAR) AS surrogate_key,
    symbol,
    candle_open,
    close_price,
    log_return,

    -- Realized volatility = annualized stddev of log returns over N candles
    -- 1-min candles → multiply by sqrt(525600) to annualize
    STDDEV(log_return) OVER (
        PARTITION BY symbol ORDER BY candle_open
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) * SQRT(525600)                               AS vol_5m_annualized,

    STDDEV(log_return) OVER (
        PARTITION BY symbol ORDER BY candle_open
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) * SQRT(525600)                               AS vol_15m_annualized,

    STDDEV(log_return) OVER (
        PARTITION BY symbol ORDER BY candle_open
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ) * SQRT(525600)                               AS vol_1h_annualized,

    updated_at
FROM with_log_return
)

SELECT *
FROM volatility

{% if is_incremental() %}
-- Silver only rewrites candles from the last few minutes, and a rewritten candle
-- changes the windows of the 59 candles after it. Re-emitting the newest 2 hours
-- covers both. The COALESCE covers a Gold table that is still empty.
WHERE candle_open > (
    SELECT COALESCE(MAX(candle_open), TIMESTAMPTZ '1970-01-01 00:00:00+00') - INTERVAL '2 hours'
    FROM {{ this }}
)
{% endif %}
