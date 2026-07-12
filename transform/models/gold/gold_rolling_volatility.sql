-- gold_rolling_volatility.sql
--
-- Realized volatility over rolling 5m, 15m, and 1h windows.
-- Uses log returns computed from consecutive candle close prices.
--
-- Demonstrates: SQL window functions for rolling statistics on streaming data.

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
    WHERE updated_at > (SELECT MAX(updated_at) - INTERVAL '2 hours' FROM {{ this }})
    {% endif %}
),

with_log_return AS (
    SELECT
        symbol,
        candle_open,
        close_price,
        LN(close_price / NULLIF(LAG(close_price) OVER (PARTITION BY symbol ORDER BY candle_open), 0))
            AS log_return,
        updated_at
    FROM candles
)

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
