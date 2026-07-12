-- gold_trade_flow.sql
--
-- Buy/sell pressure and order flow imbalance per 1-minute candle.
--
-- Order Flow Imbalance (OFI) measures whether buyers or sellers are dominant:
--   OFI = (buy_volume - sell_volume) / total_volume
--   +1.0 = entirely buy-driven, -1.0 = entirely sell-driven, 0 = balanced
--
-- Demonstrates: derived metrics from streaming aggregations.

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
    WHERE updated_at > (SELECT MAX(updated_at) - INTERVAL '2 minutes' FROM {{ this }})
    {% endif %}
)

SELECT
    symbol || '_' || CAST(candle_open AS VARCHAR)                AS surrogate_key,
    symbol,
    candle_open,
    volume,
    buy_volume,
    sell_volume,
    trade_count,

    -- Order Flow Imbalance: ranges from -1 (all sells) to +1 (all buys)
    (buy_volume - sell_volume) / NULLIF(volume, 0)              AS order_flow_imbalance,

    -- Buy ratio: what fraction of volume was buy-initiated
    buy_volume / NULLIF(volume, 0)                              AS buy_ratio,

    -- Rolling 5-candle cumulative OFI (momentum signal)
    SUM((buy_volume - sell_volume) / NULLIF(volume, 0)) OVER (
        PARTITION BY symbol ORDER BY candle_open
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )                                                           AS cumulative_ofi_5m,

    updated_at
FROM candles
