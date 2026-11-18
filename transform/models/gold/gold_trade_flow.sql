-- gold_trade_flow.sql
--
-- Buy/sell pressure and order flow imbalance per 1-minute candle.
--
-- Order Flow Imbalance (OFI) measures whether buyers or sellers are dominant:
--   OFI = (buy_volume - sell_volume) / total_volume
--   +1.0 = entirely buy-driven, -1.0 = entirely sell-driven, 0 = balanced
--
-- The rolling sum is computed over every candle in Silver, and the incremental
-- filter is applied afterwards. Filtering first would cut the window short for
-- the earliest rows of each run.

{{
  config(
    materialized = 'incremental',
    unique_key    = 'surrogate_key',
    incremental_strategy = 'delete+insert',
  )
}}

WITH flow AS (
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
FROM {{ ref('stg_candles') }}
)

SELECT *
FROM flow

{% if is_incremental() %}
-- Silver only rewrites candles from the last few minutes, and a rewritten candle
-- changes the rolling sum of the 4 candles after it. Re-emitting the newest
-- 10 minutes covers both. The COALESCE covers a Gold table that is still empty.
WHERE candle_open > (
    SELECT COALESCE(MAX(candle_open), TIMESTAMPTZ '1970-01-01 00:00:00+00') - INTERVAL '10 minutes'
    FROM {{ this }}
)
{% endif %}
