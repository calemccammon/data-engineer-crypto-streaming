-- gold_symbol_summary.sql
--
-- Latest snapshot per symbol: current price, recent volume, session high/low.
-- Refreshed on every dbt run — shows the "right now" state of each symbol.
--
-- Demonstrates: non-incremental Gold model (always full-refresh from Silver).

{{
  config(
    materialized = 'table',
    schema = 'gold',
  )
}}

WITH latest_candle AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        candle_open,
        close_price      AS latest_price,
        vwap             AS latest_vwap,
        volume           AS latest_candle_volume,
        trade_count      AS latest_candle_trades,
        updated_at
    FROM {{ ref('stg_candles') }}
    ORDER BY symbol, candle_open DESC
),

session_stats AS (
    SELECT
        symbol,
        MAX(high_price)                                      AS session_high,
        MIN(low_price)                                       AS session_low,
        SUM(volume)                                          AS session_volume,
        SUM(trade_count)                                     AS session_trades,
        MIN(candle_open)                                     AS session_start,
        MAX(candle_open)                                     AS session_end,
        -- Session VWAP
        SUM(vwap * volume) / NULLIF(SUM(volume), 0)         AS session_vwap,
        -- First price of the session
        FIRST(close_price ORDER BY candle_open)              AS session_open_price
    FROM {{ ref('stg_candles') }}
    GROUP BY symbol
)

SELECT
    l.symbol,
    l.latest_price,
    l.latest_vwap,
    l.latest_candle_volume,
    l.latest_candle_trades,
    s.session_high,
    s.session_low,
    s.session_volume,
    s.session_trades,
    s.session_vwap,
    s.session_open_price,
    l.latest_price - s.session_open_price                   AS session_change,
    (l.latest_price - s.session_open_price)
        / NULLIF(s.session_open_price, 0)                   AS session_change_pct,
    s.session_start,
    l.updated_at
FROM latest_candle l
JOIN session_stats s ON l.symbol = s.symbol
