-- Staging view: clean pass-through of Silver OHLCV candles.
-- Renames columns to match Gold layer conventions.

SELECT
    symbol,
    candle_open,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    buy_volume,
    sell_volume,
    trade_count,
    vwap,
    updated_at
FROM {{ source('silver', 'ohlcv_1m') }}
