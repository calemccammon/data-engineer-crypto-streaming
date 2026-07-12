-- assert_valid_ohlc.sql
-- Custom test: ensures high >= low, open and close are within [low, high].
-- Any row returned = test failure.

SELECT
    symbol,
    candle_open,
    open_price,
    high_price,
    low_price,
    close_price
FROM {{ ref('stg_candles') }}
WHERE
    high_price < low_price
    OR open_price < low_price
    OR open_price > high_price
    OR close_price < low_price
    OR close_price > high_price
    OR high_price <= 0
    OR low_price <= 0
