-- Staging view: clean pass-through of Silver trades.

SELECT
    trade_id,
    symbol,
    price,
    quantity,
    notional_value,
    side,
    trade_time,
    received_at,
    candle_open
FROM {{ source('silver', 'trades_clean') }}
