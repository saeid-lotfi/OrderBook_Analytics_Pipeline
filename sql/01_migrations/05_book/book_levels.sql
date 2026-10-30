CREATE OR REPLACE VIEW {database}.book_levels
AS
SELECT
    market_id,
    side,
    price,

    count() AS order_count,

    sum(remaining_base) AS base_volume,

    sum(
        toDecimal256(remaining_base, 18)
        * toDecimal256(price, 7)
    ) AS quote_notional

FROM {database}.live_orders

WHERE
    price > 0
    AND remaining_base > 0

    -- Only include orders with their latest event on the
    -- latest observed source date (Asia/Tehran), including historical replay.
    AND toDate(event_ts, 'Asia/Tehran')
        = (SELECT toDate(max(event_ts), 'Asia/Tehran')
           FROM {database}.orders_current)

GROUP BY
    market_id,
    side,
    price;
