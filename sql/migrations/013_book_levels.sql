CREATE VIEW IF NOT EXISTS {database}.book_levels
AS
SELECT
    market_id,
    side,
    price,

    count() AS order_count,

    sum(remaining_base) AS base_volume,

    sum(
        CAST(remaining_base AS Decimal(76, 18))
        * CAST(price AS Decimal(76, 7))
    ) AS quote_notional

FROM {database}.live_orders

WHERE
    price > 0
    AND remaining_base > 0

GROUP BY
    market_id,
    side,
    price;