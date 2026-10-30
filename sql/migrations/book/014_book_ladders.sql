CREATE VIEW IF NOT EXISTS {database}.book_ladders
AS
SELECT
    market_id,

    arrayReverseSort(
        x -> x.1,
        groupArrayIf(
            tuple(price, base_volume, quote_notional),
            side = 'buy'
        )
    ) AS bids,

    arraySort(
        x -> x.1,
        groupArrayIf(
            tuple(price, base_volume, quote_notional),
            side = 'sell'
        )
    ) AS asks

FROM {database}.book_levels

GROUP BY market_id;