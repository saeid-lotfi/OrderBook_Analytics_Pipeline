CREATE OR REPLACE VIEW {database}.current_book_levels AS
SELECT market_id, snapshot_at, level.1 AS side, level.2 AS price,
       level.3 AS base_volume, level.4 AS quote_notional
FROM {database}.current_book
ARRAY JOIN arrayConcat(
    arrayMap(x -> tuple('buy', x.1, x.2, x.3), bids),
    arrayMap(x -> tuple('sell', x.1, x.2, x.3), asks)
) AS level;
