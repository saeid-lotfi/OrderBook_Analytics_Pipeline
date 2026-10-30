CREATE VIEW IF NOT EXISTS {database}.live_orders
AS
SELECT
    id,
    lsn,
    market_id,
    user_id,
    side,
    price,
    amount1,
    exchanged1,
    amount1 - exchanged1 AS remaining_base,
    amount2,
    exchanged2,
    service,
    event_ts
FROM {database}.orders_current
WHERE
    op != 'd'
    AND mode = 'limit'
    AND state = 'active'
    AND req_to_cancel = false
    AND settled_in_match = false
    AND amount1 > exchanged1;