-- FINAL reconciles pending replacements on each shard before live-order filtering.
-- Every version of an ID must use the same shard and stable latest-state partition.
CREATE OR REPLACE VIEW {database}.orders_current
AS
SELECT
    id,
    lsn,
    op,
    market_id,
    user_id,
    side,
    mode,
    state,
    price,
    amount1,
    amount2,
    exchanged1,
    exchanged2,
    service,
    bot,
    req_to_cancel,
    settled_in_match,
    event_ts
FROM {database}.orders_latest FINAL;
