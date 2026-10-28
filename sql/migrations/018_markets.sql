CREATE TABLE IF NOT EXISTS {database}.markets
(
    market_id UInt64,
    code String,
    quote_ccy LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY market_id;