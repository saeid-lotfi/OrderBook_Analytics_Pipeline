CREATE TABLE IF NOT EXISTS {database}.markets
(
    market_id UInt64,
    code String,
    quote_ccy LowCardinality(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/reference/{database}/markets', '{{replica}}')
ORDER BY market_id;
