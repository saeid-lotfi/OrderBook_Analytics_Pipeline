CREATE TABLE IF NOT EXISTS {database}.orders_versions_local
(
    id UInt64,
    lsn UInt64,

    op LowCardinality(String),

    event_ts DateTime64(3, 'Asia/Tehran'),
    kafka_event_ts DateTime64(3, 'Asia/Tehran'),

    created_at Nullable(DateTime64(3, 'Asia/Tehran')),
    updated_at Nullable(DateTime64(3, 'Asia/Tehran')),

    market_id UInt64,
    user_id UInt64,

    side LowCardinality(String),
    mode LowCardinality(String),
    state LowCardinality(String),

    price Decimal(38, 7),

    amount1 Decimal(76, 18),
    amount2 Decimal(76, 18),

    exchanged1 Decimal(76, 18),
    exchanged2 Decimal(76, 18),

    service LowCardinality(String),
    bot Bool,
    bot_type String,

    req_to_cancel Bool,
    settled_in_match Bool,

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,

    ingested_at DateTime64(3, 'Asia/Tehran')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{database}/orders_versions_local', '{{replica}}')
PARTITION BY cityHash64(id) % 16
ORDER BY (id, lsn, kafka_topic, kafka_partition, kafka_offset)
TTL ingested_at + INTERVAL 7 DAY DELETE;
