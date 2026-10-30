CREATE TABLE IF NOT EXISTS {database}.raw_events_local
(
    raw_message String,

    order_id Nullable(UInt64) MATERIALIZED
        if(
            isValidJSON(raw_message),
            nullIf(JSONExtractUInt(raw_message, 'id'), 0),
            NULL
        ),

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,

    ingested_at DateTime64(3, 'Asia/Tehran')
        DEFAULT now64(3, 'Asia/Tehran')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{database}/raw_events_local', '{{replica}}')
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (kafka_topic, kafka_partition, kafka_offset)
TTL ingested_at + INTERVAL 7 DAY DELETE;
