CREATE TABLE IF NOT EXISTS {database}.dead_letter_events_local
(
    raw_message String,
    error_reason String,

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,

    ingested_at DateTime64(3, 'Asia/Tehran')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (kafka_topic, kafka_partition, kafka_offset);