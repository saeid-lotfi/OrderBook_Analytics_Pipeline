CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.mv_kafka_to_raw
TO {database}.raw_events
AS
SELECT
    raw_message,

    _topic AS kafka_topic,

    _partition AS kafka_partition,

    _offset AS kafka_offset,

    now64(3, 'Asia/Tehran') AS ingested_at

FROM {database}.kafka_events_queue;