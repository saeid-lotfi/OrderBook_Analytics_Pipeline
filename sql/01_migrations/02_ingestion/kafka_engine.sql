CREATE TABLE IF NOT EXISTS {database}.kafka_events_queue
(
    raw_message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'order-events',
    kafka_group_name = 'clickhouse-orderbook-raw-v1',
    kafka_format = 'RawBLOB',
    kafka_num_consumers = 1,
    kafka_max_block_size = 1000;