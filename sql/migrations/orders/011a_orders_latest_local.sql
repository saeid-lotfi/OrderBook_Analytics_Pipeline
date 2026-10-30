CREATE TABLE IF NOT EXISTS {database}.orders_latest_local
AS {database}.orders_versions_local
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/{database}/orders_latest_local', '{{replica}}', lsn)
PARTITION BY cityHash64(id) % 16
ORDER BY id;
