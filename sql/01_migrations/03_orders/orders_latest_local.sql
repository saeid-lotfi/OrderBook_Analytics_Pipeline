-- Keep all versions of an ID in one partition, even across ingestion days.
-- No TTL: retain current images and tombstones after audit history expires.
CREATE TABLE IF NOT EXISTS {database}.orders_latest_local
AS {database}.orders_versions_local
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/{database}/orders_latest_local', '{{replica}}', lsn)
PARTITION BY cityHash64(id) % 16
ORDER BY id;
