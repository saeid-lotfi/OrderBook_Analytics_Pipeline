CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.mv_versions_to_latest
TO {database}.orders_latest_local AS
SELECT * FROM {database}.orders_versions_local;
