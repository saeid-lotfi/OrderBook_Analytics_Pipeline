CREATE TABLE IF NOT EXISTS {database}.orders_versions
AS {database}.orders_versions_local
ENGINE = Distributed(
    '{cluster}',
    '{database}',
    'orders_versions_local',
    cityHash64(toString(id))
);