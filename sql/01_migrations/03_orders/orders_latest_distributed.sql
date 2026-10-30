CREATE TABLE IF NOT EXISTS {database}.orders_latest
AS {database}.orders_latest_local
ENGINE = Distributed('{cluster}', '{database}', 'orders_latest_local', cityHash64(toString(id)));
