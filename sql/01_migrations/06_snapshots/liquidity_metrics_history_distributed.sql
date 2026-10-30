CREATE TABLE IF NOT EXISTS {database}.liquidity_metrics_history
AS {database}.liquidity_metrics_history_local
ENGINE = Distributed(
    '{cluster}',
    '{database}',
    'liquidity_metrics_history_local',
    cityHash64(toString(market_id))
);