CREATE TABLE IF NOT EXISTS {database}.liquidity_metrics_history_local
(
    snapshot_at DateTime64(3, 'Asia/Tehran'),

    market_id UInt64,
    quote_currency LowCardinality(String),

    best_bid Nullable(Decimal(38, 7)),
    best_ask Nullable(Decimal(38, 7)),

    second_bid Nullable(Decimal(38, 7)),
    second_ask Nullable(Decimal(38, 7)),

    spread Nullable(Decimal(38, 7)),
    bid_gap Nullable(Decimal(38, 7)),
    ask_gap Nullable(Decimal(38, 7)),

    n_bid_levels UInt64,
    n_ask_levels UInt64,

    is_crossed UInt8,

    bid_depth_count Array(UInt64),
    ask_depth_count Array(UInt64),

    bid_depth_volume Array(Decimal(76, 25)),
    ask_depth_volume Array(Decimal(76, 25)),

    fill_thresholds Array(Decimal(76, 0)),

    bid_fill_impact Array(Nullable(Decimal(76, 18))),
    ask_fill_impact Array(Nullable(Decimal(76, 18)))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(snapshot_at)
ORDER BY (market_id, snapshot_at)
TTL snapshot_at + INTERVAL 7 DAY DELETE;