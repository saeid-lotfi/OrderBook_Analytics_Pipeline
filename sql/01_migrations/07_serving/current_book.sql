CREATE OR REPLACE VIEW {database}.current_book AS
SELECT *
FROM {database}.liquidity_metrics_history
WHERE snapshot_at >= now64(3, 'Asia/Tehran') - INTERVAL 30 SECOND
ORDER BY snapshot_at DESC
LIMIT 1 BY market_id;
