-- Run in database orderbook with --multiquery. Rows returned indicate alerts.
SELECT m.market_id, 'missing_fresh_snapshot' AS alert
FROM markets m LEFT ANTI JOIN current_book c USING (market_id);

SELECT market_id, snapshot_at, 'crossed_book' AS alert
FROM current_book WHERE is_crossed = 1;

SELECT error_reason, count() AS recent_rejections
FROM dead_letter_events
WHERE ingested_at >= now('Asia/Tehran') - INTERVAL 5 MINUTE
GROUP BY error_reason HAVING recent_rejections > 0;

SELECT hostName(), table, active_replicas, total_replicas, absolute_delay, is_readonly
FROM clusterAllReplicas('orderbook_cluster', system.replicas)
WHERE database='orderbook'
  AND (is_readonly OR active_replicas < total_replicas OR absolute_delay > 10);

SELECT maxOrNull(ingested_at) AS last_landing, 'landing_stalled' AS alert
FROM raw_events
HAVING last_landing IS NULL OR last_landing < now('Asia/Tehran') - INTERVAL 30 SECOND;

SELECT maxOrNull(event_ts) AS last_source_commit, 'source_stale' AS alert
FROM orders_current
HAVING last_source_commit IS NULL OR last_source_commit < now('Asia/Tehran') - INTERVAL 30 SECOND;

SELECT event_time, query_duration_ms, exception_code, exception
FROM system.query_log
WHERE event_time >= now('Asia/Tehran') - INTERVAL 5 MINUTE
  AND startsWith(query, 'INSERT INTO orderbook.liquidity_metrics_history')
  AND (type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
       OR (type='QueryFinish' AND query_duration_ms > 5000));
