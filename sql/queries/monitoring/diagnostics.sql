-- Run with clickhouse-client --database orderbook --multiquery.
-- Landing integrity (one shard copy, not both replicas).
SELECT
    (SELECT count() FROM raw_events) AS raw_rows,
    (SELECT count() FROM orders_versions) AS typed_rows,
    (SELECT count() FROM dead_letter_events) AS rejected_rows;
SELECT error_reason, count(), any(kafka_partition), any(kafka_offset), any(raw_message)
FROM dead_letter_events GROUP BY error_reason ORDER BY count() DESC;

-- Source duplicates; multiple deliveries should not change current state.
SELECT count() AS versions, uniqExact(tuple(id, lsn)) AS unique_versions,
       versions - unique_versions AS redeliveries FROM orders_versions;

-- Quantify stale, otherwise-resting order images. Includes older commit times,
-- not merely old created_at (a legitimate long-lived order may be updated today).
WITH (SELECT toDate(max(event_ts), 'Asia/Tehran') FROM orders_current) AS source_day
SELECT count() AS resting_before_heuristic,
       countIf(toDate(event_ts, 'Asia/Tehran') < source_day) AS stale_resting,
       round(100.0 * stale_resting / resting_before_heuristic, 2) AS stale_percent
FROM live_orders;
WITH (SELECT toDate(max(event_ts), 'Asia/Tehran') FROM orders_current) AS source_day
SELECT count() AS raw_rows,
       countIf(isValidJSON(raw_message)
           AND toDate(fromUnixTimestamp64Milli(toInt64(JSONExtractUInt(raw_message, '__EVENT_TS')),
                      'Asia/Tehran')) < source_day) AS older_day_rows,
       round(100.0 * older_day_rows / raw_rows, 2) AS older_day_percent
FROM raw_events;

-- Crossings before and after the date heuristic. No crossed market is suppressed.
SELECT count() AS two_sided, countIf(bid >= ask) AS crossed_before_heuristic
FROM (SELECT market_id, maxIf(price, side='buy') AS bid,
             minIf(price, side='sell') AS ask,
             countIf(side='buy') AS nb, countIf(side='sell') AS na
      FROM live_orders WHERE price > 0 GROUP BY market_id)
WHERE nb > 0 AND na > 0;
SELECT count() AS markets, countIf(is_crossed) AS crossed,
       countIf(best_bid IS NULL OR best_ask IS NULL) AS one_or_zero_sided
FROM liquidity_metrics;

-- Runtime topology, replication and freshness.
SELECT shard_num, replica_num, host_name FROM system.clusters
WHERE cluster='orderbook_cluster' ORDER BY shard_num, replica_num;
SELECT hostName(), table, total_replicas, active_replicas, is_readonly,
       queue_size, absolute_delay, last_queue_update_exception
FROM clusterAllReplicas('orderbook_cluster', system.replicas)
WHERE database='orderbook' ORDER BY table, hostName();
SELECT market_id, snapshot_at, source_as_of,
       dateDiff('second', snapshot_at, now('Asia/Tehran')) AS computation_age_seconds,
       dateDiff('second', source_as_of, now('Asia/Tehran')) AS source_age_seconds,
       is_crossed FROM current_book ORDER BY market_id;
