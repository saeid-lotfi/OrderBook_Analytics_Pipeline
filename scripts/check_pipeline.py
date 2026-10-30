"""Read-only fixture integrity and six-node replication checks."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.orderbook.clickhouse.client import ClickHouseManager, load_clickhouse_config


def main():
    manager = ClickHouseManager(load_clickhouse_config())
    host, db = manager.get_primary_host(), manager.database
    def query(sql):
        return manager.query(host, sql)
    with Path('data/events.jsonl').open('rb') as stream:
        expected = sum(bool(line.strip()) for line in stream)
    raw, typed, rejected, offsets = query(f'''
        SELECT (SELECT count() FROM {db}.raw_events),
               (SELECT count() FROM {db}.orders_versions),
               (SELECT count() FROM {db}.dead_letter_events),
               (SELECT uniqExact(tuple(kafka_topic,kafka_partition,kafka_offset))
                FROM {db}.raw_events)''')[0]
    assert raw == expected == offsets, (raw, expected, offsets)
    assert typed + rejected == raw and rejected > 0, (raw, typed, rejected)
    # The default fixture uses one partition and a clean topic, so offset is line index.
    with Path('data/events.jsonl').open('rb') as stream:
        original = [line.removesuffix(b'\n').removesuffix(b'\r').decode('utf-8')
                    for line in stream if line.strip()]
    landed = query(f'SELECT raw_message FROM {db}.raw_events ORDER BY kafka_partition,kafka_offset')
    assert [row[0] for row in landed] == original, 'Raw payload bytes differ from fixture'
    mismatch = query(f'''
        SELECT count() FROM
        (SELECT id, max(lsn) AS expected_lsn FROM {db}.orders_versions GROUP BY id) v
        FULL OUTER JOIN {db}.orders_current c ON v.id=c.id
        WHERE v.id != c.id OR v.expected_lsn != c.lsn''')[0][0]
    assert mismatch == 0, f'{mismatch} current states differ from maximum source LSN'
    topology = query("SELECT shard_num, count() FROM system.clusters "
                     "WHERE cluster='orderbook_cluster' GROUP BY shard_num ORDER BY shard_num")
    assert topology == [(1, 2), (2, 2), (3, 2)], topology
    replicas = query(f"SELECT table, total_replicas, active_replicas, is_readonly "
                     "FROM clusterAllReplicas('orderbook_cluster',system.replicas) "
                     f"WHERE database='{db}'")
    assert len(replicas) == 36, len(replicas)
    for table, total, active, readonly in replicas:
        assert total == active == (6 if table == 'markets' else 2) and readonly == 0
    for table in ('raw_events_local', 'orders_versions_local',
                  'dead_letter_events_local', 'orders_latest_local'):
        aggregate = 'uniqExact(id)' if table == 'orders_latest_local' else 'count()'
        rows = query(f"SELECT _shard_num, {aggregate} FROM clusterAllReplicas("
                     f"'orderbook_cluster', {db}.{table}) GROUP BY _shard_num ORDER BY _shard_num")
        counts = [row[1] for row in rows]
        # clusterAllReplicas treats each replica as a separate shard in replica order.
        assert len(counts) == 6
        assert all(counts[i] == counts[i+1] for i in (0, 2, 4)), (table, counts)
    latest_ddl = query(f'SHOW CREATE TABLE {db}.orders_latest_local')[0][0]
    assert 'TTL ' not in latest_ddl
    assert 'ReplicatedReplacingMergeTree' in latest_ddl
    print(f'PASS: {raw} raw = {typed} typed + {rejected} rejected; raw payloads preserved; offsets unique; '
          'current LSNs correct; three shards × two healthy replicas; retained latest state')


if __name__ == '__main__':
    main()
