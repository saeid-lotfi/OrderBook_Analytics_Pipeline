"""Isolated storage regression using the real migration schemas.

Run against an empty disposable ClickHouse server, for example:
python3 tests/check_order_storage.py | docker exec -i CONTAINER clickhouse-client --multiquery

Prints SQL; does not connect or change the deployment. Replication is replaced
with local engines so this test needs neither Keeper nor the six-node cluster.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
DATABASE = 'orderbook_storage_test'


def migration(name):
    sql = (ROOT / 'sql/01_migrations' / name).read_text().format(
        database=DATABASE, cluster='default')
    sql = re.sub(r"ReplicatedMergeTree\('[^']*', '[^']*'\)", 'MergeTree()', sql)
    return re.sub(r"ReplicatedReplacingMergeTree\('[^']*', '[^']*', lsn\)",
                  'ReplacingMergeTree(lsn)', sql)


print(f'CREATE DATABASE {DATABASE}; USE {DATABASE};')
for name in (
    '02_ingestion/raw_local.sql',
    '04_validation/dead_letter_local.sql',
    '03_orders/orders_versions_local.sql',
    '03_orders/orders_latest_local.sql',
    '03_orders/orders_latest_distributed.sql',
    '03_orders/mv_versions_to_latest.sql',
    '03_orders/orders_current.sql',
    '05_book/live_orders.sql',
):
    print(migration(name).rstrip().rstrip(';') + ';')

# No merges: FINAL must deliver correct state immediately via Distributed.
print('''
SYSTEM STOP MERGES orders_latest_local;
INSERT INTO orders_versions_local
(id, lsn, op, mode, state, amount1, ingested_at) VALUES
(1, 10, 'c', 'limit', 'active', 10, now64(3) - INTERVAL 1 DAY),
(2, 10, 'c', 'limit', 'active', 10, now64(3) - INTERVAL 1 DAY),
(3, 10, 'c', 'limit', 'active', 10, now64(3) - INTERVAL 1 DAY);
INSERT INTO orders_versions_local
(id, lsn, op, mode, state, amount1, ingested_at) VALUES
(1, 30, 'u', 'limit', 'closed', 10, now64(3)),
(2, 30, 'd', 'limit', 'active', 10, now64(3)),
(3, 30, 'u', 'limit', 'active', 20, now64(3));
-- Late arrivals must not overwrite newer LSNs, even on a later day.
INSERT INTO orders_versions_local
(id, lsn, op, mode, state, amount1, ingested_at) VALUES
(1, 20, 'u', 'limit', 'active', 10, now64(3)),
(2, 20, 'u', 'limit', 'active', 10, now64(3)),
(3, 30, 'u', 'limit', 'active', 20, now64(3));
SELECT throwIf(count() != 3 OR uniqExact(id) != 3 OR min(lsn) != 30,
               'FINAL must select one maximum-LSN image per order') FROM orders_current;
SELECT throwIf(count() != 1 OR min(id) != 3 OR min(amount1) != 20,
               'Filtering must not resurrect closed or deleted orders') FROM live_orders;
SELECT throwIf(uniqExact(partition) != 2, 'Version history must have daily partitions')
FROM system.parts WHERE database = currentDatabase()
AND table = 'orders_versions_local' AND active;
-- Check the TTL-free latest table survives expiration/removal of audit history.
TRUNCATE TABLE orders_versions_local;
SELECT throwIf(count() != 3 OR min(lsn) != 30,
               'Current state must survive history removal') FROM orders_current;
SYSTEM START MERGES orders_latest_local;
OPTIMIZE TABLE orders_latest_local FINAL;
SELECT throwIf(count() != 3, 'Background replacement must work across ingestion days')
FROM orders_latest_local;
SELECT 'PASS: daily history, Distributed FINAL, LSN ordering, duplicate delivery, tombstones, retained state';
DROP DATABASE orderbook_storage_test;
''')
