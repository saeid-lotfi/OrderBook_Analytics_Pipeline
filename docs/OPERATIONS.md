# Architecture, schema and operations

## Configuration ownership

Run commands from the repository root. Python 3.12 and the two pinned direct dependencies are installed in `docker/python/Dockerfile`. `requirements.txt` is the dependency list. Rebuild the image when it changes. SQL transformations stay in ClickHouse; Python only publishes bytes, installs DDL, loads reference data, schedules SQL and validates results.

| File | Owns |
|---|---|
| `docker-compose.yml` | Services, node identities, volumes, ports, health checks |
| `docker/clickhouse/cluster.xml` | Three shard pairs, internal replication, Keeper clients, macros |
| `docker/clickhouse/keeper.xml` | Embedded three-member Keeper quorum on nodes 1–3 |
| `config/kafka.json` | Producer endpoint/options, one-partition topic, seven-day broker retention, JSONL input path |
| `config/clickhouse.json` | Six DDL targets, query coordinator, credentials, timeouts, migration directory |
| `sql/migrations/ingestion/003_kafka_engine.sql` | ClickHouse consumer broker/topic/group and batch settings |
| `sql/migrations/<layer>/*.sql` | Table/view schemas, routing, predicates, types and TTLs |
| `sql/queries/snapshots/insert_liquidity_snapshot.sql` | Persisted snapshot column contract and write settings |

The Kafka engine DDL and producer JSON must agree on broker/topic. These settings are not environment overrides. The previous unused ClickHouse config sections for Kafka, table names and retention were removed because changing them had no effect. Retention belongs to the SQL DDL, and changing an existing table requires an ALTER or the documented clean rebuild. This repository's setup is intended for an empty deployment, not an online conversion of old unreplicated volumes.

## Apply order and tables

SQL files are grouped by architectural layer; see [the SQL directory guide](../sql/README.md). Folder names describe responsibilities, not execution order.

The executable apply order is `src/orderbook/clickhouse/migrations.py`, not lexicographic filename order. Numeric prefixes on the original migrations are preserved, with explicit dependency ordering for additions:

1. Database on all six nodes; `018_markets.sql` reference table.
2. `001` local raw tables; `002` Distributed raw wrappers.
3. `005`/`006` typed version tables; `007`/`008` dead-letter tables.
4. `009`/`010` raw parsing and rejection materialized views.
5. `011a` local retained latest state; `011b` Distributed wrapper; `011c` version-to-latest materialized view; `011` current-state view; `012` resting candidates.
6. `013` levels, `014` ladders, `015` metrics.
7. `016` local snapshot history including ladders/source timestamp; `017` Distributed history inheriting that schema; `020` current snapshot view; `021` serving levels.
8. `003` Kafka engine and `004` Kafka-to-raw materialized view on node 1, after all destinations exist.

DDL is submitted directly to every node instead of using `ON CLUSTER`; the configured six-node list is authoritative for installation. Setup uses CREATE IF NOT EXISTS; selected derivation views use CREATE OR REPLACE. It does not backfill materialized views from existing history. In particular, this is why an old pre-latest-state installation must be rebuilt cleanly. Stopping the worker does not detach Kafka ingestion.

| Object | Engine / purpose |
|---|---|
| `kafka_events_queue` | Kafka/RawBLOB, unparsed bytes, node 1 only |
| `mv_kafka_to_raw` | Adds Kafka identity and ingestion timestamp, routes to raw Distributed table |
| `raw_events_local` / `raw_events` | ReplicatedMergeTree / Distributed, raw replay and audit |
| `orders_versions_local` / `orders_versions` | ReplicatedMergeTree / Distributed, accepted version history |
| `dead_letter_events_local` / `dead_letter_events` | ReplicatedMergeTree / Distributed, original rejected bytes and first rejection reason |
| `mv_raw_to_versions`, `mv_raw_to_dead_letter` | Complementary validation routes on insert |
| `orders_latest_local` / `orders_latest` | ReplicatedReplacingMergeTree(LSN) / Distributed, retained state including terminal images |
| `mv_versions_to_latest` | Sends each accepted version to retained state |
| `orders_current` | Global argMax of one complete tuple per order ID |
| `live_orders` | Candidate resting predicate before date and positive-price filter |
| `book_levels` | Date/price-filtered aggregated base and quote sizes |
| `book_ladders` | Ordered arrays of distinct levels for markets with liquidity |
| `markets` | Small reference dataset, replicated to all six nodes |
| `liquidity_metrics` | Every reference market, including empty/one-sided books; live derivation |
| `liquidity_metrics_history_local` / `liquidity_metrics_history` | ReplicatedMergeTree / Distributed, persisted metrics and ladders |
| `current_book` | Latest persisted row per market within 30 seconds |
| `current_book_levels` | ARRAY JOIN of persisted bid/ask arrays, chart-friendly rows |

Merges compact latest state eventually. Queries still use argMax, so correctness does not depend on OPTIMIZE or merge timing. The latest-state partition key depends only on ID, ensuring all versions meet in one partition. Event history retains repeated deliveries for audit; logical current state reconciles them. Deletes must retain their LSN and tombstone; filtering deletes before reconciliation resurrects prior active images.

## Sharding, replication and failure boundaries

Orders are keyed by global order ID, not market/service/user. Hashing the same string form of ID in raw, version and latest wrappers co-locates all versions. Malformed payloads without a usable ID route by raw message hash. Book derivation globally reconciles IDs and then aggregates by market/side/price; markets span shards. Never sum shard-local resting books unless the placement invariant and per-ID reconciliation are guaranteed. If shard-local filtering removes a newer terminal image, an older active version elsewhere can survive.

Each shard's raw, version, latest, rejection and history tables share a Keeper path only with their paired replica. Reference markets deliberately share one path across all six nodes. Distributed reads address one replica per shard. `clusterAllReplicas` is for diagnostics and physical-copy accounting; summing it as a business dataset doubles data. All nodes expose Distributed wrappers and serving views, so an available coordinator can read other shards even if a preferred replica is down.

`internal_replication=true` is necessary for Distributed routing to one replicated destination per shard. Foreground distribution and insert quorum two are requested at Kafka landing and snapshot writes. Both shard copies must acknowledge those writes; if either is unavailable, writes to that shard fail rather than claiming two-copy durability. Other operations, such as the tiny market reference load, use the driver's defaults. Replication is asynchronous between tables, and a successful raw part alone does not prove every downstream materialized-view target succeeded. Offsets and the whole chain are not atomically committed together. Failed views require investigation and replay/backfill, with idempotent current-state reconciliation; a duplicate history snapshot can still be visible after an ambiguous retry.

The consumer runs only on node 1 and the scheduler uses node 1 as coordinator. Replica storage is redundant, but those jobs are not automatically relocated. Read failover is supported; end-to-end ingestion availability on node-1 failure is not claimed. Kafka itself has replication factor one. Keeper tolerates one of its three members being unavailable. Six containers on one host exercise the topology but do not provide host-level disaster protection. Back up source checkpoints and retained state before treating the deployment as durable infrastructure.

Snapshots publish across shards without a final batch-completion marker. During an insert, readers may briefly see a mixture of consecutive snapshots; `snapshot_at` makes that visible. Distributed replica selection can also observe lag. A future production release should publish a completed batch/watermark and gate freshness on source heartbeat and sequence continuity. These correctness/availability limits are explicit; they should not be disguised by retries or by dropping suspect markets.

## Commands and inspection

```bash
# Services and worker output.
docker compose ps
docker compose logs --tail 100 python

# SQL console; default database avoids qualifying every table.
docker compose exec clickhouse-1 clickhouse-client --user orderbook --password local-dev-password --database orderbook

# Repeatable read-only validation.
docker compose exec -T python python scripts/validate.py
docker compose exec -T python python scripts/check_metrics.py
docker compose exec -T python python scripts/check_pipeline.py

# Defect counts, captured rows, topology and replication queues.
docker compose exec -T clickhouse-1 clickhouse-client --user orderbook --password local-dev-password --database orderbook --multiquery < sql/queries/monitoring/diagnostics.sql

# Kafka UI starts with the default stack: http://localhost:8080.
docker compose logs --tail 50 akhq

# Stop without removing database volumes.
docker compose down
```

Manual CLIs remain available for diagnosis:

```bash
docker compose exec -T python python -m src.orderbook.cli create-topic
docker compose exec -T python python -m src.orderbook.clickhouse.cli status
docker compose exec -T python python -m src.orderbook.clickhouse.markets show
docker compose exec -T python python -m src.orderbook.clickhouse.metrics show
```

`src.orderbook.cli produce` deliberately republishes all fixture lines; it is not a resume command. `setup` is its compatibility alias. ClickHouse `setup` creates infrastructure; `setup-typed` targets the typed layer; the other create commands are troubleshooting steps with prerequisites. `metrics snapshot` inserts an additional snapshot and `metrics run --interval N` runs a foreground schedule. Run only one scheduler. The market loader's `load` command replaces reference contents, so stop computation before using it to change markets. To execute a shell in the already-running workspace, use `docker compose exec python bash`.

## Monitoring contract

`sql/queries/monitoring/alerts.sql` contains executable checks. In a live deployment, run them every ten seconds and page on:

| Condition | Threshold / action |
|---|---|
| Missing fresh snapshots | Any reference market absent for more than 30 seconds; page immediately |
| Crossed two-sided book | Any crossed row for two consecutive checks; suppress trading use |
| Rejected messages | Any new dead letter in five minutes; investigate affected IDs and schema |
| Replication fault | Any readonly table, missing active replica, or delay >10 seconds across two checks |
| Landing stalled | No landed record for 30 seconds while a live feed is expected; check Kafka offsets and source heartbeat |
| Source lag | Latest commit older than 30 seconds; require heartbeat to distinguish idle source from broken CDC |
| Slow recomputation | Any snapshot insert >5 seconds or failed insert during the last five minutes; investigate before the ten-second cadence is exhausted |

Landing/source-age alerts intentionally fire on a completed historical replay. Disable those two for fixture demonstrations; do not infer live freshness from a repeatedly recomputed old book. Dead-letter five-minute alerts likewise fire during initial poison-message replay. Empty-side warnings are distinct from crossed-book errors and must not be turned into successful execution quotes.

Use `system.kafka_consumers` for consumer exceptions and assignments, `system.distribution_queue` for pending sends, `system.replication_queue` for failed part work, and `system.query_log` for recomputation failures. The scheduled query's duration measures one small-fixture run only; it does not establish capacity at production scale. Seven-day TTL deletion happens in background merges, so disk alerts must allow for delayed reclamation.

## References

Implementation follows ClickHouse's [replication architecture](https://github.com/ClickHouse/docs-reference/blob/master/docs/en/development/architecture.md) and [retry deduplication documentation](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/guides/developer/deduplicating-inserts-on-retries.md). The runtime checks in this repository verify the actual pinned deployment; replication and retry behavior should be rechecked when upgrading ClickHouse.
