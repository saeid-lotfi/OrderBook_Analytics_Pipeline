# Order-book analytics pipeline

Reconstructs a limit order book from Kafka CDC events using ClickHouse SQL, then persists liquidity snapshots every ten seconds. Start with [FINDINGS.md](FINDINGS.md) for the measured accuracy and limits. This is a historical replay, not a live exchange feed.

## Run from a clean checkout

Requirements: Docker Engine with Compose v2, network access for images/packages, and the supplied dataset. Allocate at least 12 GB RAM and 6 CPUs for six ClickHouse processes, Kafka, and Python; this is development guidance, not a production capacity claim. Host ports 8123–8128, 9000–9005, 8080 and 9092 must be free.

Keep the supplied archive unchanged. Place its JSONL stream at `data/events.jsonl` and reference data at `data/markets.json`. In this workspace the supplied stream is also archived as `task/data/events.json`; despite that extension it contains JSONL. Copy it, do not rewrite the original. Validation defaults to `task/data/ground_truth.json`; override `--truth` if your archive is elsewhere. Dataset and task files are deliberately ignored by Git and must accompany a submission through the authorized dataset distribution.

One command starts infrastructure, creates tables, loads markets, publishes the input, waits for ingestion/replication, and starts the snapshot schedule:

```bash
docker compose up -d --build --wait --wait-timeout 300
```

`python` becomes healthy only after fresh snapshots cover all 36 reference markets. Initial fixture ingestion can take several minutes. Check `docker compose logs -f python` if startup fails. A broker coordinator-loading warning may occur during initial producer setup and is retried; the delivery summary must report zero failures. A nonzero Compose result means startup did not finish successfully.

```bash
docker compose exec -T python python scripts/validate.py
docker compose exec -T python python scripts/check_metrics.py
docker compose exec -T python python scripts/check_pipeline.py
```

The validator prints PASS/FAIL per oracle market and exits nonzero for a missing, stale, or incorrect top of book. It separately reports exact decimal matches and agreement within one binary64 ULP of the supplied numeric oracle. Five reference markets are absent from the oracle: three empty books and two bid-only books. They remain visible and are checked to be empty or one-sided. `check_metrics.py` independently checks persisted ladder ordering, levels, bands, notional sums, fill impact and null behavior using Decimal arithmetic. Neither is called by the pipeline.

For a clean rerun after implementation changes (deletes Kafka and ClickHouse data):

```bash
docker compose down -v --remove-orphans
docker compose up -d --build --wait --wait-timeout 300
```

The source dataset is preserved and mounted read-only. To stop while keeping database volumes, use `docker compose down`. A normal worker restart checks whether the Kafka topic already contains records and avoids publishing the fixture again. This is a fixture bootstrap, not a resumable file-offset producer: if publication was interrupted, inspect delivery and ingestion logs and use the clean rebuild above. After seven days Kafka/raw history expires; a fixture-only deployment should be rebuilt rather than treated as an indefinite replay service.

The original files under `task/` are preserved as supplied. Their starter-environment notes describe an earlier topology; the instructions here describe the completed six-node implementation.

## Topology and data flow

The cluster `orderbook_cluster` has **three shards and two replicas per shard**:

| Shard | Replica 1 / HTTP host port | Replica 2 / HTTP host port |
|---|---|---|
| 1 | `clickhouse-1` / 8123 | `clickhouse-4` / 8126 |
| 2 | `clickhouse-2` / 8124 | `clickhouse-5` / 8127 |
| 3 | `clickhouse-3` / 8125 | `clickhouse-6` / 8128 |

AKHQ starts with the default stack and is available at [localhost:8080](http://localhost:8080).

Native ports are 9000–9005 in node-number order. Keeper runs on nodes 1–3 with a three-member Raft quorum. `SHARD_ID` and `REPLICA_NAME` provide table-path macros. Each node has its own persistent volume. Kafka is one plaintext broker, topic replication factor one; ClickHouse replication factor two does not make Kafka highly available. Local ports bind to loopback. Credentials are `orderbook` / `local-dev-password`; these are development credentials.

```mermaid
flowchart LR
  F[Read-only JSONL] --> K[Kafka order-events]
  K --> Q[Kafka engine on node 1]
  Q --> R[Distributed raw landing]
  R --> S[3 shards × 2 replicated copies]
  S --> V[Typed version history]
  S --> D[Dead-letter history]
  V --> L[Retained latest order state]
  L --> C[Global argMax by order ID and LSN]
  C --> B[Resting predicate and source-day heuristic]
  B --> M[Levels, ladders, liquidity metrics]
  M --> H[Ten-second replicated snapshots]
  H --> U[Current book and level serving views]
```

Raw routing uses `cityHash64(toString(id))` for valid IDs and a message hash otherwise. All versions of an order stay on one shard. Typed and latest Distributed tables use the same ID hash. Snapshot history is sharded by market ID. `internal_replication=true` sends a Distributed insert to one replica; `Replicated*MergeTree` propagates the stored parts to its peer. The small market reference table intentionally has one shared Keeper path and six copies so every coordinator can join it locally.

SQL is installed on all six nodes by the migration runner; the Kafka engine and its materialized view are installed only on node 1. Parsing materialized views run on whichever replica receives the original insert. Their target tables are also replicated; part replication does not rerun parsing on the peer. Kafka-to-raw and snapshot inserts request foreground Distributed delivery and quorum two. This improves acknowledgement durability, but the chained views and Kafka offset commit are not a transaction across tables. Monitor routing and replication errors and retain replayable inputs.

All serving views exist on every node. Distributed reads select one replica per shard, avoiding double-counting. Global `argMax` still reconciles versions if an ID accidentally appears on multiple shards; summing independently filtered shard-local latest rows would double-count or resurrect an older version. Changing shard count requires explicit resharding. See [architecture and operations](docs/OPERATIONS.md) for details.

## SQL layout

The SQL folders follow the pipeline layers: reference → ingestion → validation → orders → book → snapshots → serving. Scheduled snapshot queries and monitoring queries are grouped separately. See the [SQL architecture guide](sql/README.md) for the directory tree, dependencies and execution order.

## Book semantics and metric contract

`__LSN` determines source precedence. Arrival offset, Kafka time, update time, and creation time do not. Current state is selected before applying any live-order filters; otherwise an old active image can survive its later close/delete. Equal-LSN redeliveries are assumed identical; offsets break query ties, but conflicting same-LSN images need a source contract.

A resting candidate has `op != 'd'`, `mode = 'limit'`, `state = 'active'`, no cancel request, no settled-in-match flag, positive `amount1 - exchanged1`, and positive price. Bots and all services remain included. We then keep only candidates whose latest commit is on the latest observed source date in `Asia/Tehran`. This is the explicit stale-order heuristic, inferred from events, not from ground truth and not from today's wall clock. It excludes genuine unchanged overnight orders and fails if stale images receive fresh commits. A future-dated outlier can also advance the cutoff incorrectly.

Levels sum unfilled base amounts by market, side and exact price. Notional is remaining base × limit price; `amount2 - exchanged2` is not substituted. Ladders contain `(price, base_volume, quote_notional)` tuples, bids descending and asks ascending. Empty markets stay visible, with null touch prices, zero level counts, and empty ladders. `is_crossed=1` means bid ≥ ask when both sides exist; a zero flag on an empty book is not evidence of tradable liquidity.

| Output | Meaning |
|---|---|
| `best_bid`, `best_ask` | Highest bid / lowest ask |
| `second_bid`, `second_ask` | Next distinct level; null if absent |
| `spread` | Ask minus bid in price units |
| `bid_gap`, `ask_gap` | Best-to-second absolute price distance; null if absent |
| `*_depth_count` | Level counts in inclusive bands `[0.1%, 0.5%, 1%, 2%, 5%]` from that side's touch |
| `*_depth_volume` | Quote notional in the same band order |
| `fill_thresholds` | IRT: 50M, 100M, 200M, 500M, 1B, 2B, 5B, 10B; USDT: 500, 2k, 5k, 10k, 20k, 50k, 100k |
| `*_fill_impact` | Relative move to the first level whose cumulative notional meets the matching threshold; null if insufficient |

Prices use `Decimal(38,7)`, base amounts `Decimal(76,18)`, and notionals `Decimal(76,25)`. Relative impacts use explicit `divideDecimal(...,18)`, truncating only below 18 fractional places. No monetary calculation in the pipeline uses floats. All stored timestamps explicitly use `Asia/Tehran`.

## Serving and history

Consumers should query `orderbook.current_book` or `orderbook.current_book_levels`. Both read persisted scheduled snapshots. `liquidity_metrics`, `book_ladders`, and `book_levels` are derivation/debug views and recompute when queried.

```sql
SELECT market_id, snapshot_at, source_as_of, best_bid, best_ask, spread, is_crossed
FROM orderbook.current_book ORDER BY market_id;

SELECT market_id, side, price, base_volume, quote_notional
FROM orderbook.current_book_levels WHERE market_id = 1 ORDER BY side, price;

SELECT snapshot_at, market_id, spread
FROM orderbook.liquidity_metrics_history
WHERE snapshot_at >= toDateTime('2026-10-30 00:00:00', 'Asia/Tehran')
  AND snapshot_at < toDateTime('2026-10-31 00:00:00', 'Asia/Tehran')
  AND market_id = 1 ORDER BY snapshot_at;
```

Snapshots run every ten seconds, subtracting computation time from the interval. The final small-fixture check averaged 331.5 ms across four inserts (363 ms maximum), supporting that cadence locally; see the [validation record](docs/VALIDATION.md) for its limits. If a run takes longer, the next starts immediately; jobs never overlap. `snapshot_at` is the query start time. A market falls out of `current_book` automatically once its latest snapshot is older than 30 seconds, even if historical rows remain. `source_as_of` separately exposes source progress: a fresh computation over the August fixture is still historical data. Production needs feed heartbeat/sequence monitoring; computation freshness alone cannot prove exchange freshness.

Raw events, typed versions, dead letters, and snapshots have seven-day TTLs based on ingestion/snapshot time, not the old fixture's event time. TTL deletion is asynchronous. Raw/dead-letter partitions are monthly, snapshots daily, and version partitions use 16 stable ID buckets. The latest-state table has no TTL and compacts to one image per ID, including tombstones. It prevents dormant-order loss or resurrection after history expiry, but distinct historical IDs still accumulate. Safe tombstone retirement requires a replay watermark and authoritative checkpoint; arbitrary expiry would sacrifice correctness.

## Task coverage and next stage

A: Kafka ingestion, exact decimals, LSN reconciliation, replicated storage, rejection capture, explicit timezone and retention. B: documented resting predicate, measured heuristic, aggregated levels, all required metrics and crossed flags. C: scheduled persisted books, automatic freshness expiry, levels and seven-day history. D: validation script and [400–800-word findings memo](FINDINGS.md). Optional E: implemented sharding/replication plus operational alert queries; per-order distance and bot-participation rollups are deferred. Bots are included in the reconstructed liquidity.

With another day: benchmark 340 markets / 70 million versions, reduce repeated scans and network aggregation, add source watermarks and atomic batch publication, test rolling failures under ongoing ingestion, and plan latest-state/tombstone archival. These are the next optimization and production-hardening stage; no production-scale throughput claim is made here.
