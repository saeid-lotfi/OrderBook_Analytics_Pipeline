# Order-book analytics pipeline

Reconstructs order books from Kafka CDC events in ClickHouse and saves liquidity snapshots every ten seconds. The local stack has three ClickHouse shards with two replicas each, one Kafka broker, a Python bootstrap/scheduler and AKHQ.

**Read [the findings memo](docs/FINDINGS.md) first:** this is a historical reconstruction with a documented stale-order heuristic. Recorded validation matches both touch prices exactly in 30/31 oracle markets and within one oracle floating-point ULP in 31/31. It is not an authoritative executable book.

## Project structure

```text
config/             Kafka and ClickHouse connection settings
src/orderbook/      Producer, reference loading, schema setup and scheduler
sql/01_migrations/  Version-controlled DDL and SQL transformation views
sql/02_queries/     Scheduled snapshot INSERT, diagnostics and alerts
docker/             ClickHouse cluster/Keeper configuration and Python image
scripts/            Read-only oracle, metric and pipeline validation
tests/              Disposable-server order-storage regression
docs/               Design, operations, validation evidence and findings
data/               Supplied input files (not tracked)
docker-compose.yml  Complete local deployment
```

## Start

Run commands from the repository root. Requirements: Docker Engine with Compose v2 and network access for initial image/package downloads. Allow at least 12 GB RAM and 6 CPUs for the local stack. Ports 8123–8128, 9000–9005, 8080 and 9092 must be available.

Place the supplied files in `data/`:

| File | Purpose |
|---|---|
| `data/events.jsonl` | Input CDC stream |
| `data/markets.json` | Market reference data |
| `data/ground_truth.json` | Read-only scoring input, used only by the validator |

Keep the original archive unchanged. The ignored dataset files must accompany the submission through the authorized dataset distribution. The pipeline reads only the stream and market reference; scoring data is never used in reconstruction.

```bash
docker compose up -d --build --wait --wait-timeout 300
```

This creates the schema in dependency order, loads markets, publishes the fixture if the Kafka topic is empty, waits for ingestion/replication and starts snapshots. The worker becomes healthy when fresh snapshots cover all 36 reference markets. First-time downloads add to startup time.

```bash
docker compose ps
docker compose logs --tail 100 python
```

A normal restart avoids republishing a nonempty topic. Interrupted fixture publication is not resumable; use the clean-rebuild procedure in [operations](docs/OPERATIONS.md#clean-fixture-rebuild). Stop while retaining stored data with `docker compose down`.

## Use

Open a SQL console:

```bash
docker compose exec clickhouse-1 clickhouse-client --user orderbook --password local-dev-password --database orderbook
```

```sql
-- Latest persisted snapshot per market; expires after 30 seconds.
SELECT market_id, snapshot_at, source_as_of,
       best_bid, best_ask, spread, is_crossed
FROM current_book ORDER BY market_id;

-- One row per market, side and price for charts.
SELECT market_id, side, price, base_volume, quote_notional
FROM current_book_levels
WHERE market_id = 1 ORDER BY side, price;

-- Seven-day snapshot history, partitioned daily.
SELECT snapshot_at, spread
FROM liquidity_metrics_history
WHERE market_id = 1
  AND snapshot_at >= now('Asia/Tehran') - INTERVAL 1 DAY
ORDER BY snapshot_at;
```

Consumers should use the serving views above. The intermediate `liquidity_metrics`, `book_ladders` and `book_levels` views reconstruct results when queried. `snapshot_at` measures computation freshness; `source_as_of` is the global source-event watermark, so repeated computation does not make a historical feed live. AKHQ is available at [localhost:8080](http://localhost:8080). Credentials are for local development.

## Validate

```bash
docker compose exec -T python python scripts/validate.py
docker compose exec -T python python scripts/check_metrics.py
docker compose exec -T python python scripts/check_pipeline.py
```

The oracle validator prints per-market results, exact decimal and one-ULP touch scores, scalar differences and market coverage. Its exit status requires tolerant touch agreement and coverage; scalar differences are reported separately. The other scripts independently check metric arithmetic and fixture/storage integrity. Ground truth is never read by the production pipeline.

## Documentation and task coverage

| Document / implementation | Task request addressed |
|---|---|
| [Findings memo](docs/FINDINGS.md) | **D:** measured oracle score, stale-state defect and query evidence, heuristic costs, metric trust and upstream changes |
| [Pipeline design](docs/README.md) | **A–C:** topology, LSN precedence, resting predicate, exact arithmetic, metric definitions, serving and retention; **E:** sharding |
| [SQL guide](docs/SQL.md) | **A–C:** DDL layout, dependencies and executable installation order |
| [Operations](docs/OPERATIONS.md) | **A/C:** configuration, rejection inspection, retention, failure handling and commands; **E:** replication and alert thresholds |
| [Validation record](docs/VALIDATION.md) | **A–D:** recorded deployment results, oracle/metric checks, defect measurements, failure experiments and test limits |
| [Diagnostic SQL](sql/02_queries/02_monitoring/diagnostics.sql) | **A/B/D:** captured bad messages, duplicate/stale counts and crossed books before/after mitigation |
| [Alert SQL](sql/02_queries/02_monitoring/alerts.sql) | **E:** executable monitoring conditions; external scheduling/notification is not installed |

Required parts A–D are implemented, with the accuracy and availability limits stated in the memo. Optional sharding/replication and monitoring queries are included; per-order distance-to-touch and bot-participation rollups are deferred. Bots remain included in book liquidity.

## Limits

The source-day heuristic can remove valid overnight orders and retain stale orders updated today. Snapshot batches are not published atomically across shards, and computation freshness does not prove source freshness.

Latest-order state (`orders_latest_local`) uses ID-hash partitions, not date partitions, and has no TTL. This preserves replacement across days and retains terminal images, but the population grows with distinct order IDs. `FINAL` reconciliation and live-order filtering therefore become more expensive as orders accumulate; the source-day filter cannot prune daily partitions. See [calculation cost](docs/README.md#cost-of-calculating-live-orders).

The fixture timings are not a production capacity claim.
