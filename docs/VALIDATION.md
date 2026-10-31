# Validation record

Validated on 2026-10-30 against the supplied dataset using ClickHouse `26.8.36.3`, Kafka `4.1.2`, Python `3.12`, and the dependencies pinned in `requirements.txt`.

## Clean deployment

Removed containers and Kafka/ClickHouse volumes with `docker compose down -v --remove-orphans`, then ran the README's single startup command. Startup completed successfully in **89.8 seconds** with cached images and Python dependencies. This timing excludes initial network downloads. All six ClickHouse nodes, Kafka, and the Python worker reported healthy. No old database volume was reused.

## Automated checks

The following commands all exited with status zero on the final clean deployment:

```bash
docker compose exec -T python python scripts/validate.py
docker compose exec -T python python scripts/check_metrics.py
docker compose exec -T python python scripts/check_pipeline.py
```

| Check | Observed result |
|---|---|
| Raw messages | 16,308; exactly match original JSONL payloads after removing line delimiters |
| Kafka identities | 16,308 unique topic/partition/offset tuples |
| Accepted versions | 16,282 |
| Dead letters | 26: 19 invalid JSON, 6 invalid prices, 1 invalid side |
| Current orders | 6,604; every selected LSN matches the maximum accepted source LSN |
| Duplicate accepted versions | 224 deliveries beyond distinct `(id, LSN)` pairs |
| Sharding | Three shards, two active replicas per shard |
| Replicated table health | All 36 local replication entries writable; five business tables have two replicas each, reference markets have six copies |
| Replica parity | Raw, typed and rejected counts agree within each shard pair; latest-state unique IDs also agree |
| Retained state | ReplicatedReplacingMergeTree with LSN version; no inherited seven-day TTL |
| Served markets | 36: 31 two-sided, 2 bid-only, 3 empty |
| Served levels | 1,312 |
| Crossed books | 0 after heuristic; 36 of 36 two-sided candidates before it |
| Oracle touch comparison | 30/31 literal decimal matches; 31/31 within one oracle binary64 ULP |
| Independent metric checks | Passed for all 36 markets, including empty sides |

The independent checks cover sorted distinct prices, best/second prices, gaps, spread, crossed flags, base × price notional, inclusive band counts/sums, currency-specific thresholds, insufficient-depth nulls, and fill impacts to 18 fractional places. Fill-impact comparisons allow less than `1e-18` absolute truncation error. Oracle scalar comparison reports one spread discrepancy for BTC_IRT, explained in [FINDINGS.md](FINDINGS.md); the pipeline's decimal subtraction is retained.

On the final run, four completed scheduled inserts averaged **331.5 ms**, with a maximum **363 ms** and peak coordinator query memory **45,390,354 bytes** reported by `system.query_log`. This small sample leaves substantial room within a ten-second interval locally. It is not a production-scale load test, and memory excludes other nodes and background work.

The executable diagnostic and alert SQL were also run successfully. On a completed historical replay, the landing/source-staleness alerts are expected to fire; no incoming live stream exists. Python AST parsing, JSON parsing, Compose configuration validation and `git diff --check` passed.

## Controlled failure checks

Before the final clean rebuild, stopped the Python scheduler and `clickhouse-2`, leaving `clickhouse-5` as shard 2's available replica and two Keeper members running. Reads through nodes 1 and 4 still returned **6,604 current orders**. The Distributed raw table still returned **16,308 messages**. After the thirty-second freshness window, `current_book` returned **zero rows**, while **756 history rows** remained. Restored both services; the worker resumed scheduled snapshots without republishing the fixture, and the integrity check again found only 16,308 raw messages.

These checks establish read failover and computation expiry on this fixture. They do not establish uninterrupted quorum writes while a replica is absent, automatic relocation of the node-1 consumer/scheduler, crash-atomic materialized-view delivery, or atomic publication across shards. Those limits are described in [OPERATIONS.md](OPERATIONS.md).

## Dataset preservation and remaining scope

Working copies and supplied originals have identical SHA-256 hashes:

- Events: `12054e7146d74e6053fcfdc76a5dd4f0157a9ae8dd8950238363b5e8a6d20ffb`
- Markets: `1015aa6736eccbd1c6f9f49ed7ffe91663bdd337fce4aad53d36ddb316d35838`

No production pipeline module reads ground truth. The scoring script is a separate read-only tool. TTL clauses were inspected, but seven days of wall-clock retention and long-running disk reclamation were not simulated. Production volume, rolling ingestion failures, source-gap recovery, bot-at-touch rollups and performance optimization remain future work.

## Startup convergence

AKHQ returned HTTP 200 and reported healthy in the clean stack. All three validation scripts passed after ingestion settled. A check during the first snapshot briefly reported incomplete reconstruction; subsequent snapshots converged. Readiness proves recent market coverage rather than an atomic source watermark, so it should not be interpreted as transactional publication.

## Daily partitions and current-state replacement

The storage regression in `tests/check_order_storage.py` passed against a disposable ClickHouse 26.8.36.3 server. It loads the actual migration schemas with non-replicated engines and the server's single-node `default` cluster, then exercises the actual Distributed latest-state wrapper and current/live views. With latest-state merges stopped, it verifies maximum-LSN selection, late arrivals, equal-LSN identical redelivery, updates on different ingestion days, and filtering of closed/deleted images. It checks two daily version-history partitions, retained state after history truncation, and physical replacement after a forced merge. This is a semantic regression, not a production performance or replication test; the six-node pipeline checks were not rerun at that stage; the submission verification below covers the final deployment.

Run only against an empty disposable server (the script creates and drops `orderbook_storage_test`):

```bash
python3 tests/check_order_storage.py | docker exec -i CONTAINER clickhouse-client --multiquery
```

Existing deployments must migrate or rebuild history tables when changing partition keys; rerunning setup alone does not alter table schemas. See the operations guide before changing retained data.

## Submission verification — 2026-10-31

Started the final code with `docker compose up -d --build --wait --wait-timeout 300` against newly created Kafka/ClickHouse volumes. All services became healthy. The migration runner applied all 23 SQL files, creating retained-state destinations before attaching parsing views and attaching Kafka ingestion last.

All three documented validation scripts exited zero: oracle touch agreement remained 30/31 exact and 31/31 within one oracle ULP; scalar agreement was 30/31 with the documented BTC_IRT spread difference. Independent metric checks passed for all 36 markets. Pipeline checks confirmed 16,308 raw messages = 16,282 accepted versions + 26 rejected messages, maximum-LSN reconciliation and three shards with two healthy replicas each.

Reran `diagnostics.sql`: 268/1,580 otherwise-resting orders were older-day images (16.96%); 537/16,308 landed messages were older-day rows (3.29%); crossings fell from 36/36 candidates to zero, with all 36 reference markets retained. Compose configuration, Python syntax, JSON configuration, local documentation links, the 400–800-word memo constraint and `git diff --check` passed. All 23 migration files are referenced exactly once in the runner. This rerun did not repeat the controlled failure experiment, seven-day retention simulation or production-scale benchmarking.

## Dataset path contract

The validator defaults to `data/ground_truth.json`; ingestion uses `data/events.jsonl` and `data/markets.json`. Compose mounts `data/` read-only. After unifying these paths, Python syntax, Compose configuration and path checks passed. Oracle scoring was not rerun for this path-only change because `data/ground_truth.json` is not present in the current workspace; the recorded scores above precede this change. Supply that file before running the documented oracle validation command.
