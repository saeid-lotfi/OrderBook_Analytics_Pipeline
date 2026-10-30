# SQL architecture

Numbered folders show the schema layers from reference data and ingestion to consumer-facing books, followed by runtime queries. Order storage precedes validation because parsing needs its destination tables. Each SQL file defines one table, materialized view, view, or operational query. SQL filenames describe their objects without numeric prefixes; directory prefixes show the layer order.

```text
sql/
├── 01_migrations/              Schema and transformation definitions
│   ├── 01_reference/           Market codes and quote currencies
│   ├── 02_ingestion/           Kafka queue → replicated raw landing
│   ├── 03_orders/              Version history → retained latest → current order
│   ├── 04_validation/          Parse accepted events; capture rejected messages
│   ├── 05_book/                Resting orders → price levels → ladders → metrics
│   ├── 06_snapshots/           Replicated, sharded snapshot history
│   └── 07_serving/             Fresh current books and chart-friendly levels
└── 02_queries/                 Runtime operations rather than schema installation
    ├── 01_snapshots/           Scheduled INSERT of metrics and ladders
    └── 02_monitoring/          Diagnostics and actionable alert checks
```

## How the layers connect

```mermaid
flowchart LR
  R[reference: markets] --> B
  I[ingestion: Kafka and raw events] --> V[validation: parsing and rejects]
  V --> O[orders: versions and latest state]
  O --> B[book: resting predicate and metrics]
  B --> Q[02_queries/01_snapshots: scheduled INSERT]
  Q --> H[snapshots: persisted history]
  H --> S[serving: current books and levels]
```

| Folder | Responsibility and main objects |
|---|---|
| `01_migrations/01_reference` | `markets`, available locally on every node for joins |
| `01_migrations/02_ingestion` | `kafka_events_queue`, `mv_kafka_to_raw`, `raw_events_local`, `raw_events` |
| `01_migrations/03_orders` | Accepted version tables, retained latest-state tables, `mv_versions_to_latest`, `orders_current` |
| `01_migrations/04_validation` | `mv_raw_to_versions`, `mv_raw_to_dead_letter`, dead-letter local and Distributed tables |
| `01_migrations/05_book` | `live_orders`, `book_levels`, `book_ladders`, `liquidity_metrics`; includes the explicit source-day heuristic |
| `01_migrations/06_snapshots` | Local replicated history and its Distributed wrapper; includes ladders and source timestamps |
| `01_migrations/07_serving` | `current_book` with 30-second expiry and `current_book_levels` derived from persisted arrays |
| `02_queries/01_snapshots` | `insert_liquidity_snapshot.sql`, executed by the ten-second Python scheduler |
| `02_queries/02_monitoring` | `diagnostics.sql` for investigation; `alerts.sql` for alert conditions |

`*_local` tables store data on a shard's replicas. Their Distributed counterparts route writes and read one replica per shard. Both belong together in their architectural layer. Book views compute intermediate results on demand; serving views expose scheduled persisted snapshots.

## Installation order

Use the documented Compose startup command. The [migration runner](../src/orderbook/clickhouse/migrations.py) resolves paths relative to `config/clickhouse.json`'s `sql/01_migrations` directory and applies dependencies explicitly:

1. Create the database and reference table.
2. Create raw landing tables.
3. Create order-version and dead-letter destinations, then their parsing views.
4. Create retained latest-order storage and its materialized view, then current orders.
5. Create resting-order, level, ladder and metric views.
6. Create snapshot-history tables, then serving views.
7. Attach Kafka ingestion after every downstream destination exists.

Do not apply files by alphabetically walking folders. For example, parsing lives in `04_validation` but requires the version destination in `03_orders`; Kafka ingestion is attached last, after all destinations exist. Query files are not migrations: the scheduler executes snapshot insertion, while operators run monitoring SQL separately.

Schemas are intended for clean deployment. The repository's [operations guide](OPERATIONS.md) explains replication, retention, failure boundaries and commands. [FINDINGS.md](FINDINGS.md) documents the accuracy limits of the reconstruction.
