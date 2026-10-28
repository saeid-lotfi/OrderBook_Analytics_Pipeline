import argparse
import sys
from pathlib import Path

from .client import (
    ClickHouseManager,
    load_clickhouse_config,
)
from .migrations import MigrationRunner


DEFAULT_CONFIG = Path("config/clickhouse.json")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Order Book ClickHouse Management CLI"
    )

    parser.add_argument(
        "command",
        choices=[
            "setup",
            "setup-typed",
            "create-database",
            "create-local-tables",
            "create-distributed-table",
            "create-kafka-engine",
            "create-materialized-view",
            "status",
        ],
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to ClickHouse JSON configuration",
    )

    return parser


def show_status(manager: ClickHouseManager) -> None:
    """Show row counts across all three shards."""

    database = manager.database

    tables = [
        "raw_events_local",
        "orders_versions_local",
        "dead_letter_events_local",
    ]

    for node in manager.nodes:
        host = node["host"]

        print(f"\n{host}")

        for table in tables:
            try:
                rows = manager.query(
                    host,
                    f"SELECT count() FROM {database}.{table}",
                )

                print(f"  {table}: {rows[0][0]}")

            except Exception as exc:
                print(f"  {table}: unavailable ({exc})")

    primary = manager.get_primary_host()

    print("\nCurrent orders:")

    try:
        rows = manager.query(
            primary,
            f"SELECT count() FROM {database}.orders_current",
        )

        print(rows[0][0])

    except Exception as exc:
        print(f"Unavailable: {exc}")

    print("\nLive resting orders:")

    try:
        rows = manager.query(
            primary,
            f"SELECT count() FROM {database}.live_orders",
        )

        print(rows[0][0])

    except Exception as exc:
        print(f"Unavailable: {exc}")


def main() -> None:
    parser = create_parser()
    args = parser.parse_args()

    try:
        config = load_clickhouse_config(args.config)

        manager = ClickHouseManager(config)
        runner = MigrationRunner(manager)

        commands = {
            "setup": runner.setup,
            "setup-typed": runner.setup_typed_layer,
            "create-database": runner.create_database,
            "create-local-tables": runner.create_raw_local_tables,
            "create-distributed-table":
                runner.create_distributed_table,
            "create-kafka-engine": runner.create_kafka_engine,
            "create-materialized-view":
                runner.create_materialized_view,
            "status": lambda: show_status(manager),
        }

        commands[args.command]()

    except Exception as exc:
        print(
            f"ClickHouse operation failed: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()