import argparse
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .client import (
    ClickHouseManager,
    load_clickhouse_config,
)


DEFAULT_CONFIG = Path("config/clickhouse.json")


class MetricsManager:
    """Manage ClickHouse liquidity metrics and snapshots."""

    def __init__(self, manager: ClickHouseManager):
        self.manager = manager
        self.database = manager.database
        self.host = manager.get_primary_host()

    def create_snapshot(self) -> None:
        """Insert one snapshot of all current market metrics."""

        sql = Path(
            "sql/queries/snapshots/insert_liquidity_snapshot.sql"
        ).read_text(encoding="utf-8")

        sql = sql.format(database=self.database)

        self.manager.execute(self.host, sql)

        print(
            f"[{datetime.now(ZoneInfo('Asia/Tehran')).isoformat(timespec='seconds')}] "
            "Liquidity snapshot inserted.",
            flush=True,
        )

    def show_metrics(self, limit: int = 20) -> None:
        """Display current market metrics."""

        rows = self.manager.query(
            self.host,
            f"""
            SELECT
                market_id,
                best_bid,
                best_ask,
                spread,
                n_bid_levels,
                n_ask_levels,
                is_crossed
            FROM {self.database}.liquidity_metrics
            ORDER BY market_id
            LIMIT {int(limit)}
            """,
        )

        for row in rows:
            print(row)

    def run_periodically(self, interval: int) -> None:
        """Create snapshots at a fixed interval."""

        if interval <= 0:
            raise ValueError("Interval must be positive")

        print(
            f"Starting liquidity snapshots every "
            f"{interval} seconds...",
            flush=True,
        )

        while True:
            started = time.monotonic()

            self.create_snapshot()

            elapsed = time.monotonic() - started
            time.sleep(max(0, interval - elapsed))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Order Book Liquidity Metrics CLI"
    )

    parser.add_argument(
        "command",
        choices=["snapshot", "run", "show"],
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Snapshot interval in seconds",
    )

    args = parser.parse_args()

    config = load_clickhouse_config(args.config)

    manager = ClickHouseManager(config)
    metrics = MetricsManager(manager)

    if args.command == "snapshot":
        metrics.create_snapshot()

    elif args.command == "run":
        metrics.run_periodically(args.interval)

    elif args.command == "show":
        metrics.show_metrics()


if __name__ == "__main__":
    main()
