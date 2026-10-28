import argparse
import json
from pathlib import Path

from .client import ClickHouseManager, load_clickhouse_config


DEFAULT_MARKETS_FILE = Path("data/markets.json")
DEFAULT_CONFIG_FILE = Path("config/clickhouse.json")


class MarketsLoader:
    """Load market reference data into ClickHouse."""

    def __init__(self, manager: ClickHouseManager):
        self.manager = manager
        self.database = manager.database
        self.host = manager.get_primary_host()

    def load(self, file_path: Path) -> None:
        """Replace the markets reference data from JSON."""

        if not file_path.is_file():
            raise FileNotFoundError(
                f"Markets file not found: {file_path}"
            )

        with file_path.open("r", encoding="utf-8") as file:
            markets = json.load(file)

        if not isinstance(markets, dict):
            raise ValueError(
                "Expected a JSON object keyed by market ID"
            )

        rows = []

        for market_id, details in markets.items():
            market_id = int(market_id)

            if market_id <= 0:
                raise ValueError(
                    f"Invalid market ID: {market_id}"
                )

            code = details["code"]
            quote_ccy = details["quote_ccy"]

            if not isinstance(code, str) or not code:
                raise ValueError(
                    f"Invalid code for market {market_id}"
                )

            if quote_ccy not in ("IRT", "USDT"):
                raise ValueError(
                    f"Invalid quote currency for market {market_id}"
                )

            rows.append(
                (market_id, code, quote_ccy)
            )

        rows.sort(key=lambda row: row[0])

        client = self.manager.get_client(self.host)

        try:
            # This reference dataset is small and static.
            # Replacing its contents makes repeated loads idempotent.
            client.command(
                f"TRUNCATE TABLE {self.database}.markets"
            )

            client.insert(
                table=f"{self.database}.markets",
                data=rows,
                column_names=[
                    "market_id",
                    "code",
                    "quote_ccy",
                ],
            )

        finally:
            client.close()

        print(f"Loaded {len(rows)} markets.")

    def show(self) -> None:
        """Display market reference data."""

        rows = self.manager.query(
            self.host,
            f"""
            SELECT
                market_id,
                code,
                quote_ccy
            FROM {self.database}.markets
            ORDER BY market_id
            """,
        )

        for row in rows:
            print(row)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manage ClickHouse market reference data"
    )

    parser.add_argument(
        "command",
        choices=["load", "show"],
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_FILE,
    )

    parser.add_argument(
        "--file",
        type=Path,
        default=DEFAULT_MARKETS_FILE,
    )

    args = parser.parse_args()

    config = load_clickhouse_config(args.config)
    manager = ClickHouseManager(config)

    loader = MarketsLoader(manager)

    if args.command == "load":
        loader.load(args.file)

    elif args.command == "show":
        loader.show()


if __name__ == "__main__":
    main()