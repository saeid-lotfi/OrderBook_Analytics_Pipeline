from pathlib import Path

from .client import ClickHouseManager


class MigrationRunner:
    """Manage ClickHouse schema and ingestion infrastructure."""

    def __init__(self, manager: ClickHouseManager):
        self.manager = manager
        self.config = manager.config

        self.database = manager.database
        self.cluster = manager.cluster

        self.migrations_dir = Path(
            self.config["migrations"]["directory"]
        )

    def read_sql(self, filename: str) -> str:
        path = self.migrations_dir / filename

        if not path.is_file():
            raise FileNotFoundError(
                f"Migration file not found: {path}"
            )

        return path.read_text(encoding="utf-8").format(
            database=self.database,
            cluster=self.cluster,
        )

    def execute_migration(
        self,
        filename: str,
        all_nodes: bool = True,
    ) -> None:
        sql = self.read_sql(filename)

        print(f"Applying {filename}")

        if all_nodes:
            self.manager.execute_on_all_nodes(sql)
        else:
            self.manager.execute(
                self.manager.get_primary_host(),
                sql,
            )

    def create_database(self) -> None:
        self.manager.execute_on_all_nodes(
            f"CREATE DATABASE IF NOT EXISTS {self.database}"
        )

    def create_raw_local_tables(self) -> None:
        self.execute_migration(
            "02_ingestion/raw_local.sql",
            all_nodes=True,
        )

    def create_distributed_table(self) -> None:
        self.execute_migration("02_ingestion/raw_distributed.sql")

    def create_kafka_engine(self) -> None:
        self.execute_migration("02_ingestion/kafka_engine.sql", all_nodes=False)

    def create_materialized_view(self) -> None:
        self.execute_migration("02_ingestion/kafka_materialized_view.sql", all_nodes=False)

    def create_typed_tables(self) -> None:
        self.execute_migration(
            "03_orders/orders_versions_local.sql",
            all_nodes=True,
        )

        self.execute_migration("03_orders/orders_versions_distributed.sql")

    def create_dead_letter_tables(self) -> None:
        self.execute_migration(
            "04_validation/dead_letter_local.sql",
            all_nodes=True,
        )

        self.execute_migration("04_validation/dead_letter_distributed.sql")

    def create_parsing_views(self) -> None:
        self.execute_migration(
            "04_validation/mv_raw_to_versions.sql",
            all_nodes=True,
        )

        self.execute_migration(
            "04_validation/mv_raw_to_dead_letter.sql",
            all_nodes=True,
        )

    def create_order_views(self) -> None:
        self.execute_migration("03_orders/orders_latest_local.sql")
        self.execute_migration("03_orders/orders_latest_distributed.sql")
        self.execute_migration("03_orders/mv_versions_to_latest.sql")
        self.execute_migration("03_orders/orders_current.sql")
        self.execute_migration("05_book/live_orders.sql")

    def create_metric_views(self) -> None:
        """Create the order-book and liquidity metric views."""

        self.execute_migration("05_book/book_levels.sql")
        self.execute_migration("05_book/book_ladders.sql")
        self.execute_migration("05_book/liquidity_metrics.sql")

    def create_metric_history(self) -> None:
        """Create snapshot storage across all three shards."""

        self.execute_migration(
            "06_snapshots/liquidity_metrics_history.sql",
            all_nodes=True,
        )

        self.execute_migration(
            "06_snapshots/liquidity_metrics_history_distributed.sql"
        )

    def setup_metrics(self) -> None:
        """Install metrics without rebuilding the CDC pipeline."""

        print("Creating liquidity metric infrastructure...")

        self.create_metric_views()
        self.create_metric_history()
        self.execute_migration("07_serving/current_book.sql")
        self.execute_migration("07_serving/current_book_levels.sql")

        print("Liquidity metric infrastructure created.")

    def setup_typed_layer(self) -> None:
        self.create_typed_tables()
        self.create_dead_letter_tables()
        self.create_parsing_views()
        self.create_order_views()

    def setup(self) -> None:
        """Initialize the complete ClickHouse pipeline."""

        print("Initializing ClickHouse pipeline...")

        # 1. Create database
        self.create_database()

        # 2. Create market reference table FIRST
        self.execute_migration("01_reference/markets.sql")

        # 3. Create raw CDC infrastructure
        self.create_raw_local_tables()
        self.create_distributed_table()

        # 4. Create typed CDC tables and views
        self.setup_typed_layer()

        # 5. Create liquidity metric views
        self.setup_metrics()

        # 6. Start Kafka ingestion
        self.create_kafka_engine()
        self.create_materialized_view()

        print("ClickHouse pipeline initialized.")
