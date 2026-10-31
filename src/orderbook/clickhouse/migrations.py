from pathlib import Path

from .client import ClickHouseManager


class MigrationRunner:
    """Manage ClickHouse schema and ingestion infrastructure."""

    def __init__(self, manager: ClickHouseManager):
        self.manager = manager
        self.config = manager.config

        self.database = manager.database
        self.cluster = manager.cluster

        self.migrations_dir = Path(self.config["migrations"]["directory"])

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

    def apply_migrations(self, *filenames: str, all_nodes: bool = True) -> None:
        """Apply a dependency-ordered group to the same deployment targets."""
        for filename in filenames:
            self.execute_migration(filename, all_nodes=all_nodes)

    def create_raw_local_tables(self) -> None:
        self.execute_migration("02_ingestion/raw_local.sql")

    def create_distributed_table(self) -> None:
        self.execute_migration("02_ingestion/raw_distributed.sql")

    def create_kafka_engine(self) -> None:
        self.execute_migration("02_ingestion/kafka_engine.sql", all_nodes=False)

    def create_materialized_view(self) -> None:
        self.execute_migration(
            "02_ingestion/kafka_materialized_view.sql", all_nodes=False
        )

    def create_typed_tables(self) -> None:
        self.apply_migrations(
            "03_orders/orders_versions_local.sql",
            "03_orders/orders_versions_distributed.sql",
        )

    def create_dead_letter_tables(self) -> None:
        self.apply_migrations(
            "04_validation/dead_letter_local.sql",
            "04_validation/dead_letter_distributed.sql",
        )

    def create_parsing_views(self) -> None:
        self.apply_migrations(
            "04_validation/mv_raw_to_versions.sql",
            "04_validation/mv_raw_to_dead_letter.sql",
        )

    def create_order_views(self) -> None:
        """Install retained state and its consumers before enabling parsing."""
        self.apply_migrations(
            "03_orders/orders_latest_local.sql",
            "03_orders/orders_latest_distributed.sql",
            "03_orders/mv_versions_to_latest.sql",
            "03_orders/orders_current.sql",
            "05_book/live_orders.sql",
        )

    def create_metric_views(self) -> None:
        self.apply_migrations(
            "05_book/book_levels.sql",
            "05_book/book_ladders.sql",
            "05_book/liquidity_metrics.sql",
        )

    def create_metric_history(self) -> None:
        self.apply_migrations(
            "06_snapshots/liquidity_metrics_history.sql",
            "06_snapshots/liquidity_metrics_history_distributed.sql",
        )

    def create_serving_views(self) -> None:
        self.apply_migrations(
            "07_serving/current_book.sql",
            "07_serving/current_book_levels.sql",
        )

    def setup_metrics(self) -> None:
        """Install metrics, history and serving views after the order layer."""
        self.create_metric_views()
        self.create_metric_history()
        self.create_serving_views()

    def setup_typed_layer(self) -> None:
        """Create all destinations before attaching raw-event parsing views."""
        self.create_typed_tables()
        self.create_dead_letter_tables()
        self.create_order_views()
        self.create_parsing_views()

    def setup(self) -> None:
        """Install dependencies first; attach Kafka ingestion last."""
        print("Initializing ClickHouse pipeline...")

        self.create_database()
        self.execute_migration("01_reference/markets.sql")
        self.create_raw_local_tables()
        self.create_distributed_table()
        self.setup_typed_layer()
        self.setup_metrics()
        self.create_kafka_engine()
        self.create_materialized_view()

        print("ClickHouse pipeline initialized.")
