import json
from pathlib import Path

import clickhouse_connect


DEFAULT_CONFIG_PATH = Path("config/clickhouse.json")


def load_clickhouse_config(
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> dict:
    """Load ClickHouse configuration from JSON."""

    config_path = Path(config_path)

    if not config_path.is_file():
        raise FileNotFoundError(
            f"ClickHouse configuration not found: {config_path}"
        )

    with config_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    if "clickhouse" not in config:
        raise ValueError(
            "Missing 'clickhouse' section in configuration"
        )

    return config["clickhouse"]


class ClickHouseManager:
    """Manage ClickHouse connections and execute SQL."""

    def __init__(self, config: dict):
        self.config = config

        self.database = config["database"]
        self.cluster = config["cluster"]

        self.nodes = config["nodes"]
        self.connection_config = config["connection"]

    def get_client(self, host: str):
        """Create a ClickHouse client for a specific host."""

        return clickhouse_connect.get_client(
            host=host,
            port=self.connection_config.get("port", 8123),
            username=self.connection_config["username"],
            password=self.connection_config["password"],
            database="default",
            connect_timeout=self.connection_config.get(
                "timeout", 30
            ),
            send_receive_timeout=self.connection_config.get(
                "timeout", 30
            ),
        )

    def execute(self, host: str, sql: str):
        """Execute a SQL command on a ClickHouse node."""

        client = self.get_client(host)

        try:
            return client.command(sql)
        finally:
            client.close()

    def query(self, host: str, sql: str):
        """Execute a SELECT query and return the result."""

        client = self.get_client(host)

        try:
            result = client.query(sql)
            return result.result_rows
        finally:
            client.close()

    def execute_on_all_nodes(self, sql: str):
        """Execute SQL on every configured ClickHouse node."""

        for node in self.nodes:
            host = node["host"]

            print(f"Executing SQL on {host}")

            self.execute(host, sql)

    def get_primary_host(self) -> str:
        """Return the first ClickHouse node."""

        return self.nodes[0]["host"]