"""Readiness requires a recent snapshot for every reference market."""
from .clickhouse.client import ClickHouseManager, load_clickhouse_config


def main():
    manager = ClickHouseManager(load_clickhouse_config())
    rows = manager.query(manager.get_primary_host(), f"""
        SELECT (SELECT count() FROM {manager.database}.current_book),
               (SELECT count() FROM {manager.database}.markets)
    """)
    current, expected = rows[0]
    raise SystemExit(0 if current == expected and expected > 0 else 1)


if __name__ == '__main__':
    main()
