"""Bootstrap an empty deployment and run scheduled SQL snapshots."""
import time
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from .config import load_settings
from .producer import publish_file
from .topics import ensure_topic
from .clickhouse.client import ClickHouseManager, load_clickhouse_config
from .clickhouse.markets import MarketsLoader
from .clickhouse.metrics import MetricsManager
from .clickhouse.migrations import MigrationRunner


def main():
    settings = load_settings(Path('config/kafka.json'))
    ensure_topic(settings)
    manager = ClickHouseManager(load_clickhouse_config())
    MigrationRunner(manager).setup()
    if not manager.query(manager.get_primary_host(),
                         f'SELECT count() FROM {manager.database}.markets')[0][0]:
        MarketsLoader(manager).load(Path('data/markets.json'))
    consumer = Consumer({'bootstrap.servers': settings.bootstrap_servers,
                         'group.id': 'bootstrap-inspection', 'enable.auto.commit': False})
    try:
        high = sum(consumer.get_watermark_offsets(
            TopicPartition(settings.topic_name, p), timeout=30)[1]
            for p in range(settings.partitions))
    finally:
        consumer.close()
    if high == 0:
        publish_file(settings, settings.input_file)
    with settings.input_file.open('rb') as stream:
        expected = sum(bool(line.strip()) for line in stream)
    deadline = time.monotonic() + 240
    while True:
        received = manager.query(manager.get_primary_host(),
            f'SELECT uniqExact(tuple(kafka_topic, kafka_partition, kafka_offset)) '
            f'FROM {manager.database}.raw_events')[0][0]
        if received >= expected:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(f'Ingestion incomplete: {received}/{expected}; inspect logs. '
                               'A partially published fixture needs a clean rebuild.')
        time.sleep(2)
    # Replicated materialized-view targets synchronize independently of their sources.
    for node in manager.nodes:
        for table in ('raw_events_local', 'orders_versions_local', 'orders_latest_local',
                      'dead_letter_events_local', 'markets'):
            manager.execute(node['host'], f'SYSTEM SYNC REPLICA {manager.database}.{table}')
    MetricsManager(manager).run_periodically(10)


if __name__ == '__main__':
    main()
