from pathlib import Path
from confluent_kafka import Producer
from .config import Settings


def publish_file(settings: Settings, file_path: Path) -> None:
    if not file_path.is_file():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    config = {"bootstrap.servers": settings.bootstrap_servers, **settings.producer_config}
    producer = Producer(config)
    stats = {"queued": 0, "delivered": 0, "failed": 0, "skipped_empty": 0}

    def on_delivery(error, message):
        if error is not None:
            stats["failed"] += 1
            print(f"Delivery failed: {error}")
        else:
            stats["delivered"] += 1

    with file_path.open("rb") as stream:
        for line_number, line in enumerate(stream, start=1):
            # Strip only JSONL line delimiters, never parse or rewrite the payload.
            payload = line.removesuffix(b"\n").removesuffix(b"\r")
            if not payload.strip():
                stats["skipped_empty"] += 1
                continue
            while True:
                try:
                    producer.produce(settings.topic_name, value=payload, on_delivery=on_delivery)
                    stats["queued"] += 1
                    break
                except BufferError:
                    producer.poll(0.5)
            producer.poll(0)
            if line_number % 1000 == 0:
                print(f"Read {line_number} lines; queued {stats['queued']} records")

    remaining = producer.flush(timeout=120)
    print(f"Publish summary: {stats}, remaining_in_queue={remaining}")
    if remaining or stats["failed"] or stats["delivered"] != stats["queued"]:
        raise RuntimeError("Not all records were delivered to Kafka")
