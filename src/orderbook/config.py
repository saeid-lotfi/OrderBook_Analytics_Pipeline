import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    topic_name: str
    partitions: int
    replication_factor: int
    topic_config: dict
    producer_config: dict
    input_file: Path


def load_settings(path: Path) -> Settings:
    """Load Kafka settings from a JSON configuration file."""

    with path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    kafka = config["kafka"]
    topic = kafka["topic"]

    return Settings(
        bootstrap_servers=kafka["bootstrap_servers"],
        topic_name=topic["name"],
        partitions=int(topic["partitions"]),
        replication_factor=int(topic["replication_factor"]),
        topic_config=topic.get("config", {}),
        producer_config=kafka.get("producer", {}),
        input_file=Path(kafka["input_file"]),
    )