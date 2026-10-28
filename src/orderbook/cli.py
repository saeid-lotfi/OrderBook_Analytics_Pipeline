import argparse
import sys
from pathlib import Path

from .config import load_settings
from .topics import ensure_topic
from .producer import publish_file


DEFAULT_CONFIG_PATH = Path("config/kafka.json")


def create_parser() -> argparse.ArgumentParser:
    """Create the command-line argument parser."""

    parser = argparse.ArgumentParser(
        description="Order Book Kafka Ingestion CLI"
    )

    parser.add_argument(
        "command",
        choices=["create-topic", "produce", "setup"],
        help="Operation to execute",
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Kafka configuration file (default: {DEFAULT_CONFIG_PATH})",
    )

    parser.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Override the input JSONL file configured in kafka.json",
    )

    return parser


def create_topic(settings) -> None:
    """Create or verify the configured Kafka topic."""

    print(f"Checking Kafka topic: {settings.topic_name}")
    ensure_topic(settings)


def produce_events(settings, file_path: Path | None = None) -> None:
    """Publish JSONL events to Kafka."""

    input_file = file_path or settings.input_file

    print(f"Input file: {input_file}")
    print(f"Kafka topic: {settings.topic_name}")

    publish_file(settings, input_file)


def main() -> None:
    parser = create_parser()
    args = parser.parse_args()

    try:
        # Load configuration from JSON
        settings = load_settings(args.config)

        if args.command == "create-topic":
            create_topic(settings)

        elif args.command == "produce":
            create_topic(settings)
            produce_events(settings, args.file)

        elif args.command == "setup":
            create_topic(settings)
            produce_events(settings, args.file)

    except (FileNotFoundError, ValueError, KeyError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)

    except Exception as exc:
        print(f"Execution failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()