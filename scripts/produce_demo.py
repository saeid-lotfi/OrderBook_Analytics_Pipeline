"""Send synthetic messages to Kafka to check connectivity (not the CDC pipeline)."""
import json
import os
import time
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError, KafkaException


def main():
    config = {"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]}
    # Keep synthetic messages separate from the future CDC topic.
    topic = "demo-events"
    admin = AdminClient(config)
    for future in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).values():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise

    producer = Producer(config)
    errors = []

    def delivered(error, message):
        if error:
            errors.append(str(error))
        else:
            print(f"Delivered {message.topic()}[{message.partition()}] offset={message.offset()}")

    for _ in range(10):
        event = {"id": str(uuid4()), "event_ts_ms": time.time_ns() // 1_000_000,
                 "price": "100.0000000", "amount": "1.000000000000000000"}
        producer.produce(topic, key=event["id"], value=json.dumps(event), on_delivery=delivered)
        producer.poll(0)
    remaining = producer.flush(timeout=30)
    if errors or remaining:
        raise RuntimeError(f"Delivery failed: errors={errors}, pending={remaining}")


if __name__ == "__main__":
    main()
