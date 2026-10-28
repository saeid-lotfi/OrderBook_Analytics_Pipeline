from confluent_kafka.admin import AdminClient, NewTopic
from .config import Settings


def ensure_topic(settings: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    metadata = admin.list_topics(timeout=15)
    if settings.topic_name in metadata.topics:
        topic = metadata.topics[settings.topic_name]
        if topic.error is not None:
            raise RuntimeError(f"Topic metadata error: {topic.error}")
        actual = len(topic.partitions)
        if actual != settings.partitions:
            raise ValueError(
                f"Topic {settings.topic_name!r} has {actual} partitions; "
                f"config requires {settings.partitions}. Refusing to continue."
            )
        print(f"Topic exists: {settings.topic_name} ({actual} partition(s))")
        return

    spec = NewTopic(
        settings.topic_name,
        num_partitions=settings.partitions,
        replication_factor=settings.replication_factor,
        config=settings.topic_config,
    )
    admin.create_topics([spec])[settings.topic_name].result(timeout=30)
    print(f"Created topic: {settings.topic_name} ({settings.partitions} partition(s))")
