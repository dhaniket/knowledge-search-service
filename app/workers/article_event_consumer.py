import argparse
import asyncio
import logging

from aiokafka import (
    AIOKafkaConsumer,
    TopicPartition,
)

from app.db.mongodb import (
    initialize_mongodb,
    get_database,
    close_mongodb,
)
from app.messaging.kafka import (
    kafka_connection_config,
    get_article_event_topic,
)
from app.messaging.article_created_event import (
    ArticleCreatedEvent,
)
import base64

from datetime import UTC, datetime

from pydantic import ValidationError

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


GROUPS = {
    "audit": "knowledge-audit-v1",
    "analytics": "knowledge-analytics-v1",
}

COLLECTIONS = {
    "audit": "article_creation_audit",
    "analytics": "article_creation_analytics_events",
}


async def process_record(
    consumer,
    collection,
    message,
) -> None:
    try:
        event = ArticleCreatedEvent.model_validate_json(message.value)

        if message.key != event.article_id.encode("utf-8"):
            raise ValueError("Kafka key does not match article ID")

    except (ValidationError, ValueError) as exc:

        poison_collection = collection.database["kafka_poison_events"]

        poison_id = f"{message.topic}:" f"{message.partition}:" f"{message.offset}"

        await poison_collection.update_one(
            {
                "_id": poison_id,
            },
            {
                "$setOnInsert": {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "key_base64": (
                        base64.b64encode(message.key or b"").decode("ascii")
                    ),
                    "value_base64": (
                        base64.b64encode(message.value or b"").decode("ascii")
                    ),
                    "error": str(exc)[:1000],
                    "recorded_at": datetime.now(UTC),
                }
            },
            upsert=True,
        )

        # Poison record was persisted successfully.
        # The consumer can now move past this offset.
        partition = TopicPartition(
            message.topic,
            message.partition,
        )

        await consumer.commit(
            {
                partition: message.offset + 1,
            }
        )

        logger.error(
            "Invalid Kafka event stored for review: %s",
            poison_id,
        )

        return

    # MongoDB _id is unique.
    # Replaying the same logical event does not
    # create a second record.
    result = await collection.update_one(
        {
            "_id": event.event_id,
        },
        {
            "$setOnInsert": {
                "event_type": event.event_type,
                "version": event.version,
                "article_id": event.article_id,
                "source": event.source,
                "occurred_at": event.occurred_at,
                "kafka_topic": message.topic,
                "kafka_partition": message.partition,
                "kafka_offset": message.offset,
            }
        },
        upsert=True,
    )

    # Commit only AFTER MongoDB acknowledges
    # the business-side effect.
    partition = TopicPartition(
        message.topic,
        message.partition,
    )

    await consumer.commit(
        {
            partition: message.offset + 1,
        }
    )

    logger.info(
        "Event %s processed; " "partition=%s offset=%s " "inserted=%s",
        event.event_id,
        message.partition,
        message.offset,
        result.upserted_id is not None,
    )


async def run(
    role: str,
    group_override: str | None = None,
) -> None:

    if role not in GROUPS:
        raise ValueError(f"Unknown consumer role: {role}")

    group_id = group_override or GROUPS[role]

    await initialize_mongodb()

    try:
        database = get_database()

        collection = database[COLLECTIONS[role]]

        consumer = AIOKafkaConsumer(
            get_article_event_topic(),
            **kafka_connection_config(),
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

        try:
            await consumer.start()

            logger.info(
                "Kafka %s consumer started; group=%s",
                role,
                group_id,
            )

            async for message in consumer:
                await process_record(
                    consumer=consumer,
                    collection=collection,
                    message=message,
                )

        finally:
            await consumer.stop()

    finally:
        await close_mongodb()


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--role",
        required=True,
        choices=["audit", "analytics"],
    )

    parser.add_argument(
        "--group",
        default=None,
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    try:
        asyncio.run(
            run(
                role=args.role,
                group_override=args.group,
            )
        )

    except KeyboardInterrupt:
        logger.info("Kafka consumer stopped by user")
