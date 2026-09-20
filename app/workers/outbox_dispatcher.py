import asyncio
import logging

from datetime import UTC, datetime

from app.db.mongodb import (
    initialize_mongodb,
    close_mongodb,
    ensure_outbox_indexes,
    get_database,
)
from app.messaging.kafka import (
    initialize_kafka,
    close_kafka,
    get_kafka_producer,
    get_article_event_topic,
    get_kafka_publish_timeout,
)
from app.messaging.rabbitmq import (
    initialize_rabbitmq,
    close_rabbitmq,
    get_rabbitmq_channel,
    get_article_index_queue,
)
from app.messaging.article_index_job_publisher import (
    ArticleIndexJobPublisher,
)
from app.messaging.article_event_publisher import (
    ArticleEventPublisher,
)
from app.repositories.article_repository import (
    ArticleRepository,
)
from app.repositories.article_outbox_repository import (
    ArticleOutboxRepository,
)

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class OutboxDispatcher:

    def __init__(
        self,
        outbox_repository: ArticleOutboxRepository,
        article_repository: ArticleRepository,
        rabbit_publisher: ArticleIndexJobPublisher | None,
        kafka_publisher: ArticleEventPublisher | None,
        lease_seconds: int = 120,
    ) -> None:

        self.outbox_repository = outbox_repository
        self.article_repository = article_repository

        self.rabbit_publisher = rabbit_publisher
        self.kafka_publisher = kafka_publisher

        self.lease_seconds = lease_seconds

    async def dispatch_one(
        self,
        channel: str,
    ) -> bool:

        if channel not in ("rabbitmq", "kafka"):
            raise ValueError(f"Unsupported channel: {channel}")

        publisher = (
            self.rabbit_publisher if channel == "rabbitmq" else self.kafka_publisher
        )

        # Broker unavailable: do not claim the job.
        if publisher is None:
            return False

        document = await self.outbox_repository.claim(
            channel=channel,
            now=datetime.now(UTC),
            lease_seconds=self.lease_seconds,
        )

        if document is None:
            return False

        article_id = document["_id"]

        delivery = document["outbox"][channel]

        token = delivery["token"]
        attempts = delivery["attempts"]

        try:

            if channel == "rabbitmq":

                await self.rabbit_publisher.publish(article_id=str(article_id))

            else:

                article = await self.article_repository.get_by_id(str(article_id))

                if article is None:
                    raise RuntimeError(
                        f"Article {article_id} missing " "during Kafka dispatch"
                    )

                async with asyncio.timeout(get_kafka_publish_timeout()):
                    await self.kafka_publisher.publish_article_created(article)

        except Exception as exc:

            logger.warning(
                "Outbox publish failed: " "article=%s channel=%s attempt=%s",
                article_id,
                channel,
                attempts,
                exc_info=True,
            )

            await self.outbox_repository.mark_failed(
                article_id=article_id,
                channel=channel,
                token=token,
                now=datetime.now(UTC),
                attempts=attempts,
                error=type(exc).__name__,
            )

            return True

        # Only a confirmed publication reaches here.
        #
        # Deliberately outside the publish exception
        # handler: if MongoDB cannot record success,
        # leave the lease in place for recovery.
        await self.outbox_repository.mark_published(
            article_id=article_id,
            channel=channel,
            token=token,
            now=datetime.now(UTC),
        )

        logger.info(
            "Outbox published: article=%s " "channel=%s attempt=%s",
            article_id,
            channel,
            attempts,
        )

        return True


async def main() -> None:

    try:

        await initialize_mongodb()

        await ensure_outbox_indexes()

        database = get_database()

        article_repository = ArticleRepository(database=database)

        outbox_repository = ArticleOutboxRepository(database=database)

        logger.info("Outbox dispatcher started")

        while True:

            rabbit_publisher = None
            kafka_publisher = None

            # Each broker can recover independently.

            try:
                await initialize_rabbitmq()

                rabbit_publisher = ArticleIndexJobPublisher(
                    exchange=(get_rabbitmq_channel().default_exchange),
                    queue_name=(get_article_index_queue()),
                )

            except Exception:
                logger.warning(
                    "RabbitMQ unavailable to dispatcher",
                    exc_info=True,
                )

            try:
                await initialize_kafka()

                kafka_publisher = ArticleEventPublisher(
                    producer=get_kafka_producer(),
                    topic=get_article_event_topic(),
                )

            except Exception:
                logger.warning(
                    "Kafka unavailable to dispatcher",
                    exc_info=True,
                )

            dispatcher = OutboxDispatcher(
                outbox_repository=outbox_repository,
                article_repository=article_repository,
                rabbit_publisher=rabbit_publisher,
                kafka_publisher=kafka_publisher,
            )

            processed = False

            try:

                for channel in (
                    "rabbitmq",
                    "kafka",
                ):

                    did_work = await dispatcher.dispatch_one(channel)

                    processed = processed or did_work

            except Exception:
                logger.exception("Dispatcher iteration failed")

            # Drain backlog quickly; poll an empty
            # outbox every two seconds.
            await asyncio.sleep(0 if processed else 2)

    finally:

        try:
            await close_kafka()
        finally:
            try:
                await close_rabbitmq()
            finally:
                await close_mongodb()

        logger.info("Outbox dispatcher stopped")


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Outbox dispatcher stopped by user")
