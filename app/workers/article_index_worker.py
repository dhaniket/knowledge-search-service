import asyncio
import logging

from app.cache.redis import (
    close_redis,
    get_redis_client,
    get_search_cache_ttl,
    initialize_redis,
)
from app.db.mongodb import (
    close_mongodb,
    get_database,
    initialize_mongodb,
)
from app.messaging.rabbitmq import (
    MAX_RETRIES,
    WORKER_PREFETCH,
    close_rabbitmq,
    get_article_index_queue,
    get_dead_letter_queue,
    get_rabbitmq_channel,
    get_retry_queue,
    initialize_rabbitmq,
)
from app.messaging.reliable_publisher import (
    ReliableQueuePublisher,
)
from app.repositories.article_repository import (
    ArticleRepository,
)
from app.repositories.article_search_repository import (
    ArticleSearchRepository,
)
from app.repositories.search_cache_repository import (
    SearchCacheRepository,
)
from app.search.elasticsearch import (
    close_elasticsearch,
    get_elasticsearch_client,
    get_elasticsearch_index,
    initialize_elasticsearch,
)
from app.services.article_index_processor import (
    ArticleIndexProcessor,
)
from app.workers.article_job_handler import (
    ArticleJobHandler,
)

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


async def main() -> None:

    redis_available = False

    try:
        # MongoDB and Elasticsearch are required
        # for the indexing worker.
        await initialize_mongodb()
        await initialize_elasticsearch()

        # Redis remains optional.
        try:
            await initialize_redis()
            redis_available = True

        except Exception:
            logger.warning(
                "Redis unavailable; continuing without cache",
                exc_info=True,
            )

        await initialize_rabbitmq()

        article_repository = ArticleRepository(database=get_database())

        search_repository = ArticleSearchRepository(
            client=get_elasticsearch_client(),
            index_name=get_elasticsearch_index(),
        )

        cache_repository = None

        if redis_available:
            cache_repository = SearchCacheRepository(
                client=get_redis_client(),
                ttl_seconds=get_search_cache_ttl(),
            )

        processor = ArticleIndexProcessor(
            article_repository=article_repository,
            search_repository=search_repository,
            cache_repository=cache_repository,
        )

        channel = get_rabbitmq_channel()

        # Maximum five unacknowledged deliveries.
        await channel.set_qos(prefetch_count=WORKER_PREFETCH)

        publisher = ReliableQueuePublisher(exchange=channel.default_exchange)

        handler = ArticleJobHandler(
            processor=processor,
            publisher=publisher,
            retry_queue=get_retry_queue(),
            dead_letter_queue=get_dead_letter_queue(),
            max_retries=MAX_RETRIES,
        )

        queue = await channel.declare_queue(
            get_article_index_queue(),
            durable=True,
        )

        stop_event = asyncio.Event()
        fatal_error: Exception | None = None

        async def on_message(message) -> None:
            nonlocal fatal_error

            try:
                await handler.handle(message)

            except Exception as exc:
                # Most importantly, do NOT ACK
                # the original message here.
                #
                # Stop this worker so connection
                # closure can requeue outstanding
                # unacknowledged deliveries.
                logger.exception("Fatal message-handling failure; " "stopping worker")

                fatal_error = exc
                stop_event.set()

        await queue.consume(
            on_message,
            no_ack=False,
        )

        logger.info(
            "Article indexing worker running; " "prefetch=%s retries=%s",
            WORKER_PREFETCH,
            MAX_RETRIES,
        )

        await stop_event.wait()

        if fatal_error is not None:
            raise RuntimeError(
                "Worker stopped after an unrecoverable " "message-handling failure"
            ) from fatal_error

    finally:
        logger.info("Closing worker connections")

        try:
            await close_rabbitmq()
        finally:
            try:
                await close_redis()
            finally:
                try:
                    await close_elasticsearch()
                finally:
                    await close_mongodb()

        logger.info("Worker connections closed")


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Article indexing worker stopped by user")
