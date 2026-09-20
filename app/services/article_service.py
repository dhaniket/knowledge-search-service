import logging

from app.messaging.article_index_job_publisher import (
    ArticleIndexJobPublisher,
)
from app.models.article import (
    KnowledgeArticle,
)
from app.repositories.article_repository import (
    ArticleRepository,
)
from app.schemas.article import (
    ArticleCreate,
)
import asyncio

from app.messaging.article_event_publisher import (
    ArticleEventPublisher,
)
from app.messaging.kafka import (
    get_kafka_publish_timeout,
)
from app.errors.messaging_errors import (
    MessagePublishError,
    KafkaEventPublishError,
)

logger = logging.getLogger(__name__)


class ArticleService:

    def __init__(
        self,
        article_repository: ArticleRepository,
        index_job_publisher: ArticleIndexJobPublisher | None = None,
        event_publisher: ArticleEventPublisher | None = None,
    ) -> None:

        self.article_repository = article_repository
        self.index_job_publisher = index_job_publisher
        self.event_publisher = event_publisher

    async def create_article(
        self,
        article_data: ArticleCreate,
    ) -> KnowledgeArticle:

        # 1. Authoritative persistence.
        article = await self.article_repository.create(article_data)

        # 2. RabbitMQ: indexing command.
        if self.index_job_publisher is not None:
            try:
                await self.index_job_publisher.publish(article_id=article.id)

            except MessagePublishError:
                logger.warning(
                    "Article %s saved but RabbitMQ " "index job publication failed",
                    article.id,
                    exc_info=True,
                )

        # 3. Kafka: domain event.
        # Publishing is independent of RabbitMQ success.
        if self.event_publisher is not None:
            try:
                async with asyncio.timeout(get_kafka_publish_timeout()):
                    await self.event_publisher.publish_article_created(article)

            except (KafkaEventPublishError, TimeoutError):
                logger.warning(
                    "Article %s saved but Kafka " "event publication failed",
                    article.id,
                    exc_info=True,
                )

        # 4. The MongoDB resource already exists.
        return article

    async def get_article(
        self,
        article_id: str,
    ) -> KnowledgeArticle | None:

        return await self.article_repository.get_by_id(article_id)

    async def list_articles(
        self,
        limit: int,
    ) -> list[KnowledgeArticle]:

        return await self.article_repository.list(limit=limit)
