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
    ) -> None:
        self.article_repository = article_repository

    async def create_article(
        self,
        article_data: ArticleCreate,
    ) -> KnowledgeArticle:

        return await self.article_repository.create(article_data)

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
