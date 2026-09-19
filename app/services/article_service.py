import logging

from app.errors.messaging_errors import (
    MessagePublishError,
)
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

logger = logging.getLogger(__name__)


class ArticleService:

    def __init__(
        self,
        article_repository: ArticleRepository,
        index_job_publisher: ArticleIndexJobPublisher | None = None,
    ) -> None:

        self.article_repository = article_repository

        self.index_job_publisher = index_job_publisher

    async def create_article(
        self,
        article_data: ArticleCreate,
    ) -> KnowledgeArticle:

        article = await self.article_repository.create(article_data)

        if self.index_job_publisher is not None:

            try:

                await self.index_job_publisher.publish(article_id=(article.id))

            except MessagePublishError:

                logger.warning(
                    "Article %s was saved "
                    "but its indexing job "
                    "could not be published",
                    article.id,
                    exc_info=True,
                )

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
