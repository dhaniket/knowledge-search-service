import asyncio
import logging

from app.errors.search_errors import (
    SearchIndexSyncError,
)
from app.models.article import (
    KnowledgeArticle,
)
from app.repositories.article_search_repository import (
    ArticleSearchRepository,
)
from app.repositories.search_cache_repository import (
    SearchCacheRepository,
)

logger = logging.getLogger(__name__)


class ArticleBackgroundService:

    def __init__(
        self,
        search_repository: ArticleSearchRepository | None,
        cache_repository: SearchCacheRepository | None,
        elasticsearch_timeout: float,
        redis_timeout: float,
    ) -> None:

        self.search_repository = search_repository

        self.cache_repository = cache_repository

        self.elasticsearch_timeout = elasticsearch_timeout

        self.redis_timeout = redis_timeout

    async def _index_article(
        self,
        article: KnowledgeArticle,
    ) -> None:

        if self.search_repository is None:

            logger.warning(
                "Skipping Elasticsearch "
                "indexing for article %s "
                "because Elasticsearch "
                "is unavailable",
                article.id,
            )

            return

        try:

            async with asyncio.timeout(self.elasticsearch_timeout):

                await self.search_repository.index_article(article)

        except TimeoutError:

            logger.warning(
                "Elasticsearch indexing " "timed out for article %s",
                article.id,
            )

        except SearchIndexSyncError:

            logger.warning(
                "Failed to index " "article %s",
                article.id,
                exc_info=True,
            )

    async def _clear_search_cache(
        self,
    ) -> None:

        if self.cache_repository is None:
            return

        try:

            async with asyncio.timeout(self.redis_timeout):

                await self.cache_repository.clear_all()

        except TimeoutError:

            logger.warning("Redis search-cache " "invalidation timed out")

    async def process_created_article(
        self,
        article: KnowledgeArticle,
    ) -> None:

        logger.warning(
            "BACKGROUND: processing started " "for article %s",
            article.id,
        )

        async with asyncio.TaskGroup() as group:

            group.create_task(self._index_article(article))

            group.create_task(self._clear_search_cache())

        logger.warning(
            "BACKGROUND: processing completed " "for article %s",
            article.id,
        )
