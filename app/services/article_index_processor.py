import logging

from app.errors.messaging_errors import (
    ArticleMissingForIndexing,
)
from app.messaging.article_index_job import (
    ArticleIndexJob,
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

logger = logging.getLogger(__name__)


class ArticleIndexProcessor:

    def __init__(
        self,
        article_repository: ArticleRepository,
        search_repository: ArticleSearchRepository,
        cache_repository: SearchCacheRepository | None,
    ) -> None:
        self.article_repository = article_repository
        self.search_repository = search_repository
        self.cache_repository = cache_repository

    async def process(
        self,
        job: ArticleIndexJob,
    ) -> None:

        article = await self.article_repository.get_by_id(job.article_id)

        if article is None:
            raise ArticleMissingForIndexing(f"Article {job.article_id} does not exist")

        # Uses article.id as the Elasticsearch document ID.
        await self.search_repository.index_article(article)

        # Cache invalidation follows successful indexing.
        if self.cache_repository is not None:
            await self.cache_repository.clear_all()

        logger.info(
            "Indexed article %s for job %s",
            article.id,
            job.job_id,
        )
