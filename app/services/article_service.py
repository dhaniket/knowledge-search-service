import logging

from app.errors.search_errors import (
    SearchIndexSyncError,
)
from app.models.article import (
    KnowledgeArticle,
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
from app.schemas.article import (
    ArticleCreate,
)

logger = logging.getLogger(__name__)


class ArticleService:

    def __init__(
        self,
        article_repository: ArticleRepository,
        search_repository: ArticleSearchRepository,
        cache_repository: SearchCacheRepository,
    ) -> None:

        self.article_repository = article_repository

        self.search_repository = search_repository

        self.cache_repository = cache_repository

    async def create_article(
        self,
        article_data: ArticleCreate,
    ) -> KnowledgeArticle:

        article = await self.article_repository.create(article_data)

        try:

            await self.search_repository.index_article(article)

        except SearchIndexSyncError:

            logger.warning(
                "Article %s was saved "
                "to MongoDB but could not "
                "be indexed in "
                "Elasticsearch",
                article.id,
                exc_info=True,
            )

        await self.cache_repository.clear_all()

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
