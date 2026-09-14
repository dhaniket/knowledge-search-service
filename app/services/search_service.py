import asyncio
import logging

from app.errors.search_errors import (
    SearchUnavailableError,
)
from app.repositories.article_search_repository import (
    ArticleSearchRepository,
)
from app.repositories.search_cache_repository import (
    SearchCacheRepository,
)

logger = logging.getLogger(__name__)


class SearchService:

    def __init__(
        self,
        search_repository: ArticleSearchRepository,
        cache_repository: SearchCacheRepository,
        elasticsearch_timeout: float,
        redis_timeout: float,
    ) -> None:

        self.search_repository = search_repository

        self.cache_repository = cache_repository

        self.elasticsearch_timeout = elasticsearch_timeout

        self.redis_timeout = redis_timeout

    async def _get_cached_results(
        self,
        query: str,
        limit: int,
    ) -> list[dict] | None:

        try:

            async with asyncio.timeout(self.redis_timeout):

                return await self.cache_repository.get(
                    query=query,
                    limit=limit,
                )

        except TimeoutError:

            logger.warning("Redis cache read " "timed out")

            return None

    async def _cache_results(
        self,
        query: str,
        limit: int,
        results: list[dict],
    ) -> None:

        try:

            async with asyncio.timeout(self.redis_timeout):

                await self.cache_repository.set(
                    query=query,
                    limit=limit,
                    results=results,
                )

        except TimeoutError:

            logger.warning("Redis cache write " "timed out")

    async def search_articles(
        self,
        query: str,
        limit: int,
    ) -> list[dict]:

        cached_results = await self._get_cached_results(
            query=query,
            limit=limit,
        )

        if cached_results is not None:

            return cached_results

        try:

            async with asyncio.timeout(self.elasticsearch_timeout):

                results = await self.search_repository.search(
                    query=query,
                    limit=limit,
                )

        except TimeoutError as exc:

            raise SearchUnavailableError("Search request timed out") from exc

        await self._cache_results(
            query=query,
            limit=limit,
            results=results,
        )

        return results
