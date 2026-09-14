from app.repositories.article_search_repository import (
    ArticleSearchRepository,
)
from app.repositories.search_cache_repository import (
    SearchCacheRepository,
)


class SearchService:

    def __init__(
        self,
        search_repository: ArticleSearchRepository,
        cache_repository: SearchCacheRepository,
    ) -> None:

        self.search_repository = search_repository

        self.cache_repository = cache_repository

    async def search_articles(
        self,
        query: str,
        limit: int,
    ) -> list[dict]:

        cached_results = await self.cache_repository.get(
            query=query,
            limit=limit,
        )

        if cached_results is not None:

            return cached_results

        results = await self.search_repository.search(
            query=query,
            limit=limit,
        )

        await self.cache_repository.set(
            query=query,
            limit=limit,
            results=results,
        )

        return results
