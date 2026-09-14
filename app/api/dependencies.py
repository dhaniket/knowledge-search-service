from typing import Annotated

from fastapi import Depends

from app.cache.redis import (
    get_redis_client,
    get_search_cache_ttl,
)
from app.db.mongodb import (
    get_database,
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
    get_elasticsearch_client,
    get_elasticsearch_index,
)
from app.services.article_service import (
    ArticleService,
)
from app.services.search_service import (
    SearchService,
)


async def get_article_repository() -> ArticleRepository:

    return ArticleRepository(
        database=get_database(),
    )


async def get_article_search_repository() -> ArticleSearchRepository:

    return ArticleSearchRepository(
        client=(get_elasticsearch_client()),
        index_name=(get_elasticsearch_index()),
    )


async def get_search_cache_repository() -> SearchCacheRepository:

    return SearchCacheRepository(
        client=get_redis_client(),
        ttl_seconds=(get_search_cache_ttl()),
    )


ArticleRepositoryDep = Annotated[
    ArticleRepository,
    Depends(get_article_repository),
]


ArticleSearchRepositoryDep = Annotated[
    ArticleSearchRepository,
    Depends(get_article_search_repository),
]


SearchCacheRepositoryDep = Annotated[
    SearchCacheRepository,
    Depends(get_search_cache_repository),
]


async def get_article_service(
    article_repository: ArticleRepositoryDep,
    search_repository: ArticleSearchRepositoryDep,
    cache_repository: SearchCacheRepositoryDep,
) -> ArticleService:

    return ArticleService(
        article_repository=(article_repository),
        search_repository=(search_repository),
        cache_repository=(cache_repository),
    )


ArticleServiceDep = Annotated[
    ArticleService,
    Depends(get_article_service),
]


async def get_search_service(
    search_repository: ArticleSearchRepositoryDep,
    cache_repository: SearchCacheRepositoryDep,
) -> SearchService:

    return SearchService(
        search_repository=(search_repository),
        cache_repository=(cache_repository),
    )


SearchServiceDep = Annotated[
    SearchService,
    Depends(get_search_service),
]
