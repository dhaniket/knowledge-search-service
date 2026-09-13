from typing import Annotated

from fastapi import Depends

from app.repositories.article_repository import (
    ArticleRepository,
)
from app.repositories.article_search_repository import (
    ArticleSearchRepository,
)
from app.repositories.search_cache_repository import (
    SearchCacheRepository,
)
from app.services.article_service import (
    ArticleService,
)
from app.services.search_service import (
    SearchService,
)


def get_article_repository() -> ArticleRepository:

    return ArticleRepository()


def get_article_search_repository() -> ArticleSearchRepository:

    return ArticleSearchRepository()


def get_search_cache_repository() -> SearchCacheRepository:

    return SearchCacheRepository()


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


def get_article_service(
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


def get_search_service(
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
