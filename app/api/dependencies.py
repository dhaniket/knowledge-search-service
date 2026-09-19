from typing import Annotated

from fastapi import Depends

from app.cache.redis import (
    get_redis_client,
    get_search_cache_ttl,
)
from app.config import (
    get_elasticsearch_operation_timeout,
    get_redis_operation_timeout,
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
from app.messaging.rabbitmq import (
    get_article_index_queue,
    get_rabbitmq_channel,
)

from app.messaging.article_index_job_publisher import (
    ArticleIndexJobPublisher,
)


async def get_optional_article_index_job_publisher() -> ArticleIndexJobPublisher | None:

    try:

        channel = get_rabbitmq_channel()

    except RuntimeError:

        return None

    return ArticleIndexJobPublisher(
        exchange=(channel.default_exchange),
        queue_name=(get_article_index_queue()),
    )


OptionalArticleIndexJobPublisherDep = Annotated[
    ArticleIndexJobPublisher | None,
    Depends(get_optional_article_index_job_publisher),
]


async def get_article_repository() -> ArticleRepository:

    return ArticleRepository(
        database=get_database(),
    )


def get_optional_search_repository() -> ArticleSearchRepository | None:

    try:

        client = get_elasticsearch_client()

    except RuntimeError:

        return None

    return ArticleSearchRepository(
        client=client,
        index_name=(get_elasticsearch_index()),
    )


OptionalSearchRepositoryDep = Annotated[
    ArticleSearchRepository | None,
    Depends(get_optional_search_repository),
]


def get_optional_cache_repository() -> SearchCacheRepository | None:

    try:

        client = get_redis_client()

    except RuntimeError:

        return None

    return SearchCacheRepository(
        client=client,
        ttl_seconds=(get_search_cache_ttl()),
    )


OptionalCacheRepositoryDep = Annotated[
    SearchCacheRepository | None,
    Depends(get_optional_cache_repository),
]


ArticleRepositoryDep = Annotated[
    ArticleRepository,
    Depends(get_article_repository),
]


async def get_article_service(
    article_repository: ArticleRepositoryDep,
    index_job_publisher: OptionalArticleIndexJobPublisherDep,
) -> ArticleService:

    return ArticleService(
        article_repository=(article_repository),
        index_job_publisher=(index_job_publisher),
    )


ArticleServiceDep = Annotated[
    ArticleService,
    Depends(get_article_service),
]


def get_search_service(
    search_repository: OptionalSearchRepositoryDep,
    cache_repository: OptionalCacheRepositoryDep,
) -> SearchService:

    return SearchService(
        search_repository=(search_repository),
        cache_repository=(cache_repository),
        elasticsearch_timeout=(get_elasticsearch_operation_timeout()),
        redis_timeout=(get_redis_operation_timeout()),
    )


SearchServiceDep = Annotated[
    SearchService,
    Depends(get_search_service),
]
