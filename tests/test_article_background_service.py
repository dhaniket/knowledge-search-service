import pytest

from app.errors.search_errors import (
    SearchIndexSyncError,
)
from app.services.article_background_service import (
    ArticleBackgroundService,
)


class FakeSearchRepository:

    def __init__(
        self,
        should_fail: bool = False,
    ):

        self.should_fail = should_fail

        self.index_calls = 0

    async def index_article(
        self,
        article,
    ):

        self.index_calls += 1

        if self.should_fail:

            raise SearchIndexSyncError("Elasticsearch failed")


class FakeCacheRepository:

    def __init__(self):

        self.clear_calls = 0

    async def clear_all(
        self,
    ):

        self.clear_calls += 1


@pytest.mark.anyio
async def test_background_processing_indexes_and_clears_cache(
    article,
):

    search_repository = FakeSearchRepository()

    cache_repository = FakeCacheRepository()

    service = ArticleBackgroundService(
        search_repository=(search_repository),
        cache_repository=(cache_repository),
        elasticsearch_timeout=1.0,
        redis_timeout=1.0,
    )

    await service.process_created_article(article)

    assert search_repository.index_calls == 1

    assert cache_repository.clear_calls == 1


@pytest.mark.anyio
async def test_elasticsearch_failure_does_not_stop_cache_invalidation(
    article,
):

    search_repository = FakeSearchRepository(should_fail=True)

    cache_repository = FakeCacheRepository()

    service = ArticleBackgroundService(
        search_repository=(search_repository),
        cache_repository=(cache_repository),
        elasticsearch_timeout=1.0,
        redis_timeout=1.0,
    )

    await service.process_created_article(article)

    assert search_repository.index_calls == 1

    assert cache_repository.clear_calls == 1


@pytest.mark.anyio
async def test_background_processing_handles_missing_optional_services(
    article,
):

    service = ArticleBackgroundService(
        search_repository=None,
        cache_repository=None,
        elasticsearch_timeout=1.0,
        redis_timeout=1.0,
    )

    await service.process_created_article(article)
