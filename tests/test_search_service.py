import pytest

from app.services.search_service import (
    SearchService,
)
import asyncio

from app.errors.search_errors import (
    SearchUnavailableError,
)


class SlowCacheRepository:

    def __init__(self):

        self.set_calls = 0

    async def get(
        self,
        query: str,
        limit: int,
    ):

        await asyncio.sleep(0.2)

        return None

    async def set(
        self,
        query: str,
        limit: int,
        results: list[dict],
    ):

        self.set_calls += 1


class SlowSearchRepository:

    async def search(
        self,
        query: str,
        limit: int,
    ) -> list[dict]:

        await asyncio.sleep(0.2)

        return []


class FakeSearchRepository:

    def __init__(self):

        self.call_count = 0

    async def search(
        self,
        query: str,
        limit: int,
    ) -> list[dict]:

        self.call_count += 1

        return [
            {
                "id": "1",
                "title": "Result",
            }
        ]


class FakeCacheRepository:

    def __init__(
        self,
        cached_value=None,
    ):

        self.cached_value = cached_value

        self.set_calls = 0

    async def get(
        self,
        query: str,
        limit: int,
    ):

        return self.cached_value

    async def set(
        self,
        query: str,
        limit: int,
        results: list[dict],
    ):

        self.set_calls += 1


@pytest.mark.anyio
async def test_search_cache_hit_skips_elasticsearch():

    cached = [
        {
            "id": "cached",
            "title": "Cached",
        }
    ]

    search_repository = FakeSearchRepository()

    cache_repository = FakeCacheRepository(cached_value=cached)

    service = SearchService(
        search_repository=(search_repository),
        cache_repository=(cache_repository),
        elasticsearch_timeout=1.0,
        redis_timeout=1.0,
    )
    result = await service.search_articles(
        query="payment",
        limit=10,
    )

    assert result == cached

    assert search_repository.call_count == 0


@pytest.mark.anyio
async def test_search_cache_miss_uses_elasticsearch():

    search_repository = FakeSearchRepository()

    cache_repository = FakeCacheRepository()

    service = SearchService(
        search_repository=(search_repository),
        cache_repository=(cache_repository),
        elasticsearch_timeout=1.0,
        redis_timeout=1.0,
    )

    result = await service.search_articles(
        query="payment",
        limit=10,
    )

    assert len(result) == 1

    assert search_repository.call_count == 1

    assert cache_repository.set_calls == 1


@pytest.mark.anyio
async def test_search_timeout_becomes_unavailable():

    search_repository = SlowSearchRepository()

    cache_repository = FakeCacheRepository()

    service = SearchService(
        search_repository=(search_repository),
        cache_repository=(cache_repository),
        elasticsearch_timeout=0.01,
        redis_timeout=1.0,
    )

    with pytest.raises(SearchUnavailableError):

        await service.search_articles(
            query="payment",
            limit=10,
        )


@pytest.mark.anyio
async def test_redis_timeout_falls_back_to_elasticsearch():

    search_repository = FakeSearchRepository()

    cache_repository = SlowCacheRepository()

    service = SearchService(
        search_repository=(search_repository),
        cache_repository=(cache_repository),
        elasticsearch_timeout=1.0,
        redis_timeout=0.01,
    )

    result = await service.search_articles(
        query="payment",
        limit=10,
    )

    assert len(result) == 1

    assert search_repository.call_count == 1
