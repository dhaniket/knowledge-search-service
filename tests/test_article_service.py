from datetime import UTC, datetime

import pytest

from app.errors.search_errors import (
    SearchIndexSyncError,
)
from app.models.article import (
    KnowledgeArticle,
)
from app.schemas.article import (
    ArticleCreate,
)
from app.services.article_service import (
    ArticleService,
)


class FakeArticleRepository:

    def __init__(
        self,
        article: KnowledgeArticle,
    ) -> None:

        self.article = article

        self.create_call_count = 0

    async def create(
        self,
        article_data: ArticleCreate,
    ) -> KnowledgeArticle:

        self.create_call_count += 1

        return self.article


class FakeSearchRepository:

    def __init__(
        self,
        should_fail: bool = False,
    ) -> None:

        self.should_fail = should_fail

        self.index_call_count = 0

        self.indexed_article = None

    async def index_article(
        self,
        article: KnowledgeArticle,
    ) -> None:

        self.index_call_count += 1

        self.indexed_article = article

        if self.should_fail:

            raise SearchIndexSyncError("Elasticsearch unavailable")


class FakeCacheRepository:

    def __init__(self) -> None:

        self.clear_call_count = 0

    async def clear_all(
        self,
    ) -> None:

        self.clear_call_count += 1


@pytest.fixture
def article() -> KnowledgeArticle:

    now = datetime.now(UTC)

    return KnowledgeArticle(
        id="article-123",
        title=("How to Reset Your Password"),
        content=("Open account settings and " "select Reset Password."),
        tags=[
            "account",
            "password",
            "support",
        ],
        source="help-center",
        is_active=True,
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def article_create() -> ArticleCreate:

    return ArticleCreate(
        title=("How to Reset Your Password"),
        content=("Open account settings and " "select Reset Password."),
        tags=[
            "account",
            "password",
            "support",
        ],
        source="help-center",
    )


@pytest.mark.anyio
async def test_create_article_saves_indexes_and_clears_cache(
    article: KnowledgeArticle,
    article_create: ArticleCreate,
) -> None:

    article_repository = FakeArticleRepository(article=article)

    search_repository = FakeSearchRepository()

    cache_repository = FakeCacheRepository()

    service = ArticleService(
        article_repository=(article_repository),
        search_repository=(search_repository),
        cache_repository=(cache_repository),
        elasticsearch_timeout=1.0,
        redis_timeout=1.0,
    )

    result = await service.create_article(article_create)

    assert result == article

    assert article_repository.create_call_count == 1

    assert search_repository.index_call_count == 1

    assert search_repository.indexed_article == article

    assert cache_repository.clear_call_count == 1


@pytest.mark.anyio
async def test_create_article_still_succeeds_when_elasticsearch_fails(
    article: KnowledgeArticle,
    article_create: ArticleCreate,
) -> None:

    article_repository = FakeArticleRepository(article=article)

    search_repository = FakeSearchRepository(should_fail=True)

    cache_repository = FakeCacheRepository()

    service = ArticleService(
        article_repository=(article_repository),
        search_repository=(search_repository),
        cache_repository=(cache_repository),
        elasticsearch_timeout=1.0,
        redis_timeout=1.0,
    )

    result = await service.create_article(article_create)

    assert result == article

    assert article_repository.create_call_count == 1

    assert search_repository.index_call_count == 1

    assert cache_repository.clear_call_count == 1
