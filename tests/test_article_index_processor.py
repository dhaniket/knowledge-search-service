import pytest

from app.errors.messaging_errors import (
    ArticleMissingForIndexing,
)
from app.messaging.article_index_job import (
    ArticleIndexJob,
)
from app.services.article_index_processor import (
    ArticleIndexProcessor,
)


class FakeArticleRepository:

    def __init__(self, article):
        self.article = article

    async def get_by_id(self, article_id):
        if self.article is None:
            return None

        if self.article.id == article_id:
            return self.article

        return None


class FakeSearchRepository:

    def __init__(self):
        self.documents = {}
        self.index_calls = 0

    async def index_article(self, article):
        self.index_calls += 1

        # Simulate Elasticsearch's fixed document ID.
        self.documents[article.id] = article


class FakeCacheRepository:

    def __init__(self):
        self.clear_calls = 0

    async def clear_all(self):
        self.clear_calls += 1


@pytest.mark.anyio
async def test_duplicate_job_does_not_duplicate_article(
    article,
):

    article_repository = FakeArticleRepository(article)

    search_repository = FakeSearchRepository()

    cache_repository = FakeCacheRepository()

    processor = ArticleIndexProcessor(
        article_repository=article_repository,
        search_repository=search_repository,
        cache_repository=cache_repository,
    )

    job = ArticleIndexJob.create(article_id=article.id)

    # Simulate RabbitMQ delivering the same job twice.
    await processor.process(job)
    await processor.process(job)

    assert search_repository.index_calls == 2

    # Two executions, one Elasticsearch document ID.
    assert len(search_repository.documents) == 1

    assert search_repository.documents[article.id] == article

    assert cache_repository.clear_calls == 2


@pytest.mark.anyio
async def test_missing_article_raises_permanent_error():

    processor = ArticleIndexProcessor(
        article_repository=FakeArticleRepository(None),
        search_repository=FakeSearchRepository(),
        cache_repository=None,
    )

    job = ArticleIndexJob.create(article_id="missing-article")

    with pytest.raises(ArticleMissingForIndexing):
        await processor.process(job)
