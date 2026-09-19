import pytest

from app.services.article_service import (
    ArticleService,
)
from app.errors.messaging_errors import (
    MessagePublishError,
)


class FakeIndexJobPublisher:

    def __init__(
        self,
        should_fail: bool = False,
    ):

        self.should_fail = should_fail

        self.publish_calls = 0

        self.article_ids = []

    async def publish(
        self,
        article_id: str,
    ) -> None:

        self.publish_calls += 1

        self.article_ids.append(article_id)

        if self.should_fail:

            raise MessagePublishError("RabbitMQ unavailable")


class FakeArticleRepository:

    def __init__(
        self,
        article,
    ):

        self.article = article

        self.create_calls = 0

    async def create(
        self,
        article_data,
    ):

        self.create_calls += 1

        return self.article


@pytest.mark.anyio
async def test_create_article_uses_repository(
    article,
    article_create,
):

    repository = FakeArticleRepository(article)

    service = ArticleService(article_repository=repository)

    result = await service.create_article(article_create)

    assert result == article

    assert repository.create_calls == 1


@pytest.mark.anyio
async def test_create_article_publishes_index_job(
    article,
    article_create,
):

    repository = FakeArticleRepository(article)

    publisher = FakeIndexJobPublisher()

    service = ArticleService(
        article_repository=repository,
        index_job_publisher=publisher,
    )

    result = await service.create_article(article_create)

    assert result == article

    assert repository.create_calls == 1

    assert publisher.publish_calls == 1

    assert publisher.article_ids == [article.id]


@pytest.mark.anyio
async def test_article_creation_survives_publish_failure(
    article,
    article_create,
):

    repository = FakeArticleRepository(article)

    publisher = FakeIndexJobPublisher(should_fail=True)

    service = ArticleService(
        article_repository=repository,
        index_job_publisher=publisher,
    )

    result = await service.create_article(article_create)

    assert result == article

    assert repository.create_calls == 1

    assert publisher.publish_calls == 1
