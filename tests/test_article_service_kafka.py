import pytest

from app.errors.messaging_errors import (
    KafkaEventPublishError,
)
from app.services.article_service import (
    ArticleService,
)


class FakeArticleRepository:

    def __init__(self, article):
        self.article = article

    async def create(self, data):
        return self.article


class FakeKafkaPublisher:

    def __init__(self):
        self.calls = 0

    async def publish_article_created(self, article):
        self.calls += 1

        raise KafkaEventPublishError("Kafka unavailable")


@pytest.mark.anyio
async def test_article_survives_kafka_failure(
    article,
    article_create,
):

    kafka = FakeKafkaPublisher()

    service = ArticleService(
        article_repository=(FakeArticleRepository(article)),
        index_job_publisher=None,
        event_publisher=kafka,
    )

    result = await service.create_article(article_create)

    assert result == article
    assert kafka.calls == 1
