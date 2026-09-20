import pytest

from app.messaging.outbox import new_outbox_state
from app.services.article_service import ArticleService


def test_new_outbox_has_two_pending_deliveries():

    state = new_outbox_state()

    assert set(state) == {
        "rabbitmq",
        "kafka",
    }

    for channel in ("rabbitmq", "kafka"):
        assert state[channel]["status"] == "pending"
        assert state[channel]["attempts"] == 0
        assert state[channel]["next_attempt_at"] is not None


class FakeArticleRepository:

    def __init__(self, article):
        self.article = article
        self.create_calls = 0

    async def create(self, data):
        self.create_calls += 1
        return self.article


@pytest.mark.anyio
async def test_article_service_requires_only_mongodb(
    article,
    article_create,
):

    repository = FakeArticleRepository(article)

    service = ArticleService(article_repository=repository)

    result = await service.create_article(article_create)

    assert result == article
    assert repository.create_calls == 1
