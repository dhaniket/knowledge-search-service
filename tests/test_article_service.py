import pytest

from app.services.article_service import (
    ArticleService,
)


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
