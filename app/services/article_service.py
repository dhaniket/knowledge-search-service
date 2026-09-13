from app.models.article import (
    KnowledgeArticle,
)
from app.repositories.article_repository import (
    ArticleRepository,
)
from app.schemas.article import (
    ArticleCreate,
)


class ArticleService:

    def __init__(
        self,
        repository: ArticleRepository,
    ) -> None:

        self.repository = repository

    def create_article(
        self,
        article_data: ArticleCreate,
    ) -> KnowledgeArticle:

        return self.repository.create(article_data)

    def get_article(
        self,
        article_id: str,
    ) -> KnowledgeArticle | None:

        return self.repository.get_by_id(article_id)

    def list_articles(
        self,
        limit: int,
    ) -> list[KnowledgeArticle]:

        return self.repository.list(limit=limit)
