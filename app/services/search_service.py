from app.repositories.article_search_repository import (
    ArticleSearchRepository,
)


class SearchService:

    def __init__(
        self,
        repository: ArticleSearchRepository,
    ) -> None:

        self.repository = repository

    def search_articles(
        self,
        query: str,
        limit: int,
    ) -> list[dict]:

        return self.repository.search(
            query=query,
            limit=limit,
        )
