from elasticsearch import (
    AsyncElasticsearch,
)

from app.errors.search_errors import (
    SearchIndexSyncError,
    SearchUnavailableError,
)
from app.models.article import (
    KnowledgeArticle,
)


class ArticleSearchRepository:

    def __init__(
        self,
        client: AsyncElasticsearch,
        index_name: str,
    ) -> None:

        self.client = client

        self.index_name = index_name

    async def index_article(
        self,
        article: KnowledgeArticle,
    ) -> None:

        document = {
            "title": article.title,
            "content": article.content,
            "tags": article.tags,
            "source": article.source,
            "is_active": article.is_active,
            "created_at": article.created_at,
            "updated_at": article.updated_at,
        }

        try:

            await self.client.index(
                index=self.index_name,
                id=article.id,
                document=document,
            )

        except Exception as exc:

            raise SearchIndexSyncError("Failed to index article") from exc

    async def search(
        self,
        query: str,
        limit: int = 10,
    ) -> list[dict]:

        try:

            response = await self.client.search(
                index=self.index_name,
                size=limit,
                query={
                    "bool": {
                        "must": [
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "title^3",
                                        "tags^2",
                                        "content",
                                    ],
                                }
                            }
                        ],
                        "filter": [{"term": {"is_active": True}}],
                    }
                },
            )

        except Exception as exc:

            raise SearchUnavailableError("Search service " "is unavailable") from exc

        results = []

        for hit in response["hits"]["hits"]:

            results.append(
                {
                    "id": hit["_id"],
                    "score": hit["_score"],
                    **hit["_source"],
                }
            )

        return results
