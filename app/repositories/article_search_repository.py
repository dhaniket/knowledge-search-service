from app.search.elasticsearch import (
    elasticsearch_client,
    get_elasticsearch_index,
)

from app.errors.search_errors import (
    SearchIndexSyncError,
    SearchUnavailableError,
)
from app.models.article import (
    KnowledgeArticle,
)


class ArticleSearchRepository:

    def search(
        self,
        query: str,
        limit: int = 10,
    ) -> list[dict]:

        try:

            response = elasticsearch_client.search(
                index=(get_elasticsearch_index()),
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

            raise SearchUnavailableError("Search service is unavailable") from exc

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

    def index_article(
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

            elasticsearch_client.index(
                index=(get_elasticsearch_index()),
                id=article.id,
                document=document,
            )

        except Exception as exc:

            raise SearchIndexSyncError("Failed to index article") from exc
