from app.search.elasticsearch import (
    elasticsearch_client,
    get_elasticsearch_index,
)


class ArticleSearchRepository:

    def search(
        self,
        query: str,
        limit: int = 10,
    ) -> list[dict]:

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

        results = []

        for hit in response["hits"]["hits"]:

            source = hit["_source"]

            results.append(
                {
                    "id": hit["_id"],
                    "score": hit["_score"],
                    **source,
                }
            )

        return results
