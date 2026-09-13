from app.search.elasticsearch import (
    elasticsearch_client,
    get_elasticsearch_index,
)

response = elasticsearch_client.search(
    index=get_elasticsearch_index(),
    query={"match_all": {}},
)


for hit in response["hits"]["hits"]:

    print(
        hit["_id"],
        hit["_source"]["title"],
    )
