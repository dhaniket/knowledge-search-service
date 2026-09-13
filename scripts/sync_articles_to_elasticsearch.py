from app.db.mongodb import database

from app.search.elasticsearch import (
    elasticsearch_client,
    get_elasticsearch_index,
)

collection = database["articles"]

index_name = get_elasticsearch_index()


documents = collection.find({"is_active": True})


count = 0


for document in documents:

    article_id = str(document["_id"])

    search_document = {
        "title": document["title"],
        "content": document["content"],
        "tags": document["tags"],
        "source": document["source"],
        "is_active": document["is_active"],
        "created_at": document["created_at"],
        "updated_at": document["updated_at"],
    }

    elasticsearch_client.index(
        index=index_name,
        id=article_id,
        document=search_document,
    )

    count += 1


elasticsearch_client.indices.refresh(index=index_name)


print(f"Indexed {count} articles")
