from app.search.elasticsearch import (
    elasticsearch_client,
    get_elasticsearch_index,
)

index_name = get_elasticsearch_index()


mappings = {
    "properties": {
        "title": {
            "type": "text",
        },
        "content": {
            "type": "text",
        },
        "tags": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                }
            },
        },
        "source": {
            "type": "keyword",
        },
        "is_active": {
            "type": "boolean",
        },
        "created_at": {
            "type": "date",
        },
        "updated_at": {
            "type": "date",
        },
    }
}


if elasticsearch_client.indices.exists(index=index_name):

    print(f"Index '{index_name}' " "already exists")

else:

    elasticsearch_client.indices.create(
        index=index_name,
        mappings=mappings,
    )

    print(f"Created index " f"'{index_name}'")
