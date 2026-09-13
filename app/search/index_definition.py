ARTICLE_INDEX_MAPPINGS = {
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
