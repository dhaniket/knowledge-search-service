from app.search.elasticsearch import (
    check_elasticsearch_connection,
)

check_elasticsearch_connection()

print("Elasticsearch connection successful")
