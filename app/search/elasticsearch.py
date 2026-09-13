import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()


def get_elasticsearch_url() -> str:

    url = os.getenv("ELASTICSEARCH_URL")

    if not url:
        raise RuntimeError("ELASTICSEARCH_URL " "is not configured")

    return url


def get_elastic_api_key() -> str:

    api_key = os.getenv("ELASTIC_API_KEY")

    if not api_key:
        raise RuntimeError("ELASTIC_API_KEY " "is not configured")

    return api_key


def get_elasticsearch_index() -> str:

    index_name = os.getenv("ELASTICSEARCH_INDEX")

    if not index_name:
        raise RuntimeError("ELASTICSEARCH_INDEX " "is not configured")

    return index_name


elasticsearch_client = Elasticsearch(
    get_elasticsearch_url(),
    api_key=get_elastic_api_key(),
)


def check_elasticsearch_connection() -> None:

    elasticsearch_client.info()
