import os

from dotenv import load_dotenv
from elasticsearch import (
    AsyncElasticsearch,
)

load_dotenv()


_elasticsearch_client: AsyncElasticsearch | None = None


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


async def initialize_elasticsearch() -> None:

    global _elasticsearch_client

    client = AsyncElasticsearch(
        get_elasticsearch_url(),
        api_key=(get_elastic_api_key()),
    )

    try:

        await client.info()

    except Exception:

        await client.close()

        _elasticsearch_client = None

        raise

    _elasticsearch_client = client


def get_elasticsearch_client() -> AsyncElasticsearch:

    if _elasticsearch_client is None:

        raise RuntimeError("Elasticsearch client " "is not initialized")

    return _elasticsearch_client


async def close_elasticsearch() -> None:

    global _elasticsearch_client

    if _elasticsearch_client is None:
        return

    await _elasticsearch_client.close()

    _elasticsearch_client = None
