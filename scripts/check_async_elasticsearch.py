import asyncio
import os

from dotenv import load_dotenv

from elasticsearch import (
    AsyncElasticsearch,
)

load_dotenv()


async def main() -> None:

    url = os.getenv("ELASTICSEARCH_URL")

    api_key = os.getenv("ELASTIC_API_KEY")

    if not url:
        raise RuntimeError("ELASTICSEARCH_URL " "is not configured")

    if not api_key:
        raise RuntimeError("ELASTIC_API_KEY " "is not configured")

    client = AsyncElasticsearch(
        url,
        api_key=api_key,
    )

    try:

        await client.info()

        print("Async Elasticsearch " "connection successful")

    finally:

        await client.close()


if __name__ == "__main__":

    asyncio.run(main())
