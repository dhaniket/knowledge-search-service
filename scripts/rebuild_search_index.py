import asyncio

from app.config import (
    get_elasticsearch_operation_timeout,
    get_reindex_concurrency,
)
from app.db.mongodb import (
    close_mongodb,
    get_database,
    initialize_mongodb,
)
from app.search.elasticsearch import (
    close_elasticsearch,
    get_elasticsearch_client,
    get_elasticsearch_index,
    initialize_elasticsearch,
)
from app.search.index_definition import (
    ARTICLE_INDEX_MAPPINGS,
)

BATCH_SIZE = 100


def build_search_document(
    document: dict,
) -> dict:

    return {
        "title": document["title"],
        "content": document["content"],
        "tags": document["tags"],
        "source": document["source"],
        "is_active": document["is_active"],
        "created_at": document["created_at"],
        "updated_at": document["updated_at"],
    }


async def index_document(
    document: dict,
    semaphore: asyncio.Semaphore,
) -> None:

    client = get_elasticsearch_client()

    index_name = get_elasticsearch_index()

    async with semaphore:

        async with asyncio.timeout(get_elasticsearch_operation_timeout()):

            await client.index(
                index=index_name,
                id=str(document["_id"]),
                document=(build_search_document(document)),
            )


async def index_batch(
    documents: list[dict],
    semaphore: asyncio.Semaphore,
) -> None:

    async with asyncio.TaskGroup() as group:

        for document in documents:

            group.create_task(
                index_document(
                    document=document,
                    semaphore=semaphore,
                )
            )


async def main() -> None:

    await initialize_mongodb()

    await initialize_elasticsearch()

    try:

        database = get_database()

        collection = database["articles"]

        client = get_elasticsearch_client()

        index_name = get_elasticsearch_index()

        if await client.indices.exists(index=index_name):

            await client.indices.delete(index=index_name)

        await client.indices.create(
            index=index_name,
            mappings=(ARTICLE_INDEX_MAPPINGS),
        )

        semaphore = asyncio.Semaphore(get_reindex_concurrency())

        cursor = collection.find({"is_active": True})

        batch = []

        indexed_count = 0

        async for document in cursor:

            batch.append(document)

            if len(batch) >= BATCH_SIZE:

                await index_batch(
                    documents=batch,
                    semaphore=semaphore,
                )

                indexed_count += len(batch)

                batch = []

        if batch:

            await index_batch(
                documents=batch,
                semaphore=semaphore,
            )

            indexed_count += len(batch)

        await client.indices.refresh(index=index_name)

        print("Rebuilt search index " f"with {indexed_count} " "articles")

    finally:

        await close_elasticsearch()

        await close_mongodb()


if __name__ == "__main__":

    asyncio.run(main())
