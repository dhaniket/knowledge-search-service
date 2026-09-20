import asyncio

from app.db.mongodb import (
    initialize_mongodb,
    get_database,
    close_mongodb,
)


async def main() -> None:

    await initialize_mongodb()

    try:
        collection = get_database()["article_creation_analytics_events"]

        pipeline = [
            {
                "$group": {
                    "_id": "$source",
                    "article_count": {
                        "$sum": 1,
                    },
                },
            },
            {
                "$sort": {
                    "article_count": -1,
                },
            },
        ]

        cursor = await collection.aggregate(pipeline)

        async for document in cursor:
            print(f"{document['_id']}: " f"{document['article_count']}")

    finally:
        await close_mongodb()


if __name__ == "__main__":
    asyncio.run(main())
