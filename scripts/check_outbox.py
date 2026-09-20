import asyncio

from app.db.mongodb import (
    initialize_mongodb,
    get_database,
    close_mongodb,
)


async def main() -> None:

    await initialize_mongodb()

    try:
        collection = get_database()["articles"]

        for channel in ("rabbitmq", "kafka"):

            print(f"\n{channel.upper()} OUTBOX")

            for status in (
                "pending",
                "leased",
                "published",
            ):

                count = await collection.count_documents(
                    {f"outbox.{channel}.status": status}
                )

                print(f"{status}: {count}")

            oldest = await collection.find_one(
                {f"outbox.{channel}.status": "pending"},
                projection={
                    "created_at": 1,
                },
                sort=[
                    ("created_at", 1),
                ],
            )

            if oldest is not None:
                print("Oldest pending article: " f"{oldest['_id']}")

                print("Created at: " f"{oldest['created_at']}")

    finally:
        await close_mongodb()


if __name__ == "__main__":
    asyncio.run(main())
