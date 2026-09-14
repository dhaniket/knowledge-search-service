import asyncio
import os

from dotenv import load_dotenv

from pymongo import (
    AsyncMongoClient,
)

from pymongo.server_api import (
    ServerApi,
)

load_dotenv()


async def main() -> None:

    uri = os.getenv("MONGODB_URI")

    if not uri:
        raise RuntimeError("MONGODB_URI " "is not configured")

    client = AsyncMongoClient(
        uri,
        server_api=ServerApi("1"),
    )

    try:

        await client.admin.command("ping")

        print("Async MongoDB " "connection successful")

    finally:

        await client.close()


if __name__ == "__main__":

    asyncio.run(main())
