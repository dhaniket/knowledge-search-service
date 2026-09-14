import os
from typing import Any

from dotenv import load_dotenv
from pymongo import AsyncMongoClient
from pymongo.server_api import ServerApi

load_dotenv()


_mongodb_client: AsyncMongoClient[dict[str, Any]] | None = None


def get_mongodb_uri() -> str:

    uri = os.getenv("MONGODB_URI")

    if not uri:
        raise RuntimeError("MONGODB_URI is not configured")

    return uri


def get_database_name() -> str:

    database_name = os.getenv("MONGODB_DATABASE")

    if not database_name:
        raise RuntimeError("MONGODB_DATABASE " "is not configured")

    return database_name


async def initialize_mongodb() -> None:

    global _mongodb_client

    _mongodb_client = AsyncMongoClient(
        get_mongodb_uri(),
        server_api=ServerApi("1"),
    )

    try:

        await _mongodb_client.admin.command("ping")

    except Exception:

        await _mongodb_client.close()

        _mongodb_client = None

        raise


def get_database():

    if _mongodb_client is None:

        raise RuntimeError("MongoDB client " "is not initialized")

    return _mongodb_client[get_database_name()]


async def ensure_mongodb_indexes() -> None:

    database = get_database()

    collection = database["articles"]

    await collection.create_index(
        [
            (
                "is_active",
                1,
            ),
            (
                "created_at",
                -1,
            ),
        ]
    )


async def close_mongodb() -> None:

    global _mongodb_client

    if _mongodb_client is None:
        return

    await _mongodb_client.close()

    _mongodb_client = None
