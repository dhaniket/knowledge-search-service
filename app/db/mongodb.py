import os

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()


def get_mongodb_uri() -> str:

    uri = os.getenv("MONGODB_URI")

    if not uri:
        raise RuntimeError("MONGODB_URI is not configured")

    return uri


def get_database_name() -> str:

    database_name = os.getenv("MONGODB_DATABASE")

    if not database_name:
        raise RuntimeError("MONGODB_DATABASE is not configured")

    return database_name


client = MongoClient(
    get_mongodb_uri(),
    server_api=ServerApi("1"),
)


database = client[get_database_name()]


def check_database_connection() -> None:

    client.admin.command("ping")
