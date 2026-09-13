import os

from dotenv import load_dotenv
from redis import Redis

load_dotenv()


def get_redis_url() -> str:

    redis_url = os.getenv("REDIS_URL")

    if not redis_url:
        raise RuntimeError("REDIS_URL is not configured")

    return redis_url


def get_search_cache_ttl() -> int:

    raw_value = os.getenv(
        "SEARCH_CACHE_TTL_SECONDS",
        "60",
    )

    ttl = int(raw_value)

    if ttl <= 0:
        raise RuntimeError("SEARCH_CACHE_TTL_SECONDS " "must be greater than 0")

    return ttl


redis_client = Redis.from_url(
    get_redis_url(),
    decode_responses=True,
)


def check_redis_connection() -> None:

    redis_client.ping()
