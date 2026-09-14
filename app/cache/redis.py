import os

from dotenv import load_dotenv
from redis.asyncio import Redis

load_dotenv()


_redis_client: Redis | None = None


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


async def initialize_redis() -> None:

    global _redis_client

    if _redis_client is None:

        _redis_client = Redis.from_url(
            get_redis_url(),
            decode_responses=True,
        )

    await _redis_client.ping()


def get_redis_client() -> Redis:

    if _redis_client is None:

        raise RuntimeError("Redis client " "is not initialized")

    return _redis_client


async def close_redis() -> None:

    global _redis_client

    if _redis_client is None:
        return

    await _redis_client.aclose()

    _redis_client = None
