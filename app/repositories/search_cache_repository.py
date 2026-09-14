import hashlib
import json
import logging

from redis.asyncio import Redis
from redis.exceptions import (
    RedisError,
)

logger = logging.getLogger(__name__)


class SearchCacheRepository:

    CACHE_VERSION = "v1"

    def __init__(
        self,
        client: Redis,
        ttl_seconds: int,
    ) -> None:

        self.client = client

        self.ttl_seconds = ttl_seconds

    @staticmethod
    def _normalize_query(
        query: str,
    ) -> str:

        return " ".join(query.strip().lower().split())

    def _build_key(
        self,
        query: str,
        limit: int,
    ) -> str:

        normalized_query = self._normalize_query(query)

        query_hash = hashlib.sha256(normalized_query.encode("utf-8")).hexdigest()

        return f"search:" f"{self.CACHE_VERSION}:" f"{query_hash}:" f"limit:{limit}"

    async def get(
        self,
        query: str,
        limit: int,
    ) -> list[dict] | None:

        key = self._build_key(
            query=query,
            limit=limit,
        )

        try:

            cached_value = await self.client.get(key)

        except RedisError:

            logger.warning(
                "Redis cache read failed",
                exc_info=True,
            )

            return None

        if cached_value is None:
            return None

        try:

            return json.loads(cached_value)

        except json.JSONDecodeError:

            logger.warning(
                "Invalid cached JSON",
                exc_info=True,
            )

            return None

    async def set(
        self,
        query: str,
        limit: int,
        results: list[dict],
    ) -> None:

        key = self._build_key(
            query=query,
            limit=limit,
        )

        try:

            await self.client.set(
                key,
                json.dumps(results),
                ex=self.ttl_seconds,
            )

        except RedisError:

            logger.warning(
                "Redis cache write failed",
                exc_info=True,
            )

    async def clear_all(
        self,
    ) -> None:

        try:

            cursor = 0

            while True:

                cursor, keys = await self.client.scan(
                    cursor=cursor,
                    match="search:*",
                    count=100,
                )

                if keys:

                    await self.client.delete(*keys)

                if cursor == 0:
                    break

        except RedisError:

            logger.warning(
                "Redis cache invalidation " "failed",
                exc_info=True,
            )

    async def get_ttl(
        self,
        query: str,
        limit: int,
    ) -> int:

        key = self._build_key(
            query=query,
            limit=limit,
        )

        return await self.client.ttl(key)
