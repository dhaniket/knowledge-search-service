import hashlib
import json

from app.cache.redis import (
    get_search_cache_ttl,
    redis_client,
)
import logging

from redis.exceptions import (
    RedisError,
)

logger = logging.getLogger(__name__)


class SearchCacheRepository:

    CACHE_VERSION = "v1"

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

    def get(
        self,
        query: str,
        limit: int,
    ) -> list[dict] | None:

        key = self._build_key(
            query=query,
            limit=limit,
        )

        try:

            cached_value = redis_client.get(key)

        except RedisError:

            logger.warning(
                "Redis cache read failed",
                exc_info=True,
            )

            return None

        if cached_value is None:
            return None

        return json.loads(cached_value)

    def set(
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

            redis_client.set(
                key,
                json.dumps(results),
                ex=get_search_cache_ttl(),
            )

        except RedisError:

            logger.warning(
                "Redis cache write failed",
                exc_info=True,
            )

    def get_ttl(
        self,
        query: str,
        limit: int,
    ) -> int:

        key = self._build_key(
            query=query,
            limit=limit,
        )

        return redis_client.ttl(key)

    def clear_all(
        self,
    ) -> None:

        try:

            cursor = 0

            while True:

                cursor, keys = redis_client.scan(
                    cursor=cursor,
                    match="search:*",
                    count=100,
                )

                if keys:

                    redis_client.delete(*keys)

                if cursor == 0:
                    break

        except RedisError:

            logger.warning(
                "Redis cache invalidation " "failed",
                exc_info=True,
            )
