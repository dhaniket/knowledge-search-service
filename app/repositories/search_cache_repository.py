import hashlib
import json

from app.cache.redis import (
    get_search_cache_ttl,
    redis_client,
)


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

        cached_value = redis_client.get(key)

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

        redis_client.set(
            key,
            json.dumps(results),
            ex=get_search_cache_ttl(),
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
