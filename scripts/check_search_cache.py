from app.repositories.search_cache_repository import (
    SearchCacheRepository,
)

cache = SearchCacheRepository()


ttl = cache.get_ttl(
    query="payment failed",
    limit=10,
)


print(
    "TTL:",
    ttl,
)
