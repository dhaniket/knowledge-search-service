from app.cache.redis import (
    redis_client,
)

cursor = 0
deleted = 0


while True:

    cursor, keys = redis_client.scan(
        cursor=cursor,
        match="search:*",
        count=100,
    )

    if keys:

        deleted += redis_client.delete(*keys)

    if cursor == 0:
        break


print(f"Deleted {deleted} " "search cache entries")
