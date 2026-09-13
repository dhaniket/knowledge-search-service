from app.cache.redis import (
    check_redis_connection,
    redis_client,
)

check_redis_connection()


redis_client.set(
    "test:connection",
    "works",
    ex=30,
)


value = redis_client.get("test:connection")


print("Redis connection successful")

print(
    "Stored value:",
    value,
)
