import asyncio
import os

from dotenv import load_dotenv

from redis.asyncio import Redis

load_dotenv()


async def main() -> None:

    redis_url = os.getenv("REDIS_URL")

    if not redis_url:
        raise RuntimeError("REDIS_URL " "is not configured")

    client = Redis.from_url(
        redis_url,
        decode_responses=True,
    )

    try:

        await client.ping()

        await client.set(
            "async:test",
            "works",
            ex=30,
        )

        value = await client.get("async:test")

        print("Async Redis " "connection successful")

        print(
            "Value:",
            value,
        )

    finally:

        await client.aclose()


if __name__ == "__main__":

    asyncio.run(main())
