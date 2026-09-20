import asyncio

from app.messaging.kafka import (
    initialize_kafka,
    close_kafka,
)


async def main():

    try:
        await initialize_kafka()
        print("Kafka connection OK")

    finally:
        await close_kafka()


if __name__ == "__main__":
    asyncio.run(main())
