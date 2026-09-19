import asyncio

from app.messaging.rabbitmq import (
    close_rabbitmq,
    initialize_rabbitmq,
)


async def main() -> None:

    try:

        await initialize_rabbitmq()

        print("RabbitMQ connection OK")

    finally:

        await close_rabbitmq()


if __name__ == "__main__":

    asyncio.run(main())
