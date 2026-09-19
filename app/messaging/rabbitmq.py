import os

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractRobustConnection,
)

from dotenv import load_dotenv

load_dotenv()


_connection: AbstractRobustConnection | None = None
_channel: AbstractChannel | None = None

RETRY_DELAY_MS = 5_000
MAX_RETRIES = 3
WORKER_PREFETCH = 5


def get_rabbitmq_url() -> str:
    url = os.getenv("RABBITMQ_URL")

    if not url:
        raise RuntimeError("RABBITMQ_URL is not configured")

    return url


def get_article_index_queue() -> str:
    return os.getenv(
        "ARTICLE_INDEX_QUEUE",
        "article.index.v1",
    )


def get_retry_queue() -> str:
    return f"{get_article_index_queue()}.retry.5s"


def get_dead_letter_queue() -> str:
    return f"{get_article_index_queue()}.dlq"


async def declare_article_queues(
    channel: AbstractChannel,
) -> None:
    main_queue = get_article_index_queue()

    # Existing Stage 1 queue: do not change
    # its declaration arguments.
    await channel.declare_queue(
        main_queue,
        durable=True,
    )

    await channel.declare_queue(
        get_dead_letter_queue(),
        durable=True,
    )

    await channel.declare_queue(
        get_retry_queue(),
        durable=True,
        arguments={
            "x-message-ttl": RETRY_DELAY_MS,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": main_queue,
        },
    )


async def initialize_rabbitmq() -> None:
    global _connection, _channel

    if _connection is not None:
        return

    connection = await aio_pika.connect_robust(get_rabbitmq_url())

    try:
        channel = await connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )

        await declare_article_queues(channel)

    except BaseException:
        await connection.close()
        raise

    _connection = connection
    _channel = channel


def get_rabbitmq_channel() -> AbstractChannel:
    if _channel is None:
        raise RuntimeError("RabbitMQ is not initialized")

    return _channel


async def close_rabbitmq() -> None:
    global _connection, _channel

    channel = _channel
    connection = _connection

    _channel = None
    _connection = None

    try:
        if channel is not None:
            await channel.close()
    finally:
        if connection is not None:
            await connection.close()
