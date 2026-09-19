import asyncio

from app.messaging.article_index_job_publisher import (
    ArticleIndexJobPublisher,
)
from app.messaging.rabbitmq import (
    close_rabbitmq,
    get_article_index_queue,
    get_rabbitmq_channel,
    initialize_rabbitmq,
)


async def main():

    await initialize_rabbitmq()

    try:
        channel = get_rabbitmq_channel()

        publisher = ArticleIndexJobPublisher(
            exchange=channel.default_exchange,
            queue_name=get_article_index_queue(),
        )

        # Valid MongoDB ObjectId format,
        # but intended to reference no article.
        missing_id = "000000000000000000000000"

        await publisher.publish(article_id=missing_id)

        print("Published missing-article test job")

    finally:
        await close_rabbitmq()


if __name__ == "__main__":
    asyncio.run(main())
