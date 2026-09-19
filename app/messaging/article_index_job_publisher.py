from aio_pika.abc import AbstractExchange

from app.messaging.article_index_job import (
    ArticleIndexJob,
)
from app.messaging.reliable_publisher import (
    ReliableQueuePublisher,
)


class ArticleIndexJobPublisher:

    def __init__(
        self,
        exchange: AbstractExchange,
        queue_name: str,
    ) -> None:
        self.publisher = ReliableQueuePublisher(exchange=exchange)
        self.queue_name = queue_name

    async def publish(
        self,
        article_id: str,
    ) -> None:

        job = ArticleIndexJob.create(article_id=article_id)

        await self.publisher.publish(
            queue_name=self.queue_name,
            body=job.model_dump_json().encode("utf-8"),
            message_id=str(job.job_id),
            message_type=job.job_type,
            headers={
                "x-retry-count": 0,
            },
        )
