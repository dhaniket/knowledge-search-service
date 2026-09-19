import logging

from pydantic import ValidationError

from app.errors.messaging_errors import (
    ArticleMissingForIndexing,
)
from app.messaging.article_index_job import (
    ArticleIndexJob,
)
from app.messaging.reliable_publisher import (
    ReliableQueuePublisher,
)
from app.services.article_index_processor import (
    ArticleIndexProcessor,
)

logger = logging.getLogger(__name__)


class ArticleJobHandler:

    def __init__(
        self,
        processor: ArticleIndexProcessor,
        publisher: ReliableQueuePublisher,
        retry_queue: str,
        dead_letter_queue: str,
        max_retries: int = 3,
    ) -> None:
        self.processor = processor
        self.publisher = publisher
        self.retry_queue = retry_queue
        self.dead_letter_queue = dead_letter_queue
        self.max_retries = max_retries

    @staticmethod
    def get_retry_count(message) -> int:

        headers = message.headers or {}

        count = headers.get("x-retry-count", 0)

        # A malformed counter must not bypass
        # our retry limit.
        if type(count) is not int or count < 0:
            raise ValueError("Invalid x-retry-count header")

        return count

    async def move_message(
        self,
        message,
        queue_name: str,
        retry_count: int,
        reason: str,
    ) -> None:

        headers = dict(message.headers or {})

        headers["x-retry-count"] = retry_count
        headers["x-last-error"] = reason

        # IMPORTANT:
        # Publish and await broker confirmation FIRST.
        # ACK the original delivery only afterward.
        await self.publisher.publish(
            queue_name=queue_name,
            body=message.body,
            message_id=message.message_id,
            message_type=message.type,
            headers=headers,
        )

        await message.ack()

    async def handle(
        self,
        message,
    ) -> None:

        # 1. Validate retry metadata.
        try:
            retry_count = self.get_retry_count(message)

        except ValueError:
            logger.warning("Invalid retry metadata: sending to DLQ")

            await self.move_message(
                message=message,
                queue_name=self.dead_letter_queue,
                retry_count=0,
                reason="InvalidRetryMetadata",
            )

            return

        # 2. Validate job schema.
        try:
            job = ArticleIndexJob.model_validate_json(message.body)

        except ValidationError:
            logger.warning("Invalid article job: sending to DLQ")

            await self.move_message(
                message=message,
                queue_name=self.dead_letter_queue,
                retry_count=retry_count,
                reason="InvalidJobSchema",
            )

            return

        # 3. Reject an unexpected retry count.
        if retry_count > self.max_retries:
            await self.move_message(
                message=message,
                queue_name=self.dead_letter_queue,
                retry_count=retry_count,
                reason="RetryLimitExceeded",
            )

            return

        logger.info(
            "Processing job %s article=%s retry=%s",
            job.job_id,
            job.article_id,
            retry_count,
        )

        # 4. Execute business processing.
        try:
            await self.processor.process(job)

        except ArticleMissingForIndexing as exc:

            logger.warning(
                "Job %s references a missing article: %s",
                job.job_id,
                exc,
            )

            await self.move_message(
                message=message,
                queue_name=self.dead_letter_queue,
                retry_count=retry_count,
                reason="ArticleMissing",
            )

            logger.info(
                "Job %s moved to DLQ",
                job.job_id,
            )

            return

        except Exception as exc:
            # Infrastructure/process failures are
            # retryable for a bounded number of attempts.
            logger.exception(
                "Processing failed for job %s",
                job.job_id,
            )

            if retry_count < self.max_retries:
                next_retry = retry_count + 1

                await self.move_message(
                    message=message,
                    queue_name=self.retry_queue,
                    retry_count=next_retry,
                    reason=type(exc).__name__,
                )

                logger.warning(
                    "Job %s sent to retry queue: %s/%s",
                    job.job_id,
                    next_retry,
                    self.max_retries,
                )

            else:
                await self.move_message(
                    message=message,
                    queue_name=self.dead_letter_queue,
                    retry_count=retry_count,
                    reason="RetriesExhausted",
                )

                logger.error(
                    "Job %s exhausted retries; moved to DLQ",
                    job.job_id,
                )

            return

        # 5. Success: ACK once, after processing.
        await message.ack()

        logger.info(
            "Successfully completed and ACKed job %s",
            job.job_id,
        )
