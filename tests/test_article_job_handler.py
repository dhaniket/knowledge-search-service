import pytest

from app.errors.messaging_errors import (
    ArticleMissingForIndexing,
    MessagePublishError,
)
from app.messaging.article_index_job import (
    ArticleIndexJob,
)
from app.workers.article_job_handler import (
    ArticleJobHandler,
)

MAIN_QUEUE = "article.index.v1"
RETRY_QUEUE = "article.index.v1.retry.5s"
DLQ = "article.index.v1.dlq"


class FakeMessage:

    def __init__(
        self,
        body: bytes,
        headers: dict | None = None,
    ):
        self.body = body
        self.headers = headers or {}
        self.message_id = "job-123"
        self.type = "article.index.requested"

        self.ack_count = 0

    async def ack(self):
        self.ack_count += 1


class FakePublisher:

    def __init__(
        self,
        should_fail: bool = False,
    ):
        self.should_fail = should_fail
        self.published = []

    async def publish(
        self,
        queue_name,
        body,
        message_id,
        message_type,
        headers=None,
    ):
        if self.should_fail:
            raise MessagePublishError("Broker publish failed")

        self.published.append(
            {
                "queue": queue_name,
                "body": body,
                "headers": headers,
                "message_id": message_id,
            }
        )


class FakeProcessor:

    def __init__(
        self,
        error: Exception | None = None,
    ):
        self.error = error
        self.calls = 0

    async def process(self, job):
        self.calls += 1

        if self.error is not None:
            raise self.error


def make_message(
    retry_count: int = 0,
) -> FakeMessage:

    job = ArticleIndexJob.create(article_id="article-123")

    return FakeMessage(
        body=job.model_dump_json().encode("utf-8"),
        headers={
            "x-retry-count": retry_count,
        },
    )


def make_handler(
    processor,
    publisher,
):
    return ArticleJobHandler(
        processor=processor,
        publisher=publisher,
        retry_queue=RETRY_QUEUE,
        dead_letter_queue=DLQ,
        max_retries=3,
    )


@pytest.mark.anyio
async def test_successful_job_is_acked():

    processor = FakeProcessor()
    publisher = FakePublisher()
    message = make_message()

    handler = make_handler(
        processor,
        publisher,
    )

    await handler.handle(message)

    assert processor.calls == 1
    assert message.ack_count == 1
    assert publisher.published == []


@pytest.mark.anyio
async def test_failure_schedules_retry():

    processor = FakeProcessor(error=RuntimeError("Elasticsearch down"))

    publisher = FakePublisher()
    message = make_message(retry_count=0)

    handler = make_handler(
        processor,
        publisher,
    )

    await handler.handle(message)

    assert processor.calls == 1
    assert message.ack_count == 1

    assert len(publisher.published) == 1

    published = publisher.published[0]

    assert published["queue"] == RETRY_QUEUE

    assert published["headers"]["x-retry-count"] == 1

    assert published["body"] == message.body


@pytest.mark.anyio
async def test_retry_exhaustion_goes_to_dlq():

    processor = FakeProcessor(error=RuntimeError("Still unavailable"))

    publisher = FakePublisher()
    message = make_message(retry_count=3)

    handler = make_handler(
        processor,
        publisher,
    )

    await handler.handle(message)

    assert processor.calls == 1
    assert message.ack_count == 1

    assert len(publisher.published) == 1

    assert publisher.published[0]["queue"] == DLQ

    assert publisher.published[0]["headers"]["x-last-error"] == "RetriesExhausted"


@pytest.mark.anyio
async def test_invalid_json_goes_to_dlq():

    processor = FakeProcessor()
    publisher = FakePublisher()

    message = FakeMessage(body=b"{not-valid-json")

    handler = make_handler(
        processor,
        publisher,
    )

    await handler.handle(message)

    assert processor.calls == 0
    assert message.ack_count == 1

    assert len(publisher.published) == 1

    assert publisher.published[0]["queue"] == DLQ


@pytest.mark.anyio
async def test_retry_publish_failure_leaves_original_unacked():

    processor = FakeProcessor(error=RuntimeError("Elasticsearch down"))

    publisher = FakePublisher(should_fail=True)

    message = make_message()

    handler = make_handler(
        processor,
        publisher,
    )

    with pytest.raises(MessagePublishError):
        await handler.handle(message)

    assert processor.calls == 1

    # Critical safety assertion:
    assert message.ack_count == 0


@pytest.mark.anyio
async def test_missing_article_goes_to_dlq():

    processor = FakeProcessor(error=ArticleMissingForIndexing("Article missing"))

    publisher = FakePublisher()
    message = make_message()

    handler = make_handler(
        processor,
        publisher,
    )

    await handler.handle(message)

    assert processor.calls == 1
    assert message.ack_count == 1

    assert publisher.published[0]["queue"] == DLQ


@pytest.mark.anyio
async def test_invalid_retry_metadata_goes_to_dlq():

    processor = FakeProcessor()
    publisher = FakePublisher()

    message = make_message()

    message.headers["x-retry-count"] = "invalid"

    handler = make_handler(
        processor,
        publisher,
    )

    await handler.handle(message)

    assert processor.calls == 0
    assert message.ack_count == 1

    assert publisher.published[0]["queue"] == DLQ
