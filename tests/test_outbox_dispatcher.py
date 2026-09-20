from bson import ObjectId

import pytest

from app.workers.outbox_dispatcher import (
    OutboxDispatcher,
)

ARTICLE_ID = ObjectId("6aaeff271dcdebdb5c1981e8")


class FakeOutboxRepository:

    def __init__(
        self,
        fail_status_update=False,
    ):
        self.claimed = False
        self.fail_status_update = fail_status_update

        self.published = []
        self.failed = []

    async def claim(
        self,
        channel,
        now,
        lease_seconds,
    ):
        if self.claimed:
            return None

        self.claimed = True

        return {
            "_id": ARTICLE_ID,
            "outbox": {
                channel: {
                    "token": "lease-token-123",
                    "attempts": 1,
                }
            },
        }

    async def mark_published(self, **kwargs):

        if self.fail_status_update:
            raise RuntimeError("MongoDB unavailable after publish")

        self.published.append(kwargs)

    async def mark_failed(self, **kwargs):
        self.failed.append(kwargs)


class FakeRabbitPublisher:

    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.published_ids = []

    async def publish(self, article_id):

        if self.should_fail:
            raise RuntimeError("RabbitMQ unavailable")

        self.published_ids.append(article_id)


@pytest.mark.anyio
async def test_confirmed_publish_marks_outbox_published():

    outbox = FakeOutboxRepository()
    publisher = FakeRabbitPublisher()

    dispatcher = OutboxDispatcher(
        outbox_repository=outbox,
        article_repository=None,
        rabbit_publisher=publisher,
        kafka_publisher=None,
    )

    result = await dispatcher.dispatch_one("rabbitmq")

    assert result is True

    assert publisher.published_ids == [str(ARTICLE_ID)]

    assert len(outbox.published) == 1
    assert outbox.failed == []


@pytest.mark.anyio
async def test_publish_failure_reschedules_delivery():

    outbox = FakeOutboxRepository()

    publisher = FakeRabbitPublisher(should_fail=True)

    dispatcher = OutboxDispatcher(
        outbox_repository=outbox,
        article_repository=None,
        rabbit_publisher=publisher,
        kafka_publisher=None,
    )

    await dispatcher.dispatch_one("rabbitmq")

    assert outbox.published == []

    assert len(outbox.failed) == 1

    assert outbox.failed[0]["channel"] == ("rabbitmq")

    assert outbox.failed[0]["token"] == ("lease-token-123")


@pytest.mark.anyio
async def test_confirmed_publish_with_failed_status_update():

    outbox = FakeOutboxRepository(fail_status_update=True)

    publisher = FakeRabbitPublisher()

    dispatcher = OutboxDispatcher(
        outbox_repository=outbox,
        article_repository=None,
        rabbit_publisher=publisher,
        kafka_publisher=None,
    )

    with pytest.raises(RuntimeError):
        await dispatcher.dispatch_one("rabbitmq")

    # The broker received the publication.
    assert publisher.published_ids == [str(ARTICLE_ID)]

    # We did not falsely reset or ACK
    # the MongoDB outbox record.
    assert outbox.failed == []
    assert outbox.published == []


@pytest.mark.anyio
async def test_unavailable_broker_does_not_claim_job():

    outbox = FakeOutboxRepository()

    dispatcher = OutboxDispatcher(
        outbox_repository=outbox,
        article_repository=None,
        rabbit_publisher=None,
        kafka_publisher=None,
    )

    result = await dispatcher.dispatch_one("rabbitmq")

    assert result is False
    assert outbox.claimed is False
