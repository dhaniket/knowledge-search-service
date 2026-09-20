from types import SimpleNamespace

import pytest

from app.messaging.article_created_event import (
    ArticleCreatedEvent,
)
from app.messaging.article_event_publisher import (
    ArticleEventPublisher,
)


class FakeKafkaProducer:

    def __init__(self):
        self.calls = []

    async def send_and_wait(
        self,
        topic,
        key,
        value,
    ):
        self.calls.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
            }
        )

        return SimpleNamespace(
            topic=topic,
            partition=0,
            offset=10,
        )


@pytest.mark.anyio
async def test_publishes_versioned_article_event(article):

    producer = FakeKafkaProducer()

    publisher = ArticleEventPublisher(
        producer=producer,
        topic="knowledge.article.events.v1",
    )

    await publisher.publish_article_created(article)

    assert len(producer.calls) == 1

    call = producer.calls[0]

    assert call["topic"] == ("knowledge.article.events.v1")

    assert call["key"] == article.id.encode("utf-8")

    event = ArticleCreatedEvent.model_validate_json(call["value"])

    assert event.article_id == article.id
    assert event.event_type == "article.created"
    assert event.version == 1

    assert event.event_id == (f"article.created.v1:{article.id}")
