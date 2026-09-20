from types import SimpleNamespace

import pytest

from aiokafka import TopicPartition

from app.messaging.article_created_event import (
    ArticleCreatedEvent,
)
from app.workers.article_event_consumer import (
    process_record,
)


class FakeCollection:

    def __init__(self, sequence, should_fail=False):
        self.sequence = sequence
        self.should_fail = should_fail
        self.documents = {}

    async def update_one(
        self,
        filter,
        update,
        upsert,
    ):
        self.sequence.append("mongo")

        if self.should_fail:
            raise RuntimeError("MongoDB write failed")

        event_id = filter["_id"]

        if event_id in self.documents:
            inserted = None
        else:
            self.documents[event_id] = update["$setOnInsert"]
            inserted = event_id

        return SimpleNamespace(upserted_id=inserted)


class FakeConsumer:

    def __init__(self, sequence):
        self.sequence = sequence
        self.commits = []

    async def commit(self, offsets):
        self.sequence.append("commit")
        self.commits.append(offsets)


def make_record(article):

    event = ArticleCreatedEvent.from_article(article)

    return SimpleNamespace(
        topic="knowledge.article.events.v1",
        partition=0,
        offset=42,
        key=article.id.encode("utf-8"),
        value=event.model_dump_json().encode("utf-8"),
    )


@pytest.mark.anyio
async def test_consumer_commits_after_mongo(article):

    sequence = []

    collection = FakeCollection(sequence)
    consumer = FakeConsumer(sequence)

    await process_record(
        consumer,
        collection,
        make_record(article),
    )

    assert sequence == [
        "mongo",
        "commit",
    ]

    assert consumer.commits == [
        {
            TopicPartition(
                "knowledge.article.events.v1",
                0,
            ): 43
        }
    ]


@pytest.mark.anyio
async def test_mongo_failure_does_not_commit(article):

    sequence = []

    collection = FakeCollection(
        sequence,
        should_fail=True,
    )

    consumer = FakeConsumer(sequence)

    with pytest.raises(RuntimeError):
        await process_record(
            consumer,
            collection,
            make_record(article),
        )

    assert sequence == ["mongo"]
    assert consumer.commits == []


@pytest.mark.anyio
async def test_replaying_event_does_not_duplicate_record(
    article,
):

    sequence = []

    collection = FakeCollection(sequence)
    consumer = FakeConsumer(sequence)

    record = make_record(article)

    await process_record(
        consumer,
        collection,
        record,
    )

    await process_record(
        consumer,
        collection,
        record,
    )

    assert len(collection.documents) == 1

    # The replay was safely processed;
    # both executions committed progress.
    assert len(consumer.commits) == 2


@pytest.mark.anyio
async def test_invalid_event_is_saved_before_commit(
    article,
):

    sequence = []

    class FakePoisonCollection:

        async def update_one(
            self,
            filter,
            update,
            upsert,
        ):
            sequence.append("poison_saved")

    collection = FakeCollection(sequence)

    collection.database = {"kafka_poison_events": FakePoisonCollection()}

    consumer = FakeConsumer(sequence)

    record = make_record(article)
    record.value = b"{invalid-json"

    await process_record(
        consumer,
        collection,
        record,
    )

    assert sequence == [
        "poison_saved",
        "commit",
    ]

    assert len(consumer.commits) == 1


@pytest.mark.anyio
async def test_poison_storage_failure_does_not_commit(
    article,
):

    sequence = []

    class BrokenPoisonCollection:

        async def update_one(
            self,
            filter,
            update,
            upsert,
        ):
            sequence.append("poison_write_failed")

            raise RuntimeError("MongoDB unavailable")

    collection = FakeCollection(sequence)

    collection.database = {"kafka_poison_events": BrokenPoisonCollection()}

    consumer = FakeConsumer(sequence)

    record = make_record(article)
    record.value = b"{invalid-json"

    with pytest.raises(RuntimeError):
        await process_record(
            consumer,
            collection,
            record,
        )

    assert sequence == ["poison_write_failed"]

    assert consumer.commits == []
