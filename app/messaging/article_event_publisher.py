import logging

from aiokafka import AIOKafkaProducer

from app.errors.messaging_errors import (
    KafkaEventPublishError,
)
from app.messaging.article_created_event import (
    ArticleCreatedEvent,
)
from app.models.article import KnowledgeArticle

logger = logging.getLogger(__name__)


class ArticleEventPublisher:

    def __init__(
        self,
        producer: AIOKafkaProducer,
        topic: str,
    ) -> None:
        self.producer = producer
        self.topic = topic

    async def publish_article_created(
        self,
        article: KnowledgeArticle,
    ) -> None:

        event = ArticleCreatedEvent.from_article(article)

        try:
            metadata = await self.producer.send_and_wait(
                topic=self.topic,
                key=article.id.encode("utf-8"),
                value=event.model_dump_json().encode("utf-8"),
            )

        except Exception as exc:
            raise KafkaEventPublishError(
                "Failed to publish article.created event"
            ) from exc

        logger.info(
            "Published event %s topic=%s " "partition=%s offset=%s",
            event.event_id,
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )
