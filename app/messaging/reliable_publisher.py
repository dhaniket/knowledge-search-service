import aio_pika

from aio_pika.abc import AbstractExchange

from app.errors.messaging_errors import (
    MessagePublishError,
)

PUBLISH_TIMEOUT_SECONDS = 5.0


class ReliableQueuePublisher:

    def __init__(
        self,
        exchange: AbstractExchange,
    ) -> None:
        self.exchange = exchange

    async def publish(
        self,
        queue_name: str,
        body: bytes,
        message_id: str | None,
        message_type: str | None,
        headers: dict | None = None,
    ) -> None:

        message = aio_pika.Message(
            body=body,
            content_type="application/json",
            message_id=message_id,
            type=message_type,
            headers=headers or {},
            delivery_mode=(aio_pika.DeliveryMode.PERSISTENT),
        )

        try:
            confirmation = await self.exchange.publish(
                message,
                routing_key=queue_name,
                mandatory=True,
                timeout=PUBLISH_TIMEOUT_SECONDS,
            )

            if confirmation is False or confirmation is None:
                raise MessagePublishError("Broker did not confirm publication")

        except Exception as exc:
            if isinstance(exc, MessagePublishError):
                raise

            raise MessagePublishError(f"Could not publish to {queue_name}") from exc
