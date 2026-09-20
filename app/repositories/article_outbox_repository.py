from datetime import datetime, timedelta
from uuid import uuid4

from pymongo import ReturnDocument

from app.messaging.outbox import CHANNELS


class ArticleOutboxRepository:

    def __init__(self, database) -> None:
        self.collection = database["articles"]

    @staticmethod
    def _prefix(channel: str) -> str:
        if channel not in CHANNELS:
            raise ValueError(f"Unsupported outbox channel: {channel}")

        return f"outbox.{channel}"

    async def claim(
        self,
        channel: str,
        now: datetime,
        lease_seconds: int = 120,
    ) -> dict | None:

        prefix = self._prefix(channel)

        token = str(uuid4())

        query = {
            "$or": [
                {
                    f"{prefix}.status": "pending",
                    f"{prefix}.next_attempt_at": {"$lte": now},
                },
                {
                    f"{prefix}.status": "leased",
                    f"{prefix}.lease_until": {"$lte": now},
                },
            ]
        }

        update = {
            "$set": {
                f"{prefix}.status": "leased",
                f"{prefix}.token": token,
                f"{prefix}.lease_until": (now + timedelta(seconds=lease_seconds)),
            },
            "$inc": {
                f"{prefix}.attempts": 1,
            },
        }

        return await self.collection.find_one_and_update(
            filter=query,
            update=update,
            sort=[("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def mark_published(
        self,
        article_id,
        channel: str,
        token: str,
        now: datetime,
    ) -> None:

        prefix = self._prefix(channel)

        result = await self.collection.update_one(
            {
                "_id": article_id,
                f"{prefix}.status": "leased",
                f"{prefix}.token": token,
            },
            {
                "$set": {
                    f"{prefix}.status": "published",
                    f"{prefix}.published_at": now,
                },
                "$unset": {
                    f"{prefix}.token": "",
                    f"{prefix}.lease_until": "",
                    f"{prefix}.next_attempt_at": "",
                    f"{prefix}.last_error": "",
                },
            },
        )

        if result.matched_count != 1:
            raise RuntimeError("Outbox publication lease was lost")

    async def mark_failed(
        self,
        article_id,
        channel: str,
        token: str,
        now: datetime,
        attempts: int,
        error: str,
    ) -> None:

        prefix = self._prefix(channel)

        # Exponential delay, capped at 60 seconds.
        delay = min(
            60,
            2 ** min(attempts, 6),
        )

        result = await self.collection.update_one(
            {
                "_id": article_id,
                f"{prefix}.status": "leased",
                f"{prefix}.token": token,
            },
            {
                "$set": {
                    f"{prefix}.status": "pending",
                    f"{prefix}.next_attempt_at": (now + timedelta(seconds=delay)),
                    f"{prefix}.last_error": error[:300],
                },
                "$unset": {
                    f"{prefix}.token": "",
                    f"{prefix}.lease_until": "",
                },
            },
        )

        if result.matched_count != 1:
            raise RuntimeError("Cannot reschedule a lost outbox lease")
