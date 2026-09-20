from datetime import (
    UTC,
    datetime,
)

from bson import ObjectId

from app.models.article import (
    KnowledgeArticle,
)
from app.schemas.article import (
    ArticleCreate,
)
from app.messaging.outbox import new_outbox_state


class ArticleRepository:

    def __init__(
        self,
        database,
    ) -> None:

        self.collection = database["articles"]

    @staticmethod
    def _document_to_article(
        document: dict,
    ) -> KnowledgeArticle:

        return KnowledgeArticle(
            id=str(document["_id"]),
            title=document["title"],
            content=document["content"],
            tags=document["tags"],
            source=document["source"],
            is_active=document["is_active"],
            created_at=document["created_at"],
            updated_at=document["updated_at"],
        )

    async def create(
        self,
        article_data: ArticleCreate,
    ) -> KnowledgeArticle:

        now = datetime.now(UTC)

        document = {
            "title": article_data.title,
            "content": article_data.content,
            "tags": article_data.tags,
            "source": article_data.source,
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }
        document["outbox"] = new_outbox_state()
        result = await self.collection.insert_one(document)

        document["_id"] = result.inserted_id

        return self._document_to_article(document)

    async def get_by_id(
        self,
        article_id: str,
    ) -> KnowledgeArticle | None:

        if not ObjectId.is_valid(article_id):
            return None

        document = await self.collection.find_one({"_id": ObjectId(article_id)})

        if document is None:
            return None

        return self._document_to_article(document)

    async def list(
        self,
        limit: int = 20,
    ) -> list[KnowledgeArticle]:

        cursor = (
            self.collection.find({"is_active": True})
            .sort(
                "created_at",
                -1,
            )
            .limit(limit)
        )

        documents = await cursor.to_list(length=limit)

        return [self._document_to_article(document) for document in documents]
