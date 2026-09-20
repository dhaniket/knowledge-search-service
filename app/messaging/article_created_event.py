from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from app.models.article import KnowledgeArticle


class ArticleCreatedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: Literal["article.created"]
    version: Literal[1]

    article_id: str
    source: str
    occurred_at: datetime

    @classmethod
    def from_article(
        cls,
        article: KnowledgeArticle,
    ) -> "ArticleCreatedEvent":

        return cls(
            event_id=f"article.created.v1:{article.id}",
            event_type="article.created",
            version=1,
            article_id=article.id,
            source=article.source,
            occurred_at=article.created_at,
        )
