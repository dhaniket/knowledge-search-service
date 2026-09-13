from datetime import datetime

from pydantic import BaseModel


class ArticleSearchResult(BaseModel):

    id: str

    score: float | None

    title: str

    content: str

    tags: list[str]

    source: str

    is_active: bool

    created_at: datetime

    updated_at: datetime
