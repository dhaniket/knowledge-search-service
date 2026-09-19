from datetime import (
    UTC,
    datetime,
)
from typing import Literal
from uuid import (
    UUID,
    uuid4,
)

from pydantic import (
    BaseModel,
    ConfigDict,
)


class ArticleIndexJob(BaseModel):

    model_config = ConfigDict(extra="forbid")

    job_id: UUID

    job_type: Literal["article.index.requested"]

    version: Literal[1]

    article_id: str

    created_at: datetime

    @classmethod
    def create(
        cls,
        article_id: str,
    ) -> "ArticleIndexJob":

        return cls(
            job_id=uuid4(),
            job_type=("article.index.requested"),
            version=1,
            article_id=article_id,
            created_at=datetime.now(UTC),
        )
