from datetime import datetime

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class ArticleCreate(BaseModel):

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    title: str = Field(
        min_length=1,
        max_length=200,
    )

    content: str = Field(
        min_length=1,
        max_length=10000,
    )

    tags: list[str] = Field(
        default_factory=list,
        max_length=20,
    )

    source: str = Field(
        min_length=1,
        max_length=100,
    )


class ArticleResponse(BaseModel):

    id: str

    title: str

    content: str

    tags: list[str]

    source: str

    is_active: bool

    created_at: datetime

    updated_at: datetime
