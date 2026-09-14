from datetime import (
    UTC,
    datetime,
)

import pytest

from app.models.article import (
    KnowledgeArticle,
)
from app.schemas.article import (
    ArticleCreate,
)


@pytest.fixture
def anyio_backend():

    return "asyncio"


@pytest.fixture
def article() -> KnowledgeArticle:

    now = datetime.now(UTC)

    return KnowledgeArticle(
        id="article-123",
        title=("How to Reset Your Password"),
        content=("Open account settings and " "select Reset Password."),
        tags=[
            "account",
            "password",
            "support",
        ],
        source="help-center",
        is_active=True,
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def article_create() -> ArticleCreate:

    return ArticleCreate(
        title=("How to Reset Your Password"),
        content=("Open account settings and " "select Reset Password."),
        tags=[
            "account",
            "password",
            "support",
        ],
        source="help-center",
    )
