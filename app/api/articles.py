from typing import Annotated

from fastapi import (
    APIRouter,
    HTTPException,
    Query,
)

from app.repositories.article_repository import (
    ArticleRepository,
)
from app.schemas.article import (
    ArticleCreate,
    ArticleResponse,
)
from app.services.article_service import (
    ArticleService,
)

router = APIRouter(
    prefix="/api/v1/articles",
    tags=["articles"],
)


repository = ArticleRepository()

service = ArticleService(repository)


@router.post(
    "",
    response_model=ArticleResponse,
    status_code=201,
)
def create_article(
    article_data: ArticleCreate,
):

    return service.create_article(article_data)


@router.get(
    "/{article_id}",
    response_model=ArticleResponse,
)
def get_article(
    article_id: str,
):

    article = service.get_article(article_id)

    if article is None:
        raise HTTPException(
            status_code=404,
            detail="Article not found",
        )

    return article


@router.get(
    "",
    response_model=list[ArticleResponse],
)
def list_articles(
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=100,
        ),
    ] = 20,
):

    return service.list_articles(limit=limit)
