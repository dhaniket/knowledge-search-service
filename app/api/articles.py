from typing import Annotated

from fastapi import (
    APIRouter,
    BackgroundTasks,
    HTTPException,
    Query,
)

from app.api.dependencies import (
    ArticleServiceDep,
)
from app.schemas.article import (
    ArticleCreate,
    ArticleResponse,
)

router = APIRouter(
    prefix="/api/v1/articles",
    tags=["articles"],
)


@router.post(
    "",
    response_model=ArticleResponse,
    status_code=201,
)
async def create_article(
    article_data: ArticleCreate,
    service: ArticleServiceDep,
):

    return await service.create_article(article_data)


@router.get(
    "/{article_id}",
    response_model=ArticleResponse,
)
async def get_article(
    article_id: str,
    service: ArticleServiceDep,
):

    article = await service.get_article(article_id)

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
async def list_articles(
    service: ArticleServiceDep,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=100,
        ),
    ] = 20,
):

    return await service.list_articles(limit=limit)
