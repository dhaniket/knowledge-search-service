from typing import Annotated


from fastapi import (
    APIRouter,
    HTTPException,
    Query,
)

from app.errors.search_errors import (
    SearchUnavailableError,
)
from app.api.dependencies import (
    SearchServiceDep,
)
from app.schemas.search import (
    ArticleSearchResult,
)
from app.services.search_service import (
    SearchService,
)

router = APIRouter(
    prefix="/api/v1/search",
    tags=["search"],
)


@router.get(
    "",
    response_model=list[ArticleSearchResult],
)
def search_articles(
    q: Annotated[
        str,
        Query(
            min_length=1,
            max_length=200,
        ),
    ],
    service: SearchServiceDep,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=50,
        ),
    ] = 10,
):
    try:

        return service.search_articles(
            query=q,
            limit=limit,
        )

    except SearchUnavailableError:

        raise HTTPException(
            status_code=503,
            detail=("Search service " "temporarily unavailable"),
        )
