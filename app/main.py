import logging
from contextlib import (
    asynccontextmanager,
)

from fastapi import FastAPI

from app.api.articles import (
    router as articles_router,
)
from app.api.search import (
    router as search_router,
)
from app.cache.redis import (
    close_redis,
    initialize_redis,
)
from app.db.mongodb import (
    close_mongodb,
    ensure_mongodb_indexes,
    initialize_mongodb,
)
from app.search.elasticsearch import (
    close_elasticsearch,
    initialize_elasticsearch,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(
    app: FastAPI,
):

    # MongoDB is required.
    await initialize_mongodb()

    await ensure_mongodb_indexes()

    # Elasticsearch is optional
    # for article CRUD.
    try:

        await initialize_elasticsearch()

    except Exception:

        logger.warning(
            "Elasticsearch unavailable " "during startup",
            exc_info=True,
        )

    # Redis is an optional
    # performance layer.
    try:

        await initialize_redis()

    except Exception:

        logger.warning(
            "Redis unavailable " "during startup",
            exc_info=True,
        )

    try:

        yield

    finally:

        await close_redis()

        await close_elasticsearch()

        await close_mongodb()


app = FastAPI(
    title="Knowledge Search Service",
    version="1.0.0",
    lifespan=lifespan,
)


app.include_router(articles_router)

app.include_router(search_router)


@app.get("/health")
async def health_check():

    return {"status": "ok"}
