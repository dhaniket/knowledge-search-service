from contextlib import (
    asynccontextmanager,
)

from fastapi import FastAPI

from app.api.articles import router
from app.db.mongodb import (
    check_database_connection,
)


@asynccontextmanager
async def lifespan(
    app: FastAPI,
):

    check_database_connection()

    yield


app = FastAPI(
    title="Knowledge Search Service",
    version="1.0.0",
    lifespan=lifespan,
)


app.include_router(router)


@app.get("/health")
def health_check():

    return {"status": "ok"}
