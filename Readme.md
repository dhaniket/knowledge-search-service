# Knowledge Search Service

A production-oriented asynchronous backend service for storing, searching, and caching knowledge-base articles.

## Tech Stack

- Python 3.13+
- FastAPI
- Pydantic
- PyMongo `AsyncMongoClient`
- MongoDB Atlas
- `AsyncElasticsearch`
- Elastic Cloud
- `redis.asyncio`
- Redis Cloud
- Uvicorn
- pytest
- AnyIO
- python-dotenv
- asyncio

## Architecture

```text
                         MongoDB Atlas
                         source of truth
                               │
                               │ derived indexing
                               ▼
                       Elasticsearch
                       full-text search
                               │
                               │ search results
                               ▼
                            Redis
                         search cache
```

### Article Creation

```text
POST /api/v1/articles
        │
        ▼
ArticleService
        │
        ▼
Async MongoDB write
        │
        ▼
201 Created
        │
        │ response returned
        ▼
FastAPI BackgroundTasks
        │
        ▼
ArticleBackgroundService
       / \
      /   \
     ▼     ▼
Elasticsearch   Redis
indexing        cache invalidation
```

MongoDB is the only authoritative write required before returning `201 Created`.

Elasticsearch indexing and Redis cache invalidation are derived, best-effort operations performed after the response path.

### Search

```text
GET /api/v1/search?q=...
        │
        ▼
SearchService
        │
        ▼
Redis
  │
  ├── HIT
  │     ↓
  │   return cached result
  │
  └── MISS / timeout / unavailable
        ↓
   Elasticsearch
        ↓
   relevance-ranked results
        ↓
   Redis SET + TTL
        ↓
      return
```

## Technology Responsibilities

| Technology | Responsibility |
|---|---|
| MongoDB | Authoritative article/document storage |
| Elasticsearch | Derived full-text relevance search index |
| Redis | Disposable short-lived search-result cache |

```text
MongoDB
→ source of truth

Elasticsearch
→ rebuildable derived state

Redis
→ disposable performance layer
```

## Features

- Create knowledge articles
- Fetch an article by ID
- List active articles
- Native async MongoDB persistence
- MongoDB `ObjectId` → application string ID conversion
- Embedded tags
- MongoDB compound indexing
- Full-text Elasticsearch search
- Explicit Elasticsearch mappings
- Multi-field relevance search
- Title/tag field boosting
- Active-document filtering
- Elasticsearch relevance scores
- Redis cache-aside pattern
- Search-query normalization
- Deterministic cache keys
- Cache-key versioning
- Configurable TTL
- Negative-result caching
- Search-cache invalidation
- Async timeouts for Redis and Elasticsearch
- Graceful Redis failure handling
- Graceful Elasticsearch indexing failure handling
- FastAPI background processing
- Structured concurrency with `asyncio.TaskGroup`
- Bounded reindex concurrency with `asyncio.Semaphore`
- Batched Elasticsearch rebuild
- Async client lifecycle management
- FastAPI dependency injection
- Async service-level tests with fake repositories

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Application health check |
| `POST` | `/api/v1/articles` | Create a knowledge article |
| `GET` | `/api/v1/articles` | List active articles |
| `GET` | `/api/v1/articles/{article_id}` | Fetch one article |
| `GET` | `/api/v1/search?q=...` | Full-text relevance search |

# Async Architecture

The service uses native async clients throughout the external I/O path:

```text
FastAPI async endpoint
        ↓
async service
        ↓
async repository
        ↓
┌────────────────────────┐
│ AsyncMongoClient       │
│ AsyncElasticsearch     │
│ redis.asyncio.Redis    │
└────────────────────────┘
        ↓
network I/O
        ↓
event loop can serve
other work while waiting
```

External clients are created during FastAPI lifespan startup and closed during shutdown.

```text
startup
  ↓
initialize MongoDB
initialize Elasticsearch
initialize Redis
  ↓
serve requests
  ↓
shutdown
  ↓
close Redis
close Elasticsearch
close MongoDB
```

MongoDB is required. Elasticsearch and Redis are degradable dependencies.

# MongoDB

Configured database:

```text
knowledge_app
```

Main collection:

```text
articles
```

Example document:

```json
{
  "_id": "ObjectId(...)",
  "title": "How to Reset Your Password",
  "content": "Open account settings and select Reset Password.",
  "tags": [
    "account",
    "password",
    "support"
  ],
  "source": "help-center",
  "is_active": true,
  "created_at": "...",
  "updated_at": "..."
}
```

The repository converts MongoDB `ObjectId` values into application-facing string IDs.

The current MongoDB access pattern is supported by a compound index:

```python
await collection.create_index(
    [
        ("is_active", 1),
        ("created_at", -1),
    ]
)
```

# Elasticsearch

The search index is:

```text
knowledge-articles-v1
```

MongoDB IDs are reused as Elasticsearch document IDs.

The index uses explicit mappings:

```json
{
  "title": { "type": "text" },
  "content": { "type": "text" },
  "tags": {
    "type": "text",
    "fields": {
      "keyword": { "type": "keyword" }
    }
  },
  "source": { "type": "keyword" },
  "is_active": { "type": "boolean" },
  "created_at": { "type": "date" },
  "updated_at": { "type": "date" }
}
```

Search uses a `multi_match` query:

```python
"multi_match": {
    "query": query,
    "fields": [
        "title^3",
        "tags^2",
        "content",
    ],
}
```

Only active articles are included.

Elasticsearch is treated as near-real-time derived state, not as the primary database.

# Redis

Redis implements cache-aside for search results.

```text
request
  ↓
Redis GET
  ↓
hit?
```

Hit:

```text
Redis
  ↓
return cached result
```

Miss:

```text
Redis miss
    ↓
Elasticsearch
    ↓
results
    ↓
Redis SET + TTL
    ↓
return
```

Queries are normalized before cache-key creation.

```text
"  Payment   Failed "
→ "payment failed"
```

Conceptual key:

```text
search:v1:<query-hash>:limit:10
```

Empty result sets are cached too, so `[]` is distinct from a cache miss (`None`).

Search cache keys are invalidated using the `search:*` namespace and Redis `SCAN`.

# Timeouts

External operations have configurable deadlines:

```env
REDIS_OPERATION_TIMEOUT_SECONDS=1.0
ELASTICSEARCH_OPERATION_TIMEOUT_SECONDS=5.0
```

Redis timeout behavior:

```text
Redis timeout
→ bypass cache
→ Elasticsearch
```

Elasticsearch timeout behavior:

```text
Elasticsearch timeout
→ SearchUnavailableError
→ HTTP 503
```

# Structured Concurrency

Independent post-write operations run concurrently with `asyncio.TaskGroup`.

After MongoDB creates an article:

```text
                    ┌→ Elasticsearch indexing
MongoDB article ────┤
                    └→ Redis invalidation
```

Dependent operations remain sequential.

For example:

```text
Redis GET
  ↓
MISS
  ↓
Elasticsearch
  ↓
Redis SET
```

# Background Processing

Article creation uses FastAPI `BackgroundTasks`.

Request path:

```text
POST
 ↓
MongoDB write
 ↓
201 Created
```

Post-response path:

```text
BackgroundTasks
      ↓
ArticleBackgroundService
     /                  \
Elasticsearch          Redis
indexing              invalidation
```

This lowers request latency because the client does not wait for derived search/cache work.

`201 Created` remains correct because the article already exists in MongoDB before the response is returned.

## Background Task Reliability

FastAPI background tasks run in the same process and are therefore best-effort rather than durable.

Possible failure:

```text
MongoDB write succeeds
        ↓
201 response returned
        ↓
process crashes
        ↓
background indexing never runs
```

The architecture remains recoverable because:

```text
MongoDB
→ authoritative article remains safe

Elasticsearch
→ can be rebuilt from MongoDB

Redis
→ stale entries expire by TTL
```

# Bounded Reindex Concurrency

Elasticsearch rebuilds use:

```env
REINDEX_CONCURRENCY=10
```

An `asyncio.Semaphore` limits simultaneous indexing requests.

```text
100 tasks
   ↓
Semaphore(10)
   ↓
10 active
90 waiting
```

The rebuild also batches documents to avoid creating excessive numbers of tasks at once.

```text
MongoDB async cursor
       ↓
batch
       ↓
TaskGroup
       ↓
Semaphore-limited indexing
       ↓
next batch
```

# Failure Handling

## MongoDB unavailable

```text
MongoDB unavailable
→ authoritative persistence unavailable
→ article request fails
```

## Elasticsearch unavailable during background indexing

```text
MongoDB ✅
Elasticsearch ❌
```

The article still exists and the indexing failure is logged.

## Redis unavailable

```text
Redis ❌
Elasticsearch ✅
```

Search continues without caching.

## Elasticsearch unavailable during search

```text
Redis miss
Elasticsearch ❌
```

The API returns a search-unavailable response.

# Eventual Consistency

MongoDB, Elasticsearch, and Redis do not share one ACID transaction.

```text
MongoDB
→ authoritative state

Elasticsearch
→ eventually synchronized derived state

Redis
→ eventually refreshed cached state
```

This is an eventual-consistency design.

# Elasticsearch Rebuild

Elasticsearch can be rebuilt entirely from MongoDB:

```text
delete existing search index
        ↓
create explicit mapping
        ↓
async MongoDB cursor
        ↓
batch documents
        ↓
TaskGroup
        ↓
Semaphore-bounded indexing
        ↓
refresh index
```

# Project Structure

```text
knowledge-search-service/
├── app/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── articles.py
│   │   ├── dependencies.py
│   │   └── search.py
│   ├── cache/
│   │   ├── __init__.py
│   │   └── redis.py
│   ├── db/
│   │   ├── __init__.py
│   │   └── mongodb.py
│   ├── errors/
│   │   ├── __init__.py
│   │   └── search_errors.py
│   ├── models/
│   │   ├── __init__.py
│   │   └── article.py
│   ├── repositories/
│   │   ├── __init__.py
│   │   ├── article_repository.py
│   │   ├── article_search_repository.py
│   │   └── search_cache_repository.py
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── article.py
│   │   └── search.py
│   ├── search/
│   │   ├── __init__.py
│   │   ├── elasticsearch.py
│   │   └── index_definition.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── article_service.py
│   │   ├── article_background_service.py
│   │   └── search_service.py
│   ├── config.py
│   └── main.py
├── scripts/
│   ├── async_lab.py
│   ├── check_async_mongodb.py
│   ├── check_async_elasticsearch.py
│   ├── check_async_redis.py
│   ├── create_search_index.py
│   ├── sync_articles_to_elasticsearch.py
│   ├── rebuild_search_index.py
│   ├── clear_search_cache.py
│   └── check_search_cache.py
├── tests/
│   ├── conftest.py
│   ├── test_article_service.py
│   ├── test_article_background_service.py
│   └── test_search_service.py
├── .env
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```

# Environment Configuration

```env
MONGODB_URI=mongodb+srv://your-real-mongodb-connection
MONGODB_DATABASE=knowledge_app

ELASTICSEARCH_URL=https://your-elasticsearch-url
ELASTIC_API_KEY=your-real-api-key
ELASTICSEARCH_INDEX=knowledge-articles-v1

REDIS_URL=your-real-redis-url
SEARCH_CACHE_TTL_SECONDS=60

REDIS_OPERATION_TIMEOUT_SECONDS=1.0
ELASTICSEARCH_OPERATION_TIMEOUT_SECONDS=5.0
REINDEX_CONCURRENCY=10
```

Do not commit `.env`.

Example `.env.example`:

```env
MONGODB_URI=mongodb+srv://username:password@hostname/
MONGODB_DATABASE=knowledge_app

ELASTICSEARCH_URL=https://your-elasticsearch-project-url
ELASTIC_API_KEY=your-api-key
ELASTICSEARCH_INDEX=knowledge-articles-v1

REDIS_URL=redis://username:password@hostname:port
SEARCH_CACHE_TTL_SECONDS=60

REDIS_OPERATION_TIMEOUT_SECONDS=1.0
ELASTICSEARCH_OPERATION_TIMEOUT_SECONDS=5.0
REINDEX_CONCURRENCY=10
```

# Local Setup

```bash
git clone <repository-url>
cd knowledge-search-service
```

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

# Infrastructure Checks

```powershell
python -m scripts.check_async_mongodb
python -m scripts.check_async_elasticsearch
python -m scripts.check_async_redis
```

# Elasticsearch Setup

Create the index:

```powershell
python -m scripts.create_search_index
```

Sync existing articles if needed:

```powershell
python -m scripts.sync_articles_to_elasticsearch
```

Rebuild the search index:

```powershell
python -m scripts.rebuild_search_index
```

# Run the API

```powershell
python -m uvicorn app.main:app --reload
```

Swagger:

```text
http://127.0.0.1:8000/docs
```

OpenAPI:

```text
http://127.0.0.1:8000/openapi.json
```

# Example Article

```http
POST /api/v1/articles
```

```json
{
  "title": "Async Backend Architecture",
  "content": "Async I/O allows a backend to continue serving work while network operations are waiting.",
  "tags": [
    "python",
    "async",
    "backend"
  ],
  "source": "engineering"
}
```

# Example Search

```http
GET /api/v1/search?q=async%20backend&limit=10
```

# Testing

Tests use fake repositories rather than live cloud infrastructure.

Async tests use:

```python
@pytest.mark.anyio
async def test_...():
    ...
```

Shared fixtures live in:

```text
tests/conftest.py
```

The suite covers:

- article persistence orchestration
- background indexing
- cache invalidation
- cache hit
- cache miss
- Redis timeout
- Redis absence
- Elasticsearch timeout
- Elasticsearch absence
- non-fatal derived-infrastructure failures

Run:

```powershell
pytest -v
```

# Key Engineering Concepts

## Async Python

- `async def`
- `await`
- event loop
- I/O-bound concurrency
- blocking vs non-blocking work
- `asyncio.gather`
- `asyncio.TaskGroup`
- `asyncio.timeout`
- cancellation
- `asyncio.to_thread`
- `asyncio.Semaphore`
- bounded concurrency

## MongoDB

- async MongoDB driver
- documents
- `ObjectId`
- embedding
- compound indexes
- async cursors

## Elasticsearch

- `AsyncElasticsearch`
- explicit mappings
- full-text search
- `text` vs `keyword`
- field boosting
- filters
- relevance scoring
- inverted indexes
- near-real-time search
- rebuild workflows

## Redis

- `redis.asyncio`
- cache-aside
- TTL
- normalized deterministic keys
- negative-result caching
- invalidation
- graceful degradation

## Backend Architecture

- async FastAPI endpoints
- lifespan-managed clients
- dependency injection
- router/service/repository separation
- background processing
- source of truth
- derived state
- graceful degradation
- eventual consistency

# Current Limitations

The service intentionally does not yet include:

- authentication / authorization
- update/delete article synchronization
- durable external job queue
- message broker
- retry queue
- transactional outbox
- dead-letter queue
- idempotent event consumers
- Docker
- Kubernetes
- production metrics/tracing
- semantic/vector search
- RAG
- LLM integration

FastAPI `BackgroundTasks` is intentionally used only as a lightweight in-process mechanism. It is not treated as a durable job system.

# Future Evolution

A more durable write architecture could evolve toward:

```text
API
 ↓
MongoDB
 ↓
outbox/event
 ↓
Kafka / RabbitMQ
 ↓
background consumer
 ↓
Elasticsearch
 ↓
cache invalidation
```

This would add:

```text
durable delivery
retries
consumer acknowledgements
dead-letter handling
idempotency
decoupling
```

# Design Principles

```text
Keep MongoDB authoritative

Treat Elasticsearch as rebuildable

Treat Redis as disposable

Use native async I/O for network operations

Do not confuse async with concurrency

Only run independent operations concurrently

Bound concurrency instead of launching unlimited tasks

Put deadlines around external dependencies

Keep cache failures non-fatal

Keep derived indexing failures non-authoritative

Separate request-time work from background work

Do not treat in-process background work as durable

Keep infrastructure-specific details inside repositories

Use dependency injection for replaceable components

Test failure behavior explicitly

Add infrastructure only when a real requirement justifies it
```

The result is a compact asynchronous multi-database backend demonstrating document storage, full-text search, caching, structured concurrency, failure handling, and lightweight background processing while keeping authoritative data safe.
