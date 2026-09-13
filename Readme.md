# Knowledge Search Service

A production-oriented backend service for storing, searching, and caching knowledge-base articles.

The project combines:

- **MongoDB Atlas** as the authoritative document store
- **Elasticsearch** for full-text relevance search
- **Redis** for search-result caching
- **FastAPI** for the HTTP API
- **pytest** for service-level testing

The goal is to demonstrate how multiple data technologies can be combined while keeping each one responsible for a specific problem.

---

## Architecture

```text
                         MongoDB Atlas
                         source of truth
                               │
                               │ indexing
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
        ├── MongoDB write
        │      ↓
        │   authoritative document
        │
        ├── Elasticsearch indexing
        │      ↓
        │   searchable derived copy
        │
        └── Redis cache invalidation
```

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
  └── MISS
        ↓
   Elasticsearch
        ↓
   search results
        ↓
   Redis SET + TTL
        ↓
      return
```

---

## Technology Responsibilities

| Technology | Responsibility |
|---|---|
| MongoDB | Authoritative article/document storage |
| Elasticsearch | Full-text relevance search |
| Redis | Short-lived search-result cache |

```text
MongoDB
→ source of truth

Elasticsearch
→ derived and rebuildable search index

Redis
→ disposable performance layer
```

---

## Tech Stack

- Python 3.13+
- FastAPI
- Pydantic
- PyMongo
- MongoDB Atlas
- Elasticsearch
- Elastic Cloud
- Redis
- Redis Cloud
- Uvicorn
- pytest
- python-dotenv

---

## Features

- Create knowledge articles
- Fetch an article by ID
- List active articles
- MongoDB document persistence
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
- Graceful Redis failure handling
- Graceful Elasticsearch indexing failure handling
- Elasticsearch rebuild from MongoDB
- FastAPI dependency injection
- Service-level unit tests with fake repositories

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Application health check |
| `POST` | `/api/v1/articles` | Create a knowledge article |
| `GET` | `/api/v1/articles` | List active articles |
| `GET` | `/api/v1/articles/{article_id}` | Fetch one article |
| `GET` | `/api/v1/search?q=...` | Full-text relevance search |

---

# MongoDB

MongoDB is the authoritative storage system.

The configured database is:

```text
knowledge_app
```

The main collection is:

```text
articles
```

Conceptually:

```text
knowledge_app
    ↓
articles
    ↓
document
```

An article document resembles:

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

The repository converts MongoDB's `_id` / `ObjectId` into an application-facing string ID.

```text
MongoDB ObjectId
      ↓
Repository
      ↓
string ID
      ↓
Service / API
```

This keeps persistence-specific types out of the service and HTTP layers.

---

## MongoDB Modelling

Tags are embedded directly inside the article because they belong to the document, are small, and are typically read with the article.

Example:

```json
{
  "tags": [
    "payment",
    "refund",
    "support"
  ]
}
```

A separate reference would be more appropriate for entities with an independent lifecycle or heavy reuse.

---

## MongoDB Index

The current article-list query filters active articles and sorts by creation time.

```python
collection.create_index(
    [
        ("is_active", 1),
        ("created_at", -1),
    ]
)
```

Indexes are added according to real access patterns rather than automatically indexing every field.

---

# Elasticsearch

Elasticsearch stores a derived searchable representation of MongoDB articles.

The search index is:

```text
knowledge-articles-v1
```

MongoDB IDs are reused as Elasticsearch document IDs:

```text
MongoDB _id
     ↓
string
     ↓
Elasticsearch _id
```

---

## Elasticsearch Mapping

```json
{
  "title": {
    "type": "text"
  },
  "content": {
    "type": "text"
  },
  "tags": {
    "type": "text",
    "fields": {
      "keyword": {
        "type": "keyword"
      }
    }
  },
  "source": {
    "type": "keyword"
  },
  "is_active": {
    "type": "boolean"
  },
  "created_at": {
    "type": "date"
  },
  "updated_at": {
    "type": "date"
  }
}
```

### `text` vs `keyword`

```text
text
→ analyzed full-text search

keyword
→ exact matching / filtering / aggregation
```

---

## Full-Text Search

The search endpoint uses an Elasticsearch `multi_match` query.

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

Search importance:

```text
title
→ strongest boost

tags
→ medium boost

content
→ normal weight
```

Active content is filtered with:

```text
is_active = true
```

Example:

```http
GET /api/v1/search?q=payment%20failed&limit=10
```

---

## Relevance and Inverted Indexes

Elasticsearch ranks results by relevance rather than simple chronological or numeric ordering.

At a high level, it uses an inverted-index structure conceptually like:

```text
payment
→ document A
→ document C

password
→ document B

refund
→ document D
```

This allows efficient full-text retrieval.

---

## Near-Real-Time Search

Elasticsearch is treated as a near-real-time search system.

A newly indexed document may take a short time to become searchable. Normal writes do not force an explicit refresh after every document.

Administrative setup/rebuild scripts may refresh explicitly when immediate visibility is useful.

---

# Redis

Redis caches repeated Elasticsearch search results.

Redis is not authoritative storage.

Its job is:

```text
Have we already computed this search recently?
```

If yes:

```text
return cached result
```

If no:

```text
query Elasticsearch
→ cache result
→ return
```

---

## Cache-Aside Pattern

```text
request
  ↓
Redis GET
  ↓
cache hit?
```

On hit:

```text
Redis
  ↓
return cached result
```

On miss:

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

Only successful search responses are cached.

---

## Cache Key Design

Queries are normalized before key generation.

```text
"  Payment   Failed "
```

becomes:

```text
"payment failed"
```

A cache key looks conceptually like:

```text
search:v1:<query-hash>:limit:10
```

It includes:

```text
query
limit
cache version
```

because each can affect the response.

---

## Cache-Key Versioning

```text
search:v1:
```

can later become:

```text
search:v2:
```

if ranking or response semantics change significantly.

This makes old cache entries naturally obsolete.

---

## TTL

Search results expire automatically.

Example:

```env
SEARCH_CACHE_TTL_SECONDS=60
```

Tradeoff:

```text
short TTL
→ fresher results
→ more Elasticsearch requests

long TTL
→ more cache hits
→ potentially staler results
```

---

## Empty Search Results

Empty search results are cached too.

```python
[]
```

is a valid cached response.

A cache miss is:

```python
None
```

Therefore logic must use:

```python
if cached_results is not None:
```

rather than:

```python
if cached_results:
```

Otherwise cached empty results would be treated as misses.

---

## Cache Invalidation

When searchable article data changes, the service clears matching search keys:

```text
search:*
```

The project uses incremental Redis `SCAN` rather than relying on a blocking full-keyspace lookup.

The strategy is intentionally coarse-grained because one article can influence many possible search queries.

---

# Failure Handling

## MongoDB Failure

```text
MongoDB unavailable
→ authoritative persistence unavailable
→ article creation cannot succeed
```

MongoDB is required for article storage.

## Elasticsearch Failure During Creation

```text
MongoDB write ✅
Elasticsearch indexing ❌
```

The article still exists.

The service keeps the MongoDB result, logs the indexing failure, and returns the created article.

## Redis Failure

```text
Redis unavailable
Elasticsearch available
```

Search still works.

Redis improves performance but is not required for correctness.

## Elasticsearch Failure During Search

If Redis misses and Elasticsearch is unavailable, the application cannot perform full-text search and returns a service-unavailable response.

---

# Eventual Consistency

MongoDB, Elasticsearch, and Redis do not share one ACID transaction.

There is no:

```text
BEGIN

MongoDB write
Elasticsearch write
Redis clear

COMMIT ALL
```

Instead:

```text
MongoDB
→ authoritative state

Elasticsearch
→ derived state

Redis
→ cached state
```

The derived systems can temporarily lag behind the source of truth.

This is eventual consistency.

---

# Elasticsearch Rebuild

Elasticsearch is rebuildable from MongoDB.

```text
MongoDB
   ↓
delete old Elasticsearch index
   ↓
recreate explicit mapping
   ↓
read active MongoDB articles
   ↓
index all documents
   ↓
refresh
```

This proves:

```text
MongoDB
→ authoritative

Elasticsearch
→ derived
```

---

# Dependency Injection

FastAPI dependencies construct repositories and services.

```text
FastAPI dependency
      ↓
ArticleRepository
ArticleSearchRepository
SearchCacheRepository
      ↓
ArticleService
SearchService
      ↓
Endpoint
```

This improves testability and avoids hard-coded global service construction inside routers.

---

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
│   │   └── search_service.py
│   └── main.py
├── scripts/
│   ├── check_mongodb_connection.py
│   ├── check_elasticsearch_connection.py
│   ├── check_redis_connection.py
│   ├── create_search_index.py
│   ├── sync_articles_to_elasticsearch.py
│   ├── rebuild_search_index.py
│   ├── clear_search_cache.py
│   └── check_search_cache.py
├── tests/
│   ├── test_article_service.py
│   └── test_search_service.py
├── pytest.ini
├── .env
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```

---

# Environment Configuration

Create `.env`:

```env
MONGODB_URI=mongodb+srv://your-real-mongodb-connection
MONGODB_DATABASE=knowledge_app

ELASTICSEARCH_URL=https://your-elasticsearch-url
ELASTIC_API_KEY=your-real-api-key
ELASTICSEARCH_INDEX=knowledge-articles-v1

REDIS_URL=your-real-redis-url
SEARCH_CACHE_TTL_SECONDS=60
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
```

---

# Local Setup

```bash
git clone <repository-url>
cd knowledge-search-service
```

Create and activate a virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

---

# Infrastructure Checks

```powershell
python -m scripts.check_mongodb_connection
python -m scripts.check_elasticsearch_connection
python -m scripts.check_redis_connection
```

These are manual infrastructure checks, not pytest unit tests.

---

# Elasticsearch Setup

Create the search index:

```powershell
python -m scripts.create_search_index
```

Sync existing MongoDB content:

```powershell
python -m scripts.sync_articles_to_elasticsearch
```

Completely rebuild Elasticsearch from MongoDB:

```powershell
python -m scripts.rebuild_search_index
```

---

# Run the API

```powershell
python -m uvicorn app.main:app --reload
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

OpenAPI:

```text
http://127.0.0.1:8000/openapi.json
```

---

# Example Article

```http
POST /api/v1/articles
```

```json
{
  "title": "Troubleshooting Payment Failures",
  "content": "If a payment fails, verify that your card or payment method is active and has sufficient funds. Confirm the billing information and try again.",
  "tags": [
    "payment",
    "failure",
    "billing",
    "troubleshooting"
  ],
  "source": "help-center"
}
```

Flow:

```text
FastAPI
   ↓
ArticleService
   ↓
MongoDB
   ↓
Elasticsearch
   ↓
Redis invalidation
```

---

# Example Search

```http
GET /api/v1/search?q=payment%20failed&limit=10
```

Conceptual result:

```json
[
  {
    "id": "article-id",
    "score": 4.8,
    "title": "Troubleshooting Payment Failures",
    "content": "If a payment fails...",
    "tags": [
      "payment",
      "failure",
      "billing",
      "troubleshooting"
    ],
    "source": "help-center",
    "is_active": true,
    "created_at": "...",
    "updated_at": "..."
  }
]
```

Elasticsearch scores are not expected to remain exactly the same across datasets/configurations.

---

# Testing

Core service tests use fake repositories instead of live cloud infrastructure.

This keeps tests:

```text
fast
deterministic
network-independent
credential-independent
```

## Search Service Tests

```text
SearchService
   │
   ├── FakeSearchRepository
   └── FakeCacheRepository
```

Important behavior:

```text
CACHE HIT
→ return cached result
→ Elasticsearch not called

CACHE MISS
→ call Elasticsearch
→ cache successful result
→ return result
```

## Article Service Tests

```text
ArticleService
   │
   ├── FakeArticleRepository
   ├── FakeSearchRepository
   └── FakeCacheRepository
```

Important behavior:

```text
create succeeds
→ Elasticsearch indexing attempted
→ Redis invalidation attempted
```

Failure behavior:

```text
authoritative create succeeds
Elasticsearch indexing fails
→ article is still returned
```

---

# Pytest Configuration

`pytest.ini`:

```ini
[pytest]
testpaths = tests
pythonpath = .
```

This ensures:

```text
tests/
→ automated pytest tests

scripts/
→ manual setup/connectivity/admin utilities
```

Run:

```powershell
pytest -v
```

---

# Key Engineering Concepts Demonstrated

## MongoDB

- databases
- collections
- documents
- BSON
- `ObjectId`
- embedding
- references
- compound indexes
- document-oriented modelling

## Elasticsearch

- indexes
- documents
- explicit mappings
- `text`
- `keyword`
- `multi_match`
- field boosting
- filters
- relevance scoring
- inverted indexes
- near-real-time search
- rebuild/reindex workflows

## Redis

- key/value storage
- cache-aside
- cache hits
- cache misses
- TTL
- deterministic keys
- query normalization
- cache-key versioning
- negative-result caching
- invalidation
- graceful degradation
- `SCAN`

## Backend Architecture

- FastAPI
- Pydantic validation
- router/service/repository separation
- dependency injection
- source-of-truth design
- derived data
- graceful degradation
- eventual consistency

## Testing

- unit tests
- fake repositories
- failure-path testing
- infrastructure smoke checks
- pytest collection configuration

---

# Current Limitations

The service intentionally does not yet include:

- authentication or authorization
- update/delete article endpoints
- background workers
- message brokers
- transactional outbox
- automatic retry queue for failed indexing
- advanced cache invalidation
- Docker
- Kubernetes
- production metrics/tracing
- semantic/vector search
- RAG
- LLM integration

---

# Possible Future Improvements

```text
article update/delete synchronization
background Elasticsearch indexing
retry mechanism
outbox pattern
Kafka or RabbitMQ
structured logging
metrics
distributed tracing
Docker
CI/CD
authentication
rate limiting
semantic/vector search
RAG
```

A larger production write flow could evolve toward:

```text
API
 ↓
MongoDB
 ↓
outbox/event
 ↓
message broker
 ↓
background consumer
 ↓
Elasticsearch
 ↓
cache invalidation
```

---

# Design Principles

```text
Make the source of truth explicit

Treat search indexes as rebuildable

Treat caches as disposable

Keep infrastructure concerns inside repositories

Keep HTTP concerns inside routers

Keep orchestration/business behavior inside services

Use dependency injection for replaceable components

Gracefully degrade optional infrastructure

Do not make cache availability a correctness requirement

Avoid cross-system consistency assumptions

Test architectural decisions through failure-path unit tests

Add complexity only when a real requirement justifies it
```

The result is a compact multi-database backend demonstrating how document storage, full-text search, caching, and failure handling can work together without confusing their responsibilities.
