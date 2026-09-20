# Knowledge Search Service

An asynchronous knowledge-base backend built with FastAPI, MongoDB Atlas, Elasticsearch, Redis, RabbitMQ, and Apache Kafka (Aiven Free). It demonstrates full-text search and caching alongside reliable asynchronous message delivery, independent event consumers, replay, and failure recovery.

> This README describes the intended architecture after the embedded-outbox implementation is applied and the test suite and live verification are complete. Do not treat an unrun example as proof of successful deployment.

## Architecture

```text
Client → FastAPI → ArticleService → MongoDB Atlas (knowledge_app)
                                        ├── article document
                                        ├── outbox.rabbitmq = pending
                                        └── outbox.kafka    = pending
                                                     │
                                        Outbox dispatcher (separate process)
                                               /                 \
                             publisher-confirmed RabbitMQ       Kafka article.created
                                        │                         │
                                 indexing worker         independent consumer groups
                                        │                    /                 \
                              MongoDB → Elasticsearch        audit           analytics
                                        │                     │                 │
                                 Redis invalidation     MongoDB upserts    MongoDB upserts
```

**Source of truth:** MongoDB articles. **Derived systems:** Elasticsearch full-text index and Redis search cache. RabbitMQ carries an `article.index.requested` work command. Kafka carries an `article.created` domain event.

An article and its two pending publication records are written in **one MongoDB document insert**. The API returns `201 Created` after the authoritative write. A separate dispatcher publishes both deliveries and marks each as published only after its broker confirms acceptance.

## Technologies

- Python 3.13+, FastAPI, Pydantic, Uvicorn, pytest/AnyIO.
- PyMongo `AsyncMongoClient` and MongoDB Atlas (`knowledge_app`).
- `AsyncElasticsearch` / Elastic Cloud and `redis.asyncio` / Redis Cloud.
- `aio-pika` and RabbitMQ, with durable queues, publisher confirms, manual acknowledgements, delayed retries, and a DLQ.
- `aiokafka` and Apache Kafka on Aiven Free, with SASL_SSL / SCRAM-SHA-256 authentication and two topic partitions.
- `asyncio` structured concurrency, operation deadlines, and bounded reindex parallelism.

## HTTP API

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/health` | Application liveness response; not a complete dependency-readiness report |
| `POST` | `/api/v1/articles` | Persist article and embedded outbox together; return `201` |
| `GET` | `/api/v1/articles` | List active articles |
| `GET` | `/api/v1/articles/{article_id}` | Fetch an article |
| `GET` | `/api/v1/search?q=...` | Full-text search with Redis cache-aside |

Example request:

```json
{
  "title": "Transactional Outbox",
  "content": "An article and its pending publication intents are stored atomically.",
  "tags": ["mongodb", "messaging", "reliability"],
  "source": "engineering"
}
```

The internal `outbox` field is not part of the public `ArticleResponse` contract.

## MongoDB and the embedded outbox

An article contains two independent internal delivery records:

```json
{
  "outbox": {
    "rabbitmq": {"status": "pending", "attempts": 0, "next_attempt_at": "..."},
    "kafka":    {"status": "pending", "attempts": 0, "next_attempt_at": "..."}
  }
}
```

Each record transitions from `pending` → `leased` → `published`. A failed publish is returned to `pending` with capped exponential backoff; an abandoned lease is claimable after expiry. `find_one_and_update()` claims a delivery atomically. A random lease token is checked before a dispatcher can update the claimed record, preventing a stale lease holder from overwriting a newer holder's database state.

The dispatcher uses a 120-second lease and polls for work. It retains unconfirmed work for retries rather than silently discarding it. There is no automatic finite outbox retry limit; monitor pending age and attempts.

Single-document atomicity avoids needing a multi-document MongoDB transaction for the present **create-only, one-article/two-message** workflow. This is not a general substitute for transactions if future features update multiple records or require cross-aggregate ordering. Historical articles created before outbox support do not automatically generate new `article.created` events; Elasticsearch can still be rebuilt from MongoDB.

## RabbitMQ indexing pipeline

```text
article.index.v1          main durable work queue
article.index.v1.retry.5s TTL-based delayed retry queue
article.index.v1.dlq      failed-message inspection queue
```

The dispatcher publishes persistent jobs with mandatory routing and publisher confirmations. Workers use manual ACKs and a prefetch limit of five. On success: MongoDB read → Elasticsearch index → best-effort Redis cache invalidation → ACK. Transient failures receive up to three delayed retries, followed by DLQ routing. Schema errors and missing-article probes go directly to the DLQ. Replacement messages are published and confirmed **before** the original delivery is acknowledged.

Logical RabbitMQ job identity is deterministic for an article. The Elasticsearch document ID is the article's MongoDB ID, so replay of the current article state does not create another Elasticsearch document. Message execution itself can still happen more than once. Cached results may remain stale until their TTL expires if invalidation fails.

## Kafka event streaming

- Topic: `knowledge.article.events.v1` (two partitions on Aiven Free).
- Key: article ID.
- Event type: `article.created`, schema version 1.
- Deterministic event ID: `article.created.v1:<article_id>`.
- Audit group: `knowledge-audit-v1` → `article_creation_audit`.
- Analytics group: `knowledge-analytics-v1` → `article_creation_analytics_events`.

Each consumer group independently reads retained records. Auto-commit is disabled. The consumer validates the event, upserts a record using the deterministic event ID as MongoDB `_id`, and **then** commits `offset + 1`. This makes replay and crash-before-offset-commit safe against duplicate MongoDB event records for this create-only workflow. It does **not** guarantee exactly-once execution.

Invalid JSON, schema, or key data is first persisted in `knowledge_app.kafka_poison_events` (raw key/value encoded as Base64, topic/partition/offset and validation error), then the offset is committed. A failed poison-record write must not commit the offset. Inspect poison records before attempting manual remediation or replay.

Aiven Free has limited retention (up to three days) and can automatically power off after inactivity. Monitor consumer lag and service state. Outbox publication confirmation does not mean a consumer has read the event before retention expires.

## Search, caching, and reindexing

Search uses Elasticsearch multi-match over `title^3`, `tags^2`, and `content`, filtering for active documents. Redis uses normalized, hashed, versioned cache keys, TTL and cache-aside reads. A cache miss or unavailable Redis falls back to Elasticsearch; an unavailable Elasticsearch on an uncached search produces HTTP 503.

The administrative rebuild reads the MongoDB source of truth through an async cursor, indexes in batches and uses `asyncio.Semaphore` to bound concurrent Elasticsearch requests. Elasticsearch is near-real-time; the rebuild script can explicitly refresh after bulk restoration.

## Setup

In Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create an ignored `.env` with your own credentials:

```dotenv
MONGODB_URI=mongodb+srv://username:password@host/
MONGODB_DATABASE=knowledge_app
ELASTICSEARCH_URL=https://your-elasticsearch-host
ELASTIC_API_KEY=your-api-key
ELASTICSEARCH_INDEX=knowledge-articles-v1
REDIS_URL=rediss://username:password@host:port
SEARCH_CACHE_TTL_SECONDS=60
REDIS_OPERATION_TIMEOUT_SECONDS=1.0
ELASTICSEARCH_OPERATION_TIMEOUT_SECONDS=5.0
REINDEX_CONCURRENCY=10
RABBITMQ_URL=amqps://username:password@host/vhost
ARTICLE_INDEX_QUEUE=article.index.v1
KAFKA_BOOTSTRAP_SERVERS=your-aiven-sasl-host:port
KAFKA_USERNAME=your-aiven-username
KAFKA_PASSWORD=your-aiven-password
KAFKA_CA_FILE=certs/ca.pem
KAFKA_ARTICLE_TOPIC=knowledge.article.events.v1
KAFKA_PUBLISH_TIMEOUT_SECONDS=5
```

Use the actual protocol/host/port supplied by each service. Save Aiven's CA certificate at `certs/ca.pem`. Ignore both `.env` and `certs/` in Git. The Kafka client uses `SASL_SSL` and `SCRAM-SHA-256` with certificate verification; do not disable verification to suppress connection errors.

Create the Kafka topic using Aiven's console and ensure the RabbitMQ queues are declared by initialization. The project does not require new paid broker services for the outbox.

## Run

Start each process in a separate activated PowerShell terminal:

```powershell
python -m uvicorn app.main:app --reload
python -m app.workers.outbox_dispatcher
python -m app.workers.article_index_worker
python -m app.workers.article_event_consumer --role audit
python -m app.workers.article_event_consumer --role analytics
```

Swagger UI: `http://127.0.0.1:8000/docs`.

Useful maintenance commands:

```powershell
python -m scripts.check_rabbitmq_connection
python -m scripts.check_kafka_connection
python -m scripts.check_outbox
python -m scripts.show_article_analytics
python -m scripts.rebuild_search_index
```

Keep `rebuild_search_index` for repairs; it is not an everyday message-processing step.

## Tests and failure drills

```powershell
pytest -v
python -m compileall -q app scripts tests
```

The unit suite uses fake repositories/brokers for service orchestration, outbox state, publishing failures, ACK ordering, retry/DLQ behavior, Kafka manual offset commits, replay, and idempotency. Verify that the actual `ArticleRepository.create()` submits **one insert** containing the article and both pending outbox records.

Recommended integration drills:

1. Create an article with all processes running; verify both outbox statuses become `published`, RabbitMQ indexing completes and both Kafka consumer collections contain the event.
2. Stop the dispatcher, create an article, verify pending records remain in MongoDB, restart the dispatcher, and confirm eventual publication.
3. Stop only the RabbitMQ worker, create an article, verify its broker queue retains the job, restart the worker and check Elasticsearch.
4. Inject publisher failure with a fake; verify the outbox returns to pending without deleting the publication intent.
5. Simulate a successful broker publish followed by failed MongoDB status update; verify the lease is retained, can expire, and any duplicate consumer effect remains idempotent.
6. Inject an invalid Kafka event in isolated tests; verify poison storage occurs before offset commit and that poison-storage failure prevents commit.

## Delivery guarantees and limitations

| Boundary | Behavior | Important limitation |
|---|---|---|
| API → MongoDB | Article and outbox saved in one document write | MongoDB availability and configured write concern still matter |
| MongoDB → dispatcher | Pending work survives dispatcher outage | Requires dispatcher recovery and backlog monitoring |
| Dispatcher → broker | Waits for publication confirmation | Crash after confirmation but before status update can republish |
| RabbitMQ → worker | Persistent queue/message; manual ACK, retry and DLQ | Broker durability/configuration and duplicate deliveries require care |
| Kafka → consumers | Independent group offsets, manual commit after MongoDB upsert | Retention expiry, rebalances and repeated execution remain possible |
| Elasticsearch/Redis | Derived and rebuildable/disposable | Index synchronization and cache freshness are eventual |

The system provides **atomic publication intent** for newly created articles, not an atomic transaction spanning MongoDB, RabbitMQ and Kafka. Broker publication is at-least-once in normal recoverable failure scenarios, not exactly-once delivery. For production, add persistent supervision, oldest-pending alerts, consumer-lag monitoring, broker durability review, stronger ordering rules for update/delete events, and an incident procedure for unrecoverable failures.

## Design principles

- Build small, but engineer properly: authoritative state, explicit contracts, async I/O, bounded concurrency, targeted timeouts, clear dependency boundaries and testable failure paths.
- Do not claim a message was consumed because it was published.
- Do not acknowledge or commit a message before its required database-side effect completes.
- Expect duplicate publications and delivery; make consumer effects idempotent.
- Keep credentials, CA files and local environment configuration out of version control.
