# Knowledge Search Service

An asynchronous knowledge-base backend built with FastAPI, MongoDB Atlas, Elasticsearch, Redis, RabbitMQ, and Apache Kafka (Aiven Free). It demonstrates full-text search and caching, recoverable messaging, independent event consumers, and repeatable local deployment using Docker Compose, with automated checks using GitHub Actions.

> **Implementation and verification note:** This README documents the application design and the container/CI configuration introduced for the project. It was prepared from the project documentation and planned changes, not from a checkout of your current Windows repository. Confirm that the outbox refactor, Docker files, tests, and GitHub Actions workflow are present and passing before describing this project as deployed or fully verified.

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

## Runtime and deployment overview

The same Python codebase runs as five independent processes. Docker builds **one image**, and Compose starts five containers from it with different commands. The databases and brokers remain managed externally; Compose does not start or migrate them.

```text
                      developer pushes code
                              |
                              v
                     GitHub Actions CI
                  lint -> pytest -> image build
                              |
                validated image definition
                              |
                              v
                    Docker Compose (local)
       +------------+------------+------------+
       |            |            |            |
       v            v            v            v
      API         outbox     index worker   Kafka consumers
                               RabbitMQ     audit / analytics
       |            |            |            |
       +------------+------------+------------+
                              |
        MongoDB Atlas / Elastic Cloud / Redis Cloud
                 RabbitMQ / Aiven Kafka
```

**Why separate containers?** The API can answer requests while the dispatcher or a consumer is offline. Each process has its own lifecycle and logs. Docker standardizes execution; the MongoDB outbox and broker acknowledgements—not Docker—provide messaging recovery. The compose file defines a local runtime, **not an internet-facing production deployment**.

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

## Configuration and local Python setup

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

## Run without Docker (alternative)

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

## Run using Docker Compose (recommended for local integration)

### Prerequisites and the reason for each

- Docker Desktop running in **Linux container** mode (WSL2 backend on Windows); provides the Linux container runtime.
- `Dockerfile`, `compose.yaml`, `requirements.txt`, and `.dockerignore` at the repository root; define the packaged code and multi-process runtime.
- A local `.env` populated with valid **development** credentials; Compose injects it at runtime and it must not be committed or copied into the image.
- `certs/ca.pem`, downloaded from the Aiven service; required for TLS certificate verification by the Kafka dispatcher and consumers.
- Existing reachable managed MongoDB, Elasticsearch, Redis, RabbitMQ, and Aiven Kafka services. MongoDB must be accessible from Docker's network, and Aiven's free Kafka service may need to be powered on.
- A passing local Python test baseline. Containerization does not fix application bugs.

The Docker configuration is designed as follows:

| File | Purpose | Important decision |
|---|---|---|
| `Dockerfile` | Builds a Python 3.13 Linux image, installs dependencies, copies `app/` and `scripts/` | Runs as non-root `appuser`; default process is Uvicorn. |
| `.dockerignore` | Removes unnecessary/sensitive files from the build context | Excludes `.env`, `certs/`, `.venv/`, `.git/`, `tests/`, `.ruff_cache/`. |
| `compose.yaml` | Declares five application services and their startup commands | Reuses one image; exposes the API only at `127.0.0.1:8000`. |
| `.env` | Supplies managed-service connection details at container runtime | Never embed it in the image or commit it. |
| `certs/ca.pem` | Trust certificate for Aiven Kafka TLS | Mounted read-only into Kafka-enabled containers. |

The image should use one logical Dockerfile `CMD` instruction:

```dockerfile
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Why `0.0.0.0` here?** It lets the application accept traffic through Docker's port mapping *inside* the container. Compose should publish only `127.0.0.1:8000:8000` on the Windows host, so the development API is not deliberately exposed to your LAN.

### Certificate path: Windows versus Linux

The Windows `.env` path `KAFKA_CA_FILE=certs/ca.pem` does not exist automatically inside a Linux container. In `compose.yaml`, the **outbox**, **audit**, and **analytics** services mount the local certificate and override that path:

```yaml
environment:
  KAFKA_CA_FILE: /run/certs/ca.pem
volumes:
  - ./certs/ca.pem:/run/certs/ca.pem:ro
```

The `:ro` suffix means the container cannot write to this file. Neither the certificate directory nor `.env` should be copied into the image. In PowerShell, verify that `Test-Path .\certs\ca.pem` returns `True`. Use the SASL host and port supplied by Aiven, not an HTTPS URL. Do not disable certificate verification to bypass TLS errors.

### Starting the stack

Run these commands **from the repository root in Windows PowerShell**, after stopping the five equivalent Python processes that you previously started manually:

```powershell
docker --version
docker compose version
Test-Path .\certs\ca.pem
pytest -v
docker compose config -q
docker compose up -d --build
docker compose ps
Invoke-RestMethod http://127.0.0.1:8000/health
```

`config -q` validates Compose without printing resolved configuration; avoid sharing the unredacted output of `docker compose config`, which can contain secrets. `up -d --build` builds the shared image and starts the services in the background. Swagger UI: <http://127.0.0.1:8000/docs>.

| Compose service | Entrypoint | Responsibility |
|---|---|---|
| `api` | `python -m uvicorn app.main:app --host 0.0.0.0 --port 8000` | Accept requests and atomically save articles with publication intent. |
| `outbox` | `python -m app.workers.outbox_dispatcher` | Read pending delivery records, publish to both brokers and mark confirmed deliveries. |
| `index-worker` | `python -m app.workers.article_index_worker` | Consume RabbitMQ jobs, index Elasticsearch, invalidate Redis, ACK. |
| `audit` | `python -m app.workers.article_event_consumer --role audit` | Process Kafka events for `knowledge-audit-v1`. |
| `analytics` | `python -m app.workers.article_event_consumer --role analytics` | Process Kafka events for `knowledge-analytics-v1`. |

The API's `/health` is a **liveness** check, not evidence that all five external services or background consumers are healthy. `docker compose ps` and the logs must also be inspected. Once containers are running, create a test article using Swagger; check both outbox statuses, Elasticsearch indexing, and the two Kafka-backed MongoDB event collections.

### Common operating commands

```powershell
# Status and recent logs
docker compose ps
docker compose logs --tail=50 outbox
docker compose logs -f audit

# Execute administrative scripts *inside* an existing service
docker compose exec outbox python -m scripts.check_outbox
docker compose exec analytics python -m scripts.show_article_analytics

# Practice process failure and recovery
docker compose stop outbox
# POST an article now: its outbox entries should stay pending.
docker compose start outbox
# It should publish the backlog after it starts.

# Rebuild after changing application code
docker compose up -d --build

# Stop/remove only Compose-managed local application containers
docker compose down
```

Stopping a consumer does not stop the API. Stopping the dispatcher does not erase MongoDB outbox records. Stopping the RabbitMQ **worker** does not make the RabbitMQ broker unavailable: queued work can be consumed after restart. `docker compose down` does not delete the externally hosted database/broker data.

### Linux inspection inside a container

```powershell
docker compose exec api sh
```

Inside the shell:

```sh
pwd                 # expected: /app
ls -la
id                  # should be non-root appuser
python --version
ls app
exit
```

To inspect the Aiven CA from the audit container:

```powershell
docker compose exec audit sh -c "ls -l /run/certs/ca.pem"
```

The application image is intentionally slim: commands such as `bash`, `curl`, or `ps` may be absent. Prefer `sh`, built-in Python checks, and `docker compose logs`. `docker compose restart` restarts the **existing image**; when code or dependency files change, rebuild with `docker compose up -d --build`.

### Docker troubleshooting

| Symptom | Diagnosis |
|---|---|
| Cannot connect to Docker daemon | Start Docker Desktop; confirm Linux containers/WSL2. |
| Port 8000 already allocated | Stop the previous Windows Uvicorn process. |
| Image build fails in `pip install` | Verify `requirements.txt` is complete and Linux-compatible. |
| Kafka certificate missing | Check host `certs/ca.pem`, read-only mount, and container `KAFKA_CA_FILE`. |
| Broker connection errors | Inspect `.env` values, Aiven service state, network access and TLS settings. |
| API health OK but indexing missing | Inspect dispatcher and index-worker logs plus outbox backlog. |
| Code changes not reflected | Rebuild the image; do not only restart the container. |

---

## Continuous integration: GitHub Actions

`.github/workflows/ci.yml` defines an intended Linux CI pipeline for pushes, pull requests, and manual runs:

```text
push / pull request / manual dispatch
               |
               v
      checkout repository
               |
               v
    set up Python 3.13
               |
               v
 install dependencies + Ruff
               |
               v
 compileall + focused Ruff checks
               |
               v
           pytest -v
               |
       +-------+-------+
       |               |
       v               v
    tests fail       tests pass
    stop build           |
                         v
                   Docker build
                   (push: false)
```

**Why:** Manual local tests can be forgotten. The CI test job catches code failures on a clean Linux runner; `needs: test` prevents the image-build job from starting when the tests fail. The Docker job checks that an image can be built but does **not** push it to a registry or deploy it. This is CI and a delivery foundation, **not continuous deployment**.

The workflow should do the following:

- Give its default GitHub token only `contents: read` permission.
- Install dependencies from `requirements.txt` and explicitly install missing developer tools such as `pytest` and `ruff` if they are not in that file.
- Run `python -m compileall -q app scripts tests`.
- Run `ruff check app scripts tests --select E4,E7,E9,F63,F7,F82` as an initial focused correctness gate.
- Run `pytest -v` with fake repositories/brokers; tests should not require live cloud credentials.
- Build an image using Docker Buildx with `push: false`, after the tests pass.

GitHub Action versions and build configuration live in the actual `.github/workflows/ci.yml`; review that file before a public push. A green workflow proves only its configured checks—not production readiness, external-service health, or successful deployment.

### Local checks matching CI

```powershell
python -m compileall -q app scripts tests
ruff check app scripts tests --select E4,E7,E9,F63,F7,F82
pytest -v
docker compose config -q
docker compose build
```

Inspect **GitHub repository → Actions → Backend CI** after pushing. Confirm that both the tests and the Docker-build job actually succeeded; don't claim CI is passing solely because the YAML file exists. No managed-service secrets are needed for fake-based unit tests.

---

## Tests and failure drills

```powershell
pytest -v
python -m compileall -q app scripts tests
ruff check app scripts tests --select E4,E7,E9,F63,F7,F82
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

## Security, scope and verification status

- `.gitignore` and `.dockerignore` serve different purposes: the former excludes files from Git; the latter excludes them from Docker's build context. Both should exclude `.ruff_cache/`, `.env` and `certs/` where appropriate. If a secret was previously committed, simply adding it to `.gitignore` does not remove it from Git history—revoke/rotate it and remediate the repository history separately.
- `.env` is acceptable for this private local development setup, but environment variables are not a production secrets manager. Use platform-managed secrets and least privilege for real deployments.
- There is no Kubernetes deployment, public cloud deployment target, container registry release or automatic production rollout documented here. Those are separate tasks requiring an actual infrastructure choice and verification.
- **Verification to complete in the real repository:** run the local test suite, confirm the Docker build and five-service Compose stack, perform an end-to-end article test, validate the two failure-recovery drills, and inspect a successful GitHub Actions run. This generated README does not imply those checks have already been executed on your Windows machine.

## Design principles

- Build small, but engineer properly: authoritative state, explicit contracts, async I/O, bounded concurrency, targeted timeouts, clear dependency boundaries and testable failure paths.
- Do not claim a message was consumed because it was published.
- Do not acknowledge or commit a message before its required database-side effect completes.
- Expect duplicate publications and delivery; make consumer effects idempotent.
- Keep credentials, CA files and local environment configuration out of version control.
