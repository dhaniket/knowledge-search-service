# Knowledge Search Service

An asynchronous knowledge-base backend built with FastAPI, MongoDB Atlas, Elasticsearch, Redis, RabbitMQ, and Apache Kafka (Aiven Free). It demonstrates full-text search, caching, recoverable messaging, independent event consumers, Docker-based local deployment, GitHub Actions CI, and local Kubernetes orchestration.

> **Deployment scope:** local Python, Docker Compose, and a local Kubernetes cluster (kind). Public-cloud hosting, a public IP, HTTPS ingress, and automatic production deployment are deferred. Commands and expected results below are operational documentation, not independently captured test results.

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


## Runtime and deployment overview

The codebase has **five independently managed application processes**. One Docker image is reused with different commands. MongoDB, Elasticsearch, Redis, RabbitMQ and Kafka remain externally managed; neither Compose nor kind creates those services.

```text
                      GitHub push / pull request
                                |
                          GitHub Actions
                   lint -> pytest -> Docker build
                                |
                    versioned application image
                         /             \
                        v               v
               Docker Compose        Local kind
                 five containers     Kubernetes cluster
                                        |
                              Service -> API Pod(s)
                                        |
                 outbox / indexing / audit / analytics Pods
                         \              /
                          managed services
```

The API writes article data and publication intent atomically in MongoDB. The outbox dispatcher independently publishes to RabbitMQ and Kafka; consumers record their own work. Docker and Kubernetes run these processes but **do not replace the application's message durability and idempotency mechanisms**.

### Technology and responsibility map

| Component | Responsibility |
|---|---|
| FastAPI | HTTP contract, input validation, dependency injection |
| MongoDB Atlas (`knowledge_app`) | Authoritative articles and embedded outbox |
| Elasticsearch | Rebuildable full-text index |
| Redis | Disposable search cache with TTL |
| RabbitMQ | Indexing work queue, retries and dead-letter inspection |
| Kafka (Aiven Free) | Retained domain events for independent audit/analytics groups |
| Docker | Reproducible Linux application image |
| Docker Compose | Five-process local integration environment |
| Kubernetes (kind) | Pod scheduling, desired replicas, service discovery and rollouts |
| GitHub Actions | Test/lint/build checks; **no production deployment** |

---

## Docker Compose: local integration

**Why Compose?** Five separate terminal processes are difficult to reproduce. Compose starts all five containers using one image, explicit startup commands and shared runtime configuration; it does not host or migrate external infrastructure.

Prerequisites: Docker Desktop in Linux-container mode; `Dockerfile`, `compose.yaml`, `requirements.txt`, `.dockerignore`; the ignored `.env`; Aiven CA file `certs/ca.pem`; reachable managed services. Stop duplicate Windows-native processes before starting the stack.

| Local file | Purpose |
|---|---|
| `Dockerfile` | Python 3.13 Linux image; installs dependencies; copies `app/` and `scripts/`; runs as a non-root user. |
| `.dockerignore` | Excludes `.env`, `certs/`, `.venv/`, `.git/`, tests and temporary files from image build context. |
| `compose.yaml` | API, outbox, index-worker, audit and analytics services; one shared image. |
| `.env` | Injected into containers at runtime; never added to the image or Git. |
| `certs/ca.pem` | Aiven CA mounted read-only into Kafka-enabled containers. |

The Dockerfile's default process should use a **single valid exec-form instruction**:

```dockerfile
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Uvicorn must listen on `0.0.0.0` **inside** the container for Docker networking to reach it; Compose should publish only `127.0.0.1:8000:8000` on the Windows host. This does not create a publicly reachable API.

### Kafka certificate: why a mount is required

Windows stores `certs/ca.pem` in the project folder. That file does not automatically exist in Linux containers. In the **outbox**, **audit**, and **analytics** services, Compose mounts it read-only and overrides the container's path:

```yaml
environment:
  KAFKA_CA_FILE: /run/certs/ca.pem
volumes:
  - ./certs/ca.pem:/run/certs/ca.pem:ro
```

Check the host file first:

```powershell
Test-Path .\certs\ca.pem
```

Do not disable TLS certificate verification to work around a missing or incorrect CA file.

### Start and verify

In PowerShell, at the repository root:

```powershell
pytest -v
docker compose config -q
docker compose up -d --build
docker compose ps
Invoke-RestMethod http://127.0.0.1:8000/health
```

`config -q` validates without dumping resolved environment values. Avoid publicly sharing ordinary `docker compose config` output because it can reveal secrets.

| Compose service | Startup command | Function |
|---|---|---|
| `api` | `python -m uvicorn app.main:app --host 0.0.0.0 --port 8000` | HTTP and atomic article/outbox insert |
| `outbox` | `python -m app.workers.outbox_dispatcher` | Broker publication and outbox updates |
| `index-worker` | `python -m app.workers.article_index_worker` | RabbitMQ indexing, Redis invalidation and ACK |
| `audit` | `python -m app.workers.article_event_consumer --role audit` | Kafka audit group |
| `analytics` | `python -m app.workers.article_event_consumer --role analytics` | Kafka analytics group |

Swagger: <http://127.0.0.1:8000/docs>. A successful `/health` checks API liveness, **not** broker availability, search readiness or worker progress. Create an article and verify the full outbox/consumer flow separately.

Useful commands:

```powershell
docker compose logs --tail=50 outbox
docker compose logs -f audit
docker compose exec outbox python -m scripts.check_outbox
docker compose exec analytics python -m scripts.show_article_analytics

# Confirm pending publication survives process downtime.
docker compose stop outbox
# Create an article: publication intent should remain pending.
docker compose start outbox

# Source changes are baked into the image; rebuild rather than restart only.
docker compose up -d --build

# Remove only this local Compose stack.
docker compose down
```

`docker compose down` does not delete the externally managed databases or broker data.

### Linux inspection and troubleshooting

```powershell
docker compose exec api sh
```

Inside the container, use `pwd` (expected `/app`), `ls -la`, `id` (non-root runtime user), `python --version`, then `exit`. Minimal images may omit `bash`, `curl` and `ps`; use `sh`, Python and container logs.

| Symptom | Check first |
|---|---|
| Docker daemon unavailable | Docker Desktop, Linux containers and WSL2 |
| Port 8000 unavailable | Stop the previous Uvicorn/Compose instance |
| Build fails on dependencies | Linux compatibility and completeness of `requirements.txt` |
| Kafka CA missing | Host file, bind mount and `KAFKA_CA_FILE` |
| API responds but article is not indexed | Dispatcher backlog, RabbitMQ worker and Elasticsearch logs |
| Source changes not visible | Rebuild the image (`up -d --build`) |

---

## Continuous integration: GitHub Actions

**Why CI?** A local `pytest` run can be forgotten or pass only because of an individual's machine. `.github/workflows/ci.yml` should validate pushes and pull requests on a clean Linux runner:

```text
push / pull request / manual trigger
                   |
             checkout code
                   |
       Python 3.13 + dependencies
                   |
      compileall -> Ruff -> pytest
                   |
            tests pass?
          no /      \ yes
        fail       Docker image build
                   (push: false)
```

The Docker build job should depend on the test job using `needs: test`. Keep token permissions minimal (`contents: read`), run tests using fakes without cloud credentials, and **do not** configure registry publishing or deployment in this workflow until there is a real deployment target.

Local equivalents:

```powershell
python -m compileall -q app scripts tests
ruff check app scripts tests --select E4,E7,E9,F63,F7,F82
pytest -v
docker compose config -q
docker compose build
```

Install `pytest` and `ruff` in CI explicitly if they are not already included in `requirements.txt`. Inspect the actual GitHub Actions run before calling CI green. This setup is **CI and a delivery foundation, not continuous deployment**.

---

## Local Kubernetes (kind)

**Scope:** Kubernetes runs on the developer's computer, backed by Docker. It is **not** public-cloud hosting. The cluster has one local node; multiple API Pods can recover from *Pod* failure but **not** from losing that sole node or the laptop.

### Why Kubernetes when Compose already works?

Compose runs the fixed five-process stack. Kubernetes adds declarative desired state, Pod replacement, service discovery, scaling, readiness-gated endpoints and rollout/rollback mechanics. These are **operational** capabilities, not substitutes for the outbox, broker ACKs, consumer offsets or business-level idempotency.

| Kubernetes object | Why it exists here |
|---|---|
| Namespace `knowledge` | Groups the application's cluster resources |
| Secret `knowledge-env` | Supplies runtime environment configuration without embedding credentials in YAML |
| Secret `aiven-ca` | Mounts the Kafka CA in the correct Linux path |
| Deployment `knowledge-api` | Keeps API Pods available and controls rolling updates |
| Service `knowledge-api` (`ClusterIP`) | Gives ready API Pods a stable internal address |
| Four worker Deployments | Independently restart outbox, indexing, audit and analytics processes |
| EndpointSlices | Describe ready Service backends after scaling |
| Optional HPA | CPU-based autoscaling **only if** a working metrics pipeline has been configured and tested |

The API uses HTTP startup/readiness/liveness probes at `/health`. This route reports **basic process liveness**, not end-to-end dependency readiness. Workers do not expose equivalent progress probes: inspect logs and outbox/consumer progress when diagnosing stalls.

### Files

```text
k8s/
├── kind-config.yaml       # One local control-plane node
├── api.yaml               # API Deployment + internal ClusterIP Service
├── workers.yaml           # Four worker Deployments
└── api-hpa.yaml           # Optional; do not apply unless tested
```

Cloud-only manifests, a container registry release and a public Ingress are **not** prerequisites and are intentionally outside the active deployment scope.

### Create the cluster and load the image

Prerequisites: Docker Desktop running, `kind` and `kubectl` installed, and valid local `.env` and `certs/ca.pem` files.

```powershell
# Stop the equivalent Compose processes to avoid duplicate consumers
# and a conflicting port-forward.
docker compose down

kind create cluster --name knowledge-dev --config k8s/kind-config.yaml --wait 120s
kubectl config current-context
kubectl get nodes

docker build -t knowledge-search:day9-v1 .
kind load docker-image knowledge-search:day9-v1 --name knowledge-dev
```

Expect context `kind-knowledge-dev` and a Ready node. The manifests use `knowledge-search:day9-v1` and `imagePullPolicy: IfNotPresent` because the image is loaded directly into kind; no external registry is required.

### Create runtime configuration

**Why:** Pods have a different filesystem from Windows; they cannot see the local `.env` or CA certificate unless Kubernetes supplies them. The YAML references Secret names, not credential contents.

```powershell
kubectl create namespace knowledge
kubectl create secret generic knowledge-env --from-env-file=.env -n knowledge
kubectl create secret generic aiven-ca --from-file=ca.pem=certs/ca.pem -n knowledge
kubectl get secrets -n knowledge
```

If the namespace or Secrets already exist, inspect them rather than recreating them blindly. Kafka-enabled Pods mount the certificate read-only at `/run/certs/ca.pem` and set `KAFKA_CA_FILE` to that container path.

Kubernetes Secrets are **not automatically a production secrets manager**: authorized cluster users can access them, and production use needs appropriate RBAC/encryption and rotation.

### Deploy and inspect

```powershell
kubectl apply --dry-run=client -f k8s/api.yaml
kubectl apply --dry-run=client -f k8s/workers.yaml

kubectl apply -f k8s/api.yaml
kubectl apply -f k8s/workers.yaml

kubectl get deployments -n knowledge
kubectl get pods -n knowledge
kubectl get service knowledge-api -n knowledge
kubectl rollout status deployment/knowledge-api -n knowledge --timeout=120s
```

Expected: API, outbox, index-worker, audit and analytics each have one available replica. Diagnose failures with `kubectl describe pod -n knowledge <POD_NAME>` and `kubectl logs deployment/knowledge-outbox -n knowledge --tail=100`, not by guessing at application changes.

### Access the API and verify the message flow

Keep a separate terminal open:

```powershell
kubectl port-forward -n knowledge service/knowledge-api 8000:8000
```

Then access <http://127.0.0.1:8000/docs> or check:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

Create a test article and verify all independent effects:

```text
POST article -> MongoDB article + pending outbox (atomic)
                    |
                dispatcher
                  /      \
           RabbitMQ      Kafka
              |          /   \
         Elasticsearch audit analytics
              |
        Redis invalidation
```

```powershell
kubectl exec -n knowledge deployment/knowledge-outbox -- python -m scripts.check_outbox
kubectl logs deployment/knowledge-outbox -n knowledge --tail=50
kubectl logs deployment/knowledge-index-worker -n knowledge --tail=50
kubectl logs deployment/knowledge-audit -n knowledge --tail=50
kubectl logs deployment/knowledge-analytics -n knowledge --tail=50
```

Check the actual article's Elasticsearch document and the two MongoDB event collections. An outbox status of `published` does not by itself prove consumption.

### Horizontal scaling and Service routing

**Problem:** One API Pod may become a throughput bottleneck. **Change:** Increase only the API replica count while keeping the stateful dependencies and background workers unchanged.

```powershell
kubectl scale deployment/knowledge-api --replicas=3 -n knowledge
kubectl rollout status deployment/knowledge-api -n knowledge --timeout=120s
kubectl get pods -n knowledge -l app=knowledge-api -o wide
kubectl get endpointslices -n knowledge -l kubernetes.io/service-name=knowledge-api -o wide
```

Expect three Ready API Pods listed as eligible Service backends. A `ClusterIP` routes traffic to them via the Service; **it does not promise equal round-robin distribution**. `kubectl port-forward service/...` is a diagnostic tunnel and is **not** reliable proof of per-request balancing across every Pod.

For an in-cluster request test:

```powershell
kubectl exec -n knowledge deployment/knowledge-api -- python -c "import urllib.request; [urllib.request.urlopen('http://knowledge-api:8000/docs').read() for _ in range(30)]; print('30 requests sent')"
kubectl logs -n knowledge -l app=knowledge-api --prefix=true --tail=100 --max-log-requests=10
```

The shared MongoDB and Elasticsearch services may become bottlenecks even if API Pods scale successfully.

### Self-healing demonstration

**Problem:** A Pod can crash or disappear. **Change:** Delete one Pod while the Deployment's desired replica count remains three.

```powershell
$pod = kubectl get pods -n knowledge -l app=knowledge-api -o jsonpath='{.items[0].metadata.name}'
kubectl delete pod $pod -n knowledge
kubectl get pods -n knowledge -l app=knowledge-api -w
```

Kubernetes should recreate a replacement until three Pods are Ready. This tests **Pod-level** recovery, not survival of node/laptop failure.

### Rolling update and rollback

**Problem:** Deleting all API instances for a release can interrupt service. **Change:** A RollingUpdate with `maxSurge: 1`, `maxUnavailable: 0` and readiness checks replaces Pods incrementally, subject to available resources.

Build or tag and load a versioned image:

```powershell
docker tag knowledge-search:day9-v1 knowledge-search:day9-v2
kind load docker-image knowledge-search:day9-v2 --name knowledge-dev
kubectl set image deployment/knowledge-api api=knowledge-search:day9-v2 -n knowledge
kubectl rollout status deployment/knowledge-api -n knowledge --timeout=180s
kubectl rollout history deployment/knowledge-api -n knowledge
```

This `v2` tag points to identical application code if produced with `docker tag`; the exercise demonstrates **rollout mechanics, not a new software feature**.

Rollback:

```powershell
kubectl rollout undo deployment/knowledge-api -n knowledge
kubectl rollout status deployment/knowledge-api -n knowledge --timeout=180s
kubectl get deployment knowledge-api -n knowledge -o jsonpath='{.spec.template.spec.containers[0].image}'
```

Expect `knowledge-search:day9-v1`. A failed rollout can stall; Kubernetes does not automatically promise to undo it. For real releases, update the image reference in version-controlled YAML so future `kubectl apply` operations agree with the intended release.

### Autoscaling: optional and not assumed to be enabled

The sample `k8s/api-hpa.yaml`, if present, uses `autoscaling/v2` with a CPU utilization target. CPU-based HPA requires a working metrics API and CPU requests. Check `kubectl top nodes` first. If metrics are unavailable, **do not claim HPA works**. Once HPA manages replicas, avoid competing manual changes or a conflicting fixed `replicas` field in the applied Deployment manifest.

### Restore, troubleshoot and clean up

```powershell
# After scaling experiments:
kubectl scale deployment/knowledge-api --replicas=1 -n knowledge
kubectl get deployments -n knowledge
pytest -v
git diff --check
git ls-files .env certs/
```

| Symptom | First checks |
|---|---|
| Pods `Pending` | `kubectl describe pod`; insufficient CPU/memory, missing mounts |
| `ImagePullBackOff` | Correct local tag, `kind load` target, image pull policy |
| `CrashLoopBackOff` | `kubectl logs --previous` and actual exception |
| API 200 but no indexing | Dispatcher backlog, worker logs, broker state |
| No Service backend | Pod readiness, selector labels and EndpointSlices |
| Rollout stalled | Readiness/startup probe results, capacity and Deployment events |
| Kafka errors | Aiven service state, SASL credentials, CA volume, broker reachability |

The repo can retain Kubernetes YAML without leaving a cluster running. When finished:

```powershell
kind delete cluster --name knowledge-dev
```

This deletes the local cluster and its Secrets, but **not** data held in external MongoDB, Elasticsearch, Redis, RabbitMQ or Aiven Kafka.

### Cloud deployment status

**Deferred intentionally:** no OCI account or OKE/VM resources are required, and no cloud infrastructure is asserted as deployed. The project has **no public IP, public HTTPS endpoint, Ingress/Gateway controller or registry-published release** at this stage. Kubernetes concepts were exercised locally with kind; this is not a claim of multizone/high-availability cloud operation.

A future cloud release needs a chosen budget/provider, registry image, outbound service networking, secure Secrets, authentication and TLS before public exposure. The present API and Swagger documentation must not be presented as a production-hardened public service.

---

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

## Security and verification boundaries

- `.gitignore` excludes local credentials from Git; `.dockerignore` excludes them from Docker build context. Neither removes secrets already committed in Git history; rotate exposed credentials and remediate history if necessary.
- Local `.env` and Kubernetes Secrets are development conveniences, not proof of production-grade secret isolation. Restrict access, avoid printing resolved configuration, and do not commit `.env` or `certs/`.
- `/health` is a liveness check, not a complete dependency-readiness or worker-progress guarantee.
- The Kubernetes checklist was marked complete during project preparation, but this README is not generated from a live checkout, command output or independent cloud verification. Validate `pytest`, runtime behavior and GitHub Actions against the actual repository when publishing results.
- No public cloud deployment, public IP, HTTPS ingress, multi-node resilience, HPA success or automated production rollout is claimed.

## Design principles

- Build small, but engineer properly: authoritative state, explicit contracts, async I/O, bounded concurrency, targeted timeouts, clear dependency boundaries and testable failure paths.
- Do not claim a message was consumed because it was published.
- Do not acknowledge or commit a message before its required database-side effect completes.
- Expect duplicate publications and delivery; make consumer effects idempotent.
- Keep credentials, CA files and local environment configuration out of version control.
