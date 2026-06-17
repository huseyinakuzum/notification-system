# Notification System

Event-driven notification platform in Go. The full lifecycle of a notification
(scheduling, queueing, delivery with retries, dead-lettering) runs off a single
source-of-truth table in Postgres. Change data capture streams queued work into
Kafka, where delivery workers drain it asynchronously and scale out.

## Architecture

```mermaid
flowchart TD
    client([client]) -->|POST /notifications| api[api]
    api -->|insert status=scheduled| db[(postgres<br/>notifications)]
    scheduler[scheduler] -->|flip scheduled to queued when due| db
    db -->|WAL, publication filtered to status=queued| cdc[cdc]
    cdc -->|route by priority| topics{{kafka<br/>delivery.high / normal / low}}
    topics --> delivery[delivery]
    delivery -->|send| provider[mock-provider<br/>sms / email / push]
    delivery -->|attempts exhausted or fatal| dlq{{kafka<br/>delivery.dlq}}
    delivery -->|retry: back to scheduled with next_attempt_at| db
    api -->|GET /notifications/:id| client
```

### Components

| Service | Responsibility |
|---------|----------------|
| `api` | REST ingest and read API (chi). Validates, enforces idempotency, writes rows. |
| `scheduler` | Due-engine. Claims rows whose gate (`next_attempt_at` or `scheduled_at`) has passed and flips `scheduled` to `queued`. Handles both first send and retries. |
| `cdc` | Reads the Postgres WAL through a row-filtered publication (`status='queued'`) and produces to the priority topics. |
| `delivery` | Drains the priority lanes with anti-starvation, token-bucket rate limiting per channel, backoff with jitter, and a DLQ when attempts run out. Attempt state lives in the DB. |
| `mock-provider` | Stands in for downstream sms/email/push providers. |

### Data model

One `notifications` table carries the whole lifecycle (`scheduled`, `queued`,
`processing`, `sent`, `failed`, `cancelled`) alongside delivery bookkeeping:
attempts, next_attempt_at, last_error, provider_message_id, sent_at. A
`templates` table holds reusable channel bodies. The publication is row-filtered
to `status='queued'` with `REPLICA IDENTITY FULL`, so only deliverable work
crosses into Kafka and the `scheduled` to `queued` flip is the single event that
triggers delivery. No separate outbox table.

## Quick start

Needs `podman` and `podman compose`. The stack uses no Docker-specific features,
so `docker compose` works too.

```bash
make up        # build images and start the stack
make migrate   # apply database migrations
make topics    # create Kafka topics (auto-create is off)
```

Post a notification:

```bash
curl -sS -X POST localhost:8080/notifications \
  -H 'Content-Type: application/json' \
  -d '[{"recipient":"me@example.com","channel":"email","content":"hello","priority":"high"}]'
```

Read it back:

```bash
curl -sS localhost:8080/notifications/<id>
```

Tear down (drops volumes):

```bash
make down
```

## API

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/notifications` | Create one or many (batch) notifications. |
| GET | `/notifications` | List with filters (`status`, `channel`, `from`, `to`, `limit`). |
| GET | `/notifications/{id}` | Fetch one with its delivery state. |
| GET | `/notifications/batch/{id}` | Fetch a batch with per-status counts. |
| POST | `/templates` | Create a template. |
| GET | `/templates/{name}` | Fetch a template. |
| GET | `/swagger/*` | OpenAPI UI. |

Channels: `sms`, `email`, `push`. Priorities: `high`, `normal`, `low`.
Dedup is automatic: the server derives an idempotency key by hashing
`recipient + channel + content + priority + scheduled_at`, so resubmitting an
identical payload collapses to the same row. `scheduled_at` (RFC3339) defers
delivery to the scheduler.

## Observability

Every service runs an operational HTTP server on `:9090`:

- `GET /healthz` liveness
- `GET /readyz` readiness (api, scheduler, delivery ping the DB)
- `GET /metrics` Prometheus metrics (namespace `nsys`)

Bundled UIs on the host:

| Tool | URL | Notes |
|------|-----|-------|
| Grafana | http://localhost:3000 | Anonymous admin; the "Notification System" dashboard is auto-provisioned. |
| Prometheus | http://localhost:9091 | Scrapes all four services every 5s. Host 9091 maps to container 9090. |
| Jaeger | http://localhost:16686 | Traces. |
| Kafka UI | http://localhost:8090 | Topic and consumer inspection. |

Tracing is OpenTelemetry over OTLP/gRPC into a collector that forwards to
Jaeger. Pick the exporter with `OTEL_EXPORTER` (`otlp`, `stdout`, `noop`).

## Testing

```bash
make test              # unit tests with the race detector
make test-integration  # build tag integration; needs Postgres (DB_DSN), migrate first
make e2e               # build tag e2e; drives the running stack
```

`test/e2e/pipeline_test.go` waits for the API, posts a notification, and polls
until it reaches `sent`, covering api to db to cdc to kafka to delivery to
mock-provider. Override the target with `API_BASE_URL`. Against a live stack:

```bash
make up && make migrate && make topics
make e2e
```

## CI

`.github/workflows/ci.yml` runs two jobs on push and PR. The build-test job runs
`go build`, `go vet`, golangci-lint, and the race-enabled unit tests. The
integration job spins up Postgres, applies migrations with golang-migrate, and
runs the integration suite.

## Configuration

Services read environment variables (see `internal/config/config.go`). Common
ones: `DB_DSN`, `KAFKA_BROKERS`, `HTTP_ADDR` (api, default `:8080`), `OBS_ADDR`
(default `:9090`), `OTEL_EXPORTER`, `OTEL_ENDPOINT`.
