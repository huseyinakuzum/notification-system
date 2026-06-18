# Notification System

Event-driven notification delivery in Go.

[![CI](https://github.com/huseyinakuzum/notification-system/actions/workflows/ci.yml/badge.svg)](https://github.com/huseyinakuzum/notification-system/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](go.mod)
[![Postgres](https://img.shields.io/badge/Postgres-16-4169E1?logo=postgresql&logoColor=white)](docker-compose.yml)
[![Kafka](https://img.shields.io/badge/Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)](docker-compose.yml)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-traces-425CC7?logo=opentelemetry&logoColor=white)](#observability-uis)

This project is a compact notification platform for SMS, email, and push
messages. The API accepts work, Postgres keeps the lifecycle state, Kafka carries
the rows that are ready to send, and delivery workers handle retries, priority,
rate limits, and dead letters.

The design is intentionally plain. A notification lives in one Postgres table
from creation to completion. When its status changes to `queued`, CDC publishes
that row to Kafka. Workers then deliver from the priority topics. Services do not
call one another directly; each service either moves rows through the database
state machine or consumes the topics produced from it.

## Contents

- [Architecture](#architecture)
- [Requirements coverage](#requirements-coverage)
    - [1. Notification management API](#1-notification-management-api)
    - [2. Processing engine](#2-processing-engine)
    - [3. Delivery and retry](#3-delivery-and-retry)
    - [4. Observability](#4-observability)
    - [External provider](#external-provider)
    - [Bonus items](#bonus-items)
- [Quick start](#quick-start)
- [API](#api)
- [Trying the API](#trying-the-api)
- [Observability UIs](#observability-uis)
- [Load testing](#load-testing)
- [Testing](#testing)
- [CI](#ci)
- [Configuration](#configuration)

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

| Service         | What it does                                                                                                                      |
|-----------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `api`           | REST API built with chi. Validates requests, applies idempotency, and writes notifications.                                       |
| `scheduler`     | Finds notifications whose `scheduled_at` or `next_attempt_at` gate has passed, then moves them from `scheduled` to `queued`.      |
| `cdc`           | Reads the Postgres WAL through a row-filtered publication and publishes queued work to Kafka.                                     |
| `delivery`      | Consumes the priority lanes, applies per-channel rate limits, retries with jittered backoff, and sends exhausted work to the DLQ. |
| `mock-provider` | Local replacement for an external SMS/email/push provider. It speaks the same contract used for webhook.site.                     |

### Data model

The `notifications` table carries both lifecycle and delivery state:
`scheduled`, `queued`, `processing`, `sent`, `failed`, and `cancelled`, plus
`attempts`, `next_attempt_at`, `last_error`, `provider_message_id`, and
`sent_at`. A separate `templates` table stores reusable channel bodies.

Postgres publishes only rows where `status = 'queued'`, with
`REPLICA IDENTITY FULL` enabled for the publication. That keeps Kafka focused on
deliverable work: the `scheduled` to `queued` transition is the event that
starts delivery.

## Requirements coverage

### 1. Notification management API

- **Create one or many.** `POST /notifications` accepts a JSON array with one
  item or a batch of up to 1000 items. Larger batches are rejected by
  `MaxBatchSize` in `internal/api/validate.go`. The response includes a
  `batch_id` and the created item IDs.
- **Fetch one notification.** `GET /notifications/{id}` returns the row and, once
  delivery has started, the attempt count, last error, provider message ID, and
  send timestamp.
- **Fetch a batch.** `GET /notifications/batch/{id}` returns the batch items and
  a per-status count map.
- **Cancel pending work.** `DELETE /notifications/{id}` cancels notifications
  that are still `scheduled`. Work that has already moved forward returns `409`.
- **List and filter.** `GET /notifications` supports `status`, `channel`,
  `from`, `to`, `limit`, and `offset`. Time filters use RFC3339. `limit`
  defaults to 50 and is capped at 200.

The full route list is in the [API](#api) table. Swagger is available at
`/swagger/` when the API is running.

### 2. Processing engine

- **Asynchronous workers.** The API never sends directly. It writes rows and
  returns. The scheduler, CDC service, and delivery workers move the work through
  the rest of the pipeline.
- **Rate limiting.** Each channel has its own token-bucket limiter using
  `golang.org/x/time/rate`. The default is 100 messages per second per channel
  with a matching burst (`RATE_LIMIT_PER_CHANNEL`, `DELIVERY_RATE_BURST`).
- **Priority queues.** Kafka has three delivery topics: `delivery.high`,
  `delivery.normal`, and `delivery.low`. CDC routes each row by priority.
  Delivery prefers high priority while still reserving capacity for normal and
  low so they continue to drain under load.
- **Content validation.** Required fields, known channels, known priorities, and
  channel-specific length caps are checked at ingest. Limits are SMS 160, push
  256, and email 10000 characters. Empty priority defaults to `normal`.
- **Idempotency.** The API hashes `recipient + channel + content + priority +
  scheduled_at` with sha256 and stores it behind a unique constraint. Sending the
  same payload again returns the existing row instead of scheduling a duplicate.
  Change any field to create a new notification.

### 3. Delivery and retry

- **Provider contract.** Delivery sends `POST {to, channel, content}` and expects
  `202 {messageId, status, timestamp}`. The target is `PROVIDER_BASE_URL` plus
  `PROVIDER_UUID`.
- **Outcome handling.** `202` is treated as sent. `429`, `5xx`, and transport
  failures are retryable. Other `4xx` responses are fatal.
- **Backoff.** Retryable failures move the row back to `scheduled` with a future
  `next_attempt_at`. Backoff is exponential with jitter, and attempts are stored
  in Postgres so a worker restart does not reset delivery history.
- **Dead letters.** Fatal responses and exhausted retries are written to
  `delivery.dlq`; the notification row ends in `failed`.

### 4. Observability

- **Metrics.** Prometheus uses the `nsys` namespace:
    - `nsys_api_http_requests_total`
    - `nsys_api_http_request_duration_seconds`
    - `nsys_scheduler_flipped_total`
    - `nsys_scheduler_poll_errors_total`
    - `nsys_delivery_attempts_total{outcome,channel,priority}`
    - `nsys_delivery_duration_seconds{channel}`
    - `nsys_delivery_dlq_produced_total{channel}`
    - `nsys_cdc_events_total{op}`
    - `nsys_kafka_reader_*{topic}` for reader lag, offsets, queue depth, fetches,
      errors, rebalances, and timeouts.
- **Structured logs.** Services use `log/slog`. The API reads or creates an
  `X-Correlation-ID`, echoes it on the response, and logs it as
  `correlation_id`.
- **Health checks.** Services expose `/healthz` for liveness and `/readyz` for
  readiness on `:9090`. API, scheduler, and delivery readiness checks include a
  database ping.

### External provider

The default provider target is webhook.site:

```text
PROVIDER_BASE_URL=https://webhook.site/
PROVIDER_UUID=<your-webhook-uuid>
```

For local development, `docker-compose.yml` starts `mock-provider` and points the
delivery service at it. That keeps `make up` self-contained. Set
`PROVIDER_UUID` and `PROVIDER_BASE_URL` if you want to watch real webhook.site
requests arrive.

### Bonus items

| Item                                          | Status                                                          |
|-----------------------------------------------|-----------------------------------------------------------------|
| Failure handling with retry, backoff, and DLQ | Implemented in delivery.                                        |
| Scheduled notifications                       | Implemented with `scheduled_at` and the scheduler gate.         |
| Template system                               | Implemented with `POST /templates` and `GET /templates/{name}`. |
| Distributed tracing                           | Implemented with OpenTelemetry over OTLP/gRPC to Jaeger.        |
| CI/CD pipeline                                | Implemented with GitHub Actions.                                |
| WebSocket real-time updates                   | Not implemented; status is read through the API.                |

## Quick start

The Makefile uses `podman compose`. The stack does not rely on Docker-specific
features, so `docker compose` works as well if that is what you use locally.

```bash
make up        # build images and start the stack
make migrate   # apply database migrations
make topics    # create Kafka topics; auto-create is disabled
```

Create a notification:

```bash
curl -sS -X POST localhost:8080/notifications \
  -H 'Content-Type: application/json' \
  -d '[{"recipient":"me@example.com","channel":"email","content":"hello","priority":"high"}]'
```

Read it back:

```bash
curl -sS localhost:8080/notifications/<id>
```

Request examples live in [`requests/`](requests/), and `scripts/send.sh` can
post sample batches from the shell.

To stop the stack and remove volumes:

```bash
make down
```

## API

| Method | Path                        | Purpose                                                             |
|--------|-----------------------------|---------------------------------------------------------------------|
| POST   | `/notifications`            | Create one notification or a batch of up to 1000.                   |
| GET    | `/notifications`            | List with `status`, `channel`, `from`, `to`, `limit`, and `offset`. |
| GET    | `/notifications/{id}`       | Fetch one notification and its delivery state.                      |
| GET    | `/notifications/batch/{id}` | Fetch a batch with per-status counts.                               |
| DELETE | `/notifications/{id}`       | Cancel a notification that is still scheduled.                      |
| POST   | `/templates`                | Create a template.                                                  |
| GET    | `/templates/{name}`         | Fetch a template.                                                   |
| GET    | `/swagger/*`                | OpenAPI UI.                                                         |

Channels are `sms`, `email`, and `push`. Priorities are `high`, `normal`, and
`low`.

## Trying the API

The [`requests/`](requests/) directory contains `.http` files for the VS Code
REST Client and IntelliJ HTTP client:

| File                          | Covers                                                                               |
|-------------------------------|--------------------------------------------------------------------------------------|
| `requests/notifications.http` | Single create, get by ID, list, filtered list, and cancel.                           |
| `requests/batch.http`         | Batch creation, mixed channels and priorities, validation failure, and batch lookup. |
| `requests/scheduled.http`     | Deferred delivery with `scheduled_at`.                                               |
| `requests/templates.http`     | Template creation and sending through a template.                                    |
| `requests/priority.http`      | Priority-lane behavior.                                                              |

The shell helper posts the same kind of traffic:

```bash
scripts/send.sh 1000
scripts/send.sh 50 sms high
```

## Observability UIs

The local stack includes dashboards and tracing without any manual setup.

| Tool       | URL                            | Notes                                                                              |
|------------|--------------------------------|------------------------------------------------------------------------------------|
| Grafana    | http://localhost:3000          | Anonymous admin. The "Notification System" dashboard is provisioned on boot.       |
| Prometheus | http://localhost:9091          | Scrapes every service every 5 seconds. Host port 9091 maps to container port 9090. |
| Jaeger     | http://localhost:16686         | Traces for API, CDC, delivery, and provider calls.                                 |
| Kafka UI   | http://localhost:8090          | Topics, partitions, offsets, and consumer groups.                                  |
| Swagger    | http://localhost:8080/swagger/ | OpenAPI UI for notifications and templates.                                        |

### Grafana dashboard

The provisioned **Notification System** dashboard opens on the last 30 minutes
and refreshes every 10 seconds. It is organized around the path a notification
takes through the system:

- **Overview:** delivery success rate, delivered messages per second, API request
  rate, API p95 latency, total Kafka lag, and DLQ rate.
- **Throughput:** API requests by status, delivery attempts by outcome, and CDC
  events by operation.
- **Latency:** API latency quantiles and heatmap, plus delivery p95 and p99 by
  channel.
- **Kafka pipeline:** consumer lag, fetch queue depth, consumed messages,
  reader errors, rebalances, timeouts, and committed offsets by lane.
- **CDC and scheduler:** scheduler flips, scheduler poll errors, and cumulative
  CDC events.
- **Errors and DLQ:** retry rate, fatal rate, and DLQ production by channel.

The tracing exporter is selected with `OTEL_EXPORTER`: `otlp`, `stdout`, or
`noop`.

## Load testing

There are two simple ways to put traffic through the pipeline.

**One-shot batches**

```bash
scripts/send.sh 1000           # 1000-item batch
scripts/send.sh 50 sms high    # 50 SMS messages at high priority
```

**Continuous randomized load**

`scripts/loadtest.sh` runs one or more worker loops. It randomizes channel,
priority, single versus batch sends, batch size, and scheduled delivery so the
API, scheduler, CDC stream, Kafka topics, and delivery workers all stay active.

```bash
scripts/loadtest.sh                         # run until Ctrl-C
scripts/loadtest.sh -w 4 -d 120             # 4 workers for 120 seconds
scripts/loadtest.sh -w 8 -m 0 -M 1          # faster pace, useful for building lag
scripts/loadtest.sh -b 1 -B 20 -s 80 -S 50  # batches up to 20, 80% single, 50% scheduled
```

| Flag      | Env                           | Default                 | Meaning                                               |
|-----------|-------------------------------|-------------------------|-------------------------------------------------------|
| `-w`      | `WORKERS`                     | 1                       | Parallel worker loops.                                |
| `-d`      | `DURATION`                    | 0                       | Run time in seconds. `0` means run until interrupted. |
| `-m`/`-M` | `MIN_SLEEP_DS`/`MAX_SLEEP_DS` | 2/7                     | Per-request sleep range in deciseconds.               |
| `-b`/`-B` | `BATCH_MIN`/`BATCH_MAX`       | 2/8                     | Batch size range.                                     |
| `-s`      | `SINGLE_PCT`                  | 60                      | Percentage of requests sent as single notifications.  |
| `-S`      | `SCHED_PCT`                   | 25                      | Percentage of items scheduled for later.              |
| `-c`/`-p` | `CHANNELS`/`PRIORITIES`       | all                     | Comma-separated channel or priority sets.             |
| `-u`      | `API_BASE_URL`                | `http://localhost:8080` | API base URL.                                         |

Each worker prints a count every 50 requests and exits cleanly on Ctrl-C, a
duration cap, or `pkill -f loadtest.sh`.

## Testing

```bash
make test              # unit tests with the race detector
make test-integration  # integration tests; needs Postgres and migrations
make e2e               # end-to-end test against the running stack
```

`test/e2e/pipeline_test.go` waits for the API, posts a notification, and polls
until it reaches `sent`. That covers the full route: API, Postgres, CDC, Kafka,
delivery, and the mock provider.

Against a local stack:

```bash
make up && make migrate && make topics
make e2e
```

Use `API_BASE_URL` to point the e2e test somewhere else.

## CI

`.github/workflows/ci.yml` runs on push and pull request. The build job runs
`go build`, `go vet`, golangci-lint, and race-enabled unit tests. The
integration job starts Postgres, applies migrations with golang-migrate, and
runs the integration suite.

## Configuration

Services read environment variables through `internal/config/config.go`. Common
ones:

| Variable                 | Default                 | Purpose                                               |
|--------------------------|-------------------------|-------------------------------------------------------|
| `DB_DSN`                 | none                    | Postgres connection string.                           |
| `KAFKA_BROKERS`          | none                    | Comma-separated broker list.                          |
| `HTTP_ADDR`              | `:8080`                 | API listen address.                                   |
| `OBS_ADDR`               | `:9090`                 | Health and metrics listen address.                    |
| `RATE_LIMIT_PER_CHANNEL` | `100`                   | Per-channel send rate in messages per second.         |
| `DELIVERY_RATE_BURST`    | `100`                   | Per-channel token-bucket burst.                       |
| `PROVIDER_BASE_URL`      | `https://webhook.site/` | Provider base URL.                                    |
| `PROVIDER_UUID`          | zero UUID               | Provider path segment, usually a webhook.site bucket. |
| `OTEL_EXPORTER`          | `stdout`                | `otlp`, `stdout`, or `noop`.                          |
| `OTEL_ENDPOINT`          | `otel-collector:4317`   | Collector endpoint for OTLP.                          |
