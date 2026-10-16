# metri-engine

> **Español**: [README.md](README.md) — this file is the faithful English translation.

The data engine of the **metri** platform — a multitenant CMMS/BI SaaS for asset management, work orders, and IoT telemetry. **100% Rust**, deployed as a single AWS ARM64 Lambda (`provided.al2023`) exposing a **gRPC** server (Tonic) behind Function URL + CloudFront + WAF.

It unifies, in a single process:

- **Unified HTAP (`.mbf` — Metri Vortex)** — an in-house binary engine combining immutable EAV transactional datoms (EAVT/AEVT/AVET/VAET, ACID transactions, full-text search, hierarchies) and vectorized columnar analytical execution over S3 (with `/tmp` NVMe caching and in-memory `MbfStorageClient`), with zero SQL compilation or external query engines.
- **Security** — embedded ABAC authorization (Cedar Policy), fail-closed tenant isolation, per-tenant block encryption (AES-256-GCM + HKDF), and Zero-Trust censorship of the master tenant.
- **Quotas** — atomic per-tenant ledger with reservations for AI consumption (Bedrock).

## Architecture at a glance

```
   Clients (metri-app Flutter · system-bff · Metri Q Assistant)
                   │  gRPC (tonic · grpc-web · reflection)
                   ▼
┌──────────────────────────────────────────────────────────────┐
│                metri-engine · ARM64 Lambda                   │
│                                                              │
│  Interceptors: HMAC · session · Cedar (fail-closed)          │
│        │                                                     │
│  ┌─────▼──────┐  ┌──────────┐  ┌──────────────────────────┐  │
│  │  janus     │  │  aegis   │  │  eav + mbf               │  │
│  │  read path │─▶│ executor │─▶│  Metri Vortex (.mbf)     │  │
│  └────────────┘  └──────────┘  └──────────────────────────┘  │
│  ┌────────────┐  ┌──────────┐  ┌──────────────────────────┐  │
│  │  iop       │  │  codice  │  │  quota                   │  │
│  │ write path │  │ schemas  │  │  ledger + AI reservations│  │
│  └────────────┘  └──────────┘  └──────────────────────────┘  │
│        │  eda — outbox · faults (EventBridge / SQS)          │
└────────┼─────────────────────────────────────────────────────┘
         ▼
   S3 (.mbf storage + CSV exports) · SQS FIFO (outbox + DLQ) · EventBridge
```

| Module | Role |
|---|---|
| `mbf` | Unified **Metri Vortex (`.mbf`)** HTAP binary engine: columnar layout, per-tenant encryption, S3 CAS (`ETag`) + `/tmp` NVMe cache and `MbfStorageClient` |
| `janus` | Read path: compiles queries into a FlatBuffers AST IR (zero-copy), aggregations, multi-series, FTS |
| `janus_router` | Write path: transactional routing to `.mbf`, sagas (`scheduled_job`), monotonic ULID generation |
| `aegis` | `.mbf`/OLTP executors over AST IR + sandboxed formula engine |
| `eav` | Immutable datom engine over `.mbf`: `types`, `reader`, `writer`, and `fts` |
| `codice` | SSOT registry of the 60 JSON models (`config/models/`) — coercion, validation, and sequences |
| `cedar` | ABAC authorization (Cedar Policy), principal cache, system security rules |
| `quota` | Atomic per-tenant quotas: authoritative ledger, AI reservations, sweeper |
| `iop` | Step-based ingest pipeline: Cedar → Quota → Janus |
| `eda` | Event-driven architecture: outbox (`moira`), fault detection (`sherlog`), routing |
| `grpc` | gRPC/Tonic-Web server (`MetriService`, `MoiraRoutingService`, `QuotaService`, `AgentConfigService`) |
| `temporal` | Timezone-aware time primitives (windows, calendar shifts) |
| `domain` | Canonical errors, 1:1 TOML error catalog, domain events, protocols |
| `application` | Application ports (DIP) — traits bounded by consumer needs |
| `infrastructure` | AWS SDK adapters (S3, SQS, EventBridge), HMAC sessions, and audit |
| `otel` | OpenTelemetry traces (OTLP) |

## gRPC API

The contract lives in [`proto/metri.proto`](proto/metri.proto) and [`proto/eda.proto`](proto/eda.proto). Main service `metri.MetriService` (plus `MoiraRoutingService`, `QuotaService`, and `AgentConfigService`):

| RPC | Description |
|---|---|
| `Discovery` | Schemas and metadata for UIs and AI agents |
| `Explore` | Autocompletions and value domains |
| `Query` | Transactional and analytical query over `.mbf` with server-side streaming |
| `ListEntities` | Census of ids per type, with a hard cap and explicit `truncated` |
| `Transact` | ACID transactional writes into the `.mbf` engine |
| `CompositeTransact` | Atomic composite writes (root entity + child entities in a single TX) |
| `BulkIngest` | Bulk ingestion and IoT telemetry |
| `ListSequenceConfigs` / `UpsertSequenceConfig` | Per-tenant auto-increment sequence management |
| `MatchRoutingRulesBatch` | EDA routing-rule evaluation (canonical in `MoiraRoutingService`) |

`tonic-reflection` is enabled, so the server is explorable with `grpcurl` without importing the protos:

```bash
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext localhost:9090 describe metri.MetriService
```

## Requirements

- Rust stable + [`cargo-lambda`](https://www.cargo-lambda.info/) (for the Lambda build)
- `flatc` `25.12.19` and `protoc` (FlatBuffers and gRPC schema compilation)
- AWS SAM CLI (to deploy)
- `grpcurl` (optional smoke tests)

## Local quickstart

The engine runs standalone locally using in-memory / filesystem `.mbf` storage without requiring external containers:

```bash
make run      # starts the gRPC server on localhost:9090 (stub mode without AWS)
make smoke    # gRPC smoke test with grpcurl
```

## Tests and CI Verification

```bash
make test               # full test suite (unit + in-memory E2E integration tests)
make test-integration   # runs integration test binaries in tests/
make ci                 # local CI mirror: fmt, reference, check_docs, Result pattern, cargo doc, clippy, and tests
```

## Repository structure

```
src/
  mbf/            unified Metri Vortex (.mbf) HTAP binary engine
  grpc/           gRPC server, interceptors, service implementations
  domain/         protocols (traits) and errors — zero infrastructure coupling
  janus/          query compiler → AST IR (FlatBuffers)
  janus_router/   write routing (.mbf), sagas, and ULIDs
  aegis/          .mbf/OLTP executors + formula engine
  eav/            immutable datom engine (writer, reader, FTS, types)
  codice/         model registry, coercion, and sequences
  cedar/          ABAC authorization
  quota/          quotas, ledger, reservations
  iop/            ingest pipeline
  eda/            domain events (Moira, Sherlog)
  temporal/       time primitives
  infrastructure/ AWS SDK adapters (S3, SQS, EventBridge) and sessions
  otel/           traces
config/
  models/         60 JSON models — source of truth for schemas
  errors/         canonical error catalog (TOML) — validated fail-fast at boot
  prompts/        LLM prompts for Metri Q Assistant
  locales/        i18n (es / en)
proto/            gRPC contracts — single source of the API
scripts/          reference generation, CI guards, and operational utilities
tests/            Rust e2e and integration suites
template.yaml     full SAM stack (Lambda, CloudFront, Route53, WAFv2, KMS, S3, SecretsManager, SQS)
```

## Configuration

Main environment variables read by the binary (see [`.env.example`](.env.example); boot fails fast if anything critical is missing in production):

| Variable | Default | Description |
|---|---|---|
| `GRPC_PORT` | `9090` | gRPC server port |
| `ENVIRONMENT` | `development` | `production`/`prod`/`staging` enables the security fail-fast |
| `HMAC_SECRET` | — | HMAC signing secret; mandatory and strong in production |
| `MASTER_TENANT_ID` | `system` | Master tenant identifier |
| `EAV_TABLE_NAME` | `metri-mbf-local` | Logical identifier for the `.mbf` storage namespace |
| `MBF_S3_BUCKET` | — | S3 bucket for `.mbf` persistence (if omitted or `S3_MODE=stub`, runs in memory) |
| `MBF_S3_PREFIX` | `metri-engine` | Object key prefix for `.mbf` files in S3 |
| `MBF_CACHE_DIR` | `/tmp/metri-mbf-cache` | Local NVMe directory for `ETag`-revalidated `.mbf` block cache |
| `AWS_S3_EXPORT_BUCKET` | `metri-engine` | S3 bucket for asynchronous query export files |
| `CODICE_MODELS_DIR` | `config/models` | Códice JSON models directory |
| `ERROR_CATALOG_PATH` | `config/errors/error_catalog.toml` | Path to canonical error catalog |
| `PROMPTS_DIR` | `config/prompts` | AI prompt templates directory |
| `EVENTBRIDGE_MODE` · `S3_MODE` · `SQS_MODE` | — (`stub` locally) | `stub`/real switches per AWS service — boots fully without AWS |
| `OUTBOX_QUEUE_URL` | — | SQS FIFO queue URL for the transactional outbox (`moira`) |
| `EVENTBRIDGE_BUS_NAME` / `FAULT_BUS_NAME` | `metri-events` / `metri-faults` | EventBridge buses for domain events and faults (`sherlog`) |

## Deployment

Production deploys run on **GitHub Actions** (`.github/workflows/deploy-production.yml`): every push to `main` builds, tests, and deploys via SAM after the `production` environment approval. Credentials come from OIDC — no access keys in secrets. Details, one-time setup, and rollback in [`docs/guides/deployment.md`](docs/guides/deployment.md).

Emergency manual deploy:

```bash
make deploy   # cargo build --release + sam build + sam deploy (metri-dev profile, us-east-1)
```

The SAM stack ([`template.yaml`](template.yaml)) creates: an ARM64 Lambda function with Function URL (`RESPONSE_STREAM`), a CloudFront distribution with custom domain, Route53, and WAFv2, a KMS key for per-tenant encryption, an HMAC secret in Secrets Manager, the unified S3 bucket (`.mbf` + exports), and two SQS FIFO queues (outbox + DLQ). Parameters are pinned in `samconfig.toml`.

## Status and documentation

The engine is in production. Architecture documentation lives under [`docs/`](docs/README.md) — the navigation index is [`docs/README.md`](docs/README.md); auto-generated reference docs for models, error codes, and gRPC API live in [`docs/reference/`](docs/reference/).
