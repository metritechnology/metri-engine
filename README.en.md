# metri-engine

> **Español**: [README.md](README.md) — this file is the faithful English translation.

The data engine of the **metri** platform — a multitenant CMMS/BI SaaS for asset and work-order management. **100% Rust**, deployed as a single AWS ARM64 Lambda (`provided.al2023`) exposing a **gRPC** server (Tonic) behind Function URL + CloudFront + WAF.

It combines, in one process:

- **OLTP** — an in-house immutable EAV engine on DynamoDB (single-table, EAVT/AEVT/AVET/VAET datoms) with ACID transactions, full-text search and hierarchies.
- **OLAP** — Parquet data lake on S3 + Glue Data Catalog + Athena, with `tenant_id` as the mandatory first partition.
- **Security** — embedded ABAC authorization (Cedar Policy), fail-closed tenant isolation and Zero-Trust censorship of the master tenant.
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
│  │  janus     │  │  aegis   │  │  eav                     │  │
│  │  read path │─▶│ compiler │─▶│  DynamoDB single-table   │  │
│  └────────────┘  └──────────┘  └──────────────────────────┘  │
│  ┌────────────┐  ┌──────────┐  ┌──────────────────────────┐  │
│  │  iop       │  │  codice  │  │  quota                   │  │
│  │ write path │  │ schemas  │  │  ledger + AI reservations│  │
│  └────────────┘  └──────────┘  └──────────────────────────┘  │
│        │  eda — outbox · faults (EventBridge / SQS)          │
└────────┼─────────────────────────────────────────────────────┘
         ▼
   S3 (Parquet) → Glue → Athena · Kinesis Firehose · EventBridge
```

| Module | Role |
|---|---|
| `janus` | Read path: compiles queries into a FlatBuffers AST IR (zero-copy), aggregations, multi-series, FTS |
| `janus_router` | Write path: OLTP/OLAP routing, sagas, partitioning, ULID generation |
| `aegis` | SQL compiler for Athena (via `sea-query`, injection impossible by construction) + OLTP executor + formula engine |
| `eav` | Immutable EAV storage engine: datoms, transactions, FTS, cursors, hierarchies |
| `codice` | SSOT registry of the ~60 JSON models (`config/models/`) — coercion and validation |
| `cedar` | ABAC authorization (Cedar Policy 3), principal cache, system security rules |
| `quota` | Atomic per-tenant quotas: authoritative ledger, AI reservations, sweeper |
| `iop` | Step-based ingest pipeline: Cedar → Quota → Janus |
| `eda` | Event-driven architecture: outbox, fault detection, routing |
| `temporal` | Timezone-aware time primitives (windows, calendar shifts) |
| `domain` | Canonical errors, TOML error catalog, domain events, protocols |
| `application` | Application ports (DIP) — traits bounded by consumer needs |
| `infrastructure` | AWS SDK adapters (DynamoDB, S3, Athena, Kinesis, SQS, EventBridge, Glue) |
| `otel` | OpenTelemetry traces (OTLP) |

## gRPC API

The contract lives in [`proto/metri.proto`](../proto/metri.proto) — the single source of truth for the API. Main service `metri.MetriService` (plus `QuotaService` and `AgentConfigService`):

| RPC | Description |
|---|---|
| `Discovery` | Schemas and metadata for UIs and AI agents |
| `Explore` | Autocompletions and value domains |
| `Query` | OLTP/OLAP query with server-side streaming |
| `ListEntities` | Census of ids per type, with a hard cap and explicit `truncated` |
| `Transact` | Transactional writes into the EAV engine |
| `BulkIngest` | Bulk ingestion and IoT telemetry |
| `MatchRoutingRulesBatch` | EDA routing-rule evaluation |

`tonic-reflection` is enabled, so the server is explorable with grpcurl without importing the protos:

```bash
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext localhost:9090 describe metri.MetriService
```

## Requirements

- Rust stable + [`cargo-lambda`](https://www.cargo-lambda.info/) (for the Lambda build)
- Docker + Docker Compose
- AWS SAM CLI (to deploy)
- `grpcurl` (smoke tests)

## Local quickstart

```bash
make infra    # DynamoDB Local (docker compose)
make dev      # engine with hot-reload (cargo-watch) — gRPC on localhost:9090
make seed     # recreates local tables — demo data: scripts/dev/seed_base.py with the engine up
make smoke    # gRPC smoke test with grpcurl
```

Alternative without hot-reload:

```bash
make engine   # infra + compiled server running in the background
```

## Tests

```bash
make test               # unit suite (cargo test)
make test-integration   # #[ignore] integration tests against DynamoDB Local
```

Integration tests run with variables pointing at the local infra (`DYNAMODB_ENDPOINT=http://localhost:8000`, tables `metri-*-local`); the Makefile already defines them.

## Repository structure

```
src/
  grpc/           gRPC server, interceptors, service implementation
  domain/         protocols (traits) and errors — zero infrastructure coupling
  janus/          query compiler → AST IR (FlatBuffers)
  janus_router/   write routing, sagas, projections
  aegis/          Athena SQL compiler + OLTP executor + formula engine
  eav/            EAV storage engine (writer, reader, FTS, cursors)
  codice/         model registry and coercion
  cedar/          ABAC authorization
  quota/          quotas, ledger, reservations
  iop/            ingest pipeline
  eda/            domain events (Moira, Sherlog)
  temporal/       time primitives
  infrastructure/ AWS SDK adapters
  otel/           traces
config/
  models/         ~60 JSON models — source of truth for schemas
  errors/         canonical error catalog (TOML) — validated fail-fast at boot
  prompts/        LLM prompts for Metri Q Assistant
  locales/        i18n (es / en)
proto/            gRPC contracts — single source of the API
scripts/          seed, debugging and operations
tests/            e2e, reliability and unit (Rust + Python)
template.yaml     full SAM stack (Lambda, CloudFront, WAF, KMS, S3, Glue, DynamoDB, SQS)
```

## Configuration

Main environment variables (boot fails fast if anything critical is missing in production):

| Variable | Default | Description |
|---|---|---|
| `GRPC_PORT` | `9090` | gRPC server port |
| `ENVIRONMENT` | `development` | `production`/`prod`/`staging` enables the security fail-fast |
| `HMAC_SECRET` | — | HMAC signing secret; mandatory and strong in production |
| `MASTER_TENANT_ID` | `system` | Master tenant identifier |
| `DYNAMODB_ENDPOINT` | AWS | DynamoDB endpoint (local: `http://localhost:8000`) |
| `EAV_TABLE_NAME` | — | EAV engine table |
| `QUOTA_TABLE` | — | Quota ledger table |
| `CODICE_MODELS_DIR` | `config/models` | JSON models directory |
| `ERROR_CATALOG_PATH` | `config/errors/…` | Canonical error catalog |
| `ATHENA_MODE` · `KINESIS_MODE` · `EVENTBRIDGE_MODE` · `S3_MODE` · `SQS_MODE` | `stub` | stub/real switches per AWS service — the engine boots fully without AWS |
| `AWS_S3_LAKE_BUCKET` / `AWS_S3_EXPORT_BUCKET` | — | Data lake and CSV export buckets |
| `GLUE_DATABASE_NAME` · `ATHENA_WORKGROUP` | — | OLAP channel resources |

## Deployment

Production deploys run on **GitHub Actions** (`.github/workflows/deploy-production.yml`): every push to `main` builds, tests and deploys via SAM after the `production` environment approval. Credentials come from OIDC — no access keys in secrets. Details, one-time setup and rollback in [`docs/guides/deployment.md`](docs/guides/deployment.md).

Emergency manual deploy:

```bash
make deploy   # cargo build --release + sam build + sam deploy (metri-dev profile, us-east-1)
```

The SAM stack creates: an ARM64 Lambda function with Function URL, a CloudFront distribution with a custom domain and WAFv2, a KMS key, an HMAC secret in Secrets Manager, the data-lake S3 bucket, a Glue database, two DynamoDB tables (EAV and schemas) and two SQS queues (outbox + DLQ). Parameters are pinned in `samconfig.toml`.

## Status and documentation

The engine is in production. Architecture documentation lives under [`docs/`](docs/README.md) — the navigation index is [`docs/README.md`](docs/README.md); documentation rules are in [`docs/architecture/PLAN_DOCUMENTACION.md`](docs/architecture/PLAN_DOCUMENTACION.md).
