# metri-engine

> **English**: [README.en.md](README.en.md) · **Índice de documentación**: [docs/README.md](docs/README.md)

Motor de datos de la plataforma **metri** — CMMS/BI SaaS multitenant para gestión de activos, ordenes de trabajos, etc... .**100% Rust**, desplegado como una única AWS Lambda ARM64 (`provided.al2023`) que expone un servidor **gRPC** (Tonic) detrás de Function URL + CloudFront + WAF.

Combina en un solo proceso:

- **OLTP** — motor EAV inmutable propio sobre DynamoDB (single-table, datoms EAVT/AEVT/AVET/VAET) con transacciones ACID, full-text search y jerarquías.
- **OLAP** — data lake Parquet en S3 + Glue Data Catalog + Athena, con `tenant_id` como primera partición obligatoria.
- **Seguridad** — autorización ABAC embebida (Cedar Policy), aislamiento por tenant fail-closed y censura Zero-Trust del tenant maestro.
- **Cuotas** — ledger atómico por tenant con reservas para consumo de IA (Bedrock).

## Arquitectura en un vistazo

```
   Clientes (metri-app Flutter · system-bff · Metri Q Assistant)
                   │  gRPC (tonic · grpc-web · reflection)
                   ▼
┌──────────────────────────────────────────────────────────────┐
│                metri-engine · Lambda ARM64                   │
│                                                              │
│  Interceptors: HMAC · sesión · Cedar (fail-closed)           │
│        │                                                     │
│  ┌─────▼──────┐  ┌──────────┐  ┌──────────────────────────┐  │
│  │  janus     │  │  aegis   │  │  eav                     │  │
│  │  read path │─▶│ compiler │─▶│  DynamoDB single-table   │  │
│  └────────────┘  └──────────┘  └──────────────────────────┘  │
│  ┌────────────┐  ┌──────────┐  ┌──────────────────────────┐  │
│  │  iop       │  │  codice  │  │  quota                   │  │
│  │ write path │  │ esquemas │  │  ledger + reservas IA    │  │
│  └────────────┘  └──────────┘  └──────────────────────────┘  │
│        │  eda — outbox · faults (EventBridge / SQS)          │
└────────┼─────────────────────────────────────────────────────┘
         ▼
   S3 (Parquet) → Glue → Athena · Kinesis Firehose · EventBridge
```

| Módulo | Rol |
|---|---|
| `janus` | Read path: compila queries a un AST IR en FlatBuffers (zero-copy), agregaciones, multi-series, FTS |
| `janus_router` | Write path: routing OLTP/OLAP, sagas, particionado, generación de ULIDs |
| `aegis` | Compilador SQL para Athena (via `sea-query`, inyección imposible por construcción) + executor OLTP + motor de fórmulas |
| `eav` | Motor de almacenamiento EAV inmutable: datoms, transacciones, FTS, cursors, jerarquías |
| `codice` | Registro SSOT de los ~60 modelos JSON (`config/models/`) — coerción y validación |
| `cedar` | Autorización ABAC (Cedar Policy 3), cache de principals, reglas de seguridad del sistema |
| `quota` | Cuotas atómicas por tenant: ledger con autoridad, reservas IA, sweeper |
| `iop` | Pipeline de ingesta por pasos: Cedar → Quota → Janus |
| `eda` | Arquitectura orientada a eventos: outbox, fault detection, routing |
| `temporal` | Primitivas temporales timezone-aware (ventanas, shifts de calendario) |
| `domain` | Errores canónicos, catálogo TOML 1:1, eventos de dominio, protocolos |
| `application` | Puertos de aplicación (DIP) — traits delimitados por la necesidad del consumidor |
| `infrastructure` | Adaptadores AWS SDK (DynamoDB, S3, Athena, Kinesis, SQS, EventBridge, Glue) |
| `otel` | Trazas OpenTelemetry (OTLP) |

## API gRPC

El contrato vive en [`proto/metri.proto`](proto/metri.proto) — la única fuente de verdad del API. Servicio principal `metri.MetriService` (además de `QuotaService` y `AgentConfigService`):

| RPC | Descripción |
|---|---|
| `Discovery` | Esquemas y metadata para UIs y agentes IA |
| `Explore` | Autocompletados y dominios de valores |
| `Query` | Consulta OLTP/OLAP con streaming server-side |
| `ListEntities` | Censo de ids por tipo, con tope duro y `truncated` explícito |
| `Transact` | Escritura transaccional en el motor EAV |
| `BulkIngest` | Ingesta masiva y telemetría IoT |
| `MatchRoutingRulesBatch` | Evaluación de reglas de routing EDA |

`tonic-reflection` está habilitado, así que el server es explorable con grpcurl sin importar los protos:

```bash
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext localhost:9090 describe metri.MetriService
```

## Requisitos

- Rust stable + [`cargo-lambda`](https://www.cargo-lambda.info/) (para el build de Lambda)
- Docker + Docker Compose
- AWS SAM CLI (para desplegar)
- `grpcurl` (smoke tests)

## Quickstart local

```bash
make infra    # DynamoDB Local (docker compose)
make dev      # engine con hot-reload (cargo-watch) — gRPC en localhost:9090
make seed     # recrea las tablas locales — datos demo: scripts/dev/seed_base.py con el engine arriba
make smoke    # smoke test gRPC con grpcurl
```

Alternativa sin hot-reload:

```bash
make engine   # infra + servidor compilado corriendo en background
```

## Tests

```bash
make test               # suite unitaria (cargo test)
make test-integration   # tests #[ignore] de integración contra DynamoDB Local
```

Los tests de integración se ejecutan con variables apuntando a la infra local (`DYNAMODB_ENDPOINT=http://localhost:8000`, tablas `metri-*-local`); el Makefile ya las define.

## Estructura del repositorio

```
src/
  grpc/           servidor gRPC, interceptors, implementación del servicio
  domain/         protocolos (traits) y errores — cero dependencia de infraestructura
  janus/          compilador de queries → AST IR (FlatBuffers)
  janus_router/   routing de escritura, sagas, proyecciones
  aegis/          compilador SQL Athena + executor OLTP + motor de fórmulas
  eav/            motor de almacenamiento EAV (writer, reader, FTS, cursors)
  codice/         registro de modelos y coerción
  cedar/          autorización ABAC
  quota/          cuotas, ledger, reservas
  iop/            pipeline de ingesta
  eda/            eventos de dominio (Moira, Sherlog)
  temporal/       primitivas de tiempo
  infrastructure/ adaptadores AWS SDK
  otel/           trazas
config/
  models/         ~60 modelos JSON — fuente de verdad de los esquemas
  errors/         catálogo canónico de errores (TOML) — validado fail-fast en el arranque
  prompts/        prompts LLM de Metri Q Assistant
  locales/        i18n (es / en)
proto/            contratos gRPC — fuente única del API
scripts/          seed, debugging y operación
tests/            e2e, reliability y unit (Rust + Python)
template.yaml     stack SAM completo (Lambda, CloudFront, WAF, KMS, S3, Glue, DynamoDB, SQS)
```

## Configuración

Variables de entorno principales (el arranque falla rápido si falta algo crítico en producción):

| Variable | Default | Descripción |
|---|---|---|
| `GRPC_PORT` | `9090` | Puerto del servidor gRPC |
| `ENVIRONMENT` | `development` | `production`/`prod`/`staging` activa el fail-fast de seguridad |
| `HMAC_SECRET` | — | Secreto de firma HMAC; obligatorio y fuerte en producción |
| `MASTER_TENANT_ID` | `system` | Identificador del tenant maestro |
| `DYNAMODB_ENDPOINT` | AWS | Endpoint DynamoDB (local: `http://localhost:8000`) |
| `EAV_TABLE_NAME` | — | Tabla del motor EAV |
| `QUOTA_TABLE` | — | Tabla del ledger de cuotas |
| `CODICE_MODELS_DIR` | `config/models` | Directorio de modelos JSON |
| `ERROR_CATALOG_PATH` | `config/errors/…` | Catálogo canónico de errores |
| `ATHENA_MODE` · `KINESIS_MODE` · `EVENTBRIDGE_MODE` · `S3_MODE` · `SQS_MODE` | `stub` | Interruptores stub/real por servicio AWS — el engine levanta completo sin AWS |
| `AWS_S3_LAKE_BUCKET` / `AWS_S3_EXPORT_BUCKET` | — | Buckets del data lake y de exports CSV |
| `GLUE_DATABASE_NAME` · `ATHENA_WORKGROUP` | — | Recursos del canal OLAP |

## Despliegue

El deploy a producción corre por **GitHub Actions** (`.github/workflows/deploy-production.yml`): cada push a `main` compila, prueba y despliega vía SAM tras la aprobación del environment `production`. Credenciales por OIDC — sin access keys en secretos. Detalles, puesta en marcha y rollback en [`docs/guides/deployment.md`](docs/guides/deployment.md) (en inglés).

Deploy manual de emergencia:

```bash
make deploy   # cargo build --release + sam build + sam deploy (perfil metri-dev, us-east-1)
```

El stack SAM crea: función Lambda ARM64 con Function URL, distribución CloudFront con dominio propio y WAFv2, clave KMS, secreto HMAC en Secrets Manager, bucket S3 del data lake, base de datos Glue, dos tablas DynamoDB (EAV y esquemas) y dos colas SQS (outbox + DLQ). Los parámetros están fijados en `samconfig.toml`.

## Estado y documentación

El motor está en producción. El índice navegable de toda la documentación está en [`docs/README.md`](docs/README.md); las reglas de documentación (idioma, títulos, cobertura por archivo) en [`docs/architecture/PLAN_DOCUMENTACION.md`](docs/architecture/PLAN_DOCUMENTACION.md); el plan vivo de rediseño de la arquitectura, con el estado verificado del código y el roadmap, en [`docs/architecture/PLAN_REFACTORIZACION.md`](docs/architecture/PLAN_REFACTORIZACION.md).
