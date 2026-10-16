# metri-engine

> **English**: [README.en.md](README.en.md) · **Índice de documentación**: [docs/README.md](docs/README.md)

Motor de datos de la plataforma **metri** — CMMS/BI SaaS multitenant para gestión de activos, órdenes de trabajo y telemetría IoT. **100 % Rust**, desplegado como una única AWS Lambda ARM64 (`provided.al2023`) que expone un servidor **gRPC** (Tonic) detrás de Function URL + CloudFront + WAF.

Unifica en un solo proceso:

- **HTAP Unificado (`.mbf` — Metri Vortex)** — motor binario propio que combina almacenamiento transaccional de datoms inmutables (EAVT/AEVT/AVET/VAET, transacciones ACID, full-text search, jerarquías) y ejecución analítica columnar vectorizada sobre S3 (con caché NVMe en `/tmp` y `MbfStorageClient` en memoria), sin compilación SQL ni motores externos.
- **Seguridad** — autorización ABAC embebida (Cedar Policy), aislamiento por tenant fail-closed, cifrado de bloques por tenant (AES-256-GCM + HKDF) y censura Zero-Trust del tenant maestro.
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
│  │  janus     │  │  aegis   │  │  eav + mbf               │  │
│  │  read path │─▶│ executor │─▶│  Metri Vortex (.mbf)     │  │
│  └────────────┘  └──────────┘  └──────────────────────────┘  │
│  ┌────────────┐  ┌──────────┐  ┌──────────────────────────┐  │
│  │  iop       │  │  codice  │  │  quota                   │  │
│  │ write path │  │ esquemas │  │  ledger + reservas IA    │  │
│  └────────────┘  └──────────┘  └──────────────────────────┘  │
│        │  eda — outbox · faults (EventBridge / SQS)          │
└────────┼─────────────────────────────────────────────────────┘
         ▼
   S3 (.mbf storage + CSV exports) · SQS FIFO (outbox + DLQ) · EventBridge
```

| Módulo | Rol |
|---|---|
| `mbf` | Motor binario HTAP unificado **Metri Vortex (`.mbf`)**: layout columnar, cifrado por tenant, S3 CAS (`ETag`) + caché NVMe en `/tmp` y `MbfStorageClient` |
| `janus` | Read path: compila queries a un AST IR en FlatBuffers (zero-copy), agregaciones, multi-series, FTS |
| `janus_router` | Write path: routing transaccional hacia `.mbf`, sagas (`scheduled_job`), generación de ULIDs monotónicos |
| `aegis` | Ejecutores `.mbf`/OLTP sobre AST IR + motor de fórmulas sandboxeado |
| `eav` | Motor de datoms inmutables sobre `.mbf`: `types`, `reader`, `writer` y `fts` |
| `codice` | Registro SSOT de los 60 modelos JSON (`config/models/`) — coerción, validación y secuencias |
| `cedar` | Autorización ABAC (Cedar Policy), cache de principals, reglas de seguridad del sistema |
| `quota` | Cuotas atómicas por tenant: ledger con autoridad, reservas IA, sweeper |
| `iop` | Pipeline de ingesta por pasos: Cedar → Quota → Janus |
| `eda` | Arquitectura orientada a eventos: outbox (`moira`), fault detection (`sherlog`), routing |
| `grpc` | Servidor gRPC/Tonic-Web (`MetriService`, `MoiraRoutingService`, `QuotaService`, `AgentConfigService`) |
| `temporal` | Primitivas temporales timezone-aware (ventanas, shifts de calendario) |
| `domain` | Errores canónicos, catálogo TOML 1:1, eventos de dominio, protocolos |
| `application` | Puertos de aplicación (DIP) — traits delimitados por la necesidad del consumidor |
| `infrastructure` | Adaptadores AWS SDK (S3, SQS, EventBridge), sesiones HMAC y auditoría |
| `otel` | Trazas OpenTelemetry (OTLP) |

## API gRPC

El contrato vive en [`proto/metri.proto`](proto/metri.proto) y [`proto/eda.proto`](proto/eda.proto). Servicio principal `metri.MetriService` (además de `MoiraRoutingService`, `QuotaService` y `AgentConfigService`):

| RPC | Descripción |
|---|---|
| `Discovery` | Esquemas y metadata para UIs y agentes IA |
| `Explore` | Autocompletados y dominios de valores |
| `Query` | Consulta transaccional y analítica sobre `.mbf` con streaming server-side |
| `ListEntities` | Censo de ids por tipo, con tope duro y `truncated` explícito |
| `Transact` | Escritura transaccional ACID en el motor `.mbf` |
| `CompositeTransact` | Escritura atómica compuesta (entidad raíz + entidades hijas en una sola TX) |
| `BulkIngest` | Ingesta masiva y telemetría IoT |
| `ListSequenceConfigs` / `UpsertSequenceConfig` | Gestión de secuencias autonuméricas por tenant |
| `MatchRoutingRulesBatch` | Evaluación de reglas de routing EDA (canónico en `MoiraRoutingService`) |

`tonic-reflection` está habilitado, así que el servidor es explorable con `grpcurl` sin importar los protos:

```bash
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext localhost:9090 describe metri.MetriService
```

## Requisitos

- Rust stable + [`cargo-lambda`](https://www.cargo-lambda.info/) (para el build de Lambda)
- `flatc` `25.12.19` y `protoc` (compilación de esquemas FlatBuffers y gRPC)
- AWS SAM CLI (para desplegar)
- `grpcurl` (smoke tests opcionales)

## Quickstart local

El motor ejecuta de forma autónoma en local usando almacenamiento `.mbf` en memoria / filesystem sin requerir contenedores externos:

```bash
make run      # levanta el servidor gRPC en localhost:9090 (modo stub sin AWS)
make smoke    # smoke test gRPC con grpcurl
```

## Tests y Verificación CI

```bash
make test               # suite completa (unitarios + integración E2E en memoria)
make test-integration   # ejecuta los binarios de integración en tests/
make ci                 # espejo local de CI: fmt, referencia, check_docs, patrón Result, cargo doc, clippy y tests
```

## Estructura del repositorio

```
src/
  mbf/            motor binario HTAP unificado Metri Vortex (.mbf)
  grpc/           servidor gRPC, interceptors, implementación de servicios
  domain/         protocolos (traits) y errores — cero dependencia de infraestructura
  janus/          compilador de queries → AST IR (FlatBuffers)
  janus_router/   routing de escritura (.mbf), sagas y ULIDs
  aegis/          ejecutores .mbf/OLTP + motor de fórmulas
  eav/            motor de datoms inmutables (writer, reader, FTS, types)
  codice/         registro de modelos, coerción y secuencias
  cedar/          autorización ABAC
  quota/          cuotas, ledger, reservas
  iop/            pipeline de ingesta
  eda/            eventos de dominio (Moira, Sherlog)
  temporal/       primitivas de tiempo
  infrastructure/ adaptadores AWS SDK (S3, SQS, EventBridge) y sesiones
  otel/           trazas
config/
  models/         60 modelos JSON — fuente de verdad de los esquemas
  errors/         catálogo canónico de errores (TOML) — validado fail-fast en el arranque
  prompts/        prompts LLM de Metri Q Assistant
  locales/        i18n (es / en)
proto/            contratos gRPC — fuente única del API
scripts/          generación de referencia, guards de CI y utilidades operativas
tests/            suites e2e y de integración en Rust
template.yaml     stack SAM completo (Lambda, CloudFront, Route53, WAFv2, KMS, S3, SecretsManager, SQS)
```

## Configuración

Variables de entorno principales leídas por el binario (ver [`.env.example`](.env.example); el arranque falla rápido si falta algo crítico en producción):

| Variable | Default | Descripción |
|---|---|---|
| `GRPC_PORT` | `9090` | Puerto del servidor gRPC |
| `ENVIRONMENT` | `development` | `production`/`prod`/`staging` activa el fail-fast de seguridad |
| `HMAC_SECRET` | — | Secreto de firma HMAC; obligatorio y fuerte en producción |
| `MASTER_TENANT_ID` | `system` | Identificador del tenant maestro |
| `EAV_TABLE_NAME` | `metri-mbf-local` | Identificador lógico del espacio de almacenamiento `.mbf` |
| `MBF_S3_BUCKET` | — | Bucket S3 de persistencia de archivos `.mbf` (si se omite o `S3_MODE=stub`, opera en memoria) |
| `MBF_S3_PREFIX` | `metri-engine` | Prefijo de objetos `.mbf` dentro del bucket S3 |
| `MBF_CACHE_DIR` | `/tmp/metri-mbf-cache` | Directorio local NVMe para caché de bloques `.mbf` revalidados por `ETag` |
| `AWS_S3_EXPORT_BUCKET` | `metri-engine` | Bucket S3 para exportación asíncrona de resultados |
| `CODICE_MODELS_DIR` | `config/models` | Directorio de modelos JSON del Códice |
| `ERROR_CATALOG_PATH` | `config/errors/error_catalog.toml` | Ruta al catálogo canónico de errores |
| `PROMPTS_DIR` | `config/prompts` | Directorio de plantillas de prompts de IA |
| `EVENTBRIDGE_MODE` · `S3_MODE` · `SQS_MODE` | — (`stub` en local) | Interruptores `stub`/real por servicio AWS — permite correr sin AWS |
| `OUTBOX_QUEUE_URL` | — | URL de la cola SQS FIFO del transactional outbox (`moira`) |
| `EVENTBRIDGE_BUS_NAME` / `FAULT_BUS_NAME` | `metri-events` / `metri-faults` | Buses EventBridge de eventos de dominio y fallos (`sherlog`) |

## Despliegue

El deploy a producción corre por **GitHub Actions** (`.github/workflows/deploy-production.yml`): cada push a `main` compila, prueba y despliega vía SAM tras la aprobación del environment `production`. Credenciales por OIDC — sin access keys en secretos. Detalles, puesta en marcha y rollback en [`docs/guides/deployment.md`](docs/guides/deployment.md).

Deploy manual de emergencia:

```bash
make deploy   # cargo build --release + sam build + sam deploy (perfil metri-dev, us-east-1)
```

El stack SAM ([`template.yaml`](template.yaml)) crea: función Lambda ARM64 con Function URL (`RESPONSE_STREAM`), distribución CloudFront con dominio propio, Route53 y WAFv2, clave KMS para cifrado por tenant, secreto HMAC en Secrets Manager, bucket S3 unificado (`.mbf` + exports) y dos colas SQS FIFO (outbox + DLQ). Los parámetros están fijados en `samconfig.toml`.

## Estado y documentación

El motor está en producción. El índice navegable de toda la documentación está en [`docs/README.md`](docs/README.md); la referencia autogenerada de modelos, códigos de error y API gRPC vive en [`docs/reference/`](docs/reference/).
