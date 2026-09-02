# Estrategia de Migración: Clojure → Rust (Metri Engine)

**Objetivo:** Migrar el ecosistema completo de `metri-engine` de Clojure/JVM a Rust nativo, manteniendo cero-downtime, paridad funcional total, y aprovechando las ventajas de performance que motivaron la migración.

---

## MÓDULO 0: Diagnóstico del Ecosistema Clojure

### Inventario de Componentes

| Módulo Clojure | Archivos | Responsabilidad | Prioridad Migración |
|:--|:--|:--|:--|
| `main.clj` | 1 | Entry point JVM (Integrant bootstrap, estado global) | FASE 1 |
| `bootstrap.clj` | 1 | Inicialización Integrant — mapa de componentes del sistema | FASE 4 |
| `stubs.clj` | 1 | Stubs de desarrollo/test — no va a producción Rust | IGNORAR |
| `lambda/handler.clj` | 1 | AWS Lambda entry point real (13KB — deserializa InputStream, routing gRPC-Web) | FASE 1 |
| `application/core.clj` | 1 | Bridge de compatibilidad SAM: delega a `lambda.handler` o `main` | FASE 1 |
| `config/readers.clj` | 1 | EDN config readers (Integrant env vars) | FASE 1 |
| `codice/` | 8 | Schema Registry (Códice) — fuente de verdad del sistema | FASE 1 (crítico) |
| `domain/` | 5 | Contratos del dominio: protocols (traits), errors, audit, pipeline | FASE 1 |
| `infrastructure/` | 12 | Clientes AWS: DynamoDB, Athena, EventBridge, SQS, Glue, Kinesis, tenant_guard, session_store | FASE 1 |
| `grpc/` | 5 | Protobuf/gRPC: server, dispatcher, interceptors, translator (40KB), service | FASE 1 |
| `aegis/sql/` | 10 | SQL compiler → Athena (HoneySQL) | FASE 2 |
| `aegis/datalog/` | N | Datalog executor → Datahike | FASE 1 (reemplazado por Metri EAV) |
| `janus/` | 9 | Query pipeline: AST compiler, normalizer, filter compiler | FASE 2 |
| `janus_router/` | 5+ | Router OLTP/OLAP: core, partition, ulid, channels (olap+oltp), projections | FASE 2 |
| `temporal/` | 4 | Aritmética temporal canónica: `shift-by-calendar`, `truncate-to-unit`, `parse-athena-ts`, shortcuts TIME_SHIFT | FASE 2 |
| `otel/spans.clj` | 1 | OpenTelemetry span helpers | FASE 2 |
| `iop/` | 4 | Ingestion Orchestration Pipeline (IOP) | FASE 3 |
| `cedar/stub.clj` | 1 | ⚠️ Placeholder — Cedar auth no implementado aún en Clojure | FASE 2 (implementar en Rust con `cedar-policy` crate) |
| `quota/stub.clj` | 1 | ⚠️ Placeholder — Quota Guard no implementado aún | FASE 3 |
| `eda/stub.clj` | 1 | ⚠️ Placeholder — EDA Outbox no implementado aún | FASE 3 |
| `moira/stub.clj` | 1 | ⚠️ Placeholder — Auditoría no implementada aún | FASE 3 |

### Razones para Migrar

| Problema Clojure/JVM | Impacto | Solución Rust |
|:--|:--|:--|
| Cold start Lambda ~4,000ms | UX inaceptable | < 50ms (Rust native binary) |
| GC pauses Clojure | Latencia P99 variable ±400ms | Zero GC — latencia determinística |
| Datahike blob monolítico en DynamoDB | Corrupciones bajo alta concurrencia | Metri EAV (datoms individuales) |
| `levenshtein_distance()` en Athena (full scan) | $0.50/query en producción masiva | DL1 DFA en CPU Rust → LIKE+regexp |
| JVM memory overhead ~256MB | Costo Lambda elevado | Rust binary ~8MB, ~32MB RAM |
| EDN como contrato interno | Parse lento, no tipado | FlatBuffers zero-copy |
| Integrant (DI runtime) | Complejidad en Lambda | Composición estática Rust |

---

## MÓDULO I: Principios de Migración

### I.1 — Strangler Fig Pattern

La migración NO es un rewrite big-bang. Se usa el patrón **Strangler Fig**:

```
┌─────────────────────────────────────────────────────┐
│  Fase 0: Clojure 100%, Rust 0%                     │
│  Fase 1: Clojure 80%, Rust 20% (EAV + gRPC)       │
│  Fase 2: Clojure 40%, Rust 60% (Janus + Aegis)    │
│  Fase 3: Clojure 10%, Rust 90% (IOP + EDA)        │
│  Fase 4: Clojure 0%,  Rust 100% (Sunset JVM)      │
└─────────────────────────────────────────────────────┘
```

En cada fase, el tráfico se redirige incrementalmente al nuevo componente Rust vía **feature flags por tenant**, permitiendo rollback inmediato sin deploy.

### I.2 — Contratos de Compatibilidad

- **gRPC externo:** El contrato Protobuf (`metri.proto`) **NO cambia** en ninguna fase. El cliente nunca sabe que el backend cambió de JVM a Rust.
- **DynamoDB:** La tabla `metri-eav-prod` (nueva) coexiste con `metri-datahike-prod` durante la transición.
- **Athena/S3:** El schema Iceberg y las tablas no cambian — solo cambia quién genera el SQL.

### I.3 — Feature Flags por Tenant

```rust
/// Cada request consulta el flag store (DynamoDB TTL) para decidir qué engine usar.
enum EngineVersion { Clojure, Rust }

fn select_engine(tenant_id: &str, operation: &str) -> EngineVersion {
    // Flag store en DynamoDB: T#<tenant>#FLAG#engine_version
    // Default: Clojure — Rust solo si el flag está activo para ese tenant
    FLAG_STORE.get(tenant_id, operation)
        .unwrap_or(EngineVersion::Clojure)
}
```

---

## MÓDULO II: Fases de Migración Detalladas

### FASE 1 — Fundación Rust (Duración estimada: 6-8 semanas)

**Objetivo:** Tener el binario Rust desplegado en producción sirviendo tráfico real, aunque sea parcial.

#### 1.1 — Lambda Entry Point + gRPC Handler

**Clojure actual:** `main.clj` + `grpc/translator.clj`
**Rust nuevo:** `src/main.rs` + `src/grpc/handler.rs`

```
Migrar:
  ✅ Lambda handler (tonic gRPC server)
  ✅ Protobuf deserialization (prost)
  ✅ HMAC-SHA256 token verification (reemplaza Cedar en primera iteración)
  ✅ Health check endpoint
  
NO migrar aún:
  ❌ Lógica de negocio (sigue en Clojure via gRPC call)
```

El patrón en Fase 1: La Lambda Rust recibe el request, verifica el token, y **delega al Clojure** vía invocación Lambda interna o SQS. Cero lógica de negocio aún.

#### 1.2 — Codice Registry (Schema-Driven Core)

**Clojure actual:** `codice/registry.clj`, `codice/api.clj`
**Rust nuevo:** `src/codice/registry.rs`

Este es el componente más crítico — todos los demás dependen de él.

```rust
/// Compilado en bootstrap desde los ~51 modelos JSON en /docs/architecture/models/
/// Lookup O(1) por entity name o attr_id (u16)
pub struct CodeRegistry {
    by_entity: HashMap<String, EntitySchema>,
    by_attr_id: Vec<AttributeDescriptor>,  // índice por u16 ID
}

/// Cargado desde S3 (los JSONs) en cold start — luego cacheado en memoria estática.
static REGISTRY: OnceLock<CodeRegistry> = OnceLock::new();
```

Paridad con Clojure: `codice/api.clj` expone `entity-model`, `attr-descriptor`, `sequence-scope`. El Rust registry debe exponer las mismas funciones con los mismos contratos de datos.

#### 1.3 — Metri EAV Engine (Reemplaza Datahike)

**Clojure actual:** `infrastructure/datahike.clj`, `aegis/datalog.clj`, `infrastructure/tenant_guard.clj`
**Rust nuevo:** `src/eav/` (diseñado en `Metri EAV - OLPT.md`)

Este es el reemplazo más impactante. Datahike desaparece completamente en Fase 1.

```
Clojure (Datahike path):
  TransactRequest → datahike.clj → d/transact → DynamoDB (blob monolítico)

Rust (Metri EAV path):
  TransactRequest → eav/writer.rs → TransactWriteItems → DynamoDB (datoms EAVT/AEVT/AVET/VAET)
```

**Dual-write durante transición:**
```rust
// Fase 1b: Escribir a AMBOS para validar consistencia
async fn dual_write(payload: &TransactPayload, tenant: &str) {
    let (eav_result, clj_result) = tokio::join!(
        eav_write(payload, tenant),
        clojure_write(payload, tenant),  // Lambda invoke al engine Clojure
    );
    // Comparar resultados — alertar si divergen
    validate_consistency(eav_result, clj_result);
}
```

---

### FASE 2 — Pipeline de Consultas (Duración estimada: 8-10 semanas)

**Objetivo:** El pipeline completo de lectura (Janus + Aegis) corre en Rust.

#### 2.1 — Janus: AST Compiler

**Clojure actual:** `janus/ast_compiler.clj`, `janus/filter_compiler.clj`, `janus/abac_clauses.clj`
**Rust nuevo:** `src/janus/` (diseñado en `JANUS - Rust.md`)

Módulos a portar en orden de dependencia:

```
Orden de implementación:
  1. abac.rs         ← abac_clauses.clj  (sin dependencias propias)
  2. filter.rs       ← filter_compiler.clj
  3. ast_compiler.rs ← ast_compiler.clj  (depende de 1 y 2)
  4. normalizer.rs   ← normalizer.clj    (depende de 3)
  5. core.rs         ← janus/core.clj    (orchestrator — depende de todo)
```

Contratos de paridad (test de regresión):
```rust
/// Para cada query de producción conocida, el AST IR de Rust debe ser
/// funcionalmente equivalente al de Clojure.
/// Ejecutar contra el conjunto de 500+ queries en janus_ast_reliability_test.py
#[test]
fn test_ast_parity_with_clojure() {
    // Cargar queries de /docs/architecture/janus_ast_reliability_test.py
    // Ejecutar ambos pipelines, comparar SQL generado
}
```

#### 2.2 — Janus Router: Channel Selection

**Clojure actual:** `janus_router/core.clj`, `janus_router/channels/`
**Rust nuevo:** `src/router/`

```rust
enum Channel { OltpEav, OlapAthena }

fn route(plan: &EavQueryPlan, registry: &CodeRegistry) -> Channel {
    let schema = registry.get(plan.entity()).unwrap();
    match schema.engine.as_str() {
        "oltp" => Channel::OltpEav,
        "olap" => Channel::OlapAthena,
        _      => Channel::OltpEav, // default seguro
    }
}
```

#### 2.3 — Aegis: SQL Compiler para Athena

**Clojure actual:** `aegis/sql/` (10 archivos)
**Rust nuevo:** `src/aegis/` (diseñado en `AEGIS - SQL.md`)

```
Orden de implementación:
  1. fuzzy.rs        ← fuzzy_sql.clj   (sin dependencias)
  2. helpers.rs      ← helpers.clj
  3. where.rs        ← where.clj       (depende de fuzzy + helpers)
  4. select.rs       ← select.clj + aggregation.clj
  5. comparison.rs   ← comparison.clj  (CTEs TIME_SHIFT)
  6. compiler.rs     ← compiler.clj    (orchestrator)
```

#### 2.4 — Cedar Authorization (ABAC)

**Clojure actual:** `cedar/` (evaluador de políticas Cedar)
**Rust nuevo:** `src/cedar/`

Amazon Cedar tiene un SDK oficial en Rust (`cedar-policy` crate) — migración directa:

```toml
[dependencies]
cedar-policy = "3.x"
```

```rust
use cedar_policy::{Authorizer, Context, Entities, PolicySet, Request};

fn authorize(ctx: &CedarContext, action: &str, resource: &str) -> Decision {
    let authorizer = Authorizer::new();
    let request    = Request::new(ctx.principal(), action, resource, ctx.attrs());
    authorizer.is_authorized(&request, &POLICY_SET, &ENTITIES)
}
```

#### 2.5 — OpenTelemetry

**Clojure actual:** `otel/`
**Rust nuevo:** `opentelemetry` crate (OTLP exporter)

```toml
[dependencies]
opentelemetry       = "0.23"
opentelemetry-otlp  = "0.16"
tracing-opentelemetry = "0.24"
```

---

### FASE 3 — Pipeline de Escritura Avanzada (Duración estimada: 6-8 semanas)

**Objetivo:** IOP, EDA, Quota, Auditoría en Rust.

#### 3.1 — IOP: Ingestion Orchestration Pipeline

**Clojure actual:** `iop/core.clj`, `iop/pipeline.clj`
**Rust nuevo:** `src/iop/`

El IOP en Clojure implementa: Integrity Validation → Payload Enrichment → ACID Transaction → Outbox Processing.

```rust
/// Pipeline IOP en Rust — Railway-Oriented Programming
/// Retorna Ok(TransactResult) o el primer Err(IopError) que cortocircuita.
async fn run_iop(cmd: IngestCommand, ctx: &IopContext) -> Result<TransactResult, IopError> {
    validate_integrity(&cmd, &ctx.registry)?;         // Paso 1
    let enriched = enrich_payload(cmd, &ctx.registry).await?; // Paso 2 (auto-generate)
    let tx_result = acid_transact(enriched, ctx).await?;      // Paso 3
    publish_outbox(&tx_result, ctx).await?;                   // Paso 4
    Ok(tx_result)
}
```

#### 3.2 — EDA: Outbox + EventBridge

**Clojure actual:** `eda/`
**Rust nuevo:** `src/eda/`

```rust
/// Outbox pattern — escribe evento junto a la transacción en DynamoDB
/// EventBridge publish es asíncrono y no bloquea la respuesta al cliente
async fn publish_outbox(tx: &TransactResult, ctx: &IopContext) {
    if ctx.disable_eda { return; }
    let event = build_event(tx, &ctx.event_rules);
    // Se escribe como parte del TransactWriteItems (atómico con los datoms)
    // EventBridge poller lo lee y publica de forma asíncrona
}
```

#### 3.3 — Quota Guard

**Clojure actual:** `quota/`
**Rust nuevo:** `src/quota/`

```rust
/// Check pre-transacción: ¿el tenant tiene cuota disponible?
/// Lectura contra DynamoDB (tabla de quotas) con cache en memoria.
async fn check_quota(tenant_id: &str, entity: &str) -> Result<(), QuotaError> {
    let quota = QUOTA_CACHE.get_or_fetch(tenant_id, entity).await?;
    if quota.remaining == 0 {
        Err(QuotaError::Exhausted { tenant_id: tenant_id.to_string() })
    } else {
        Ok(())
    }
}
```

#### 3.4 — Auditoría (Moira / Sherlog)

**Clojure actual:** `moira/`, `iop/sherlog.clj`
**Rust nuevo:** `src/audit/`

Cada transacción genera un registro de auditoría inmutable. En Rust: un datom de sistema `sys/internal_audit_hash` escrito junto al payload como parte del mismo `TransactWriteItems`.

---

### FASE 4 — Sunset JVM (Duración estimada: 2-4 semanas)

**Objetivo:** Eliminar completamente la Lambda Clojure de producción.

```
Checklist de Sunset:
  ✅ 100% del tráfico en Rust durante 30 días sin incidentes
  ✅ Todos los tests de regresión pasan contra el nuevo engine
  ✅ DynamoDB: datos de Datahike migrados a Metri EAV (o archivados)
  ✅ SAM template: Lambda Clojure removida
  ✅ Dependencias JVM eliminadas del pipeline CI/CD
  ✅ metri-datahike-prod en DeletionPolicy: Retain (conservar por 90 días post-sunset)
```

---

## MÓDULO III: Mapa de Dependencias entre Componentes

```
                    CodeRegistry (codice/)
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          IOP (iop/)  Janus     Janus Router
                      (janus/)  (janus_router/)
              │          │          │
              ▼          ▼          ▼
         Metri EAV    Aegis SQL  Cedar Auth
         (eav/)       (aegis/)   (cedar/)
              │          │
              ▼          ▼
           DynamoDB    Athena
           (EAVT/AEVT  (Iceberg
            /AVET/VAET)  tables)
              │
         EDA Outbox
         (eda/)
              │
         EventBridge
```

**Regla:** Los componentes del nivel inferior NO deben depender de los del nivel superior. Las flechas indican dirección de dependencia.

---

## MÓDULO IV: Estructura de Carpetas del Proyecto Rust

```
metri-engine/                       # ← repositorio existente (NO se crea uno nuevo)
├── Cargo.toml                      # NUEVO — workspace Rust (coexiste con project.clj)
├── build.rs                        # NUEVO — compila .fbs y .proto
├── template.yaml                   # MODIFICADO — Runtime: provided.al2023, arm64
├── src/
│   ├── metri/                      # EXISTENTE — Clojure (NO tocar durante la migración)
│   │   ├── aegis/
│   │   ├── janus/
│   │   ├── infrastructure/
│   │   └── ...
│   │
│   ├── eav/                        # ⚠️ NUEVO — Rust puro, hermano de src/metri/
│   │   ├── mod.rs                  # Re-exports públicos del módulo EAV
│   │   ├── types/                  # Datom, DatomValue (12 tipos), SK encoding binario
│   │   │   ├── datom.rs
│   │   │   ├── encoding.rs
│   │   │   └── value_type.rs
│   │   ├── registry/               # AttributeDescriptor, Registry O(1), migration/diff
│   │   │   ├── descriptor.rs
│   │   │   ├── registry.rs
│   │   │   ├── compiler.rs
│   │   │   └── migration.rs
│   │   ├── writer/                 # Write path ACID: transact, chunker, outbox
│   │   │   ├── transact.rs
│   │   │   ├── chunker.rs
│   │   │   ├── enricher.rs
│   │   │   └── outbox.rs
│   │   ├── reader/                 # Read path: pull, query plan, assembler, time-travel
│   │   │   ├── pull.rs
│   │   │   ├── query.rs
│   │   │   ├── assembler.rs
│   │   │   └── as_of.rs
│   │   ├── index/                  # Los 4 GSIs: EAVT, AEVT, AVET, VAET
│   │   │   ├── eavt.rs
│   │   │   ├── aevt.rs
│   │   │   ├── avet.rs
│   │   │   └── vaet.rs
│   │   ├── fts/                    # Full-text search: trigrams, index writer, searcher
│   │   │   ├── trigram.rs
│   │   │   ├── index_writer.rs
│   │   │   └── searcher.rs
│   │   ├── cursor/                 # CompositeCursor Base64+MessagePack
│   │   │   └── composite.rs
│   │   ├── hierarchy/              # Materialized Paths → O(1) subtree query
│   │   │   └── path.rs
│   │   ├── sharding/               # Write sharding + scatter-gather
│   │   │   └── shard.rs
│   │   └── telemetry/              # IoT epoch-bucketing + LZ4 compression
│   │       └── bucket.rs
│   │
│   ├── main.rs                     # NUEVO — Lambda entry point Rust (Tonic gRPC)
│   ├── codice/                     # NUEVO — Schema Registry Rust
│   │   ├── registry.rs             ← codice/registry.clj
│   │   ├── api.rs                  ← codice/api.clj
│   │   ├── sequence.rs             ← codice/sequence.clj
│   │   └── generator.rs            ← codice/generator.clj
│   ├── janus/                      # NUEVO — Query pipeline Rust
│   │   ├── abac.rs                 ← janus/abac_clauses.clj
│   │   ├── filter.rs               ← janus/filter_compiler.clj
│   │   ├── ast_compiler.rs         ← janus/ast_compiler.clj
│   │   ├── normalizer.rs           ← janus/normalizer.clj
│   │   └── core.rs                 ← janus/core.clj
│   ├── router/                     # NUEVO — JanusRouter Rust
│   │   ├── core.rs                 ← janus_router/core.clj
│   │   ├── partition.rs            ← janus_router/partition.clj
│   │   ├── ulid.rs                 ← janus_router/ulid.clj
│   │   ├── channels/
│   │   │   ├── oltp.rs             ← janus_router/channels/oltp.clj
│   │   │   └── olap.rs             ← janus_router/channels/olap.clj
│   │   └── projections/
│   │       └── protocol.rs         ← janus_router/projections/protocol.clj
│   ├── aegis/                      # NUEVO — SQL compiler Rust (SeaQuery)
│   │   ├── compiler.rs             ← aegis/sql/compiler.clj
│   │   ├── where.rs                ← aegis/sql/where.clj
│   │   ├── select.rs               ← aegis/sql/select.clj
│   │   ├── fuzzy.rs                ← aegis/sql/fuzzy_sql.clj
│   │   └── comparison.rs           ← aegis/sql/comparison.clj
│   ├── temporal/                   # NUEVO — Aritmética temporal Rust (chrono_tz)
│   │   ├── core.rs                 ← temporal/core.clj
│   │   ├── comparison.rs           ← temporal/comparison.clj
│   │   ├── time_frame.rs           ← temporal/time_frame.clj
│   │   └── adapters.rs             ← temporal/adapters.clj
│   ├── iop/                        # NUEVO — Ingestion pipeline Rust
│   │   ├── pipeline.rs             ← iop/pipeline.clj
│   │   └── sherlog.rs              ← iop/sherlog.clj
│   ├── cedar/                      # NUEVO — ABAC (cedar-policy crate, desde cero)
│   │   └── authorizer.rs
│   ├── quota/                      # NUEVO — Quota Guard Rust
│   │   └── guard.rs
│   ├── eda/                        # NUEVO — EDA Outbox Rust
│   │   ├── outbox.rs               ← eda/outbox
│   │   └── eventbridge.rs
│   ├── audit/                      # NUEVO — Auditoría Rust
│   │   └── moira.rs
│   ├── grpc/                       # NUEVO — gRPC server Rust (Tonic)
│   │   ├── server.rs               ← grpc/server.clj
│   │   ├── dispatcher.rs           ← grpc/dispatcher.clj
│   │   ├── interceptors.rs         ← grpc/interceptors.clj
│   │   ├── service.rs              ← grpc/service.clj
│   │   ├── translator.rs           ← grpc/translator.clj (40KB)
│   │   └── generated/              # prost generado desde .proto
│   ├── infrastructure/             # NUEVO — Clientes AWS Rust
│   │   ├── dynamodb.rs             ← infrastructure/dynamodb.clj
│   │   ├── athena.rs               ← infrastructure/athena.clj
│   │   ├── eventbridge.rs          ← infrastructure/eventbridge.clj
│   │   ├── sqs.rs                  ← infrastructure/sqs.clj
│   │   ├── kinesis.rs              ← infrastructure/kinesis.clj
│   │   ├── glue.rs                 ← infrastructure/glue.clj
│   │   ├── tenant_guard.rs         ← infrastructure/tenant_guard.clj
│   │   └── session_store.rs        ← infrastructure/session_store.clj
│   ├── domain/                     # NUEVO — Contratos del dominio Rust
│   │   ├── errors.rs               ← domain/errors.clj
│   │   ├── protocols.rs            ← domain/protocols.clj
│   │   └── audit/
│   │       └── protocol.rs         ← domain/audit/protocol.clj
│   └── otel/                       # NUEVO — OpenTelemetry Rust
│       └── tracer.rs               ← otel/spans.clj
├── proto/
│   └── metri.proto                 # Sin cambios — contrato gRPC
└── schemas/
    └── eav_query_plan.fbs          # Contrato FlatBuffers Janus→EAV
```


---

## MÓDULO V: Tabla de Equivalencias Clojure → Rust

| Clojure | Rust equivalente |
|:--|:--|
| `Integrant` (DI container) | Composición estática en `main.rs` — `OnceLock<T>` para singletons |
| `EDN` como contrato | `FlatBuffers` (interno) + `Protobuf` (externo) |
| `HoneySQL` (SQL builder) | `SeaQuery` — builder tipado, `to_string(MysqlQueryBuilder)` único punto de materialización |
| `taoensso.timbre` (logging) | `tracing` crate + `tracing-subscriber` |
| `[:ok val] / [:error map]` (Railway) | `Result<T, E>` nativo de Rust |
| `defmulti` / `defmethod` | `enum` + `match` o `trait` implementations |
| `core.async` (go blocks) | `tokio::spawn` + `async/await` |
| `atom` / `ref` (estado compartido) | `Arc<RwLock<T>>` o `DashMap` |
| `d/transact` (Datahike) | `eav::writer::transact()` → `TransactWriteItems` |
| `d/q` (Datalog query) | `eav::reader::query()` → DynamoDB GSI queries |
| `hsql/format` | `sea_query::SelectStatement::to_string(MysqlQueryBuilder)` |
| `ex-info` (excepciones) | `Err(DomainError { code, reason, detail })` |
| `grpc/translator.clj` (40KB proto ↔ domain) | `grpc/translator.rs` — `prost` structs, mismo mapeo 1:1 |
| `grpc/server.clj` (Aleph/Netty) | `grpc/server.rs` — `tonic::transport::Server::builder()` |
| `grpc/interceptors.clj` (HMAC auth) | `grpc/interceptors.rs` — `tonic::Interceptor` trait |
| `grpc/dispatcher.clj` | `grpc/dispatcher.rs` — `match req.method` sobre `tonic::Request` |
| `infrastructure/dynamodb.clj` | `infrastructure/dynamodb.rs` — `aws_sdk_dynamodb::Client` (mismo API) |
| `infrastructure/athena.clj` | `infrastructure/athena.rs` — `aws_sdk_athena::Client` (polling async) |
| `infrastructure/eventbridge.clj` | `infrastructure/eventbridge.rs` — `aws_sdk_eventbridge::Client` |
| `infrastructure/sqs.clj` | `infrastructure/sqs.rs` — `aws_sdk_sqs::Client` (async queue) |
| `infrastructure/kinesis.clj` | `infrastructure/kinesis.rs` — `aws_sdk_kinesis::Client` (Firehose) |
| `infrastructure/glue.clj` | `infrastructure/glue.rs` — `aws_sdk_glue::Client` (schema discovery) |
| `infrastructure/tenant_guard.clj` | `infrastructure/tenant_guard.rs` — HMAC-SHA256 verify (sin roundtrip Secrets Manager) |
| `infrastructure/session_store.clj` | `infrastructure/session_store.rs` — DynamoDB TTL blacklist |
| `domain/protocols.clj` (Clojure protocols) | `domain/protocols.rs` — `trait IASTCompiler`, `trait IJanusChannel`, etc. |
| `domain/errors.clj` | `domain/errors.rs` — `enum DomainError` con códigos JANUS_*, AEG_*, EAV_* |
| `janus_router/channels/oltp.clj` | `router/channels/oltp.rs` — ejecuta transacción ACID en Metri EAV |
| `janus_router/channels/olap.clj` | `router/channels/olap.rs` — compila y ejecuta SQL en Athena |
| `janus_router/ulid.clj` (ULID gen) | `router/ulid.rs` — `ulid::Ulid::new()` con `u128` nativo |
| `taoensso.timbre` (logging) | `tracing` crate + `tracing-subscriber` |
| `[:ok val] / [:error map]` (Railway) | `Result<T, E>` nativo de Rust |
| `defmulti` / `defmethod` | `enum` + `match` o `trait` implementations |
| `core.async` (go blocks) | `tokio::spawn` + `async/await` |
| `atom` / `ref` (estado compartido) | `Arc<RwLock<T>>` o `DashMap` |
| `d/transact` (Datahike) | `eav::writer::transact()` → `TransactWriteItems` |
| `d/q` (Datalog query) | `eav::reader::query()` → DynamoDB GSI queries |
| `hsql/format` | `aegis::compiler::compile_athena_sql()` |
| `ex-info` (excepciones) | `Err(DomainError { code, reason, detail })` |
| `clojure.string/lower-case` | `.to_lowercase()` |
| `System/currentTimeMillis` | `std::time::SystemTime::now()` |
| `UUID/randomUUID` | `uuid::Uuid::new_v4()` |
| `temporal/core::shift-by-calendar` | `temporal::core::shift_by_calendar()` — usa `chrono::TimeZone` + `chrono_tz::Tz` (equivalente a `java.time.ZonedDateTime`) |
| `temporal/core::truncate-to-unit` | `temporal::core::truncate_to_unit()` — implementa `date_trunc` en Rust para OLTP in-memory |
| `temporal/core::parse-athena-ts` | `temporal::core::parse_athena_ts()` — usa `chrono::NaiveDateTime::parse_from_str` con los mismos 5 patrones Athena |
| `temporal/core::ms->s` / `s->ms` | `fn ms_to_s(ms: i64) -> i64 { ms / 1000 }` / `fn s_to_ms(s: i64) -> i64 { s * 1000 }` |
| `temporal/comparison::resolve-comparison-period` | `temporal::comparison::resolve_comparison_period()` — mismos 4 tipos: `TimeShiftRelative`, `Shortcut`, `Absolute`, `Benchmark` |
| `temporal/comparison::resolve-shortcut` | `temporal::comparison::resolve_shortcut()` — todos los shortcuts usando `chrono_tz` (bisiesto-safe, DST-aware) |
| `temporal/comparison::smart-history-window` | `temporal::comparison::smart_history_window()` — ventana de 90 días para anomaly detection |

---

## MÓDULO VI: Cronograma y Métricas de Éxito

### Timeline

```
Semana 1-2:   Infraestructura base — Lambda Rust + gRPC handler vacío
Semana 3-4:   CodeRegistry compilado desde modelos JSON
Semana 5-8:   Metri EAV Engine — write path + read path + 4 índices
Semana 9-12:  Janus pipeline — AST compiler + normalizer + filter compiler
Semana 13-16: Aegis SQL compiler — WHERE + SELECT + fuzzy + CTE
Semana 17-20: IOP pipeline — integrity + enrichment + ACID + outbox
Semana 21-22: Cedar auth + Quota guard + Audit
Semana 23-24: Sunset JVM — migración final + cleanup
```

### Métricas de Éxito por Fase

| Métrica | Clojure baseline | Objetivo Rust Fase 1 | Objetivo Final |
|:--|:--|:--|:--|
| Cold start | ~4,000ms | < 100ms | < 50ms |
| Latencia P50 (GET entity) | ~15ms | < 3ms | < 2ms |
| Latencia P99 (query) | ~800ms | < 30ms | < 15ms |
| Memory per Lambda | ~256MB | < 64MB | < 32MB |
| Costo Lambda mensual | Baseline | -60% | -80% |
| Corrupciones Datahike/mes | ~3 incidentes | 0 | 0 |

---

## MÓDULO VII: Gestión de Riesgos

| Riesgo | Probabilidad | Impacto | Mitigación |
|:--|:--|:--|:--|
| Regresión en lógica de negocio | Media | Alto | Test suite de 500+ queries (`janus_ast_reliability_test.py`) corriendo en CI contra ambos engines |
| Inconsistencia OLTP durante dual-write | Baja | Alto | Reconciliation worker compara DynamoDB Datahike vs Metri EAV diariamente |
| Límite 100 items `TransactWriteItems` | Media | Medio | Módulo XII del EAV doc — chunking transaccional |
| Hot partitions DynamoDB | Baja | Alto | Write sharding (Módulo XI EAV) — activable por flag |
| Cedar SDK Rust diferente a Clojure | Baja | Medio | Test doble con mismas policies — validar mismas decisiones de autorización |
| Athena SQL diferente (dialect) | Media | Medio | Ejecutar SQL generado por ambos compiladores contra mismos datos en Athena sandbox |

---

## Próximos Pasos Inmediatos

1. **Configurar `metri-engine-rust/`** — Crear el `Cargo.toml` con todas las dependencias y el `build.rs` para FlatBuffers + Protobuf.
2. **Implementar `codice/registry.rs`** — Parser de los 51 modelos JSON → `CodeRegistry` con lookup O(1).
3. **Implementar `eav/datom.rs`** — Encoding binario de `DatomValue` + los 4 builders de Sort Key.
4. **Setup dual-write** — Lambda Rust recibe y escribe a Metri EAV + Datahike en paralelo para validación.
5. **CI pipeline** — Ejecutar `janus_ast_reliability_test.py` contra el engine Rust en cada PR.

---

## MÓDULO VIII: Cambios en `template.yaml` — Migración a Rust + Graviton

### VIII.1 — Por qué Graviton (ARM64) sobre x86_64

| Métrica | x86_64 (actual) | ARM64 Graviton3 (Rust) |
|:--|:--|:--|
| Precio compute Lambda | Baseline | **-20% más barato** |
| Performance single-thread | Baseline | **+40% más rápido** (Graviton3 vs Cascade Lake) |
| Cold start Rust binary | N/A | < 10ms (vs 4,000ms JVM) |
| SnapStart requerido | **Sí** — esencial para JVM | **No** — Rust no necesita warm-up |
| Runtime | `java21` | `provided.al2023` (custom runtime) |
| Compilación | JVM bytecode + AOT | `aarch64-unknown-linux-musl` (static binary) |

> [!IMPORTANT]
> **SnapStart se elimina completamente.** SnapStart existe para mitigar el cold start de la JVM (~4s). El binario Rust arranca en < 10ms, haciendo SnapStart no solo innecesario sino contraproducente (agrega latencia al snapshot restore).

---

### VIII.2 — Diff del Bloque `MetriEngineFunction`

**Estado actual (`template.yaml` líneas 570–585):**

```yaml
# ── Clojure Engine ──
MetriEngineFunction:
  Type: AWS::Serverless::Function
  Metadata:
    BuildMethod: makefile          # usa Makefile target build-MetriEngineFunction
  Properties:
    CodeUri: .
    Handler: metri.lambda.handler  # Namespace Clojure
    Runtime: java21                # ← JVM
    Architectures:
      - x86_64                     # ← Intel/AMD

    KmsKeyArn: !GetAtt MetriKmsMasterKey.Arn
    AutoPublishAlias: live
    SnapStart:                     # ← Existe solo para mitigar cold start JVM
      ApplyOn: PublishedVersions
```

**Estado objetivo (Rust + Graviton):**

```yaml
# ── Rust Engine (Graviton3 ARM64) ──
MetriEngineFunction:
  Type: AWS::Serverless::Function
  Metadata:
    BuildMethod: makefile          # usa Makefile target build-MetriEngineFunction
  Properties:
    CodeUri: .
    Handler: bootstrap             # ← Binario Rust compilado (nombre estándar AWS)
    Runtime: provided.al2023       # ← Custom runtime — ejecuta el binario directamente
    Architectures:
      - arm64                      # ← Graviton3 (-20% costo, +40% perf)

    KmsKeyArn: !GetAtt MetriKmsMasterKey.Arn
    AutoPublishAlias: live
    # SnapStart: ELIMINADO          ← Rust no necesita warm-up — cold start < 10ms
```

### VIII.3 — Cambios en `Globals` y `MemorySize`

**Actual:**
```yaml
Globals:
  Function:
    Timeout: 900
    MemorySize: 3008     # JVM necesita mucha RAM para el heap
```

**Objetivo:**
```yaml
Globals:
  Function:
    Timeout: 900
    MemorySize: 256      # ← Rust binary ~8MB, EAV registry ~64MB — suficiente con 256MB
                         #   En Graviton, 256MB cuesta ~5x menos que 3008MB
```

> [!TIP]
> Reducir de 3008MB → 256MB no solo reduce costo directamente, sino que también **reduce el precio por ms** de ejecución. En Lambda, la facturación es `Duration × Memory`, así que 256MB a la misma velocidad = 11.7x más barato en compute.

### VIII.4 — Cambios en el `Makefile` / Build

El `BuildMethod: makefile` actual llama a un target que compila el JAR Clojure con `lein uberjar`. Para Rust se reemplaza por cross-compilación a ARM64:

**Target Makefile actual:**
```makefile
build-MetriEngineFunction:
    lein uberjar
    cp target/metri-engine-standalone.jar $(ARTIFACTS_DIR)/
```

**Target Makefile Rust (Graviton):**
```makefile
build-MetriEngineFunction:
    # Cross-compilar a ARM64 Linux (musl para binario estático)
    cross build --release --target aarch64-unknown-linux-musl
    # El runtime "provided.al2023" espera un binario llamado "bootstrap"
    cp target/aarch64-unknown-linux-musl/release/metri-engine-rust \
       $(ARTIFACTS_DIR)/bootstrap
```

Dependencias del entorno CI:
```bash
# Instalar cross (compilación cruzada sin Docker custom)
cargo install cross --git https://github.com/cross-rs/cross

# Target ARM64 musl (binario completamente estático — sin dependencias .so)
rustup target add aarch64-unknown-linux-musl
```

### VIII.5 — Descripción del `template.yaml` Actualizada

```yaml
Description: >
  metri-engine

  AWS SAM Template para Metri Engine (Rust Nativo) - gRPC Puro
  Arquitectura Graviton3 ARM64 con Runtime provided.al2023.
  Cold start < 10ms — SnapStart no requerido.
  Lambda Function URLs para gRPC Streams sin límite de 29s.
```

### VIII.6 — Sin cambios en FunctionUrlConfig ni Policies

La configuración de CORS, headers gRPC-Web, `AllowMethods`, `ExposeHeaders`, y todas las IAM Policies (`DynamoDBCrudPolicy`, `S3CrudPolicy`, etc.) se mantienen **sin modificación**. El contrato externo (gRPC-Web via Function URL) es idéntico — solo cambia el runtime interno.

### VIII.7 — Resumen de Cambios en `template.yaml`

| Campo | Valor Actual | Valor Rust+Graviton |
|:--|:--|:--|
| `Description` | "Clojure JVM — SnapStart" | "Rust Nativo — Graviton3 ARM64" |
| `Globals.Function.MemorySize` | `3008` | `256` |
| `MetriEngineFunction.Handler` | `metri.lambda.handler` | `bootstrap` |
| `MetriEngineFunction.Runtime` | `java21` | `provided.al2023` |
| `MetriEngineFunction.Architectures` | `[x86_64]` | `[arm64]` |
| `MetriEngineFunction.SnapStart` | `ApplyOn: PublishedVersions` | **ELIMINADO** |
| `Makefile build target` | `lein uberjar` | `cross build --target aarch64-unknown-linux-musl` |
| Binario deployado | `metri-engine-standalone.jar` | `bootstrap` (ELF ARM64 static) |

---

## MÓDULO IX: Protocolo de Traducción 1:1 (Garantía de Paridad)

Para garantizar que no se pierda **absolutamente ninguna funcionalidad** durante la reescritura, la migración de la carpeta `src/metri/` (Clojure) a `src/` (Rust) se ejecutará bajo un estricto mecanismo de rastreo archivo-por-archivo.

### Regla Cero: Cumplimiento Estricto de Blueprints

Durante la ejecución del mecanismo de traducción documentado a continuación, **ES OBLIGATORIO** adherirse estrictamente a las definiciones arquitecturales, convenciones de nombrado y contratos de memoria especificados en los siguientes tres *Technical Blueprints* base. Cualquier código Rust que contradiga estos documentos será rechazado:

1. **`Metri EAV - OLPT.md`**: Define la capa de almacenamiento, los 4 índices canónicos de DynamoDB (GSIs), la persistencia ACID (`TransactWriteItems`), y el manejo de los *Sort Keys* binarios. Todo el código de lectura/escritura (`src/eav/`) debe cumplir con este modelo.
2. **`JANUS - Rust.md`**: Define el motor analítico, la compilación de filtros (`abac_clauses`, `normalizer`), y el contrato de memoria *FlatBuffers*. Todo el enrutamiento de consultas (`src/janus/`) debe apegarse a esta estructura sin estado (stateless).
3. **`AEGIS - SQL.md`**: Define la compilación SQL vía `SeaQuery` para el OLAP (AWS Athena). Todo el código de generación SQL (`src/aegis/`) debe usar el trait de AST compilado y el generador `SeaQuery` para evitar inyecciones.

### El Mecanismo de Garantía al 100%

La migración no se hará por "bloques funcionales abstractos", sino mediante una asignación directa de responsabilidad por archivo. El protocolo es el siguiente para cada uno de los `*.clj` existentes:

#### PASO 1: Inventario Físico (El Checkpoint)
Antes de tocar código, se ejecutará un script que listará recursivamente cada archivo en `metri-engine/src/metri/**/*.clj`. Esta lista será el "To-Do" inmutable de la migración. Ningún archivo se da por completado hasta que pase por los pasos 2 al 5.

#### PASO 2: Diseño de Interfaces (Trait Mapping)
Clojure es funcional y dinámico. Rust es tipado y estricto. Por cada archivo Clojure:
1. Se identifican todas las funciones públicas (`defn`).
2. Se extrae el estado global implícito (ej. dependencias inyectadas por Integrant).
3. Se diseña la interfaz equivalente en Rust usando `pub struct` (para el estado) y `pub trait` o `impl` (para los métodos).

#### PASO 3: Traducción Aislada (Zero-Drop Policy)
Se escribe el código Rust `*.rs` equivalente. 
* **Regla de Oro:** Si una función en Clojure maneja un caso borde oscuro (ej. `(when (nil? x) ...)`), ese mismo caso borde DEBE existir en Rust (usando `Option::None`), incluso si parece código muerto. No se asume limpieza de deuda técnica durante la traducción para evitar regresiones lógicas.

#### PASO 4: Etiquetado Físico de Deprecación (El Sello Criptográfico)
**Esta es la garantía visual y auditable:**
Inmediatamente después de que el archivo Rust compila y pasa las pruebas de unidad, el desarrollador DEBE abrir el archivo Clojure original y añadir el siguiente comentario en la primera línea:

```clojure
;; [PORTED_TO_RUST: src/eav/writer.rs]
;; NO MODIFICAR ESTE ARCHIVO.
;; La fuente de verdad para esta lógica ahora reside en Rust.
(ns metri.aegis.datalog.writer ...)
```

* **Beneficio:** Esto permite saber en cualquier momento exacto del proyecto qué porcentaje del código fuente ya está migrado. Un simple `grep -L "[PORTED_TO_RUST]" src/metri/**/*.clj` listará exactamente los archivos que faltan.

#### PASO 5: Validación Cruzada (Shadow Execution)
Para módulos críticos (ej. `janus/ast_compiler.clj` vs `janus/ast_compiler.rs`), se usarán los mismos payloads JSON de prueba. Se inyecta el mismo payload en la función compilada de Clojure y en el binario de Rust. El hash SHA-256 de las respuestas debe ser **idéntico**. Si la salida de Rust no coincide exactamente con la de Clojure, el archivo no se marca como "Ported".

### Resumen del Flujo por Archivo
`[Leer .clj] → [Escribir .rs] → [Test Cruzado] → [Añadir ;; [PORTED_TO_RUST] al .clj]`
