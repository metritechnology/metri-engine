# JANUS — Motor de Consultas en Rust

**Propósito:** Reemplazar el pipeline Clojure (JanusASTCompiler + Normalizer + Aegis) con una implementación Rust nativa que compile el AST IR y lo envíe directamente al motor **Metri EAV** (EAVT/AEVT/AVET/VAET).

---

## MÓDULO 0: La Decisión de Formato de Contrato

### ¿Por qué no `.edn`?

El contrato actual en Clojure usa EDN (Extensible Data Notation) — un formato textual de Clojure. En Rust, EDN no existe como formato nativo y parsearlo requiere crates externos lentos. El contrato debe migrar.

### Opciones Evaluadas

| Formato | Velocidad parse | Zero-copy | Schema tipado | Binario | Veredicto |
|:--|:--|:--|:--|:--|:--|
| EDN | Lento (textual) | ❌ | ❌ | ❌ | ❌ Descartado |
| JSON | Lento (textual) | ❌ | ❌ | ❌ | Solo debugging |
| MessagePack | Rápido | Parcial | ❌ | ✅ | Bueno |
| Protobuf | Muy rápido | ✅ | ✅ | ✅ | Candidato |
| **FlatBuffers** | **Nativo zero-copy** | **✅✅** | **✅** | **✅** | **🏆 Elegido** |

### La Elección: FlatBuffers para el AST IR Interno

**FlatBuffers** es el formato elegido para el contrato interno entre Janus y Metri EAV porque:

1. **Zero-copy access:** El motor accede a los campos del AST sin deserializar — la Lambda nunca copia el buffer en memoria.
2. **Schema tipado en Rust:** El compilador `flatc` genera structs Rust con tipos exactos — cero errores de runtime por campos ausentes.
3. **Sub-microsegundo:** El parse de un `EavQueryPlan` de 500 bytes tarda < 500ns vs ~8μs de JSON.
4. **Backward compatible:** FlatBuffers permite añadir campos nuevos sin romper clientes viejos — crítico para schema evolution.

> [!IMPORTANT]
> El contrato **gRPC externo** (`metri.proto` → `QueryRequest`) **NO cambia**. FlatBuffers es el formato del **AST IR interno** que viaja entre los módulos Rust dentro de la misma Lambda. El cliente gRPC-Web sigue usando Protobuf como siempre.

---

## MÓDULO I: Arquitectura del Pipeline de Consultas

### I.1 — Flujo Completo

```
Cliente gRPC-Web
    │
    │  QueryRequest (Protobuf — sin cambios)
    ▼
┌─────────────────────────────────────┐
│  gRPC Handler (Tonic)               │
│  Deserializa QueryRequest → Clojure │
│  proto::QueryRequest struct (Rust)  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  JANUS RUST — Pipeline              │
│                                     │
│  Paso 1: Validación de Entrada      │
│  Paso 2: Resolución de Identidad    │
│  Paso 3: ABAC / Cedar Policy        │
│  Paso 4: AST IR Compilation         │
│  Paso 5: Plan Selection             │
│  Paso 6: EAV Query Execution        │
│  Paso 7: Result Assembly            │
│  Paso 8: Normalization              │
└──────────────┬──────────────────────┘
               │
               │  EavQueryPlan (FlatBuffers)
               ▼
┌─────────────────────────────────────┐
│  Metri EAV Engine                   │
│  (EAVT / AEVT / AVET / VAET)        │
│  DynamoDB Executor                  │
└──────────────┬──────────────────────┘
               │
               │  EavResultChunk (streaming)
               ▼
┌─────────────────────────────────────┐
│  Normalizer Rust                    │
│  VizMeta / Pagination / HATEOAS     │
└──────────────┬──────────────────────┘
               │
               │  QueryResponse (Protobuf)
               ▼
          Cliente gRPC-Web
```

### I.2 — Los 8 Pasos del Pipeline

#### Paso 1 — Validación de Entrada

```rust
/// Valida que el QueryRequest sea estructuralmente correcto antes de cualquier I/O.
/// Sin acceso a DynamoDB — puro CPU. < 100μs garantizado.
fn validate_request(req: &QueryRequest) -> Result<(), JanusError> {
    if req.tenant_id.is_empty() {
        return Err(JanusError::new("JANUS_400", "tenant_id requerido"));
    }
    if req.queries.is_empty() {
        return Err(JanusError::new("JANUS_400", "queries no puede estar vacío"));
    }
    Ok(())
}
```

#### Paso 2 — Resolución de Identidad (Cedar / HMAC)

```rust
/// Verifica el token HMAC-SHA256 y extrae el contexto del usuario.
/// Lee el HMAC secret desde el cache en memoria (cargado en bootstrap).
/// Sin roundtrip a Secrets Manager en caliente — < 1ms.
struct CedarContext {
    tenant_id: String,
    user_id:   String,
    roles:     Vec<String>,
    scope:     AccessScope,  // OWN | ASSIGNED | OWN_OR_ASSIGNED | ALL
}

async fn resolve_identity(token: &str, secret: &[u8]) -> Result<CedarContext, JanusError> {
    // 1. Verificar firma HMAC-SHA256
    // 2. Extraer claims del payload (tenant_id, user_id, roles)
    // 3. Construir CedarContext
}
```

#### Paso 3 — ABAC: Inyección de Nodos Tenant + Scope

Equivalente al `abac-clauses/build-abac-node` de Clojure pero compilado en Rust:

```rust
/// Construye los nodos de seguridad que se inyectan en el WHERE del AST.
/// SIEMPRE se ejecuta — no bypasseable.
fn build_security_nodes(
    entity: &str,
    ctx: &CedarContext,
    schema: &EntitySchema,
) -> Vec<FilterNode> {
    let mut nodes = vec![
        // 4a: tenant isolation — CARDINAL, siempre primero
        FilterNode::Eq {
            field: "tenant_id".into(),
            value: FilterValue::Str(ctx.tenant_id.clone()),
        },
    ];

    // 4c: scope predicate (OWN / ASSIGNED / OWN_OR_ASSIGNED)
    match ctx.scope {
        AccessScope::Own => {
            if let Some(owner_field) = schema.owner_field() {
                nodes.push(FilterNode::Eq {
                    field: owner_field.into(),
                    value: FilterValue::Str(ctx.user_id.clone()),
                });
            }
        }
        AccessScope::Assigned => {
            if let Some(assignee_field) = schema.assignee_field() {
                nodes.push(FilterNode::Contains {
                    field: assignee_field.into(),
                    value: FilterValue::Str(ctx.user_id.clone()),
                });
            }
        }
        AccessScope::All => {} // Admin — sin restricción de scope
        _ => {}
    }
    nodes
}
```

#### Paso 4 — Compilación del AST IR → EavQueryPlan (FlatBuffers)

Este es el núcleo del Janus Rust. Transforma el `AnalyticsRequest` del proto en un `EavQueryPlan` tipado:

```rust
/// El contrato interno entre Janus y Metri EAV.
/// Serializado como FlatBuffer — zero-copy en el receptor.

// Schema FlatBuffers (archivo: eav_query_plan.fbs)
// table FilterNode {
//   op: FilterOp;           // EQ, NEQ, GT, LT, GTE, LTE, IN, CONTAINS, FUZZY
//   field: string;
//   value: Value;           // union { StringVal, U64Val, F64Val, BoolVal, ListVal }
//   children: [FilterNode]; // para AND / OR recursivo
// }
//
// table EavQueryPlan {
//   tenant_id:    string;
//   entity:       string;
//   select:       [string];   // atributos a proyectar — vacío = pull [*]
//   where_root:   FilterNode; // árbol de filtros (tenant + abac + user filters)
//   order_by:     [SortSpec];
//   limit:        uint32;
//   cursor:       string;     // CompositeCursor opaco
//   output_cast:  OutputCast; // KPI, TIMESERIES, TABLE, PIE, BUBBLE, CSV_EXPORT
//   metrics:      [MetricSpec];
//   dimensions:   [DimensionSpec];
//   time_frame:   TimeFrame;
//   as_of_tx:     uint64;    // 0 = latest, >0 = time-travel
//   query_fingerprint: uint64; // Hash del plan para cursor validation
// }

fn compile_ast(
    analytics_req: &AnalyticsRequest,
    ctx: &CedarContext,
    schema: &EntitySchema,
    registry: &AttributeRegistry,
) -> Result<EavQueryPlanBytes, JanusError> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

    // 1. Construir nodos de seguridad (tenant + scope)
    let security_nodes = build_security_nodes(&analytics_req.entity, ctx, schema);

    // 2. Compilar filtros del usuario (FilterNode tree recursivo)
    let user_filters = compile_filter_nodes(&analytics_req.filters, schema, registry)?;

    // 3. Validar FTS fields contra el registry
    let search_node = if let Some(term) = &analytics_req.search {
        build_fts_node(term, schema, registry)
    } else {
        None
    };

    // 4. Ensamblar WHERE = AND(security + abac + search + user_filters)
    let where_root = merge_filter_nodes(security_nodes, user_filters, search_node);

    // 5. Serializar a FlatBuffer
    let plan = build_eav_query_plan(
        &mut builder,
        &analytics_req.entity,
        &ctx.tenant_id,
        &analytics_req.select_tree,
        where_root,
        &analytics_req.order_by,
        analytics_req.limit,
        &analytics_req.output_cast,
        &analytics_req.metrics,
        &analytics_req.dimensions,
    );

    builder.finish(plan, None);
    Ok(EavQueryPlanBytes(builder.finished_data().to_vec()))
}
```

#### Paso 5 — Plan Selection (Selección de Índice Óptimo)

El Query Planner analiza el `EavQueryPlan` y selecciona el índice EAV más eficiente:

```rust
enum QueryPlan {
    /// d/pull: GET entity by ULID → Main Table EAVT
    PointLookup { entity_id: u64 },

    /// WHERE attr = value → GSI-AVET (único o filtro por valor)
    AvetSingleFilter { attr_id: u16, value: DatomValue },

    /// WHERE attr1 = v1 AND attr2 = v2 → 2 AVET queries en paralelo + intersección
    AvetIntersection { filters: Vec<(u16, DatomValue)> },

    /// WHERE entity/type = X (sin filtros adicionales) → GSI-AEVT
    AevtScan { entity_type: String, shard: Option<u8> },

    /// Reverse reference: "¿Quién apunta a entity X?" → GSI-VAET
    VaetReverseLookup { ref_entity_id: u64, attr_id: Option<u16> },

    /// FTS fuzzy search → GSI-FTS trigram + EAVT reconstituir
    FtsSearch { term: String, attr_ids: Vec<u16> },

    /// Time-travel: as-of TX T → EAVT con SK ≤ T
    AsOfSnapshot { entity_id: u64, as_of_tx: u64 },
}

fn select_plan(plan: &EavQueryPlan, registry: &AttributeRegistry) -> QueryPlan {
    // Prioridad de selección de índice:
    // 1. Si hay ULID explícito en el WHERE → PointLookup (más barato: 1 Query)
    // 2. Si hay exactamente 1 filtro con index=true → AvetSingleFilter
    // 3. Si hay N filtros con index=true → AvetIntersection (paralelo)
    // 4. Si hay búsqueda FTS → FtsSearch
    // 5. Si solo filtra por tipo → AevtScan (con sharding si aplica)
    // 6. Si hay as_of_tx > 0 → AsOfSnapshot
}
```

#### Paso 6 — Ejecución en Metri EAV

```rust
/// Ejecuta el QueryPlan seleccionado contra DynamoDB.
/// Retorna un stream de EavResultChunk (100 filas por chunk).
async fn execute_plan(
    plan: QueryPlan,
    eav_query: &EavQueryPlan,
    ddb: &DynamoDbClient,
    registry: &AttributeRegistry,
) -> impl Stream<Item = Result<EavResultChunk, EavError>> {
    match plan {
        QueryPlan::PointLookup { entity_id } => {
            eav_pull(entity_id, eav_query.select(), ddb, registry).await
        }
        QueryPlan::AvetSingleFilter { attr_id, value } => {
            eav_avet_query(attr_id, &value, eav_query, ddb, registry).await
        }
        QueryPlan::AvetIntersection { filters } => {
            // Ejecutar todas las AVET queries en paralelo (tokio::join!)
            // Intersectar los entity_id sets → BatchGetItem para reconstituir
            eav_avet_intersection(filters, eav_query, ddb, registry).await
        }
        QueryPlan::AevtScan { entity_type, shard } => {
            eav_aevt_scan(&entity_type, shard, eav_query, ddb, registry).await
        }
        QueryPlan::FtsSearch { term, attr_ids } => {
            eav_fts_search(&term, &attr_ids, eav_query, ddb, registry).await
        }
        QueryPlan::VaetReverseLookup { ref_entity_id, attr_id } => {
            eav_vaet_lookup(ref_entity_id, attr_id, eav_query, ddb, registry).await
        }
        QueryPlan::AsOfSnapshot { entity_id, as_of_tx } => {
            eav_pull_as_of(entity_id, as_of_tx, eav_query.select(), ddb, registry).await
        }
    }
}
```

#### Paso 7 — Result Assembly (Reconstituir Entidades)

```rust
/// Convierte datoms crudos de DynamoDB en rows de negocio.
/// Aplica OutputCast (KPI aggregation, TIMESERIES bucketing, PIE grouping).
fn assemble_results(
    datoms: Vec<Datom>,
    plan: &EavQueryPlan,
    registry: &AttributeRegistry,
) -> EavResultChunk {
    // 1. Agrupar datoms por entity_id
    // 2. Para cada entity: tomar max(tx) por attr_id donde op=true
    // 3. Convertir a HashMap<String, Value> (row de negocio)
    // 4. Aplicar OutputCast:
    //    KPI       → StreamAggregator (SUM/COUNT/AVG/MIN/MAX) — 1 fila
    //    TIMESERIES → epoch bucketing por intervalo
    //    PIE/BUBBLE → GROUP BY dimensiones
    //    TABLE      → rows directas
}
```

#### Paso 8 — Normalización (Equivalente al Normalizer Clojure)

```rust
/// Garantiza que TODOS los campos del contrato gRPC estén presentes.
/// Mismo rol que metres.janus.normalizer en Clojure pero en Rust.
fn normalize_chunk(chunk: EavResultChunk, query_ctx: &QueryContext) -> QueryResponse {
    QueryResponse {
        status:     build_status(&chunk),
        metadata:   build_metadata(&chunk, query_ctx),
        pagination: build_pagination(&chunk, query_ctx),
        rows_json:  build_rowset(&chunk),
        viz_meta:   build_viz_meta(&chunk),
        links:      build_hateoas_links(&chunk, query_ctx),
        query_key:  query_ctx.query_key.clone(),
    }
}
```

---

## MÓDULO II: El Contrato FlatBuffers (Schema Completo)

### II.1 — Archivo `eav_query_plan.fbs`

```flatbuffers
// Contrato interno Janus → Metri EAV
// Compilar con: flatc --rust eav_query_plan.fbs

namespace metres.eav;

// ── Tipos de operación de filtro ─────────────────────────────────────────────
enum FilterOp : byte {
  And = 0,
  Or,
  Eq,
  Neq,
  Gt,
  Gte,
  Lt,
  Lte,
  In,
  NotIn,
  Contains,      // substring exacto
  StartsWith,
  Fuzzy,         // Damerau-Levenshtein ≤ threshold
  IsNull,
  IsNotNull,
}

// ── Tipos de valor en filtros ─────────────────────────────────────────────────
union FilterValue {
  StringVal,
  U64Val,
  F64Val,
  BoolVal,
  StringListVal,
}
table StringVal    { v: string; }
table U64Val       { v: uint64; }
table F64Val       { v: double; }
table BoolVal      { v: bool; }
table StringListVal{ v: [string]; }

// ── Nodo de filtro (árbol recursivo) ─────────────────────────────────────────
table FilterNode {
  op:       FilterOp;
  field:    string;         // nombre del atributo (ej: "status", "tenant_id")
  value:    FilterValue;    // valor a comparar
  children: [FilterNode];  // para AND / OR
  fuzz_threshold: byte = 1; // Levenshtein threshold para Fuzzy
}

// ── Sort ──────────────────────────────────────────────────────────────────────
enum SortDirection : byte { Asc = 0, Desc = 1 }
table SortSpec {
  field:     string;
  direction: SortDirection;
}

// ── OutputCast ────────────────────────────────────────────────────────────────
enum OutputCast : byte {
  Table = 0,
  Kpi,
  Timeseries,
  Pie,
  Bubble,
  CsvExport,
}

// ── TimeFrame ─────────────────────────────────────────────────────────────────
enum TimeInterval : byte {
  Minute = 0, Hour, Day, Week, Month, Quarter, Year
}
table TimeFrame {
  start_ts: uint64;   // epoch ms
  end_ts:   uint64;   // epoch ms
  interval: TimeInterval = Day; // para TIMESERIES bucketing
}

// ── Métricas y Dimensiones ───────────────────────────────────────────────────
enum Aggregation : byte {
  Sum = 0, Count, Avg, Min, Max, CountDistinct
}
table MetricSpec {
  field:       string;
  aggregation: Aggregation;
  alias:       string;        // nombre de la columna en el resultado
}
table DimensionSpec {
  field:    string;
  interval: TimeInterval = Day; // solo para dimensiones temporales
  alias:    string;
}

// ── Plan Principal ────────────────────────────────────────────────────────────
table EavQueryPlan {
  // Identidad y seguridad (inyectados por Janus — no bypasseables)
  tenant_id:         string (required);
  entity:            string (required);

  // Proyección
  select:            [string];    // vacío = pull [*]

  // Filtrado (árbol AND con nodos de seguridad ya inyectados)
  where_root:        FilterNode;

  // Orden y paginación
  order_by:          [SortSpec];
  limit:             uint32 = 100;
  cursor:            string;      // CompositeCursor opaco (Base64+MessagePack)

  // Routing analítico
  output_cast:       OutputCast = Table;
  metrics:           [MetricSpec];
  dimensions:        [DimensionSpec];
  time_frame:        TimeFrame;

  // Time-travel
  as_of_tx:          uint64 = 0;  // 0 = latest

  // Optimistic locking
  entity_version:    uint64 = 0;  // 0 = sin locking (bulk import)

  // Metadata interna
  query_fingerprint: uint64;      // Hash del plan para cursor validation
  query_id:          string;      // UUID para trazabilidad OTel
}

root_type EavQueryPlan;
```

---

## MÓDULO III: Comparativa con el Contrato EDN Anterior

| Aspecto | EDN (Clojure actual) | FlatBuffers (Rust nuevo) |
|:--|:--|:--|
| Parse time | ~15μs (textual, GC pressure) | < 500ns (zero-copy) |
| Tipo de campo `output_cast` | Keyword `:KPI` (string) | Enum byte (1 byte) |
| Validación de schema | Runtime (ex-info) | Compile-time (flatc) |
| Backward compatibility | Manual (cond keys) | Automático (tabla versionada) |
| Tamaño en wire | ~800B (texto EDN) | ~150B (binario compacto) |
| Soporte de cursors binarios | Imposible (string encode) | Nativo (bytes field) |
| Debuggability | Alto (legible) | Medio (flatc --json para debug) |

> [!TIP]
> Para debugging en desarrollo, el módulo Janus incluye una función `plan_to_json(plan: &EavQueryPlan) -> String` que serializa el plan a JSON legible usando `serde_json`. Nunca se activa en producción (compilado con `#[cfg(debug_assertions)]`).

---

## MÓDULO IV: Crates Necesarios

```toml
[dependencies]
# FlatBuffers — contrato interno Janus → Metri EAV
flatbuffers = "23.5"

# gRPC — contrato externo con cliente
tonic       = "0.12"
prost       = "0.13"

# Async runtime
tokio       = { version = "1", features = ["full"] }

# AWS DynamoDB
aws-sdk-dynamodb = "1.x"
aws-config       = "1.x"

# Cursor codec
rmp-serde = "1"        # MessagePack
base64    = "0.22"

# HMAC token verification
hmac      = "0.12"
sha2      = "0.10"

# Telemetría
tracing             = "0.1"
tracing-subscriber  = "0.3"
opentelemetry       = "0.23"

# ULID
ulid = "1"

[build-dependencies]
# Genera structs Rust desde eav_query_plan.fbs en tiempo de compilación
flatc-rust = "0.2"
```

---

## Próximos Pasos

1. **`eav_query_plan.fbs`** — Crear el archivo schema y configurar `build.rs` para compilar con `flatc`.
2. **`plan_selector.rs`** — Implementar la lógica de selección de índice (Paso 5).
3. **`filter_compiler.rs`** — Portar el `filter-compiler.clj` a Rust con soporte completo de `FilterOp`.
4. **`abac.rs`** — Portar los nodos ABAC (tenant + scope) desde `abac-clauses.clj`.
5. **Integration test** — Query end-to-end: `QueryRequest` → `EavQueryPlan` → DynamoDB → `QueryResponse`.
