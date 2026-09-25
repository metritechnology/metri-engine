# Plan — DynamoDB TTL Key-Value Cache for the Janus Query Layer

> **English summary:** Design and implementation plan for a distributed read-through cache backed by a dedicated DynamoDB table with native TTL, inserted in the Janus query layer (OLTP + OLAP channels). Cache keys are SHA-256 digests of the *compiled* AST (post-ABAC injection) plus resolved time window and schema fingerprint, so correctness (tenant isolation, ABAC, FLS) holds by construction. Ports live in `domain`, the adapter in `infrastructure`, wiring in the composition root. Rollout is **shadow-first**: a zero-cost mode measures the real hit rate in production before a single RCU/WCU is spent; go/no-go is arithmetic against the cost model (§9). Zero proto / FBS / error-catalog changes.

- **Estado:** **implementado (código de F0-F3) · 2026-09-25** — pendientes solo los gates operativos: deploy en `shadow` (Gate ROI-1), canary `ddb` (Gate ROI-2), T5/T6. Ver §12 para el runbook y `tests/e2e/cache_verify.py` para las suites de producción.
- **Alcance:** `src/janus/`, `src/domain/protocols.rs`, `src/codice/registry.rs`, `src/infrastructure/`, `src/grpc/server.rs`, `src/eav/writer/cache_policy.rs`, `template.yaml`, `scripts/dev/reset_local.py`, docs.
- **Verificado contra el código:** 25 de septiembre de 2026. Todas las referencias `archivo:línea` fueron verificadas.

---

## 0. TL;DR de decisiones

| # | Decisión | Resumen |
|---|----------|---------|
| D1 | **Qué se cachea** | La salida del executor (`run_oltp_query_fbs` para OLTP; `QueryResults` de Athena para OLAP) — lo caro y user-independiente. Todo post-proceso dependiente del llamador (FLS, overlays, redacción de columnas) queda **fuera** y corre en cada request. |
| D2 | **Clave** | `SHA-256` del JSON canónico del **AST compilado post-ABAC** + ventana temporal resuelta + fingerprint del schema Códice + versión de namespace. La dependencia del resultado respecto a (tenant, usuario, scope) queda capturada *by construction*. |
| D3 | **Expiración** | Doble: `expires_at` lógico (verificado por la app en cada lectura) + atributo `ttl` físico de DynamoDB (`expires_at + 1 h`). El TTL de DDB **nunca** es mecanismo de corrección. |
| D4 | **Invalidación por escritura** | Fase 3: sello de *generación* por tenant (`GEN#QC#<tenant>`, contador atómico `UpdateItem ADD 1`) embutido en el embudo único de invalidación post-commit (`eav/writer/cache_policy.rs`). Antes de Fase 3, el TTL acota el staleness — mismo contrato que hoy documentan las cachés de proceso. |
| D5 | **Arquitectura** | Puerto `IQueryCache` + `QueryCacheInvalidator` en `domain/protocols.rs`; adaptador `DynamoKvCache` en `infrastructure`; Null Object `NoopQueryCache`; orquestación en `janus::cache` (frontend + gates + key builder); wiring en `grpc/server.rs`. |
| D6 | **Punto de inserción** | Dentro de `janus/router/oltp.rs` y `olap.rs`, alrededor del executor — **no** en el handler gRPC ni en `process_single_query` como caja negra. |
| D7 | **Rollout shadow-first** | `QUERY_CACHE_MODE=shadow`: calcula claves y registra el hit rate *candidato* **sin tocar DynamoDB** (cero RCU/WCU). Los gates de fase (§12) comparan ese hit rate contra el modelo de costo (§9) — la decisión de encender es aritmética, no intuición. |
| D8 | **Presupuesto de latencia** | El lookup añade un round trip síncrono solo en el miss path: p50 +5-10 ms, p99 ≤ +50 ms (§8.4). El hit **ahorra** 50-300 ms (OLTP) o 0,5-5 s (OLAP). Ejecución especulativa en paralelo (miss ≈ +0 ms) queda como palanca F4, no v1. |
| D9 | **Verificación en producción (canary + diferencial + drill de rollback)** | Las pruebas reales (§11.5) corren contra el motor de producción sobre un **tenant canario** con datos sembrados: smoke post-deploy (T0), equivalencia diferencial contra goldens (T1), ABAC/FLS en hit (T2), read-your-write (T3), compartimiento multi-instancia (T4), carga acotada (T5) y un **drill de rollback** que valida el plan B antes de necesitarlo (T6). Rollout por allowlist de tenant. |

---

## 1. Contexto y evidencia

### 1.1 Por qué una caché distribuida, y por qué ahora

1. **El staleness multi-instancia ya es un problema conocido y documentado.** Las cachés actuales son *de proceso* (`Lazy<RwLock<HashMap>>`): `EAV_CACHE` (60 s, `eav/reader/pull.rs:41-48`) y `AEVT_SCAN_CACHE` (15 s, `eav/reader/query.rs:19-37`). Sus comentarios registran el incidente 2026-09-23 (1,5 h de retraso leído por otra instancia Lambda) y la decisión de que "el read-your-write entre instancias se garantiza por TTL". Con la caché en DynamoDB, **todas** las instancias comparten el mismo estado: el miss costoso (fan-out EAV / ejecución Athena) se paga una vez por clave, no una por instancia.
2. **El canal OLAP es el más caro.** Cada query OLAP dispara `start_query` + polling de resultados (`janus/router/olap.rs:129-161`) sobre Parquet/S3 vía Athena; `docs/architecture/MEDICION_COSTO_OLAP.md` y `PLAN_COSTO_OLAP.md` documentan esa presión. Una caché de resultados con TTL elimina la ejecución completa en hit.
3. **El contrato ya reservó el lugar.** `QueryMetadata.cache_hits` (proto `metri.proto:634`) y `cache_ttl_seconds` (`metri.proto:639`, "Seconds remaining before this result becomes stale") existen desde el diseño y hoy llegan hardcodeados a 0 (`janus/normalizer/query.rs:46`; mapeo en `grpc/handlers/query_support.rs:362,368-371`). Este plan los activa — **cero cambios de proto**.
4. **DynamoDB es el único almacén compartido disponible.** La VPC con Valkey fue eliminada deliberadamente (nota en `template.yaml:397-417`); añadir ElastiCache resucitaría ese costo y complejidad. DDB ya tiene precedentes de TTL (`MetriSchemasTable`, `template.yaml:637-640`).

### 1.2 Ganchos existentes que el diseño reutiliza (DRY)

| Gancho | Ubicación | Uso en este plan |
|---|---|---|
| `analytics_request_to_json(&AnalyticsRequestT) -> Value` | `janus/router/translator.rs:117` | Serialización canónica para construir la clave (no se inventa otra). |
| `compile_ast_fbs` / `compile_ast_internal` | `janus/ast_compiler.rs:271-561` / `:85-265` | La clave se calcula **sobre el AST ya compilado** (tenant + ABAC inyectados). |
| `DynamoClient` genérico por tabla | `infrastructure/dynamodb.rs:23-27` (`get_item/put_item/delete_item/query` toman `table_name`) | El adaptador reusa el cliente; solo se añaden `batch_get_item` y `update_item_add` (genéricos, reutilizables). |
| `map_sdk_error(e, code, table)` | `infrastructure/dynamodb.rs:334-352` | Mapeo de errores del adaptador — sin códigos nuevos (familia `INFRA_DDB_*` ya existe en el catálogo). |
| Embudo único de invalidación post-commit | `eav/writer/cache_policy.rs:35-63` (`invalidate_entity_caches`, usada por `transact.rs:555-572`; variante bulk `oltp_channel.rs:213-214`) | Fase 3: el bump de generación vive ahí — un solo lugar, todos los writers (transact/bulk/composite) lo pagan. |
| Convención de modos `*_MODE` | `ATHENA_MODE` (`server.rs:95-99`), `KINESIS_MODE`, `S3_MODE`, `SQS_MODE`, `EVENTBRIDGE_MODE` | `QUERY_CACHE_MODE=off|shadow|ddb` (default `off`): el motor arranca sin la tabla (invariante "el engine levanta sin AWS", `docs/components/infrastructure.md`). |
| Exclusión de entidades con overlay | Trait `RowOverlay` con `entity()` (`aegis/oltp/executor.rs:28-33`; hoy `QuotaUsageOverlay` sobre `domain_quota`, `server.rs:47-50`) | Gate de bypass: las entidades con overlay registrado **nunca** se cachean (la lista se deriva del registry, no se duplica a mano). |
| Fakes para "cacheado no toca storage" | `cedar/tests/fakes.rs` + `test_cache_hit_prevents_db_query` (`cedar/tests/principal_graph.rs:56`) | Patrón de test replicado para `IQueryCache`. |
| `reset_local.py` `TABLES_CONFIG` | `scripts/dev/reset_local.py:6-90` | Alta de la tabla local con `TimeToLiveAttribute: "ttl"`. |
| Fingerprints SHA-256 en Códice | `sha2` ya usado para fingerprints (`Cargo.toml:38`) | `CodeRegistry::model_fingerprint()` memoizado en `build()` (§4.6). |

### 1.3 Flujo afectado (verificado)

```
gRPC Query (server-streaming)
  └─ grpc/handlers/query.rs:248 ─▶ janus::router::process_single_query   (router/mod.rs:131-173)
       ├─ OLTP: oltp.rs::execute_oltp_query
       │    1. compile_ast_fbs        (ABAC + tenant inyectados)   ← D2 clave aquí
       │    2. explain_plan? → dry-run (sin ejecutar)               ← bypass
       │    3. executor.run_oltp_query_fbs → {data,total,pagination} ← D1 caché aquí
       │    4. unpack + apply_output_cast_fbs + post_processor       ← SIEMPRE por request
       │    5. chunk body + normalize_chunk (metadata)               ← §8 wiring cache_hits
       └─ OLAP: olap.rs::execute_olap_query
            1. compile_ast_internal + compile_athena_sql             ← D2 clave = hash(SQL+ventana)
            2. security gate + time_frame + Glue db
            3. athena.start_query → get_query_results → QueryResults  ← D1 caché aquí
            4. strip cols + comparaciones + paginación + post_processor ← SIEMPRE por request
            5. chunk body + normalize_chunk
```

**Por qué el resultado depende del usuario y eso NO rompe el diseño:** la dependencia del usuario entra por dos vías.
- *Pre-ejecución (ABAC):* `compile_ast_fbs` inyecta filtros `owner_field = user_id`, geo-IN, denegación por scope NONE (`ast_compiler.rs:329-511`). Al hashear el AST **compilado**, dos usuarios con scopes distintos producen claves distintas (aislamiento por construcción); dos usuarios del mismo tenant con scope idéntico comparten entrada (hit rate de dashboards) — exactamente la semántica correcta.
- *Post-ejecución (FLS/roles):* `redact_sensitive_attributes` (`post_processor.rs:153-211`) y la omisión de `password_hash` para no-`system-bff` (`query_support.rs:282-284`) se ejecutan **después** del executor, fuera del perímetro cacheado, en cada request. La entrada cacheada contiene el output crudo del executor y cada llamador aplica su propia redacción. **Este es el invariante de carga del diseño: nada que dependa del llamador se ejecuta antes del punto de caché.**

---

## 2. Objetivos / no objetivos

**Objetivos**
1. Caché KV distribuida (compartida entre instancias Lambda) para resultados del read path Janus, OLTP y OLAP.
2. Aislamiento tenant + ABAC correcto *by construction* (clave = AST compilado), FLS correcto por perímetro (post-cache).
3. Expiración: TTL lógico verificado en lectura + TTL físico de DynamoDB; invalidación por escritura (fase 3) sin tocar los writers uno a uno.
4. **Medir antes de gastar:** el hit rate candidato se mide en producción (modo shadow, costo cero) y la decisión de encender cada canal es un gate aritmético contra §9.
5. Fail-open en disponibilidad (cualquier error de caché ⇒ bypass + warn, la query nunca falla por la caché), fail-closed en corrección (entrada vencida/mal firmada/generación distinta ⇒ miss).
6. Motor arrancable sin la tabla (`QUERY_CACHE_MODE=off` default) — sin regresión para desarrollo/CI.
7. Activar `cache_hits` / `cache_ttl_seconds` del contrato existente.
8. Latencia acotada y presupuestada: el miss path no puede añadir más de un round trip a DynamoDB (D8).

**No objetivos**
1. Caché de escrituras, de sesiones, de principals o de cuotas (esas ya tienen su mecanismo).
2. Comprimir el payload (msgpack) — v1 en JSON por inspeccionabilidad; palanca de tuning (F4).
3. Time-travel (`_history`, `AsOfSnapshot`) — bypass en v1 aunque en teoría es cacheable (el pasado no cambia); palanca futura.
4. Single-flight anti-stampede y ejecución especulativa in-process — v1 los tolera (PutItem idempotente, último-gana); F4.
5. Cambios en `list_entities_impl` / `explore` OLTP directo (`explore.rs:315`), que hoy bypassan el router — se documenta como limitación conocida.

---

## 3. Decisiones de diseño y alternativas

### D1 — Qué se cachea: salida del executor, no el chunk final

| Alternativa | Veredicto | Razón |
|---|---|---|
| **(elegida) Output del executor** (`{data,total,pagination}` OLTP; `QueryResults` OLAP) | ✅ | User-independiente, es el costo dominante (fan-out EAV + hidratación + fórmulas + relaciones / ejecución Athena). El post-proceso (FLS, labels por llamador, overlays, viz_ext) es barato y dependiente del llamador → queda fuera. |
| Chunk final (`QueryChunk.body`) cacheado por usuario | ❌ | La clave tendría que incluir `user_id`+roles (el hit rate muere) o filtrar post-hit (complejidad). Además congela `viz_ext`/`metadata` generados por request. |
| Solo caché física (más `EAV_CACHE`/`AEVT_SCAN_CACHE` en DDB) | ❌ | No evita la ejecución OLAP (Athena) ni la agregación/fórmulas; granularidad fina = más items y más claves; los datoms cambian más que los resultados agregados. |
| Migrar `EAV_CACHE` a DDB | ❌ fuera de alcance | Otra tarea (item por entidad, churn alto); este plan no la toca. |

### D2 — La clave: hash canónico del AST compilado + ventana resuelta + schema

Payload canónico (serde_json `Value` usa `BTreeMap` por defecto → claves ordenadas → serialización determinista):

```json
{
  "ns":     "qc1",
  "ch":     "oltp",
  "tenant": "<tenant_id>",
  "ast":    <analytics_request_to_json(ast_compilado)>,
  "win":    [start_ms, end_ms],
  "schema": "<sha256 del modelo Códice de la entity>"
}
```

- `ns` (`CACHE_KEY_VERSION`): bump manual cuando cambie la semántica de canonicalización — invalida todo el namespace sin tocar la tabla.
- `win`: la ventana temporal **resuelta** (`resolve_fbs_time_frame` es matemática de fechas pura y barata). Esto neutraliza la volatilidad de `TODAY`/`LAST_N_DAYS`: el request cambia de clave cuando rota la ventana, no cuando vence el TTL. El executor re-resuelve internamente (coste despreciable, sin cambios).
- `schema`: fingerprint del modelo vía `CodeRegistry::model_fingerprint()` — memoizado **una vez por arranque**, no por request (§4.6). Un cambio de modelo en Códice (label templates, tipos) cambia la clave → invalidación automática por deploy de modelos.
- OLAP: `ast` se sustituye por `sql` (el SQL ya contiene tenant + ABAC por el gate `ast_contains_tenant`, `olap.rs:65-77`) + `db` Glue.
- Digest: `SHA-256` (crate `sha2` ya presente para fingerprints de Códice). PK = `QC#v1#<hex64>`.

**Contramedida defensiva (Zero-Trust):** el item guarda `t` (tenant) y la lectura verifica `entry.tenant == tenant_id` antes de servir. Un hipotético error de canonicalización no puede cruzar tenants.

### D3 — Expiración dual

- `exp` (N): epoch-secs lógico. La lectura compara `now < exp`; vencido ⇒ miss (el item puede seguir físicamente).
- `ttl` (N): `exp + 3600`, atributo con TTL habilitado en la tabla. DynamoDB borra en horas (best-effort ≤ 48 h); es limpieza de costo, jamás corrección. Precedente idéntico: `MetriSchemasTable` y los comentarios de `reset_local.py:169-171` ("El TTL es limpieza física, no lógica de negocio").
- Jitter: `ttl_efectivo = base + rand(0..jitter)` (crate `rand` ya presente) para desincronizar vencimientos de dashboards consultados en ráfaga.

### D4 — Invalidación por escritura: sello de generación por tenant (fase 3)

1. Item de control: `PK = GEN#QC#<tenant>`, `v: N` (u64). Se incrementa con `UpdateItem SET v = v + :one` (atómico) — **una vez por commit** en el embudo único `invalidate_entity_caches` (`eav/writer/cache_policy.rs:35-54`), que ya ejecutan todos los writers (transact `Immediate`, bulk al final del lote vía `oltp_channel.rs:213-214`).
2. Escritura de entrada: `put` copia la generación vigente (`gen`) al item.
3. Lectura: `BatchGetItem [entrada, GEN]` — un solo round trip; el item GEN se pide con lectura fuerte (`ConsistentRead: true`, soportado en BatchGetItem para tabla base) para acotar la ventana de visibilidad del bump al ~segundo.
4. Servicio solo si `entry.gen == gen_actual`. Cualquier escritura del tenant invalida lógicamente **todas** sus entradas sin borrar nada (las purga el TTL físico).

**Trade-offs declarados:** granularidad por tenant = conservador (una escritura invalida todo el tenant; correcto-first). Granularidad por `entity_type` se deja como tuning futuro con la salvedad documentada de que `select_tree` arrastra entidades relacionadas. La invalidación es *pronta pero best-effort*: existe una carrera acotada (leer gen=5 justo antes de que un commit lo suba a 6 sirve una entrada un instante vieja) — indistinguible del contrato TTL-only, y el TTL sigue siendo la cota dura.

### D5 — Arquitectura de puertos y adaptadores

```
┌─ domain (cero deps de infra) ─────────────────────────────┐
│  IQueryCache            QueryCacheInvalidator             │
│  CacheEntry / CacheGet / CachePut                         │
└──────────▲───────────────────────▲────────────────────────┘
           │ impl                  │ impl
┌──────────┴──────────┐  ┌────────┴─────────────────────────┐
│ infrastructure      │  │ janus::cache (caso de uso)       │
│  DynamoKvCache      │  │  QueryCacheFrontend (modos       │
│  DynamoGenInvalid.  │  │   Off|Shadow|Ddb + gates +       │
│  NoopQueryCache  ───┼──┤   degradación)                   │
│  (Null Object)      │  │  keys::CacheKeyBuilder (SHA-256) │
└─────────────────────┘  │  policy::QueryCachePolicy        │
                         └────────▲─────────────────────────┘
                                  │ usa
                     grpc/server.rs (raíz de composición, ADR-004)
                                  │ inyecta
                     janus/router/{oltp,olap}.rs (punto de inserción)
```

El **modo shadow es responsabilidad del frontend**, no del adaptador (§4.2): en shadow el lookup registra y devuelve siempre *miss* — la respuesta al cliente es byte-idéntica al modo `off`, por construcción. `IQueryCache` queda como puerto puro de almacenamiento.

### D6 — Inserción en el router, no en el handler

- `execute_oltp_query` ya tiene el AST compilado y el executor como parámetros (`oltp.rs:13-21`); envolver ahí la llamada al executor es quirúrgico.
- Cachear en `process_single_query` (caja negra) o en `query.rs` requeriría re-derivar canal + ABAC + redacción, duplicando lógica (violación DRY) o cachando chunks user-dependientes (D1 rechazada).
- `run_query_pipeline` (`router/mod.rs:59-126`, hoy sin llamantes en producción) se actualiza por consistencia de firma.

### D7 — Rollout shadow-first (medir antes de gastar)

El ROI de la caché depende por completo de una cifra que nadie conoce hoy: **la tasa de repetición real de queries** dentro de la ventana de TTL. El plan no la asume — la mide:

1. `QUERY_CACHE_MODE=shadow` despliega el key builder y el frontend en producción **sin tabla DynamoDB y sin una sola unidad de capacidad gastada**.
2. El frontend mantiene por instancia un registro acotado de claves recientes con su `exp` (mismo patrón `Lazy<RwLock<HashMap>>` + expulsión 20% que `EAV_CACHE`, capacidad 10 000). Si una clave solicitada sigue viva en ese registro ⇒ `SHADOW_HIT`; si no ⇒ `SHADOW_MISS`. Ambos casos ejecutan la query normalmente (la respuesta nunca cambia).
3. Los spans JSON (`cache_status = SHADOW_HIT|SHADOW_MISS`, §8.2) se agregan en CloudWatch Insights (§8.3) durante ≥ 1 semana con tráfico real de dashboards.
4. **Semántica de la medida:** el registro es por instancia, así que el shadow mide solo repeticiones *dentro de la misma instancia* — un **límite inferior conservador** del hit rate distribuido (la tabla DDB además agruparía aciertos entre instancias). Si el shadow ya supera el umbral, el real lo superará.

El gate de cada fase (§12) compara esa cifra contra el breakeven de §9. Si el shadow no supera el umbral, el proyecto se detiene en F0 con costo cero — esa es la definición de no-apostar.

### D8 — Presupuesto de latencia del miss path

El lookup es un `await` síncrono entre compilar el AST y ejecutar. El presupuesto declarado:

| Escenario | Delta de latencia | Origen |
|---|---|---|
| **Hit OLTP** | **−50 a −300 ms** | Se evita fan-out EAV + hidratación + fórmulas + relaciones. |
| **Hit OLAP** | **−0,5 a −5 s** | Se evita `start_query` + polling Athena. |
| Miss (cualquier canal) | **+5-10 ms p50, ≤ +50 ms p99** | Un round trip GetItem/BatchGetItem intra-región (~5-15 ms) + deserialización JSON (µs-ms según tamaño). |
| Miss en shadow | +0 ms | Sin red: solo cálculo de clave + registro en memoria. |
| Bypass (gates §7) | +0 ms | La evaluación es previa al lookup y en memoria. |

Decisión v1: **aceptar el delta del miss** — es un orden de magnitud menor que la ejecución que lo rodea y solo aplica a canales habilitados. La alternativa (arrancar la ejecución especulativamente en paralelo con el lookup y descartar el perdedor, miss ≈ +0 ms al costo de trabajo desperdiciado en cada miss) queda especificada como palanca F4 con su trade-off explícito. El DoD (§15) fija "p95 sin regresión > 15 ms en canales cacheados" como verificación.

### Alternativas de backend consideradas

| Backend | Veredicto | Razón |
|---|---|---|
| **DynamoDB dedicada + TTL** | ✅ | Único almacén compartido sin VPC; ya operado (3 tablas); PAY_PER_REQUEST; TTL nativo con precedente; costo por lookup ~1-1,5 RRU frente a decenas de RRU del fan-out EAV o segundos de Athena (§9). |
| ElastiCache/Valkey | ❌ | La VPC fue eliminada a propósito (`template.yaml:397-417`); resucitarla por una caché invierte esa decisión. El puerto `IQueryCache` deja la puerta abierta (misma anticipación que `cedar/ports.rs` documenta para principals). |
| Solo in-process (moka/LRU) | ❌ | No resuelve multi-instancia (el problema documentado); añade dependencia nueva (Cargo.toml hoy no tiene crates de caché — se mantiene así). |
| Athena workgroup result reuse | ❌ como sustituto | Evita re-ejecutar pero sigue pagando `start_query` + polling; puede coexistir como optimización de infra, no reemplaza la caché de capa Janus. |

---

## 4. Diseño del código

### 4.1 Puerto de dominio — `src/domain/protocols.rs` (añadir ~70 líneas)

Siguiendo la granularidad ISP de `ISessionStore`/`IQueryEngine` (protocolos pequeños):

```rust
/// Entrada cacheada del read path. `expires_at` en epoch-secs (lógica);
/// la expiración física la hace DynamoDB TTL sobre el atributo `ttl`.
pub struct CacheEntry {
    pub payload:      serde_json::Value,
    pub expires_at:   i64,
    pub generation:   Option<u64>,   // Fase 3
    pub stored_at_ms: i64,
}

pub struct CachePut {
    pub tenant_id: String,
    pub key:       String,           // PK completo "QC#v1#<sha256>"
    pub payload:   serde_json::Value,
    pub ttl_secs:  u64,              // ya con jitter aplicado
}

/// Puerto de lectura/escritura de la caché de consultas (L4/DIP).
/// Implementaciones: `infrastructure::DynamoKvCache`, `NoopQueryCache`, fakes de test.
#[async_trait]
pub trait IQueryCache: Send + Sync {
    /// `Ok(None)` = miss. El llamador decide la degradación ante `Err`.
    async fn get(&self, key: &str, tenant_id: &str) -> DomainResult<Option<CacheEntry>>;
    /// Best-effort; la entrada nunca debe ser observable como error de query.
    async fn put(&self, entry: &CachePut) -> DomainResult<()>;
}

/// Puerto de invalidación (ISP: el write path solo necesita esto).
#[async_trait]
pub trait QueryCacheInvalidator: Send + Sync {
    /// Invalida lógicamente todas las entradas del tenant (bump de generación).
    async fn invalidate_tenant(&self, tenant_id: &str) -> DomainResult<()>;
}
```

Errores: el adaptador reusa `map_sdk_error` (familia `INFRA_DDB_*` del catálogo). **Sin variantes nuevas** → sin tocar `error_catalog.toml` → sin regenerar docs de referencia por errores.

### 4.2 Caso de uso — `src/janus/cache/` (nuevo módulo)

```
src/janus/cache/
├── mod.rs        // QueryCacheFrontend (modos Off|Shadow|Ddb) + gates + LookupOutcome
├── keys.rs       // CacheKeyBuilder (canonicalización + SHA-256)
└── tests.rs      // unit tests (ver §11)
```

```rust
/// Política resuelta una vez en la raíz de composición (ADR-004).
pub struct QueryCachePolicy {
    pub mode:            CacheMode,          // Off | Shadow | Ddb
    pub channels:        ChannelFlags,       // { oltp, olap }
    pub ttl_base_secs:   u64,                // default 60 (OLTP) / 300 (OLAP)
    pub ttl_jitter_secs: u64,                // default 10
    pub max_item_bytes:  usize,              // default 256 * 1024 (límite DDB: 400 KB)
}

pub enum LookupOutcome {
    Hit   { entry: CacheEntry },             // fresca + generación válida + tenant verificado
    Miss,                                    // ejecutar y poblar (en Shadow: siempre, tras registrar)
    Bypass { reason: BypassReason },         // no elegible (§7); ejecutar sin poblar
}

pub struct QueryCacheFrontend {
    mode:    CacheMode,
    backend: Arc<dyn IQueryCache>,           // NoopQueryCache salvo en Ddb
    policy:  QueryCachePolicy,
    /// Solo en modo Shadow: claves recientes con exp, para medir hit rate
    /// candidato sin red. Límite inferior del hit rate distribuido (D7).
    shadow:  ShadowKeyRing,                  // RwLock<HashMap>, cap 10_000, expulsión 20%
    /// Claves medida > cap recientemente: evita re-serializar payloads
    /// gigantes en cada request (G4-negativo, §7).
    oversize_seen: OversizeKeyRing,          // HashSet acotado (cap 1_000)
}

impl QueryCacheFrontend {
    /// Gate único de elegibilidad (§7). Pure function testeable.
    pub fn evaluate(&self, req: &CacheCandidate) -> LookupOutcome { /* gates */ }

    /// Off  → Bypass siempre.
    /// Shadow → registra la clave en el ring, emite SHADOW_HIT/SHADOW_MISS,
    ///           y devuelve SIEMPRE Miss (la respuesta no cambia jamás).
    /// Ddb   → get + verificaciones de corrección: expiración lógica,
    ///           tenant defensivo y (Fase 3) generación. Fallo ⇒ Miss/Bypass.
    pub async fn lookup(&self, key: &str, tenant: &str) -> LookupOutcome { /* ... */ }

    /// put con cap de tamaño (saltado si la clave está en oversize_seen);
    /// errores se tragan con `warn!` (fail-open). En Shadow es no-op.
    pub async fn store(&self, put: CachePut) { /* ... */ }
}
```

Firmas de las funciones clave del router (cambio mínimo de firma — un parámetro):

```rust
// janus/router/oltp.rs — dentro de execute_oltp_query, reemplaza la llamada directa:
//   let raw = executor.run_oltp_query_fbs(&tenant, &ast_ir).await;
// por el bloque cache-aside:
let cache_key = cache.build_oltp_key(&ast_ir, &resolved_window, &schema, &tenant);
match cache.lookup(&cache_key, &tenant).await {
    LookupOutcome::Hit { entry } => raw = entry.payload,                       // Value idéntico
    _ => {
        raw = executor.run_oltp_query_fbs(&tenant, &ast_ir).await?;            // camino actual
        cache.store(CachePut::oltp(&cache_key, &tenant, &raw, &policy)).await; // spawn interno
    }
}
// El resto de execute_oltp_query (unpack, cast, post_processor, chunk) queda intacto.
```

OLAP es simétrico alrededor de `athena.start_query/get_query_results` (`olap.rs:129-161`), cacheando `{exec_id, columns, rows}`; `exec_id` se reinyecta en el body para trazabilidad.

### 4.3 Adaptador — `src/infrastructure/dynamodb_query_cache.rs` (nuevo)

```rust
/// Caché KV sobre tabla DynamoDB dedicada con TTL nativo.
/// Reusa `DynamoClient` (mismo cliente HTTP, mismo mapeo de errores).
pub struct DynamoKvCache {
    ddb:   Arc<DynamoClient>,
    table: String,
    clock: fn() -> i64,          // inyectable para tests de expiración
}

#[async_trait]
impl IQueryCache for DynamoKvCache {
    async fn get(&self, key, tenant) -> ... {
        // Fase 3: BatchGetItem [entrada, GEN#QC#tenant] (1 round trip; gen ConsistentRead)
        // - item sin atributo `v`/`exp` vencido  ⇒ Ok(None)  (miss lógico)
        // - entry.t != tenant                     ⇒ Ok(None) + warn (defensa Zero-Trust)
        // - entry.gen != gen_actual               ⇒ Ok(None)  (invalidado por escritura)
    }
    async fn put(&self, entry: &CachePut) -> ... {
        // PutItem idempotente (último-gana). Item layout §5.
        // ttl = exp + 3600 (limpieza física DDB).
    }
}

/// Invalidador por generación (Fase 3): UpdateItem atómico ADD 1.
pub struct DynamoGenInvalidator { ddb: Arc<DynamoClient>, table: String }
#[async_trait]
impl QueryCacheInvalidator for DynamoGenInvalidator { /* UpdateItem SET v = v + 1 */ }
```

Ampliaciones genéricas a `src/infrastructure/dynamodb.rs` (reutilizables fuera de la caché):

```rust
impl DynamoClient {
    /// BatchGetItem con reintentos de UnprocessedKeys (análogo a batch_write_item:192-238).
    pub async fn batch_get_item(&self, table: &str, keys: Vec<HashMap<String, AttributeValue>>,
        consistent_read: bool) -> Result<Vec<Option<HashMap<String, AttributeValue>>>, DomainError>;

    /// UpdateItem atómico numérico (contador de generación).
    pub async fn update_item_add(&self, table: &str, pk: &str,
        attr: &str, delta: i64) -> Result<i64, DomainError>;
}
```

### 4.4 Wiring — raíz de composición (`src/grpc/server.rs`) y propagación

```rust
// server.rs (después de DynamoClient/ddb_client, ~L36):
let query_cache = match env QUERY_CACHE_MODE {           // default "off"
    "ddb"    => QueryCacheFrontend::new(CacheMode::Ddb,
        Arc::new(DynamoKvCache::new(Arc::clone(&ddb_client), env QUERY_CACHE_TABLE)),
        QueryCachePolicy::from_env()),
    "shadow" => QueryCacheFrontend::new(CacheMode::Shadow, Arc::new(NoopQueryCache),
        QueryCachePolicy::from_env()),
    _        => QueryCacheFrontend::disabled(),          // Off + NoopQueryCache
};
// Fase 3: DynamoGenInvalidator se inyecta además en EavWriter (embudo cache_policy.rs).
```

- `ServiceDeps` + `MetriGrpcService` ganan `pub query_cache: Arc<QueryCacheFrontend>` (junto a `athena_engine`, `service.rs:22-43`).
- El handler lo clona en el spawn por sub-query (mismo patrón que `exec_clone`, `query.rs:198-207`) y lo pasa a `process_single_query(&qk, &qm, &cedar, &exec, athena.as_ref(), explain_plan, &query_cache)` (`query.rs:248`, `explore.rs:261`).
- `process_single_query` / `execute_oltp_query` / `execute_olap_query` / `run_query_pipeline` añaden el parámetro `cache: &QueryCacheFrontend` (siempre presente; no `Option` — Null Object cuando está desactivado).
- `main.rs` no cambia (la caché no es global de proceso: respeta la composición existente; ADR-004).

### 4.5 Invalidación (Fase 3) — embudo único

```rust
// eav/writer/cache_policy.rs — extendido con dependencia inyectada:
pub struct PostCommitInvalidation {
    cache: Option<Arc<dyn QueryCacheInvalidator>>,   // None = modo off/shadow
}
pub async fn invalidate_entity_caches(tenant_id: &str, written: &[WrittenEntity<'_>],
                                      post: &PostCommitInvalidation) {
    // 1. Lo que ya hace hoy: expulsar EAV_CACHE + AEVT_SCAN_CACHE (intra-instancia).
    // 2. NUEVO: post.cache.invalidate_tenant(tenant_id) — un bump por commit (no por entidad).
}
```

- `EavWriter` recibe `PostCommitInvalidation` en la construcción (composition root); los sitios de commit existentes (`transact.rs:555-572`, `oltp_channel.rs:213-214`) ya llaman al embudo, así que **ningún writer cambia individualmente**.
- Dirección de dependencias correcta: `eav` (capa engine) importa un puerto de `domain` — nunca `infrastructure` (DIP, igual que `EntityReader` en `cedar/ports.rs:26-42`).
- Coste: 1 WRU por commit — despreciable frente a la `TransactWriteItems` que lo dispara.

### 4.6 Fingerprint de schema memoizado — `src/codice/registry.rs`

La clave incluye el fingerprint del modelo Códice (D2). Para no re-hashear el JSON del modelo en cada request:

```rust
impl CodeRegistry {
    /// SHA-256 del modelo serializado, calculado UNA vez por entity en
    /// `CodeRegistry::build()` y guardado junto al modelo. Costo por
    /// request: un clone de String.
    pub fn model_fingerprint(&self, entity: &str) -> Option<&str>;
}
```

Se computa para los ~60 modelos en arranque (cold start: +µs por modelo) — mismo patrón de cómputo único que ya hace `build()` para la coerción y ULIDs.

### 4.7 Puntos explícitamente NO tocados

- `src/janus/janus_ir_ast.fbs` y `src/janus/fbs.rs` — el contrato FlatBuffers no cambia → **los snapshots de `contract_tests.rs` no se regeneran** (ADR-003 intacto).
- `proto/metri.proto` — `cache_hits`/`cache_ttl_seconds` ya existen; solo se les da valor.
- `config/errors/error_catalog.toml` — sin códigos nuevos.
- Cachés existentes (`EAV_CACHE`, `AEVT_SCAN_CACHE`, `PrincipalCache`, `QuotaResolver`) — conviven; la caché de resultados es una capa superior y no las reemplaza.

---

## 5. Modelo de datos DynamoDB

### 5.1 Item de entrada

| Atributo | Tipo | Contenido |
|---|---|---|
| `PK` | S | `QC#v1#<sha256_64hex>` |
| `t` | S | tenant_id (verificación defensiva en lectura + debugging) |
| `ch` | S | `oltp` \| `olap` |
| `e` | S | entity (OLTP) o hash corto del SQL (OLAP) — debugging |
| `v` | S | payload JSON (output del executor) |
| `len` | N | bytes del payload (cap §6) |
| `exp` | N | epoch-secs lógico de expiración |
| `ttl` | N | `exp + 3600` — atributo con TTL habilitado (físico) |
| `gen` | N | generación del tenant al escribir (Fase 3) |
| `wa` | N | written_at epoch-ms |

Item de control (Fase 3): `PK = GEN#QC#<tenant>` · `v: N` (contador). Sin TTL (persistente).

### 5.2 Tabla — `template.yaml` (nuevo recurso `MetriQueryCacheTable`)

```yaml
  QueryCacheTableName:
    Type: String
    Default: metri-query-cache          # prod: metri-query-cache-<env> via parameter_overrides

  MetriQueryCacheTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Ref QueryCacheTableName
      AttributeDefinitions:
        - { AttributeName: PK, AttributeType: S }
      KeySchema:
        - { AttributeName: PK, KeyType: HASH }
      BillingMode: PAY_PER_REQUEST
      TimeToLiveSpecification:
        AttributeName: ttl
        Enabled: true                   # precedentes: MetriSchemasTable (L637-640)
      SSESpecification:                 # misma postura que el EAV (L541-544):
        SSEEnabled: true                # el payload duplica datos del EAV → KMS CMK
        KMSMasterKeyId: !Ref MetriKmsKey
      # Sin GSI (la invalidación es lógica por generación, no por borrado)
      # Sin PITR: es caché regenerable (ahorro vs. tablas de datos)
      DeletionPolicy: Delete
```

IAM (bloque `MetriEngineFunction.Policies`, junto a L758-773) — mínimo privilegio, **sin** Query/Scan/Transact:

```yaml
        - PolicyName: QueryCacheAccess
          PolicyDocument:
            Statement:
              - Effect: Allow
                Action: [dynamodb:GetItem, dynamodb:PutItem, dynamodb:DeleteItem,
                         dynamodb:BatchGetItem, dynamodb:UpdateItem]
                Resource: !GetAtt MetriQueryCacheTable.Arn
```

Env vars de la Lambda (L814-845): `QUERY_CACHE_TABLE: !Ref QueryCacheTableName` y `QUERY_CACHE_MODE` (ver rollout §12; arrancar con la tabla creada y modo `shadow` u `off` también es válido).

### 5.3 Local — `scripts/dev/reset_local.py` (`TABLES_CONFIG`, L6-90)

```python
"metri-query-cache-local": {
    "KeySchema": [{"AttributeName": "PK", "KeyType": "HASH"}],
    "AttributeDefinitions": [{"AttributeName": "PK", "AttributeType": "S"}],
    "TimeToLiveAttribute": "ttl",      # igual que metri-eav-local / metri-quota-local
},
```

Nota: DynamoDB Local **no** ejecuta el borrado por TTL — irrelevante: la expiración lógica (`exp`) la verifica la app en cada lectura, y `reset_local --recreate` borra la tabla completa.

### 5.4 Por qué una tabla exclusiva (y no un prefijo `QC#` en la tabla EAV)

1. **Blast radius del TTL.** La tabla EAV de producción no tiene TTL habilitado (solo `MetriSchemasTable`, `template.yaml:637-640`). Habilitarlo allí para la caché permitiría a DynamoDB borrar físicamente *datoms* si cualquier código escribiese `ttl` en un item de datos — pérdida silenciosa en el sistema de registro. En tabla exclusiva, el TTL solo puede borrar entradas de caché (peor caso: un miss).
2. **Invariante append-only.** La política IAM de la EAV excluye `DeleteItem` a propósito (ADR-001: datoms inmutables, time-travel). La caché tiene el modelo opuesto — sobrescribir y expirar. Cada tabla queda con su política de mínimo privilegio (caché: `Get/Put/Delete/BatchGet/Update`, sin `Query/Scan/Transact`).
3. **Aislamiento de throughput.** Ráfagas de `PutItem` de caché (p. ej. tras un deploy) no comparten el sobre de capacidad de las `TransactWriteItems` ACID del write path.
4. **Postura operativa distinta.** La caché es regenerable: `DeletionPolicy: Delete`, sin PITR, alarmas propias. La EAV es el sistema de registro: PITR on, retención.
5. **Costo neutro.** PAY_PER_REQUEST no cobra por tabla; reusa la KMS CMK existente. Patrón ya establecido en el repo (tabla-por-propósito: EAV, SequenceRegistry, Schemas).

El "single-table design" de ADR-001 refiere al modelo de datoms y sus GSIs EAVT/AEVT/AVET/VAET — no a que todo el motor viva en una tabla física.

---

## 6. Configuración (ADR-004: leída una vez, en la raíz de composición)

| Env | Default | Significado |
|---|---|---|
| `QUERY_CACHE_MODE` | `off` | `off` \| `shadow` \| `ddb`. `off` ⇒ Null Object (motor sin tabla, CI sin cambios). `shadow` ⇒ medición sin costo (D7). Convención `*_MODE` de `infrastructure.md`. |
| `QUERY_CACHE_TABLE` | `metri-query-cache-local` | Tabla KV (parámetro SAM en prod). |
| `QUERY_CACHE_TTL_OLTP_SECS` | `60` | Cota de staleness OLTP — alineada con `EAV_CACHE_TTL` (60 s), el umbral que el equipo ya aceptó tras el incidente 2026-09-23. |
| `QUERY_CACHE_TTL_OLAP_SECS` | `60` inicial | Arranca en 60 s; se sube a 300 s con datos reales del shadow y del gate ROI-2 (§12). |
| `QUERY_CACHE_TTL_JITTER_SECS` | `10` | Desincroniza expiraciones de ráfagas. |
| `QUERY_CACHE_MAX_ITEM_KB` | `256` | Cap de payload (límite duro DDB 400 KB/item). |
| `QUERY_CACHE_CHANNELS` | `oltp,olap` | Encendido por canal para rollout gradual. |
| `QUERY_CACHE_TENANT_ALLOWLIST` | *(vacío = todos)* | Lista de tenant IDs (separados por coma) habilitados para caché — **canary de producción** (§11.5). Evaluado como primer gate (G0, §7); vacío = sin restricción. |

Constantes sin env (menos knobs): capacidad del shadow ring 10 000, capacidad del ring oversize 1 000 — espejo de `EAV_CACHE`/`AEVT_SCAN_CACHE`.

Validación fail-fast en `QueryCachePolicy::from_env()` (TTL > 0, max ≤ 380 KB, canales conocidos, `ddb` exige tabla): mal config ⇒ `DomainError::InfraConfig001`, igual que `resolve_hmac_secret` (`domain/config.rs:102-126`).

---

## 7. Reglas de bypass (gates de elegibilidad)

Evaluadas en orden por `QueryCacheFrontend::evaluate`; cualquier bypass ⇒ ejecución normal + `BypassReason` en el span (nunca en la respuesta).

| # | Regla | Fuente de verdad (DRY) |
|---|---|---|
| G0 | Tenant fuera del allowlist canario — bypass (rollout gradual por tenant) | `QUERY_CACHE_TENANT_ALLOWLIST` (§6); primer gate evaluado |
| G1 | `explain_plan` — nunca | flag del request (`oltp.rs:41-64`) |
| G2 | Entidad con `RowOverlay` registrado — nunca (dato mutado en lectura: `QuotaUsageOverlay` sobre `domain_quota`) | `OltpExecutor::overlay_entities()` — nuevo accessor sobre `overlays: Vec<Arc<dyn RowOverlay>>` (`executor.rs:35-49`); la lista se deriva del registry, registrar un overlay nuevo excluye su entidad automáticamente (OCP) |
| G3 | Time-travel (`_history`, `AsOfSnapshot`) — bypass v1 | paths especiales del executor (`executor.rs:522-531`) |
| G4 | Payload serializado > cap — no se almacena; la clave se marca en `oversize_seen` (ring in-process, expira con el TTL) para **no re-serializar** en requests siguientes | `QueryCachePolicy.max_item_bytes` + `OversizeKeyRing` |
| G5 | Canal deshabilitado por política | `QUERY_CACHE_CHANNELS` |
| G6 | Request no-cacheable estructural (p. ej. cursor inválido ya validado) — follow-up del validador | `janus/validator.rs` |

Notas deliberadas:
- **Cursor/paginación: sí se cachea.** El cursor forma parte del request ⇒ parte de la clave ⇒ cada página es una entrada independiente. Consecuencia aceptada y listada en riesgos (§13, R9): una misma navegación puede leer la página 1 de una entrada casi vencida y ejecutar la página 2 fresca — ventana acotada por TTL, comparable al comportamiento actual de `AEVT_SCAN_CACHE`.
- **FTS (`search`): sí se cachea** — determinista dado el dato y el request.
- **Volatilidad temporal:** cubierta por D2 (`win` resuelto en la clave). Si el motor de fórmulas gana funciones volátiles tipo `NOW()` no ligadas a ventana, se añade un gate G7 con la lista de funciones volátiles definida junto al `FunctionRegistry` de `aegis/formula` (fuente única) — especificado, no implementado, hasta que exista tal función.

---

## 8. Observabilidad

### 8.1 Contrato gRPC (activación real)

- El router inserta en el body del chunk `cache: { "hit": bool, "remaining_secs": i64, "channel": "oltp"|"olap" }` **antes** de `normalize_chunk` — solo en modo `ddb` (en `shadow`/`off` el body no cambia).
- `janus/normalizer/query.rs::ensure_metadata` (hoy hardcodea `cache_hits: 0`, L46) pasa a leer `body.cache` en ambas ramas y escribir `metadata.cache_hits` (0/1) y `metadata.cache_ttl_seconds` (`remaining_secs`). Único lugar que escribe el metadata ⇒ DRY; `map_metadata` (`query_support.rs:339-373`) ya los mapea al proto — sin cambios.
- Semántica: `cache_hits=1` significa "el executor no se ejecutó"; `cache_ttl_seconds` = segundos restantes de frescura de esa entrada.

### 8.2 Spans y el detalle de `RUST_LOG` (sin esto, shadow no produce datos)

La Lambda corre hoy con `RUST_LOG=warn` (`template.yaml:822`) — un span `info` de la caché quedaría **silenciado** y el modo shadow no mediría nada. El plan exige ajustar explícitamente en `template.yaml`:

```yaml
RUST_LOG: !Sub "warn,metri_engine::janus::cache=info"
```

(span objetivo `janus.cache.*`, sin bajar el umbral del resto del motor):

```rust
#[tracing::instrument(name = "janus.cache.lookup",
    fields(tenant_id, cache_key = %key_short, cache_status = Empty,
           cache_channel, error.code = Empty), level = "info")]
// cache_status ∈ HIT | MISS | BYPASS(reason) | STALE | GEN_MISMATCH
//                | SHADOW_HIT | SHADOW_MISS      ← solo modo shadow
```

`store` instrumenta `janus.cache.store` con `payload_bytes`. Todos los logs de arranque/fail-open degradan a `warn` (ya visible).

### 8.3 Métricas desde CloudWatch (sin esperar a OTel)

`otel/tracer.rs` es no-op hoy ("FASE 2 TODO"); no se bloquea la medición por eso. Los spans JSON ya van a CloudWatch Logs; con **metric filters** sobre el LogGroup de la Lambda (`template.yaml:851-855`) se crean métricas custom:

```
Filter: { $.cache_status = "HIT" }        → Metric: MetriEngine/QueryCache/Hits[+1]
Filter: { $.cache_status = "MISS" }       → MetriEngine/QueryCache/Misses
Filter: { $.cache_status = "SHADOW_HIT" } → MetriEngine/QueryCache/ShadowHits
Filter: { $.cache_status = "SHADOW_MISS" }→ MetriEngine/QueryCache/ShadowMisses
Filter: { $.cache_status = "BYPASS" }     → MetriEngine/QueryCache/Bypass
```

Consulta Insights para el reporte del gate ROI (§12):

```
filter name = "janus.cache.lookup"
| stats count(*) as total,
        sum(cache_status like /HIT/) as hits
  by cache_channel, bin(1h)
```

**Alarmas mínimas del stack** (se agregan a `template.yaml` junto a la tabla):

| Alarma | Umbral inicial | Acción |
|---|---|---|
| `QueryCacheHitRateLow` | hit/(hit+miss) < breakeven de §9 durante 24 h en canal habilitado | Revisar TTL/tumbling de dashboards; considerar apagar canal |
| `QueryCacheLookupErrors` | `error.code` presente en `janus.cache.lookup` > 0,5 % de lookups | La caché degrada (fail-open); investigar DDB |
| `QueryCacheStoreOversize` | `BYPASS(oversize)` > 20 % de stores | Subir `QUERY_CACHE_MAX_ITEM_KB` o revisar queries anómalas |

### 8.4 Presupuesto de latencia

La p95 del canal cacheado se verifica contra la línea base pre-caché en cada gate (§12). Presupuesto declarado en D8: hit ahorra 50 ms-5 s; miss añade ≤ 1 round trip DDB (p50 5-10 ms, p99 ≤ 50 ms); bypass/shadow +0 ms. CloudWatch Duration de la Lambda + el campo `execution_time_ms` del metadata (ya existente) son la fuente; no se inventan métricas nuevas para esto.

---

## 9. Modelo de costo y ROI (supuestos declarados, validar con shadow)

Precios lista us-east-1 on-demand (órdenes de magnitud; confirmar contra la factura real): **RRU ≈ $0,25/M**, **WRU ≈ $1,25/M**, Athena **$5/TB** escaneado. Costos de S3 gets del lake omitidos (menores).

### 9.1 Costo por operación de la caché

| Operación | Unidades | Costo aprox. |
|---|---|---|
| Lookup hit (pre-F3) | 1 GetItem eventual ≤ 4 KB header (payload en el item: 1 RRU por cada 4 KB) — típico 64 KB ⇒ 16 RRU | $0,000004 |
| Lookup hit (F3) | BatchGet: entrada (eventual) + GEN (fuerte, ≤ 4 KB = 1 RRU) | + $0,00000025 |
| Store 64 KB | 64 WRU | $0,00008 |
| Store 256 KB | 256 WRU | $0,00032 |

(El lookup lee el item completo — el payload VIAJA en cada hit; ver nota de tuning en §9.3.)

### 9.2 Costo de lo que un hit evita

| Canal evitado | Costo directo | Latencia evitada |
|---|---|---|
| OLTP exec (fan-out AVET/AEVT + hidratación + fórmulas) | decenas de RRU (order 20-100) + trabajo CPU | 50-300 ms |
| OLAP exec (Athena) | $5/TB × escaneado por query (dashboard típico 0,1-10 GB ⇒ $0,0005-$0,05) | 0,5-5 s |

### 9.3 Breakeven (la aritmética del gate)

- **OLAP:** un store de 64 KB cuesta $0,00008; un hit evita ≥ $0,0005 de Athena. **Un solo re-hit paga la entrada.** Con hit rate ≥ 20 % el canal es claramente positivo; con ≥ 5 % ya suma.
- **OLTP:** un hit cuesta ~16 RRU ($0,000004) y evita ~20-100 RRU de fan-out + 50-300 ms de latencia. **Breakeven ≈ 2-10 % de hit rate** (más el valor de latencia, que domina en UX de dashboards).

**Umbral de decisión del gate ROI-1 (§12):** shadow ≥ 10 % OLTP y/o ≥ 20 % OLAP durante la ventana medida ⇒ habilitar ese canal; por debajo ⇒ quedarse en `off` (costo cero).

Tuning anotado para F4 si el lookup de items grandes pesa: particionar el payload (header + blobs por rangos) o msgpack (~30-40 % menos bytes ⇒ menos RRU por hit y menos WRU por store).

---

## 10. Mapeo SOLID · DRY · Clean Code

| Principio | Aplicación concreta en este diseño |
|---|---|
| **SRP** | `keys.rs` solo canonicaliza; `policy/gates` solo deciden elegibilidad; `DynamoKvCache` solo habla DDB; el frontend solo orquesta (y posee el modo shadow, que es política de medición, no almacenamiento); la invalidación vive en el embudo del writer. |
| **OCP** | Nuevo backend (Valkey/Redis mañana) = nuevo adaptador del puerto, cero cambios en janus. Nuevo motivo de bypass = nueva variante de `BypassReason` en un solo `match`. Registrar un `RowOverlay` nuevo excluye su entidad de la caché sin tocar código de caché. Nuevo modo de operación = nueva variante de `CacheMode` sin tocar los routers. |
| **LSP** | `NoopQueryCache`, `DynamoKvCache` y los fakes son intercambiables detrás de `IQueryCache`; el frontend no conoce el backend. El Null Object garantiza que "apagado" sea un caso particular, no un `if` disperso. |
| **ISP** | Dos puertos mínimos: `IQueryCache` (read path) y `QueryCacheInvalidator` (write path solo necesita bump) — espejo de `ISqsBus`/`ISessionStore`. |
| **DIP** | `janus` y `eav::writer` dependen de `domain::protocols`; `infrastructure` implementa. Precedente exacto: `IQueryEngine` consumido por `janus/router` e implementado por `AthenaQueryEngine`/`LocalS3QueryEngine`. |
| **DRY** | Sin duplicar: serialización canónica (`analytics_request_to_json`), mapeo de errores (`map_sdk_error`), exclusiones (registry de overlays), invalidación (embudo `cache_policy.rs`), metadata de caché (normalizer único), infra local (`TABLES_CONFIG`), cliente DDB (`DynamoClient` + 2 métodos genéricos nuevos), fingerprints (`CodeRegistry`). |
| **Clean Code / Clean Architecture** | `domain` sin AWS; `infrastructure` sin lógica de negocio; composición solo en `server.rs`; errores tipados `DomainError` (thiserror, prohibido anyhow — R4 de `PLAN_PATRON_RESULT.md`); funciones pequeñas y puras donde se puede (`evaluate`, key builder), efectos en la frontera (adaptador); comentarios solo para invariantes no obvios (p. ej. el perímetro post-cache de FLS). |

---

## 11. Testing

### 11.1 Unitarios (`cargo test --lib`, corren en CI sin infra)

| Test | Archivo | Verifica |
|---|---|---|
| `key_is_deterministic` | `janus/cache/tests.rs` | Mismo request compilado ⇒ misma clave (estabilidad serial canónica). |
| `key_changes_with_abac_scope` | idem | ASTs con distinto filtro ABAC ⇒ claves distintas. |
| `key_changes_with_window_rollover` | idem | `TODAY` resuelto en días distintos ⇒ claves distintas. |
| `key_changes_with_schema_fingerprint` | idem | Cambio del modelo Códice ⇒ clave distinta. |
| `gate_rules` (tabla) | idem | G1-G5: explain, overlay-entity, time-travel, oversize, canal off. |
| `gate_derives_overlay_entities_from_executor` | idem | El accessor del executor alimenta la exclusión (G2). |
| `oversize_marker_prevents_reserialization` | idem | Segundo request de clave marcada ⇒ ni serializa ni llama al backend (spy). |
| `frontend_serves_only_fresh_matching_tenant` | idem | Expirado ⇒ miss; `t != tenant` ⇒ miss + warn (reloj inyectado). |
| `frontend_degrades_open_on_backend_error` | idem | `get` Err ⇒ ejecución normal, sin propagación. |
| `cache_hit_prevents_execution` | idem | Con fake `IQueryCache` precargado + executor espía: el executor no se invoca (patrón `cedar/tests/principal_graph.rs:56`). |
| `shadow_mode_never_changes_outcome` | idem | En Shadow, lookup devuelve Miss aunque el ring tenga la clave; el body nunca lleva `cache`. |
| `shadow_ring_measures_hits` | idem | Misma clave dentro de TTL ⇒ span `SHADOW_HIT`; vencida ⇒ `SHADOW_MISS`. |
| `metadata_reflects_cache_status` | `janus/normalizer` tests | `body.cache` → `cache_hits`/`cache_ttl_seconds` en metadata (hit, miss y ausencia). |
| `put_respects_size_cap` | idem | Payload > cap ⇒ no se llama al backend (spy). |

### 11.2 Integración (`#[ignore]`, `make test-integration` contra DynamoDB Local)

- `put_then_get_roundtrip` — item layout completo, payload JSON idéntico.
- `expired_entry_is_logical_miss` — escribir con `exp` en el pasado (reloj inyectado) ⇒ `get` = None aunque el item exista.
- `fase3_generation_bump_invalidates` — put con gen 5; bump a 6 ⇒ miss; re-put con gen 6 ⇒ hit.
- `fase3_batch_get_single_roundtrip` — entrada + GEN en un `BatchGetItem`.
- Alta de tabla por `reset_local.py` (§5.3); DynamoDB Local sin TTL activo no afecta (expiración lógica verificada por la app).

### 11.3 Fiabilidad / e2e (Python existentes)

- Extender `tests/reliability/janus_ast_reliability_test.py`: emitir la misma QueryRequest dos veces; segunda respuesta debe traer `metadata.cache_hits = 1` y `cache_ttl_seconds > 0` (campos ya mapeados por `map_metadata`). Caso shadow: respuestas byte-idénticas a `off`.
- `make smoke` sin cambios (modo `off` por defecto ⇒ comportamiento idéntico pre-rollout).

### 11.4 Regresiones que NO deben moverse

- Snapshots de `janus/testing/contract_tests.rs` — intactos (sin cambios FBS).
- `tests/contract_conformance.rs` — intacto (sin cambios de proto).
- `cargo clippy` con línea base vigente (88 warnings, `make ci`).
- Pre-commit: no se tocan proto/modelos/errores ⇒ `gen_reference.py --check` pasa sin regenerar.

### 11.5 Pruebas reales en producción (estrategia de verificación T0-T6)

**Principio:** cada prueba corre contra el **motor de producción** (gRPC-Web vía CloudFront+WAF, el mismo camino que los clientes), sobre un **tenant canario** con datos sembrados y estables, con criterios de aborto explícitos y un veredicto atado a los gates de §12. Ninguna prueba usa credenciales commiteadas: `ENGINE_HOST` + `HMAC_SECRET` por entorno, como ya hace `tests/e2e/production_smoke_test.py`.

**Vehículo:** extender `tests/e2e/production_smoke_test.py` (29 KB, ya apunta a producción por defecto con cliente gRPC-Web propio y tokens HMAC) con un subcomando `--cache-verify <suite>`; fixtures y goldens en `tests/e2e/cache_verify/`. Se reusan además dos activos existentes: el patrón de concurrencia de `tests/reliability/scale_reliability_test.py` (`concurrent.futures`) y `scripts/ops/invalidate_lambda_caches.py` (invalidación de cachés in-process en producción vía transacts dummy).

**Setup único (por rollout):** crear/sembrar el tenant canario (p. ej. `metri-cache-canary`) con un corpus determinista — entidades estables, dos usuarios con scopes Cedar distintos (ALL vs OWN) y uno con acceso a atributo sensible — siguiendo el mismo flujo de seed que `integration_e2e.py` ejecuta en local. Datos de solo-lectura salvo T3.

#### T0 — Smoke post-deploy (cada deploy con la caché activa, < 30 s)

1. Query A sobre el canario (entidad estable) → success, sin `cache_hits=1`.
2. Query A otra vez → **`data` idéntica**, `cache_hits=1`, `0 < cache_ttl_seconds ≤ TTL`.
3. `cache_ttl_seconds` de la 2ª llamada < el de la 1ª (frescura viva).
4. Query con `explain_plan` → `channel=explain`, sin efecto en caché.
5. Aserción global: cero `JANUS_5xx`.

**Veredicto:** fallo ⇒ el deploy no se da por bueno.

#### T1 — Equivalencia diferencial contra goldens (la prueba de corrección de la caché)

- **Corpus:** ~50 queries sobre el canario cubriendo ambos canales, casts (KPI/TABLE/PIE/TIMESERIES/CSV), filtros compuestos, paginación por cursor y FTS.
- **Baseline:** con `QUERY_CACHE_MODE=off` se graban las respuestas → goldens versionados en `tests/e2e/cache_verify/goldens/` (se excluyen los campos no deterministas: `metadata.query_id`, `execution_time_ms`, `links`).
- **Replay con caché:** cada query se dispara 2× (miss → hit). Doble comparación estricta: (a) respuesta del hit ≡ respuesta del miss; (b) ambas ≡ golden. Una diferencia en (a) delata un bug de serialización del payload; en (b), un cambio semántico no previsto.

**Veredicto:** cualquier diff ⇒ rollback inmediato (flip a `off`) antes de ampliar el allowlist.

#### T2 — ABAC/FLS en hit (los invariantes de carga, probados con Cedar real de producción)

1. Misma query con usuario scope ALL vs usuario scope OWN → resultados distintos (claves distintas; ambos success).
2. Query que incluye atributo `sensitive: true` con el usuario no-privilegiado → atributo ausente **también en la respuesta servida desde caché** (prueba exactamente que la redacción FLS corre post-cache, §1.3).
3. Repetir 2× para garantizar que el caso probado es el camino *hit* (`cache_hits=1`).

#### T3 — Read-your-write / invalidación (activa con F3; de caracterización antes)

1. Query X → hit.
2. `Transact` que modifica la entidad del canario (por gRPC, mismo canal de escritura que los clientes).
3. Query X inmediata → **debe reflejar el cambio** (bump de generación); query X de nuevo → hit con el valor nuevo.
4. Pre-F3 el mismo script documenta la ventana TTL (valor viejo hasta `exp`) — caracteriza el contrato que el producto está aceptando.

#### T4 — Compartimiento multi-instancia (el beneficio central, probado de verdad)

1. Ráfaga de ~20-30 queries idénticas concurrentes sobre el canario → fuerza instancias warm adicionales; todas success y las réplicas reportan `cache_hits=1` — hits servidos por instancias que **no** ejecutaron la query (prueba que la entrada DDB se comparte cross-instance, que es la razón de ser de la tabla).
2. Separación de capas: correr `scripts/ops/invalidate_lambda_caches.py` (invalida solo cachés in-proc) → la siguiente query **sigue dando hit** desde DynamoDB.

#### T5 — Carga acotada (una vez por rollout, ventana valle, con aprobación)

- Patrón de `scale_reliability_test.py` con **techos explícitos**: ≤ 100 concurrentes, ≤ 3 min, solo tenant canario, ventana de bajo tráfico.
- **Criterios de aborto en vivo:** error rate > 0,5 %, cualquier alarma de §8.3 en ALARM, o throttleo DDB visible.
- **Salidas:** p50/p95 de Lambda Duration con caché vs línea base (verifica D8 con números reales de producción), hit rate bajo concurrencia, y confirmación de que WAF/CloudFront absorben la ráfaga (el tráfico pasa por el WAF de `template.yaml:129-316` — no se salta; si corta, es un dato sobre los límites operativos, no un obstáculo a evadir).

#### T6 — Drill de rollback (probar el plan B antes de necesitarlo)

En ventana acordada (deploy con aprobación humana del environment `production`): flip `QUERY_CACHE_MODE=off` → deploy → replay del corpus T1 → respuestas ≡ goldens, sin `cache_hits`, y **tráfico de la tabla a ~0** en las métricas DDB (ConsumedReadCapacityUnits). Ejecutar **una vez antes de ampliar el allowlist a todos los tenants**: un rollback que nunca se ensayó no es un rollback, es una esperanza.

#### Cadencia y gobernanza

| Suite | Cuándo | Veredicto |
|---|---|---|
| T0 | Cada deploy con caché activa | Fallo ⇒ deploy no válido |
| T1, T2, T3, T4 | Diarios durante la semana canaria; T1 además tras cada cambio de TTL | T1 fallo ⇒ rollback |
| T5 | Una vez por rollout (ventana valle) | p95 dentro de D8 y sin alarmas |
| T6 | Una vez, antes de quitar el allowlist | Fallo ⇒ no ampliar hasta arreglar el runbook |

Seguridad transversal: solo el tenant canario recibe tráfico sintético (salvo T5, acotado); escrituras únicamente en T3; sin credenciales de producción en el repo (patrón vigente de `production_smoke_test.py`); ventanas T5/T6 coordinadas con el aprobador del deploy.

---

## 12. Fases, gates y runbook

Los **gates ROI** son condiciones medibles con los datos del shadow/métricas (§8.3); si no se cumplen, el plan se detiene en la fase anterior con costo cero o mínimo.

> **Estado de implementación (2026-09-25):** el código de F0-F3 está completo y type-checkeado (`cargo check` verde; la ejecución de tests quedó documentada como paso pendiente por la licencia Xcode de la máquina — ver DoD). Faltan exclusivamente los pasos operativos: deploy `shadow` → Gate ROI-1 → canary `ddb` → Gate ROI-2 → T5/T6.

### Fase 0 — Puerto, esqueleto y shadow (sin tabla, sin RCU/WCU)
- [x] `IQueryCache`, `QueryCacheInvalidator`, `CacheEntry`, `CachePut` en `domain/protocols.rs`.
- [x] `janus/cache/{mod,keys,tests}.rs`: `QueryCachePolicy::from_env()`, modos `Off|Shadow|Ddb`, gates, key builder, `NoopQueryCache`, `ShadowKeyRing`, `OversizeKeyRing`.
- [x] Fingerprint memoizado en `CodeRegistry::build()` (§4.6 — `get_cache_fingerprint`, SHA-256 del JSON completo del modelo).
- [x] Wiring: `QUERY_CACHE_MODE` (default `off`), `ServiceDeps.query_cache`, parámetro `cache: &QueryCacheFrontend` en `process_single_query`/`execute_*_query`/`run_query_pipeline` + 2 call sites (`query.rs`, `explore.rs`).
- [x] `OltpExecutor::overlay_entities()` (accessor).
- [x] Spans §8.2 + **`RUST_LOG` ajustado en `template.yaml`** (`warn,metri_engine::janus::cache=info`).
- [x] Unit tests de §11.1 (incl. shadow y metadata del normalizer). Gate: `make ci` verde con modo `off` — **pendiente de ejecutar** (verificado hasta `cargo check` por el bloqueo de licencia Xcode).

### ▶ Gate ROI-1 (datos, no opinión)
- [ ] Desplegar F0 en producción con `QUERY_CACHE_MODE=shadow` (deploy estándar; aprobación humana del environment `production`).
- [ ] Recolectar ≥ 1 semana por canal: hit rate shadow, distribución de `BYPASS`, tamaño de payloads (`janus.cache.store` no existe aún — tamaño se estima muestreando `total` en metadata).
- [ ] **Criterio:** shadow ≥ 10 % OLTP y/o ≥ 20 % OLAP (§9.3) ⇒ habilitar ese canal; si no ⇒ cerrar el plan en F0 (costo: el desarrollo de F0).
- [ ] Archivar el reporte en `docs/architecture/MEDICION_CACHE_JANUS.md` (fuente para los TTLs finales).

### Fase 1 — Adaptador DynamoDB + canal OLTP
- [x] `DynamoClient::batch_get_item` + `update_item_add` (genéricos, con reintentos análogos a `batch_write_item`).
- [x] `infrastructure/dynamodb_query_cache.rs`: `DynamoKvCache` (get/put, expiración lógica, verificación tenant, item §5.1).
- [x] Cache-aside en `oltp.rs` (bloque §4.2) + `spawn` del `put` para no sumar latencia al hit path.
- [x] `body.cache` + normalizer (`cache_hits`/`cache_ttl_seconds` reales).
- [x] Infra: `MetriQueryCacheTable` + IAM + env + **metric filters y alarma §8.3** en `template.yaml`; entrada en `reset_local.py`.
- [x] Integración §11.2 (tests `#[ignore]` contra DynamoDB Local) + `tests/e2e/cache_verify.py` (suites T0/T1 de §11.5; goldens en `tests/e2e/cache_verify/`).
- [x] Docs: ADR-008 aceptado, fichas `components/janus.md` + `infrastructure.md`, índice `docs/README.md`.
- [ ] (operativo) Tenant canario sembrado (2 usuarios con scopes distintos + uno con acceso a atributo sensible).

### ▶ Gate ROI-2 (el modelo contra la realidad)
- [ ] Con OLTP habilitado en producción ≥ 1 semana: hit rate real ≥ shadow rate (esperado: la tabla agrupa entre instancias), costo DDB de la tabla vs. §9, p95 sin regresión > 15 ms (§8.4).
- [ ] Si el costo real por hit supera el modelo 3× (p. ej. payloads más grandes): tuning (cap, F4 particionado) o reversión del canal con flip de env.

### Fase 2 — Canal OLAP
- [x] Cache-aside en `olap.rs` alrededor de `start_query/get_query_results`; payload `{exec_id, columns, rows}`; reinyección de `exec_id` en body.
- [ ] TTL OLAP inicial 60 s; subir a 300 s solo con datos de Gate ROI-2bis (mismos criterios, canal OLAP).
- [ ] Tests: roundtrip OLAP + reliability con query OLAP repetida (motor `LocalS3QueryEngine` en local).
- [ ] Medición de ahorro contra `MEDICION_COSTO_OLAP.md` (queries/día repetidas × costo Athena evitado) — se añade a `MEDICION_CACHE_JANUS.md`.

### Fase 3 — Invalidación por generación (write path)
- [x] `DynamoGenInvalidator` + item `GEN#QC#<tenant>` (UpdateItem ADD 1, BatchGet fuerte en get). Toggle `QUERY_CACHE_GENERATION=on|off` (default off).
- [x] Invalidador inyectado en `EavWriter` (`with_query_cache_invalidator`); bump en el commit del transact (embudo `cache_policy`) y UN bump por lote en la ruta bulk (`oltp_channel`).
- [x] `entry.gen` escrito en put y verificado en get.
- [x] Test `generation_bump_invalidates_entries` (§11.2, `#[ignore]`).

### Fase 4 — Opcionales (decidir con datos de producción)
- [ ] Ejecución especulativa lookup‖ejecución (miss ≈ +0 ms; costo: trabajo desperdiciado por miss).
- [ ] Single-flight in-process por clave (anti-stampede en ráfagas cold).
- [ ] Payload msgpack (`rmp-serde` ya es dependencia) o particionado de items grandes (§9.3).
- [ ] Granularidad de generación por `entity_type` (documentando el caveat de `select_tree`/relaciones).
- [ ] Contadores OTel cuando la Fase 5 de observabilidad aterrice (los metric filters de §8.3 se quedan como fuente de respaldo).
- [ ] Script ops `scripts/ops/purge_query_cache.py` (Scan+Delete por tenant) para incidentes — hoy innecesario (bump de generación lo cubre).

### Runbook de rollout y rollback

1. Deploy con tabla creada y `QUERY_CACHE_MODE=shadow` → 1 semana → Gate ROI-1.
2. **Canary:** `ddb` + `QUERY_CACHE_CHANNELS=oltp` (TTL 60 s) + `QUERY_CACHE_TENANT_ALLOWLIST=<canario>` → T0 en cada deploy y T1-T4 diarios (§11.5) durante 1 semana → Gate ROI-2 sobre el canario.
3. **Drill T6** (rollback ensayado) → si verde, quitar el allowlist (todos los tenants) y ejecutar T1 completo + T5 en ventana valle.
4. Añadir `olap` (TTL 60 s) → subir TTLs con datos.
5. F3 (generación) → re-ejecutar T3 (read-your-write real) → subir TTLs con confianza si el producto lo pide.
6. **Rollback en cualquier paso = flip de `QUERY_CACHE_MODE` a `off`** (o quitar el canal de `QUERY_CACHE_CHANNELS`) y redeploy. Sin migración de datos, sin ventanas; las entradas restantes expiran solas por TTL físico. La tabla puede quedarse (costo ~0 sin tráfico). El drill T6 garantiza que este paso fue probado, no solo documentado.

**Dependencias:** F1 depende de F0; F2 solo del cache-aside de F1; F3 es ortogonal al canal y requerida antes de subir TTLs por encima del umbral de seguridad (p. ej. > 300 s).

---

## 13. Riesgos y mitigaciones

| # | Riesgo | Impacto | Mitigación |
|---|---|---|---|
| R1 | Servir dato vencido/generación distinta | Corrección | `exp` lógico verificado en cada lectura (D3) + gen check (D4). El TTL físico DDB jamás participa en la decisión. |
| R2 | Fuga entre usuarios con mismo scope | Seguridad | El post-proceso dependiente del llamador (FLS `redact_sensitive_attributes`, omisión de `password_hash`) está **fuera** del perímetro y corre en cada request. Invariante §1.3 protegido por tests (`cache_hit_prevents_execution` + redacción post-hit). |
| R3 | Fuga entre tenants | Seguridad | Tenant dentro del payload hasheado + verificación defensiva `entry.t == tenant` en cada get + tabla con KMS/IAM propio mínimo-privilegio. |
| R4 | Dato sensible en la entrada cacheada | Seguridad | El payload duplica datoms que ya viven (cifrados, KMS CMK) en la tabla EAV — misma clase de protección; exposición acotada por TTL; tabla sin Scan/Query en IAM. |
| R5 | Error de DDB tumba queries | Disponibilidad | Fail-open: todo error de caché ⇒ bypass + `warn!` (test §11.1). La caché nunca aparece como error de query. |
| R6 | Entradas grandes | Costo/latencia | Cap 256 KB (G4) + ring `oversize_seen` que evita re-serializar por request; KPI/timeseries/PIE caben holgado; TABLE enorme ⇒ bypass silencioso con span y alarma `QueryCacheStoreOversize`. |
| R7 | Stampede en ráfaga cold | Costo | Tolerado en v1 (PutItem idempotente); jitter de TTL; single-flight y ejecución especulativa en F4. |
| R8 | **Partición caliente (hot key).** Las claves llevan tenant: el peor caso es el dashboard idéntico de un solo tenant cacheado como UN item — DynamoDB satura una partición alrededor de ~3 000 RCU/s sostenidos sobre la misma clave. | Disponibilidad/costo | Improbable a escala actual (requiere ~1 200 hits/s al mismo dashboard); las claves ya distribuyen por tenant+query; monitoreo `QueryCache/Hits` por canal; si apareciera: jitter TTL + F4 single-flight (coalesce la ráfaga in-process) o particionar la clave (`key#N` con sticky-routing por instancia). |
| R9 | **Inconsistencia entre páginas de una paginación cacheada** (página 1 servida de una entrada de 50 s y página 2 ejecutada fresca tras una escritura) | UX | Ventana acotada por TTL y comparable al comportamiento existente de `AEVT_SCAN_CACHE` (15 s). Aceptado en v1; si el producto lo exige: F3 (generación) reduce la ventana a ~1 s, o incluir el `exp` de la primera página en el cursor (anotado, no planeado). |
| R10 | Invalidación gruesa por tenant (F3) | Hit rate | Decisión correct-first deliberada; tuning por entity_type en F4 con caveat documentado. |
| R11 | Regresión de arranque sin AWS | DX/CI | `QUERY_CACHE_MODE=off` default + Null Object; `make ci`/pre-commit no requieren tabla (gate F0). |
| R12 | Clave inestable entre versiones del código | Hit rate | `ns = CACHE_KEY_VERSION` + fingerprint de schema: cualquier cambio semántico de canonicalización o de modelos rota el namespace limpiamente. Cambios de compilador (nuevos filtros ABAC) cambian el AST ⇒ nueva clave: invalidación segura por deploy, deseada. |
| R13 | Shadow no produce datos por filtrado de logs | Medición | `RUST_LOG=warn,metri_engine::janus::cache=info` fijado en `template.yaml` como parte de F0 (§8.2) — y verificado en el smoke del gate. |
| R14 | Pruebas reales contra producción (tráfico sintético, escrituras T3, carga T5) tocan el sistema vivo | Operación | Solo tenant canario (allowlist G0); escrituras solo en T3 sobre datos semilla; T5 con techos (≤100 concurrentes, ≤3 min, ventana valle) y criterios de aborto en vivo; ventanas T5/T6 coordinadas con el aprobador del deploy; sin credenciales de prod en el repo (patrón vigente de `production_smoke_test.py`). |

---

## 14. Preguntas abiertas (a cerrar con datos de los gates)

1. **TTLs finales** — ya no se eligen a mano: el shadow y los gates aportan la distribución real de repetición; 60 s es el arranque, 300 s OLAP la meta si los datos acompañan.
2. **Orden OLTP vs OLAP** — el shadow mide ambos canales por separado desde F0; se habilita primero el que supere su umbral (esperado: OLAP, por §9.3).
3. **`explore` OLTP directo** (`explore.rs:315`) y `list_entities_impl` quedan fuera de la caché; ¿aceptable como estado permanente o se homologan al router después?

---

## 15. Definition of Done

> **Estado (2026-09-25):** implementación completa y verificada: `cargo build --all-targets` limpio, `cargo test --lib` 510/510 ✓, `cargo fmt --check` ✓, clippy sin hallazgos nuevos en el código de la caché. (La licencia de Xcode sin aceptar en la máquina solo afecta al shim `cc` por defecto; se ejecutó con el toolchain de CommandLineTools: `PATH=/Library/Developer/CommandLineTools/usr/bin:$PATH CC=.../clang SDKROOT=.../SDKs/MacOSX.sdk`. El CI de GitHub ejecuta en Linux y es el gate definitivo.) Pendientes: `make test-integration` con DynamoDB Local y los ítems operativos (gates ROI, T5/T6).

- [ ] `make ci` verde: fmt, docs check, result-pattern strict, clippy (≤ línea base), `cargo test --lib`.
- [ ] `make infra && make seed && make test-integration` verde con la tabla local creada.
- [ ] Con `QUERY_CACHE_MODE=off`: salida de `janus_ast_reliability_test.py` byte-idéntica a pre-cambio (sin `cache_hits`).
- [ ] Con `shadow`: idéntica a `off` (test `shadow_mode_never_changes_outcome`) y métricas `ShadowHits/ShadowMisses` visibles en CloudWatch.
- [ ] Con `ddb`: segunda query idéntica reporta `cache_hits = 1` y `cache_ttl_seconds > 0`.
- [ ] **Suite de producción §11.5 implementada** (`production_smoke_test.py --cache-verify`): T0-T4 verdes durante la semana canaria; T5 ejecutado con p95 dentro del presupuesto D8; T6 (drill de rollback) ejecutado y documentado antes de quitar el allowlist.
- [ ] Escritura (transact) invalida lecturas cacheadas del tenant dentro de la ventana del TTL (F3: inmediata).
- [ ] Gate ROI-1 documentado en `docs/architecture/MEDICION_CACHE_JANUS.md` con hit rate por canal y decisión tomada por los umbrales de §9.3.
- [ ] Alarmas §8.3 creadas; p95 del canal cacheado sin regresión > 15 ms contra línea base.
- [ ] Snapshots FBS y `contract_conformance.rs` sin cambios.
- [ ] ADR-008 aceptado; fichas `janus.md`/`infrastructure.md` e índice `docs/README.md` actualizados.
- [ ] Cold start medido sin regresión (el wiring no añade awaits en arranque: el cliente DDB ya existe; el fingerprint se computa en `CodeRegistry::build()`).

---

## Apéndice A — Borrador ADR-008 (mecánico al cerrar F1)

> **ADR-008 — Caché de consultas Janus sobre DynamoDB con TTL**
>
> **Estado:** propuesto · 2026
>
> **Contexto.** Las cachés del read path son de proceso (`EAV_CACHE`, `AEVT_SCAN_CACHE`) y su invalidación por escritura solo alcanza a la instancia que commitó; el read-your-write multi-instancia se delega al TTL (incidente 2026-09-23). El canal OLAP paga Athena por cada ejecución. No existe almacén compartido besides DynamoDB (la VPC/Valkey fue retirada en 2026).
>
> **Decisión.** Caché read-through KV en una tabla DynamoDB dedicada con TTL nativo, insertada en `janus/router/{oltp,olap}.rs` alrededor del executor. La clave es el SHA-256 del AST compilado post-ABAC + ventana resuelta + fingerprint de schema: el aislamiento tenant/ABAC es por construcción y el post-proceso dependiente del llamador (FLS) queda fuera del perímetro. Expiración lógica (`exp`) verificada en lectura; el TTL de DDB es solo limpieza. Invalidación por escritura vía generación por tenant en el embudo `cache_policy.rs`. Rollout shadow-first con gates de ROI medibles. Puerto `IQueryCache` en `domain/protocols.rs`; adaptador DynamoDB en `infrastructure`.
>
> **Consecuencias.** (+) Miss costoso pagado una vez por clave en todo el fleet; `cache_hits`/`cache_ttl_seconds` del contrato se activan; rollback = flip de env sin migración — **ensayado en producción (drill T6, §11.5) antes de necesitarse**; rollout medido: shadow-first con gates de ROI y canary por allowlist de tenant, verificado con pruebas diferenciales contra goldens, ABAC/FLS en hit y read-your-write real. (−) Un round trip extra en el miss path (presupuestado en D8); invalidación por tenant gruesa en F3; costo RCU/WRU de la tabla (modelo §9, gate ROI). Alternativas rechazadas: ElastiCache (resucita la VPC retirada), solo in-process (no resuelve multi-instancia), result-reuse de Athena (no evita `start_query`).
