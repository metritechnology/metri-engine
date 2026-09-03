# Plan de refactorización — `src/cedar`

> **Componente:** `metri-engine/src/cedar` — PDP (Policy Decision Point) Zero-Trust sobre cedar-policy 3.x
> **Estado verificado:** 2 de septiembre de 2026 — contra el código, no contra la documentación
> **EJECUTADO (3 de septiembre de 2026):** fases 0–5 completadas en los commits `a0a099c` → `ee42555` (más el cierre). Suite: 375 tests en verde, clippy 0 hallazgos en `src/cedar`. Desviaciones del plan: (1) no se creó `CedarConfig` propio — la configuración única es `engine_config()` de ADR-004, que cedar ya consulta; (2) la tarea de invalidación sigue levantándose al construir la caché (el test de pub-sub la exigía) — la lógica vive extraída en `cache/invalidation.rs` con el fix de `Lagged`; (3) el 5C de métricas otel queda pendiente — no existe infraestructura de métricas que imitar y no se quiso inventar en el mismo refactor.

> **Línea base:** `cargo test --lib cedar` → **14 passed, 0 failed** (1,19 s)
> **Naturaleza:** el autorizador está en producción cuidando cada petición; **ningún commit intermedio puede romperlo**. Cada fase termina en verde y commiteable.
> **Coordenada en el plan general:** descarga el ítem 3 de funciones gigantes de `PLAN_REFACTORIZACION.md` (`step4_mutational`) y materializa ADR-002.

---

## Verificación de partida

```bash
cargo test --lib cedar    # 14 passed, 0 failed — 2 sep 2026
```

| Métrica | Valor |
|---|---|
| Archivos | 4 (`authorizer.rs` 1.681 · `evaluator.rs` 599 · `principal_graph.rs` 519 · `mod.rs` 1) |
| Líneas totales | 2.800, de las cuales **~1.020 son tests dentro de `authorizer.rs`** (líneas 659–1.681) |
| Consumidores | 14 archivos, 38 referencias (`grpc/handlers/*`, `grpc/service.rs`, `grpc/server.rs`, `iop/cedar_step.rs`, `iop/quota_step.rs`, `janus/ast_compiler.rs`) |
| Función más grande | `step4_mutational` — 364 líneas (`evaluator.rs:144-508`) |
| Estado global propio | 2 (`INVALIDATION_TX`, y mutación del `EAV_CACHE` ajeno) |
| Lecturas de `env` en camino de petición | 4 (`HMAC_SECRET`, `METRI_MASTER_TENANT_ID` ×2, `engine_config()`) |

---

# Parte I — Diagnóstico

## Lo que ya está bien (y no se toca)

El desmonte de la fase 2 dejó cimientos sólidos: los pasos tienen identidad propia (`principal_graph.rs`, `evaluator.rs`), `CedarAuthorizer` ya es una fachada fina sobre el motor Cedar, `ISessionStore` y `PrincipalCache` ya son traits (hay inyección real en `intercept`), el schema va en `OnceLock`, y `step3b_validate_time_window` ya recibe el reloj como parámetro. La estructura de errores con `with_stage("cedar")` es consistente. Este plan **construye sobre esas costuras**, no las tira.

## Lo que hay que arreglar

### Hallazgos de seguridad (justifican el refactor por sí solos)

| # | Hallazgo | Evidencia | Impacto |
|---|---|---|---|
| S1 | **El fast-path `mk_` se salta la lista de revocación.** `step1_extract_token` verifica la firma HMAC localmente y devuelve la sesión sin consultar el store; el chequeo de blacklist vive en `HmacTokenStore::get_session` (`src/infrastructure/session_store.rs:106-120`), que este camino nunca llama (`authorizer.rs:415-419`). | Un token **revocado** sigue autorizado mientras no expire. | Alta |
| S2 | **Fallback de secreto HMAC silencioso.** `verify_hmac_token_local_in_step` usa `"secret-key-development-metri-256-bits!!!"` si falta `HMAC_SECRET` (`authorizer.rs:282-283`). El arranque falla rápido con secreto débil (`grpc/server.rs:104`), pero esta función reabre la puerta en ejecución. | Fail-open ante misconfiguración — contradice ADR-004. | Alta |
| S3 | **El suscriptor de invalidación muere en silencio.** `while let Ok(msg) = rx.recv().await` (`authorizer.rs:144-145`): un `Err(Lagged)` del canal broadcast (capacidad fija 100) **termina el loop** y la caché de principals deja de invalidarse para siempre — decisiones autorizadas con roles/grupos obsoletos. | Stale authz sin rastro en logs. | Alta |
| S4 | **Tres definiciones divergentes de "tenant maestro".** `is_master_tenant` acepta `"system" \| "tnt_master" \| env` (`authorizer.rs:309-312`); `step4_analytical` compara solo contra `env` con default `"system"` (`evaluator.rs:544-547`); `assemble_principal_graph` usa otra variante (`principal_graph.rs:339-341`). | Un tenant puede ser maestro para una capa y no para otra. | Media |

### SOLID

| Principio | Hallazgo | Evidencia |
|---|---|---|
| **SRP** | `authorizer.rs` sigue mezclando seis responsabilidades: tipos de dominio, caché de principals + su tarea de invalidación, sesión en memoria, verificación HMAC, reglas del sistema y extracción de metadatos `tonic`. | `authorizer.rs:27-594` |
| **SRP** | `intercept` autentica, resuelve el principal, valida ventana horaria, **hidrata el recurso vía pull EAV** (bloque de 36 líneas con `assigned_company_id`) y evalúa Cedar — cinco pasos en una función. | `authorizer.rs:476-547` |
| **SRP** | `assemble_principal_graph` recorre el grafo Y parsea grants en tres formatos distintos (string JSON, array de strings, array de objetos). | `principal_graph.rs:285-519` |
| **OCP** | Las acciones y sus grupos están hardcodeadas como JSON dentro de Rust; añadir `PATCH` exige editar código. Además `is_mutational_action` excluye `GET` pero el JSON de entidades mete `GET` en el grupo mutacional — inconsistencia latente. | `evaluator.rs:215-439`, `evaluator.rs:597-599` |
| **LSP** | `InMemorySessionStore::revoke_session` es no-op (`authorizer.rs:113-119`): viola el contrato que `HmacTokenStore` sí cumple. Hoy solo lo usan tests, pero es una trampa. | `authorizer.rs:103-120` |
| **DIP** | Globales: `INVALIDATION_TX` publicado desde handlers gRPC (`grpc/handlers/bulk.rs:242`, `validations.rs:344`), `EAV_CACHE` de eav mutado desde cedar (`authorizer.rs:148-151`), `codice::registry::global_opt()` (`evaluator.rs:69`). | — |
| **DIP** | `fetch_children_eav` construye su dependencia dentro del cuerpo (`EavQueryExecutor::new(eav_reader.ddb.clone(), …)`); `EavReader` es tipo concreto con campos públicos `ddb`/`table` — no hay puerto que mockear. | `principal_graph.rs:269-283` |

### DRY

| # | Duplicación | Evidencia |
|---|---|---|
| D1 | **Tres implementaciones de verificación HMAC** del mismo formato de token: `verify_hmac_token_local_in_step` (`cedar/authorizer.rs:268`), `verify_hmac_token_local` (`grpc/interceptors.rs:41`), `verify_signature` (`infrastructure/session_store.rs`). Derivan por separado — S1 y S2 son síntomas. | 3 archivos |
| D2 | Bootstrap del `policy_cache` copiado: los mismos 7 `insert` en `grpc/service.rs:99-108` y `iop/cedar_step.rs:35-45`. | 2 archivos |
| D3 | `is_master_only_entity` e `is_quota_exempt` son la misma lista dos veces. | `authorizer.rs:319-324` y `:381-386` |
| D4 | `get_principal_data` duplica la pipeline de `intercept` (step1→2→3→3b). | `authorizer.rs:640-645` vs `:484-488` |
| D5 | Chequeo de suspendido duplicado en `step2_query_oltp` (camino caché y camino fresco). | `principal_graph.rs:46-55` y `:71-85` |
| D6 | El patrón `tokio::spawn` + `expand_hierarchy` + `join!` está escrito 4 veces en `step3_consolidate` (grupos/roles × locations/assets). | `principal_graph.rs:109-192` |
| D7 | Fallback hardcodeado de 17 dominios que duplica el conocimiento del Códice. | `evaluator.rs:72-91` |
| D8 | `std::env::var("METRI_MASTER_TENANT_ID").unwrap_or("system")` repetido 3 veces. | `evaluator.rs:545`, `principal_graph.rs:339`, + `is_master_tenant` |

### Testabilidad

- Los tests unitarios construyen un `DynamoClient` **real** con tabla `"invalid-table"` (`authorizer.rs:945, 974, 1117`) y siembran el `EAV_CACHE` global (`:1085-1114`): la semilla de un test puede filtrarse a otro que corra en paralelo.
- `std::env::set_var` en tests (`:1172, :1441`) es una carrera entre hilos del mismo binario de test — origen de flakes intermitentes.
- `InMemoryPrincipalCache::new()` hace **spawn de una tarea tokio** que se suscribe al canal global (`authorizer.rs:139-182`): efecto secundario en el constructor, imposible de aislar sin runtime.
- El secreto HMAC se lee de `env` dentro de la función: no se puede probar sin mutar el entorno.

### Escalabilidad

- `step4_mutational` reconstruye **~20 entidades JSON + `Entities::from_json_str` + validación de schema por petición** (`evaluator.rs:215-448`) cuando solo varían User y Resource. Es el mayor coste evitable del camino caliente.
- `assemble_principal_graph` hace pulls **secuenciales N+1** a DynamoDB (rol por rol `principal_graph.rs:337`, grupo por grupo `:436`) mientras `step3_consolidate` sí paraleliza — el grafo es el cuello de botella del login path.
- La expulsión de ambas cachés es `keys().take(n/5)` sobre un `HashMap` — orden arbitrario, ni LRU ni LFU: bajo carga sostenida puede expulsar entradas calientes (`authorizer.rs:88-97`, `:200-210`).
- `CedarAuthorizer::is_authorized` colapsa la decisión a `bool` y pierde los diagnósticos de Cedar en el caso Allow-with-errors; aceptable, pero hay que documentarlo o devolver `Decision`.

---

# Parte II — Arquitectura objetivo

## Estructura de módulos

```
src/cedar/
├── mod.rs                    // API pública estable (re-exports) — los 14 consumidores no se rompen
├── types.rs                  // PrincipalData, RoleBoundary, TimeRestriction, CedarContext, InvalidationMsg
├── config.rs                 // CedarConfig { master_tenant_id, hmac_secret, cache_limits, hierarchy_depth }
├── ports.rs                  // traits: PrincipalCache, EntityReader, PolicyStore, HmacVerifier, Clock, InvalidationBus
├── authn.rs                  // UNA verificación HMAC (secreto inyectado) + extracción de token transport-agnóstica
├── request.rs                // DTO AuthRequest { headers } + adapter desde tonic (el acoplamiento tonic queda aquí)
├── pipeline.rs               // AuthPipeline compuesta UNA vez; intercept() y principal_for() son fachadas sobre ella
├── rules.rs                  // SystemSecurityRules con listas únicas + is_master_tenant único (de CedarConfig)
├── resource_hydrator.rs      // enriquecimiento assigned_company_id vía EntityReader (sale de intercept)
├── principal_graph.rs        // step2/step3/assemble sobre EntityReader inyectado; pulls paralelos por lotes
├── evaluator/
│   ├── mod.rs                // evaluate(): enrutamiento por Strategy
│   ├── strategy.rs           // MutationalStrategy / AnalyticalStrategy (+ SystemBffShortcut)
│   ├── entities_builder.rs   // base estática precompilada (OnceLock) + overlay User/Resource por petición
│   ├── grants.rs             // collect_user_grants + grant_allows_action (puros, con dominios inyectados)
│   └── action_registry.rs    // acción → grupo, derivado de cedar-schema.json (datos, no código)
└── cache/
    ├── mod.rs
    ├── principal.rs          // InMemoryPrincipalCache SIN spawn en el ctor
    └── invalidation.rs       // suscriptor con manejo explícito de Lagged (S3)
```

**Regla de compatibilidad:** `cedar/authorizer.rs` se convierte en un módulo de re-exports (`pub use` de todo lo público) durante las fases 1–4. Los 38 puntos de consumo siguen compilando sin cambios; el renombre de call-sites es una fase propia (5B) y mecánica. `mod.rs` mantendrá `pub mod authorizer;` hasta que el renombre termine.

## Patrones de diseño aplicados (y por qué)

| Patrón | Dónde | Problema que resuelve |
|---|---|---|
| **Port/Adapter** | `ports.rs` + `EavEntityReader`, `GrpcRequestAdapter` | `EavReader` concreto con `ddb`/`table` públicos impide mockear; `tonic` dentro del core obliga a tests con requests sintéticos |
| **Facade** | `pipeline.rs::intercept` | Hoy 5 pasos inline; mañana una composición única reusada por `intercept` y `get_principal_data` (mata D4) |
| **Strategy** | `evaluator/strategy.rs` | `is_mutational_action` + if/else en `step4_evaluate_cedar` → estrategias intercambiables; añadir una clase de decisión no toca el enrutador |
| **Builder** | `evaluator/entities_builder.rs` | Las 364 líneas de `step4_mutational` son un builder manual: base estática + dos overlays |
| **Registry** | `evaluator/action_registry.rs` | Acción→grupo como datos derivados del schema (OCP); la inconsistencia del `GET` se vuelve imposible |
| **Observer (explícito)** | `cache/invalidation.rs` + trait `InvalidationBus` | `INVALIDATION_TX` global → bus inyectado con `Lagged` manejado (resincronizar o al menos log-error + continuar); los publicadores gRPC reciben el bus por inyección |
| **Singleton acotado** | `OnceLock` extendido a la base de entidades precompilada | Lo que no varía por petición se construye una vez |
| **Null Object** | `AuthenticationPolicy::{Production, DevBypass}` | El bool `dev_auth_bypass` hilado como parámetro se vuelve un valor de configuración explícito decidido en la raíz de composición |
| **Template Method (ligero)** | `AuthPipeline` | Los pasos step1/2/3/3b conservan su orden contractual; la composición deja de copiarse |

## Mapeo SOLID → acción

- **S:** cada archivo nuevo del árbol objetivo tiene una sola razón de cambio (types/ports/authn/cache/evaluator/pipeline).
- **O:** nuevas acciones y grupos vía `action_registry` (datos); nuevas estrategias de decisión vía trait `DecisionStrategy`; nuevos stores de sesión/caché vía traits existentes.
- **L:** `InMemorySessionStore` se mueve a `#[cfg(test)]` y su `revoke_session` respeta el contrato (mantiene un set de jti revocados) — o se elimina si los tests usan el fake del propio módulo de tests.
- **I:** `EntityReader` expone solo `pull` y `children_with_attr` (lo que cedar usa); nada del dominio cedar depende de `DynamoClient`.
- **D:** cero globales en `src/cedar` al terminar (las precompilaciones `OnceLock` de datos inmutables son la única excepción admitida); cero `std::env::var` en camino de petición — todo por `CedarConfig` inyectada desde `main.rs`/`server.rs` (ADR-004).

---

# Parte III — Fases

Cada fase = uno o más commits que dejan `cargo test --lib` y `cargo check --all-targets` en verde. Orden pensado para que **primero entren las costuras y después se mueva código**.

## Fase 0 — Red de caracterización (día 1)

Antes de mover una línea, fijar el comportamiento actual en tests que sobrevivan al refactor. Solo funciones puras — nada que necesite las costuras nuevas:

1. Tests de `collect_user_grants` (wildcard dominio/acción, acción string vs array, burst `BulkIngestData`).
2. Tests de `grant_allows_action` y del enrutamiento de `step4_evaluate_cedar` (system-bff mutacional/analítico, mutacional vs analítico).
3. Test de `step3b_validate_time_window` ya existe — conservar.
4. Snapshot de los bordes de decisión de `step4_analytical` (tenant maestro vs normal, dominio sin grants → Auth403).

**Verificación:** `cargo test --lib cedar` (14 + ~10 nuevos, en verde).
**Commit:** `test(cedar): red de caracterización previa al desmonte`

## Fase 1 — Puertos y configuración (DIP primero)

1. `types.rs` con los tipos de dominio movidos textualmente; `authorizer.rs` re-exporta.
2. `config.rs` con `CedarConfig` y `ports.rs` con los traits:
   - `EntityReader` (`pull`, `children_with_attr`) — `EavReader` lo implementa como adapter.
   - `PolicyStore` (reemplaza `&HashMap<String, PolicySet>` en firmas).
   - `HmacVerifier` (secreto inyectado — mata S2), `Clock` (ya existe la costura en step3b, se generaliza), `InvalidationBus`.
3. `CedarConfig` se construye en `grpc/server.rs` a partir de `engine_config()` y se inyecta hacia abajo. **Fuera del camino de petición no queda ningún `env::var`** (S4 incluido: una sola definición de maestro).

**Riesgo bajo:** son agregaciones, no cambios de comportamiento.
**Commit:** `refactor(cedar): puertos y config inyectada — adiós a env y globales en el camino caliente`

## Fase 2 — Desmonte de `authorizer.rs` (SRP) y saneamiento de seguridad

1. `authn.rs`: **una sola** implementación HMAC del formato `mk_` (D1), con blacklist consultada también en el fast-path (**corrige S1**) y secreto inyectado (S2 ya cerrado en fase 1). `grpc/interceptors.rs` y `infrastructure/session_store.rs` pasan a usarla — tres implementaciones quedan en una.
2. `cache/`: `InMemoryPrincipalCache` sin spawn en el constructor; la tarea de invalidación se levanta explícitamente en la raíz de composición, consume un `InvalidationBus` inyectado y **maneja `Lagged`** (**corrige S3**): log-error + continuar (la ventana perdida se cubre con TTL de la caché, que esta fase añade).
3. `rules.rs`: listas deduplicadas (D3) + `is_master_tenant` única de config (S4).
4. `InMemorySessionStore` a `#[cfg(test)]` con contrato de revocación honesto (LSP).
5. Eliminar `always_allow_stub` y el stub `extract_req_body`/parámetro `body` (código muerto verificado).

**Commits:** uno por punto; el de S1 va primero y con su propia nota.
**Verificación:** los 14 tests de caracterización + fase 0 en verde; `grep -rn "INVALIDATION_TX\|EAV_CACHE" src/cedar` vacío.

## Fase 3 — Evaluator (OCP + Builder + Registry)

1. `action_registry.rs`: acción → grupo derivado de `cedar-schema.json`; el JSON de entidades base deja de estar hardcodeado en Rust y la inconsistencia `GET` queda resuelta por datos.
2. `entities_builder.rs`: la base de entidades (acciones, grupos) se precompila una vez en `OnceLock`; por petición solo se ensamblan User + Resource (el 90 % de las 364 líneas de `step4_mutational` muere).
3. `strategy.rs`: `MutationalStrategy`, `AnalyticalStrategy`, atajo `SystemBff` como estrategia de entrada; selección de `PolicySet` por boundary extraída a helper único.
4. `grants.rs`: `collect_user_grants` recibe la lista de dominios (del Códice) — el fallback de 17 dominios (D7) se va.

**Verificación:** caracterización en verde sin cambios — mismo comportamiento, menos asignaciones.
**Commit:** `refactor(cedar): evaluator como registry+builder — entidades precompiladas y estrategias`

## Fase 4 — Pipeline única (DRY + Facade) y desacople de tonic

1. `request.rs`: DTO `AuthRequest` (action, entity_type, entity_id, domains, auth_header) con adapter desde `tonic::Request<T>`. El core cedar deja de depender de `tonic` (solo el adapter lo toca).
2. `pipeline.rs`: `AuthPipeline::run(&AuthRequest)` compone authn→principal→ventana→hidratación→evaluación. `intercept` y `get_principal_data` delegan en ella (D4 muere).
3. `resource_hydrator.rs`: el bloque de `assigned_company_id` sale de `intercept` y va sobre `EntityReader`.
4. `principal_graph.rs`: dedupe del spawn pattern (D6), dedupe del chequeo de suspendido (D5), y los pulls de roles/grupos pasan a concurrentes acotados (o `BatchGetItem` si el adapter lo permite) — el N+1 secuencial del login path baja a O(1) rondas.

**Commit:** `refactor(cedar): pipeline de autorización única y core sin tonic`

## Fase 5 — Consumidores y tests (el gran renombre, mecánico)

1. **5A — Consumidores:** renombrar call-sites de los 14 archivos a las rutas nuevas (`use crate::cedar::{...}`); `authorizer.rs` desaparece dejando `mod.rs` como fachada. Búsqueda y reemplazo verificado por compilador.
2. **5B — Tests de calidad:** los tests que hoy necesitan `DynamoClient("invalid-table")` + siembra de `EAV_CACHE` + `set_var` se reescriben contra `EntityReader` fake en memoria y `CedarConfig` explícita: paralelizables, sin carreras, sin red. Los tests de jerarquía (`test_user_group_hierarchy_and_refinement`) son el caso protagonista.
3. **5C — Observabilidad:** contador allow/deny y latencia por paso con `otel` existente; log estructurado de denegaciones con `policy_id` de los diagnósticos Cedar.

**Commits:** `refactor(cedar): consumidores migrados a la nueva fachada` · `test(cedar): suite sin Dynamo ni globales` · `feat(cedar): métricas de decisión`

---

# Parte IV — Riesgos y orden de desbloqueo

| Riesgo | Mitigación | Fase |
|---|---|---|
| Cambio accidental de semántica al unificar las 3 implementaciones HMAC (S1 cambia comportamiento: tokens revocados empiezan a ser rechazados) | S1 es una corrección, no una regresión — commit aislado con nota; verificar con test de token revocado vía fast-path | 2 |
| El spawn en `InMemoryPrincipalCache::new()` tiene consumidores que asumen la invalidación activa | La tarea se levanta explícitamente en `server.rs`/`cedar_step.rs` antes de servir tráfico; test de integración de invalidación end-to-end | 2 |
| Las entidades Cedar precompiladas difieren de las actuales | Caracterización de fase 0 compara `Entities` JSON antes/después en los mismos casos; snapshot del JSON generado | 3 |
| Renombre de 38 call-sites rompe algo que compila pero no se usa | Fase 5A es puramente mecánica y la valida `cargo check --all-targets` + tests de integración `#[ignore]` corridos manualmente | 5A |
| `ListEntities`/`PLAN_REFACTORIZACION.md` toca los mismos handlers en paralelo | Este plan no toca handlers hasta 5A; coordinar la extracción del helper de autorización de lectura (`authz.rs`) con la fase 4 — sale gratis de la pipeline única | 4–5 |

## Métricas de éxito (verificables al cerrar)

| Métrica | Hoy | Objetivo |
|---|---|---|
| Líneas de `authorizer.rs` (código, sin tests) | 1.681 (659 productivas + 1.020 tests) | 0 — archivo eliminado |
| Función más grande del módulo | `step4_mutational` 364 | < 80 |
| Implementaciones de verificación HMAC | 3 | 1 |
| `env::var` en camino de petición de cedar | 4 | 0 |
| Globales mutables tocadas desde cedar | 2 (`INVALIDATION_TX`, `EAV_CACHE`) | 0 |
| Construcciones de `Entities` por petición mutacional | 1 completa (~20 entidades) | overlay de 2 sobre base cacheada |
| Tests unitarios con `DynamoClient` real | 3 | 0 |
| Tests del módulo | 14 | ≥ 30, todos paralelos-seguros |
