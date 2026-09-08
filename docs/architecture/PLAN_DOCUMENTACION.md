# PLAN_DOCUMENTACION.md — Plan de documentación de metri-engine

> **Alcance:** este plan *únicamente describe* la documentación que debe existir en metri-engine — qué se documenta, dónde y con qué formato — siguiendo las mejores prácticas de documentación en Rust. No incluye cambios de código, refactors ni ajustes de comportamiento.
>
> **Estado actual medido (2026-09-05):** 536 ítems públicos · 0 documentos a nivel de crate · 3 archivos con doc de módulo (`//!`) · ~1.000 líneas de comentarios `//` que rustdoc no renderiza · sin sección `[lints]` en `Cargo.toml`.

---

## 1. Dos capas de documentación

metri-engine tendrá dos capas complementarias, cada una con su formato canónico:

| Capa | Formato | Herramienta | Audiencia | Qué contiene |
|---|---|---|---|---|
| **En el código** | Doc-comments (`//!`, `///`) | `rustdoc` (`cargo doc`) | Quien programa contra o dentro del módulo | API de cada ítem: qué hace, `# Errors`, `# Panics`, `# Examples`, invariantes |
| **Markdown** | Archivos `.md` en `docs/` | Navegador / GitHub | Quien quiere entender el sistema | Arquitectura por componente, flujos, decisiones (ADR), guías operativas |

Regla de reparto: **si describe cómo usar un tipo o función, va en doc-comment; si describe cómo funciona o por qué está diseñado así, va en `.md`.** El doc-comment nunca duplica el `.md`: se enlazan por nombre de ADR/ documento (ej. «ver ADR-002»).

**Idioma:** documentación en español (convención ya establecida en README y `docs/`), conservando en inglés los términos técnicos de Rust (`trait`, `lifetimes`, `fail-closed`, nombres de tipos).

---

## 2. Estándar de documentación en Rust (obligatorio para todos los módulos)

Basado en las guías oficiales: *Rust API Guidelines* (sección C-DOCUMENT: `C-CRATE-DOC`, `C-MODULE-DOC`, `C-EXAMPLE`, `C-FAILURE`, `C-FAILURE`), *The rustdoc Book* y RFC 1574.

### 2.1 Jerarquía de doc-comments

1. **`//!` — documentación contenedora** (del crate y de cada módulo). Va al inicio de `lib.rs`, de cada `mod.rs` y de cada archivo de módulo hoja. Responde: *qué es este módulo, qué garantiza, cómo se relaciona con los demás*.
2. **`///` — documentación de ítem** (structs, enums, traits, funciones, constantes, campos públicos). Responde: *qué hace este ítem, qué errores puede devolver, cuándo entra en pánico, cómo se usa*.
3. **`//` — comentario de implementación.** Solo para notas internas de la implementación (por qué un algoritmo hace X). **Nunca** para documentar la API: rustdoc no lo renderiza.

### 2.2 Reglas de redacción

- **Primera línea = frase-resumen** de una sola línea, en presente, sin nombre repetido ("Genera un código Base36 de longitud `length`", no "Esta función que genera…"). rustdoc la muestra en índices y listados de autocompletado.
- **Markdown habilitado** en doc-comments: encabezados de sección con `#`, listas, `código en línea` con backticks, bloques de código con triple backtick.
- **Enlaces intra-documentación** con [`Corchetes`]: `` [`Datom`] ``, `` [`TransactError::UniqueClaim`] ``, `` [`crate::eav::writer`] ``. rustdoc los valida; un enlace roto es error de compilación de docs.
- **Invariantes en el tipo, no repartidas por las funciones**: si un struct garantiza inmutabilidad o aislamiento por tenant, se declara en el doc del struct.
- Los comentarios de portado ("En el stack anterior…", "reemplaza java.time…") **migran al doc de módulo** como sección `# Origen` o se eliminan; no quedan como `//` sueltos.

### 2.3 Secciones obligatorias por tipo de ítem

| Ítem | Secciones requeridas |
|---|---|
| Todo ítem público | Primera línea resumen + descripción + `# Examples` (al menos uno por módulo público, preferible en cada ítem) |
| Función que devuelve `Result` | `# Errors` — enumerar las variantes concretas y la condición que las dispara |
| Función que puede entrar en pánico | `# Panics` — condición exacta (aunque sea "si `length` > 36") |
| `unsafe fn` (no previsto en este codebase) | `# Safety` — contratos que el llamador debe cumplir |
| Trait | Doc por método (incluidos los con implementación default) + ejemplo de implementación mínima |
| Const/estático público | Unidad y rango válido |

### 2.4 Doctests

- Los bloques ```` ```rust ```` de los `# Examples` se compilan **y ejecutan** con `cargo test --doc`. Deben ser ejemplos reales y minimalistas.
- Para código que requiere I/O (DynamoDB, Athena, gRPC server) usar ```` ```no_run ```` (se compila, no se ejecuta) o ```` ```compile_fail ```` cuando el ejemplo documenta una construcción prohibida (útil en `aegis/sql/security`).
- Líneas de setup invisibles anteponiendo `# ` (imports, fixtures de test).

### 2.5 Lints de documentación (progresivos)

Añadir a `Cargo.toml` al cierre de la Fase 0, y subir el nivel al terminar cada fase:

```toml
[lints.rustdoc]
broken_intra_doc_links = "deny"
private_intra_doc_links = "deny"

[lints.rust]
missing_docs = "warn"        # subir a "deny" en la Fase 6, cuando el backlog esté en cero
```

`missing_docs` a nivel crate genera ruido inicial; la estrategia es incorporar módulo a módulo: al dar por documentado un módulo, ya no admite ítems públicos sin `///`.

### 2.6 Generación y verificación

```bash
cargo doc --no-deps --document-private-items   # generar y revisar localmente
cargo test --doc                                # los ejemplos se ejecutan
cargo clippy -- -W missing_docs                 # chequeo por módulo antes del lint global
```

---

## 3. Plantillas (copiar-pegar por módulo)

### 3.1 Cabecera de módulo (`//!`) — plantilla

```rust
//! Motor EAV inmutable sobre DynamoDB single-table.
//!
//! # Qué garantiza
//! - Inmutabilidad: un datom persistido jamás se actualiza ni borra.
//! - ACID dentro de una [`Transact`], vía claims de unicidad optimistas.
//! - Aislamiento por tenant: toda lectura/escritura lleva `tenant_id`.
//!
//! # Submódulos
//! - [`types`] — datom, encoding de valores, tipos primitivos.
//! - [`index`] — los cuatro índices: EAVT, AEVT, AVET, VAET.
//! - [`writer`] / [`reader`] — transacciones y consultas.
//!
//! # Origen
//! Port del motor EAV del stack Clojure anterior (Datomic-style).
//!
//! Ver ADR-001 (motor EAV sobre DynamoDB).

pub mod types;
```

### 3.2 Ítem público (`///`) — plantilla

```rust
/// Inserta o valida un lote de datoms dentro de una transacción.
///
/// Los claims de unicidad se resuelven de forma optimista: si dos
/// transacciones concurrentes reclaman el mismo `[Entidad, Atributo, Valor]`,
/// exactamente una comete y la otra recibe [`TransactError::UniqueClaim`].
///
/// # Examples
///
/// ```
/// # use metri_engine::eav::writer::transact;
/// let tx = transact::Builder::new(tenant).with_attribute("asset.code", "A-123");
/// assert!(tx.build().is_ok());
/// ```
///
/// # Errors
///
/// - [`TransactError::UniqueClaim`] — colisión de unicidad con otra tx.
/// - [`TransactError::SchemaViolation`] — el valor no cumple el modelo Codice.
///
/// # Panics
///
/// Entra en pánico si `tenant` es el tenant maestro (fail-closed).
```

---

## 4. Plan por componente

Cada componente lista: rol, qué debe documentarse **dentro de los archivos** y qué referencia cruzada existe. Prioridad: **P0** = se documenta primero (base de todo), **P1** = núcleo de negocio, **P2** = superficie/infraestructura.

### 4.1 Raíces del crate — `lib.rs`, `main.rs` · P0

**Dentro de los archivos.** `lib.rs` recibe el doc de crate (`//!`): qué es metri-engine (motor de datos CMMS/BI, Lambda ARM64, gRPC Tonic), el diagrama de flujo de alto nivel (reutilizar el ASCII del README), la tabla módulo→rol (hoy vive solo como comentarios `//` de una línea: migrar a doc real), y `# Origen` del port. `main.rs` documenta el binario `bootstrap` (requisito de `provided.al2023`) y su secuencia de arranque.

### 4.2 `domain` — tipos y contratos fundamentales · P0

**Rol:** errores canónicos, catálogo de errores, eventos de dominio, protocolos, config, resultados de pipeline.

**Dentro de los archivos.** Doc de módulo con el modelo de errores canónicos (cada variante documentada con su código, semántica HTTP/gRPC y cuándo se produce — ADR-005). `events/envelope.rs` y `events/delta.rs`: contrato del sobre de eventos y del delta (ADR-003, contrato único). `protocols.rs` y `pipeline/result.rs`: garantía de `Result` tipado sin `unwrap` en el dominio. Ejemplos doctest de construcción de error y de sobre de evento.

### 4.3 `codice` — registro SSOT de esquemas · P0

**Rol:** los ~60 modelos JSON de `config/models/` como única fuente de verdad; coerción, validación, generación de ids.

**Dentro de los archivos.** `registry.rs`: cómo se cargan/validan los modelos al arranque (ADR-004) y qué pasa con un modelo inválido (fail-fast). `coercion.rs`: tabla de coerciones aceptadas por tipo y ejemplos de cada una — es el ítem que más dudas genera a consumidores externos. `validator.rs`: cada regla con su error. `generator.rs` + `sequence.rs` + `base36.rs`: política zero-drop, probabilidades de colisión (ya bien documentado en `base36.rs`: es el estándar de referencia del repo). `materialization_spec.rs`: qué es la materialización genérica (ADR-007, recién incorporado — documentarlo antes de que crezca).

### 4.4 `eav` — motor de almacenamiento · P0

**Rol:** datoms inmutables, cuatro índices, transacciones ACID, FTS, jerarquías, cursors, sharding, telemetría. El componente con más invariantes del sistema.

**Dentro de los archivos.** Doc de módulo raíz con el layout single-table y el propósito de cada índice: `index/eavt` (entidad→attrs), `index/aevt` (attr→entidades), `index/avet` (valores/orden), `index/vaet` (referencias inversas). `types/datom.rs` y `types/encoding.rs`: invariantes de encoding y orden de bytes — documentar con ejemplos de codificación de cada `ValueType`. `writer/transact.rs`: semántica exacta de la transacción (orden de claims, chunker, enricher, `constraints/unique_claim.rs` con la política de colisión concurrente). `reader/pull.rs` y `reader/query.rs`: qué patrón de acceso sirve cada uno y cuándo usar cuál. `cursor/composite.rs`: formato MessagePack del cursor y estabilidad paginada. `fts/trigram.rs`: por qué trigramas y límites de consulta. `hierarchy/path.rs`: semántica de materialized path. Ejemplos doctest de transact y de pull mínimo.

### 4.5 `aegis` — compiladores (SQL Athena + OLTP + fórmulas) · P1

**Rol:** tres compiladores bajo un mismo paraguas: SQL→Athena (OLAP), executor OLTP sobre el EAV, y el lenguaje de fórmulas.

**Dentro de los archivos.** `mod.rs` de `aegis`, de `aegis/sql`, de `aegis/oltp` y de `aegis/formula`: cada submódulo con su propia introducción y su invariante reina. `sql/`: garantizar "inyección imposible por construcción" — documentar en `sql/security.rs` qué superficies son parametrizadas y marcar con ```` ```compile_fail ```` los ejemplos de construcción prohibida; `sql/dialect.rs` documenta los backends soportados (MySQL/Postgres via `sea-query`, Athena). `oltp/`: contrato del `channel.rs` y del `executor.rs` (pipeline filter→aggregation→hydrator→hierarchy), `fuzzy.rs` (semántica de similitud), `pagination.rs` (comportamiento con `truncated`). `formula/`: es un lenguaje completo — merece doc de módulo tipo libro: gramática resumida, tabla del catálogo de `functions_registry.rs` (una entrada por función con firma y ejemplo doctest), semántica de `security.rs` (qué está sandboxeado), y ciclo lexer→parser→resolver→compiler→evaluator con enlace entre etapas.

### 4.6 `temporal` — primitivas temporales · P1

**Rol:** ventanas, comparaciones y shifts de calendario timezone-aware (puerto de `metres.temporal`).

**Dentro de los archivos.** `core.rs` y `time_frame.rs`: semántica exacta de los time frames (`chrono-tz`), DST con ejemplos reales (un shift por calendario cruzando DST, ejemplo doctest). `comparison.rs`: operadores soportados y su comportamiento con zonas horarias distintas.

### 4.7 `janus` — read path · P1

**Rol:** normaliza requests gRPC → valida → selecciona plan → compila AST IR en FlatBuffers → agrega/multi-series.

**Dentro de los archivos.** Doc de módulo con el pipeline de lectura completo y un diagrama ASCII de etapas: `normalizer` (por variante: `query`, `bulk`, `discovery`, `explore`, `transaction` — cada uno con su regla de `match_rules.rs`/`strategy.rs`) → `validator.rs` → `plan_selector.rs` → `ast_compiler.rs`. `fbs.rs`: contrato FlatBuffers zero-copy con `janus_router` y `eav` (ADR-003) — documentar el ownership del buffer. `aggregator.rs` y `multi_series.rs`: semántica de agregación y de series múltiples con un ejemplo numérico. `abac_clauses.rs`: dónde se inyectan las cláusulas Cedar.

### 4.8 `janus_router` — write path · P1

**Rol:** enrutamiento OLTP/OLAP, sagas de escritura, particionado, generación de ULIDs.

**Dentro de los archivos.** `router.rs`: árbol de decisión de ruteo (OLTP vs OLAP) como diagrama. `saga.rs`: pasos de la saga, compensaciones, garantía de idempotencia y qué pasa con un fallo a mitad (los tests de `tests/saga_tests.rs` son la fuente: documentar el comportamiento que fijan). `partition.rs`: función de particionado y sus invariantes. `ulid.rs`: monotonía y por qué ULID y no UUIDv4.

### 4.9 `cedar` — autorización ABAC · P1

**Rol:** Cedar Policy 3, evaluación fail-closed, cache de principals, reglas del sistema, zero-trust del tenant maestro.

**Dentro de los archivos.** `mod.rs`: el modelo de autorización completo con referencia a ADR-002. `pipeline.rs`: orden de evaluación. `evaluator/` (action_registry, grants, analytical, mutational, entities_builder): cada variante con su semántica de decisión — documentar explícitamente qué devuelve un evaluador ante error de entidades (fail-closed, no `allow`). `authn.rs`: distinguir autenticación (HMAC/sesión) de autorización (Cedar). `cache/` (principal, session, invalidation): TTLs, claves de invalidación y cuándo se invalida. `rules.rs`: las reglas del sistema como tabla documentada.

### 4.10 `quota` — cuotas por tenant · P2

**Rol:** ledger atómico con autoridad, reservas para IA, sweeper, proyección.

**Dentro de los archivos.** Ya es el módulo mejor documentado (239 líneas `///`): el trabajo aquí es **regularizarlo al estándar** (secciones `# Errors`/`# Examples` completas, doc de módulo `//!` que hoy no existe) y documentar el modelo de atomicidad del `ledger.rs` (por qué DynamoDB da la autoridad), el ciclo de vida de una reserva (`reservations.rs` → `sweeper.rs` expira) y la semántica de `resolver.rs`/`projection.rs`.

### 4.11 `iop` — pipeline de ingesta · P2

**Rol:** orquestación por pasos Cedar → Quota → Janus.

**Dentro de los archivos.** `core.rs`: el contrato de un "step" (entrada/salida/aborto). `pipeline.rs`: orden garantizado y comportamiento de corto-circuito al primer fallo. Un doc por step (`cedar_step.rs`, `quota_step.rs`, `janus_step.rs`) con su única responsabilidad y su error dominante. `error_response.rs`: mapeo a respuestas gRPC. `sherlog.rs`: qué se loguea y qué nunca (datos sensibles).

### 4.12 `eda` — arquitectura orientada a eventos · P2

**Rol:** outbox y detección de fallas (EventBridge/SQS).

**Dentro de los archivos.** Es el módulo más pequeño (2 archivos) y el **peor documentado** (0 `///`): doc de módulo completo con el patrón outbox (por qué outbox y no publicación directa), el ciclo del evento, semántica de reintento de `moira.rs` y el routing a EventBridge/SQS. Ejemplo doctest del sobre `eda`.

### 4.13 `grpc` — superficie de API · P2

**Rol:** server Tonic, interceptores, traducción proto↔dominio, handlers por RPC.

**Dentro de los archivos.** `server.rs`: puertos, reflection, TLS. `interceptors.rs`: **el orden de los interceptores documentado como secuencia obligatoria** (HMAC → sesión → Cedar fail-closed) — es un contrato de seguridad que hoy solo vive en código. `translator.rs`: mapeo bidireccional proto↔dominio y política ante campos desconocidos. `handlers/`: un doc por handler RPC con el flujo interno y los errores canónicos que puede devolver (referencia cruzada a `docs/reference/api-grpc.md`). `bootstrap.rs`: secuencia de arranque de servicios AWS.

### 4.14 `infrastructure` — adaptadores AWS · P2

**Rol:** adaptadores del SDK (DynamoDB, S3, Athena, Kinesis, SQS, EventBridge, Glue) + sesión, tenant guard, auditoría, bus de eventos, spawner de work orders, motor local de query S3.

**Dentro de los archivos.** Convención por adaptador: doc de módulo con el contrato (qué operación de dominio implementa), timeouts/reintentos, y cómo se emula en local (Docker/localstack según `docs/guides/desarrollo-local.md`). `local_s3_query_engine/` (recién refactorizado en `sql_parse.rs`/`pipeline.rs`/`lake_reader.rs`): documentar el pipeline en memoria puro y el corpus de caracterización. `domain_event_bus.rs` y `work_order_spawner.rs` (nuevos): documentarlos **ya**, en verde, antes de que cristalicen sin doc. `tenant_guard.rs`: el mecanismo de aislamiento — doc con severidad máxima y ejemplo de bloqueo. `seeder.rs` y `session_store.rs`: propósito y ámbito (¿solo dev?).

### 4.15 `otel` — trazas · P2

**Rol:** OpenTelemetry OTLP.

**Dentro de los archivos.** Qué se traza (spans por RPC, atributos de tenant), dónde se exportan, y cómo desactivarlo en local.

### 4.16 `application` — puertos de aplicación (DIP) · P2

**Rol:** contratos `metri-contracts` (ports) y scheduling (hooks, spawn).

**Dentro de los archivos.** `ports.rs`: cada trait puerto con su implementador esperado y ejemplo de implementación mínima (patrón C-TRAIT de las guías). `scheduling/spawn.rs`: qué dispara un work order y su relación con `infrastructure/work_order_spawner.rs`.

### 4.17 Binarios y tests · P2

- `src/bin/firehose-seeder.rs`: doc de cabecera con propósito, uso y ámbito (¿herramienta de desarrollo?).
- Cada archivo `*_tests.rs` con doc de módulo de una línea que diga **qué comportamiento caracteriza** (convención ligera: no se exige `///` en cada test).

---

## 5. Documentación `.md`

La estructura existente en `docs/` es sana y se conserva; el plan la completa y le añade índice.

```text
docs/
├── README.md                      # NUEVO — índice navegable de toda la documentación
├── architecture/
│   ├── 00_OVERVIEW.md             # existe — revisar que refleje los módulos nuevos
│   ├── ADR-001…ADR-007            # existen — ADR-007 ya en repo
│   └── PLAN_*.md                  # existen (incluye este plan)
├── components/                    # NUEVO — un .md por componente (sección 5.2)
│   ├── eav.md, aegis.md, janus.md, … (15 archivos)
├── guides/
│   ├── desarrollo-local.md        # existe
│   └── despliegue.md              # existe
└── reference/
    ├── api-grpc.md                # existe — actualizar con RPCs nuevos
    ├── codigos-error.md           # existe — validar contra error_catalog.rs
    └── modelos-codice.md          # existe — regenerar de config/models/
```

### 5.1 `docs/README.md` — índice

Tabla con: documento → audiencia → cuándo consultarlo. Enlaces desde README.md raíz. Regla: ningún `.md` huérfano (todo documento debe ser alcanzable desde el índice).

### 5.2 `docs/components/<modulo>.md` — ficha por componente

Quince documentos cortos (1–3 páginas), con plantilla fija para que sean comparables entre sí:

1. **Propósito** — qué problema resuelve el componente (2–4 frases).
2. **Responsabilidades / no-responsabilidades** — qué NO hace (evita que crezca por absorción).
3. **Flujo interno** — diagrama ASCII de etapas.
4. **Invariantes** — lista numerada; cada invariante con el test que la protege.
5. **Puntos de entrada** — los 3–5 tipos/funciones públicos que un consumidor debe conocer (enlazando al rustdoc generado).
6. **Errores** — los errores canónicos que produce.
7. **Decisiones** — enlaces a los ADR que lo respaldan.
8. **Riesgos conocidos / TODO** — deudas documentadas.

El contenido de cada ficha sale de la Fase correspondiente de doc-comments: el `.md` se escribe cuando el módulo ya está documentado en código, extrayendo la vista arquitectónica (no duplicando la API).

### 5.3 `README.md` raíz

Ya cumple bien su rol (entrada con arquitectura en un vistazo). Cambios: enlazar `docs/README.md`, y actualizar la tabla de módulos con los nuevos (`application`, spawner de work orders). Regla permanente: el README nunca explica internas — eso es de `docs/components/`.

---

## 6. Fases de ejecución

Orden bottom-up por dependencias: primero lo que todos consumen, al final la superficie.

| Fase | Alcance | Entregable | Criterio de aceptación |
|---|---|---|---|
| **0** | Estándar + plantillas (secciones 2–3) | Este plan aprobado; lints `broken_intra_doc_links = deny` en `Cargo.toml` | `cargo doc` compila sin enlaces rotos |
| **1** | Raíces: `lib.rs`, `main.rs` + `docs/README.md` | Doc de crate + índice | Diagrama y tabla de módulos en el rustdoc del crate |
| **2** | P0: `domain`, `codice`, `eav` | Doc-comments completos | 100% ítems públicos con `///`; ≥1 doctest por módulo público; `# Errors` en toda fn con `Result` |
| **3** | P1: `aegis`, `janus`, `temporal` | Ídem + gramática de fórmulas + contrato FlatBuffers | Ídem; `compile_fail` en los ejemplos prohibidos de `aegis/sql` |
| **4** | P1: `janus_router`, `cedar`, `quota`, `iop`, `eda` | Ídem; `eda` pasa de 0 doc a estándar completo | Orden de interceptores y saga documentados |
| **5** | P2: `grpc`, `infrastructure`, `otel`, `application`, bins | Ídem | Orden de interceptores en `grpc::interceptors` con su secuencia obligatoria |
| **6** | `.md`: 15 fichas en `docs/components/` + actualización de `reference/` + README | Documentación md cerrada | Todo documento alcanzable desde índice; `missing_docs = "deny"` en `Cargo.toml` |

En cada fase: `cargo doc --no-deps` sin warnings de rustdoc, `cargo test --doc` en verde, y un PR por módulo (revisable, sin PRs monolíticos de doc).

---

## 7. Mantenimiento (reglas permanentes)

1. **Definición de hecho** de todo PR futuro: si añade un ítem público, trae su `///` con secciones completas — `missing_docs = deny` lo hace cumplir en CI.
2. **Regla de las dos capas**: un cambio de arquitectura actualiza su `docs/components/<modulo>.md` y, si cambia la decisión, genera ADR nuevo; un cambio de API actualiza solo doc-comments.
3. **Los ADR son inmutables**: una decisión revertida genera un ADR nuevo que la invalida, nunca se edita el original.
4. Los doc-tests son tests de CI: un ejemplo que deja de compilar rompe el build — la documentación no puede podrirse en silencio.
