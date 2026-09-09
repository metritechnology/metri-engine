# PLAN_DOCUMENTACION.md — metri-engine Documentation Plan

> **Alcance:** este plan *únicamente describe* la documentación que debe existir en metri-engine — qué se documenta, dónde y con qué formato — siguiendo las mejores prácticas de documentación en Rust. No incluye cambios de código, refactors ni ajustes de comportamiento.
>
> **Estado actual medido (re-medición 2026-09-08):** 514 ítems públicos · 0 documentos a nivel de crate (`//!`) · 3 archivos con doc de módulo · ~1.060 líneas de comentarios `//` que rustdoc no renderiza · sin sección `[lints]` en `Cargo.toml`.
>
> **Ejecución (2026-09-08):** Fases 0–6 **implementadas y verificadas** — auditor `scripts/dev/check_docs.py --strict` en verde (278/278 archivos `.rs` con cabecera `//!`, allowlist vacío), lints `[lints.rustdoc]` en `deny`, doc de crate en `lib.rs`/`main.rs`/bins, `docs/README.md` bilingüe, `README.en.md`, 15 fichas en `docs/components/` y `docs/reference/glosario.md`. Pendiente del alcance interior: cobertura `///` completa de los ítems públicos con `# Errors`/`# Examples` por módulo; `missing_docs` entra en `warn` cuando ese backlog cierre (activarlo antes rompería el gate de warnings de clippy en CI).
>
> **Cambios desde la v1 de este plan:** (1) el stack de materialización fue **eliminado** (ADR-007 SUPERADO — la composición de OTs vive en metri-cmms-plugin), así que aquí ya no se contempla documentar `work_order_spawner`, `materialization_spec`, `application/scheduling` ni `orchestration_service`; (2) `eav/writer` estrenó cinco módulos (`datom_plan`, `outbox`, `system_attrs`, `cache_policy`, `fts_dispatch`); (3) `cedar` incorporó la validación semántica de grants (F5); (4) el `PLAN_PATRON_RESULT.md` quedó **cumplido** — el crate vive bajo lints `deny` de pánico/unwrap y CI ya ejecuta un auditor estricto que este plan aprovecha en vez de duplicar; (5) se incorpora la política de documentación bilingüe español + inglés (§1); (6) los títulos de toda la documentación —H1 de los `.md` y primera línea de los doc-comments— se escriben siempre en inglés (§1, §2.2); (7) cobertura universal: cada archivo `.rs` abre con su cabecera `//!`, verificada por auditor propio con ratchet (§2.7).

---

## 1. Capas de documentación y política de idioma

metri-engine tendrá dos capas complementarias, cada una con su formato canónico:

| Capa | Formato | Herramienta | Audiencia | Qué contiene |
|---|---|---|---|---|
| **En el código** | Doc-comments (`//!`, `///`) | `rustdoc` (`cargo doc`) | Quien programa contra o dentro del módulo | API de cada ítem: qué hace, `# Errors`, `# Panics`, `# Examples`, invariantes |
| **Markdown** | Archivos `.md` en `docs/` | Navegador / GitHub | Quien quiere entender el sistema | Arquitectura por componente, flujos, decisiones (ADR), guías operativas |

Regla de reparto: **si describe cómo usar un tipo o función, va en doc-comment; si describe cómo funciona o por qué está diseñado así, va en `.md`.** El doc-comment nunca duplica el `.md`: se enlazan por nombre de ADR/ documento (ej. «ver ADR-002»).

**Política bilingüe (español + inglés).** metri-engine documenta en ambas lenguas con un reparto por coste de mantenimiento, no por traducción exhaustiva. La premisa: rustdoc no soporta i18n y duplicar 514 ítems públicos en dos lenguas garantiza drift; la solución es una lengua canónica por capa y bilingüismo completo solo donde llega primero el lector externo.

| Capa | Lengua | Detalle |
|---|---|---|
| Doc-comments (`///`, `//!`) | **Cuerpo en español canónico; título y encabezados en inglés** | La primera línea (título) y los encabezados van en inglés —es lo que rustdoc muestra en índices, hovers y navegación—; el cuerpo, en español (continuidad con la convención del repo y la lengua del equipo). Los encabezados conservan la forma estándar de Rust (`# Examples`, `# Errors`, `# Panics`, `# Safety`) o equivalentes propios en inglés (`# Guarantees`, `# Origin`…). Identificadores y código de los ejemplos en inglés natural. Si el crate llegara a abrirse a público externo, el cuerpo es lo primero que se migra a inglés (decisión a revisar entonces, no ahora). |
| `.md` de entrada (bilingüe completo) | **Ambas** | `README.md` (español) ↔ `README.en.md` (inglés), enlazados en la primera línea. Igual para `docs/README.md` (índice) y `docs/architecture/00_OVERVIEW.md`. Un lector inglés debe entender el sistema sin abrir un diccionario. El H1 va en inglés en ambas versiones. |
| Fichas de componente y guías (español + resumen) | Español canónico + **resumen en inglés** | Cada `docs/components/<modulo>.md` y cada guía abre con un bloque `> **English summary:**` de 3–5 frases; título (H1) y encabezados de sección en inglés. El cuerpo no se traduce. |
| ADRs, planes y reference (solo español) | Español (título H1 en inglés) | Documentos de decisión y operación interna; el glosario bilingüe los hace seguibles para un lector inglés. |

Convenciones asociadas:

- **Títulos siempre en inglés:** el H1 de cada `.md` y la primera línea (frase-resumen) de cada doc-comment (`//!`, `///`) se escriben en inglés, en toda capa — son la «cara» de la documentación (índices de rustdoc, hovers del IDE, listados del repo) y deben ser únicos y estables; el cuerpo mantiene la lengua de su capa.
- **Nombres de archivo:** `<doc>.md` es español; la traducción inglesa —solo en la capa bilingüe completa— vive en `<doc>.en.md`, con enlace cruzado `→ English version` en la primera línea de ambos.
- **Anti-drift:** el PR que modifica un documento bilingüe actualiza también su `.en.md` en el mismo PR; la versión inglesa es traducción fiel, no reimplementación.
- **Glosario:** `docs/reference/glosario.md` fija la equivalencia EN↔ES de los términos del dominio (sobre de eventos/event envelope, plano de datos/data plane, cerradura/lock, drenaje del outbox/outbox draining…) — obligatorio para que traducciones y resúmenes usen siempre el mismo término.
- **Doctests:** los ejemplos ejecutables se escriben una sola vez (identificadores en inglés, comentarios breves en español); no se duplican por lengua.

---

## 2. Estándar de documentación en Rust (obligatorio para todos los módulos)

Basado en las guías oficiales: *Rust API Guidelines* (sección C-DOCUMENT: `C-CRATE-DOC`, `C-MODULE-DOC`, `C-EXAMPLE`, `C-FAILURE`, `C-FAILURE`), *The rustdoc Book* y RFC 1574.

### 2.1 Jerarquía de doc-comments

1. **`//!` — documentación contenedora** (del crate y de cada módulo). Va al inicio de `lib.rs`, de cada `mod.rs` y de cada archivo de módulo hoja. Responde: *qué es este módulo, qué garantiza, cómo se relaciona con los demás*.
2. **`///` — documentación de ítem** (structs, enums, traits, funciones, constantes, campos públicos). Responde: *qué hace este ítem, qué errores puede devolver, cuándo entra en pánico, cómo se usa*.
3. **`//` — comentario de implementación.** Solo para notas internas de la implementación (por qué un algoritmo hace X). **Nunca** para documentar la API: rustdoc no lo renderiza.

### 2.2 Reglas de redacción

- **Primera línea = frase-resumen, siempre en inglés** (es el título del doc: rustdoc la muestra en índices, hovers y listados de autocompletado), de una sola línea, en presente, sin nombre repetido ("Generates a Base36 code of length `length`", no "This function that generates…"). El cuerpo desarrolla en la lengua de la capa (§1).
- **Markdown habilitado** en doc-comments: encabezados de sección con `#`, listas, `código en línea` con backticks, bloques de código con triple backtick.
- **Enlaces intra-documentación** con [`Corchetes`]: `` [`Datom`] ``, `` [`TransactError::UniqueClaim`] ``, `` [`crate::eav::writer`] ``. rustdoc los valida; un enlace roto es error de compilación de docs.
- **Invariantes en el tipo, no repartidas por las funciones**: si un struct garantiza inmutabilidad o aislamiento por tenant, se declara en el doc del struct.
- Los comentarios de portado ("En el stack anterior…", "reemplaza java.time…") **migran al doc de módulo** como sección `# Origin` o se eliminan; no quedan como `//` sueltos.
- **Encabezados siempre en inglés** — tanto los marcadores estándar de rustdoc (`# Examples`, `# Errors`, `# Panics`, `# Safety`) como los encabezados propios (`# Guarantees`, `# Submodules`, `# Origin`…): los encabezados son la navegación del documento y viven en una sola lengua; la prosa debajo va en la lengua de la capa.

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

**Coexistencia con la cerradura Result (ya instalada).** El patrón Result vive como atributo interior (`#![cfg_attr(not(test), deny(clippy::unwrap_used, …))]`) en `lib.rs`, `main.rs` y `bin/firehose-seeder.rs`; los lints de documentación entran por la sección `[lints]` de `Cargo.toml` y no tocan esa cerradura. El chequeo de docs (`cargo doc --no-deps`, `cargo test --doc`) se añade a `.github/workflows/rust.yml`, junto al auditor Result (`scripts/dev/check_result_pattern.py --strict`) que ya bloquea PRs: un solo pipeline de calidad para código y documentación.

### 2.6 Generación y verificación

```bash
cargo doc --no-deps --document-private-items   # generar y revisar localmente
cargo test --doc                                # los ejemplos se ejecutan
cargo clippy -- -W missing_docs                 # chequeo por módulo antes del lint global
```

### 2.7 Cobertura por archivo: todo `.rs` lleva su documentación

La documentación no vive solo en los módulos públicos: **cada archivo `.rs` de `src/` abre con su cabecera `//!`** — sin excepciones más allá del código generado. La cabecera es breve pero obligatoria, con título en inglés y cuerpo en español (§1). Qué debe contener según el tipo de archivo:

| Tipo de archivo | Cabecera `//!` | Además |
|---|---|---|
| `mod.rs` (raíz de módulo) | Qué es el módulo, qué garantiza, mapa de submódulos, `# Origin` | `///` en todos sus ítems públicos |
| Módulo hoja con ítems públicos | Qué resuelve este archivo y su invariante principal | `///` en todos sus ítems públicos, con `# Errors`/`# Panics` |
| Módulo interno (sin ítems públicos) | Qué hace el archivo y por qué existe — un lector entiende su papel sin leer el cuerpo | `//` para notas de implementación |
| `*_tests.rs` / `tests/` | Una línea (título en inglés) que diga qué comportamiento caracteriza | No se exige `///` por test |
| Binarios (`main.rs`, `bin/*.rs`) | Qué hace el binario, cómo se invoca, ámbito (¿solo dev?) | — |

**Excepciones —solo código generado:** flatc vía `src/janus/fbs.rs` y el módulo tonic `pb` embebido en `grpc/mod.rs` quedan exentos y marcados con `// generated — do not edit`. Ninguna otra exención: si un archivo no merece una línea de documentación, no merece estar en el árbol.

**Verificación automática.** Al estilo del auditor del patrón Result, un script propio (`scripts/dev/check_docs.py`) con ratchet —nunca retrocede— falla si: (a) un `.rs` no generado carece de cabecera `//!`; (b) una cabecera no abre con título en inglés de una línea. La cobertura de `///` por ítem público sigue siendo tarea del lint `missing_docs` (§2.5); el auditor cubre lo que el lint no ve: cabeceras de archivos privados y de tests. Se integra a `.github/workflows/rust.yml` junto al auditor Result en la Fase 0.

**Baseline (2026-09-08):** 278 archivos `.rs` en `src/` (215 sin contar tests) · 3 con cabecera `//!`. El ratchet parte de esta foto y cierra en 0 al terminar la Fase 5.

---

## 3. Plantillas (copiar-pegar por módulo)

### 3.1 Cabecera de módulo (`//!`) — plantilla

```rust
//! Immutable EAV engine over a DynamoDB single table.
//!
//! # Guarantees
//! - Inmutabilidad: un datom persistido jamás se actualiza ni borra.
//! - ACID dentro de una [`Transact`], vía claims de unicidad optimistas.
//! - Aislamiento por tenant: toda lectura/escritura lleva `tenant_id`.
//!
//! # Submodules
//! - [`types`] — datom, encoding de valores, tipos primitivos.
//! - [`index`] — los cuatro índices: EAVT, AEVT, AVET, VAET.
//! - [`writer`] / [`reader`] — transacciones y consultas.
//!
//! # Origin
//! Port del motor EAV del stack Clojure anterior (Datomic-style).
//!
//! Ver ADR-001 (motor EAV sobre DynamoDB).

pub mod types;
```

### 3.2 Ítem público (`///`) — plantilla

```rust
/// Inserts or validates a batch of datoms within a transaction.
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

En ambas plantillas se aplica la política de idioma: **título (primera línea) y encabezados en inglés; cuerpo y explicaciones en español** (§1).

---

## 4. Plan por componente

Cada componente lista: rol, qué debe documentarse **dentro de los archivos** y qué referencia cruzada existe. Prioridad: **P0** = se documenta primero (base de todo), **P1** = núcleo de negocio, **P2** = superficie/infraestructura.

### 4.1 Raíces del crate — `lib.rs`, `main.rs` · P0

**Dentro de los archivos.** `lib.rs` recibe el doc de crate (`//!`): qué es metri-engine (motor de datos CMMS/BI, Lambda ARM64, gRPC Tonic), el diagrama de flujo de alto nivel (reutilizar el ASCII del README), la tabla módulo→rol — hoy la lista `pub mod` vive como comentarios `// FASE N ✓` de una línea: migran al doc real — y `# Origin` del port. El doc de crate convive con la cerradura Result que abre el archivo (la cerradura se queda; el `//!` se coloca antes o después — decisión de estilo única para todo el repo). `main.rs` documenta el binario `bootstrap` (requisito de `provided.al2023`) y su secuencia de arranque.

### 4.2 `domain` — tipos y contratos fundamentales · P0

**Rol:** errores canónicos, catálogo de errores, eventos de dominio, protocolos, config, resultados de pipeline.

**Dentro de los archivos.** Doc de módulo con el modelo de errores canónicos (cada variante documentada con su código, semántica HTTP/gRPC y cuándo se produce — ADR-005). El catálogo es documentación normativa: la correspondencia 1:1 entre variantes `ErrorCode` (72) y entradas de `config/errors/error_catalog.toml` (88, con `reserved`/`deprecated`) ya la bloquea el auditor Result, y la doc de cada variante nombra su entrada TOML y sus campos (`is_retryable`, mapeo gRPC). Las 12 invariantes de `scripts/dev/result_pattern_allowlist.json` se listan y enlazan desde el doc de módulo. `events/envelope.rs` y `events/delta.rs`: contrato del sobre de eventos y del delta (ADR-003, contrato único) — mismo formato aplanado que el payload del outbox (ver §4.4). `protocols.rs` y `pipeline/result.rs`: garantía de `Result` tipado sin `unwrap` en el dominio. Ejemplos doctest de construcción de error y de sobre de evento.

### 4.3 `codice` — registro SSOT de esquemas · P0

**Rol:** los ~60 modelos JSON de `config/models/` como única fuente de verdad; coerción, validación, generación de ids.

**Dentro de los archivos.** `registry.rs`: cómo se cargan/validan los modelos al arranque (ADR-004) y qué pasa con un modelo inválido (fail-fast). `coercion.rs`: tabla de coerciones aceptadas por tipo y ejemplos de cada una — es el ítem que más dudas genera a consumidores externos. `validator.rs`: cada regla con su error. `generator.rs` + `sequence.rs` + `base36.rs`: política zero-drop, probabilidades de colisión (ya bien documentado en `base36.rs`: es el estándar de referencia del repo). Familia `procedure` (nueva): `procedure`/`procedure_field` — plantillas tipadas con scoring — y `work_order_procedure`/`work_order_procedure_field` — instancia 1-N con provenance y captura de valor por campo; documentar la distinción plantilla↔instancia y la jerarquía OT 1-N (`parent_work_order_id`, `is_parent`, `completed_by`/`completed_at`). Nota de alcance en el doc de módulo: el motor **no** materializa OTs (ADR-007 SUPERADO — esa composición vive en metri-cmms-plugin), para que nadie busque aquí el stack eliminado.

### 4.4 `eav` — motor de almacenamiento · P0

**Rol:** datoms inmutables, cuatro índices, transacciones ACID, FTS, jerarquías, cursors, sharding, telemetría. El componente con más invariantes del sistema.

**Dentro de los archivos.** Doc de módulo raíz con el layout single-table y el propósito de cada índice: `index/eavt` (entidad→attrs), `index/aevt` (attr→entidades), `index/avet` (valores/orden), `index/vaet` (referencias inversas). `types/datom.rs` y `types/encoding.rs`: invariantes de encoding y orden de bytes — documentar con ejemplos de codificación de cada `ValueType`. `writer/transact.rs`: semántica exacta de la transacción (orden de claims, chunker, enricher, `constraints/unique_claim.rs` con la política de colisión concurrente). Los cinco módulos nuevos del escritor — ya con buena introducción en `//` que migra a `//!`: `datom_plan.rs` (planificador puro del camino de escritura: retracts del valor previo, asserts del nuevo, atributos de sistema, delta del contrato y rescate de `deleted_attrs`), `system_attrs.rs` (fuente única de los attr_ids reservados — contrato escritor↔lector que antes se duplicaba a mano), `cache_policy.rs` (invalidación de `EAV_CACHE`/`AEVT_SCAN_CACHE` tras el commit, incluidas las entidades proyectadas de saga), `fts_dispatch.rs` (índice FTS fuera de la transacción ACID — la consistencia degradada se documenta como política decidida, con el spawn después del commit) y `outbox.rs` (la fila `outbox_event` nace en la misma transacción que la mutación; su id es el ULID de la mutación y la llave de orden del ledger; payload aplanado idéntico al delta de metri-contracts — documentar el vínculo con `infrastructure/domain_event_bus.rs` y purgar de su introducción a los consumidores ya eliminados). `reader/pull.rs` y `reader/query.rs`: qué patrón de acceso sirve cada uno y cuándo usar cuál. `cursor/composite.rs`: formato MessagePack del cursor y estabilidad paginada. `fts/trigram.rs`: por qué trigramas y límites de consulta. `hierarchy/path.rs`: semántica de materialized path. Ejemplos doctest de transact y de pull mínimo.

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

**Dentro de los archivos.** `mod.rs`: el modelo de autorización completo con referencia a ADR-002. `pipeline.rs`: orden de evaluación. `evaluator/` (action_registry, grants, analytical, mutational, entities_builder): cada variante con su semántica de decisión — documentar explícitamente qué devuelve un evaluador ante error de entidades (fail-closed, no `allow`). `evaluator/grants.rs` (F5, reciente): validación semántica de grants — expansión del conjunto plano `dominio:acción`, rechazo temprano de acciones desconocidas y wildcard de dominio que expande todos los dominios inyectados; semántica no obvia, ejemplos doctest obligatorios. `authn.rs`: distinguir autenticación (HMAC/sesión) de autorización (Cedar). `cache/` (principal, session, invalidation): TTLs, claves de invalidación y cuándo se invalida. `rules.rs`: las reglas del sistema como tabla documentada.

### 4.10 `quota` — cuotas por tenant · P2

**Rol:** ledger atómico con autoridad, reservas para IA, sweeper, proyección.

**Dentro de los archivos.** Ya es el módulo mejor documentado (239 líneas `///`): el trabajo aquí es **regularizarlo al estándar** (secciones `# Errors`/`# Examples` completas, doc de módulo `//!` que hoy no existe) y documentar el modelo de atomicidad del `ledger.rs` (por qué DynamoDB da la autoridad), el ciclo de vida de una reserva (`reservations.rs` → `sweeper.rs` expira) y la semántica de `resolver.rs`/`projection.rs`.

### 4.11 `iop` — pipeline de ingesta · P2

**Rol:** orquestación por pasos Cedar → Quota → Janus.

**Dentro de los archivos.** `core.rs`: el contrato de un "step" (entrada/salida/aborto). `pipeline.rs`: orden garantizado y comportamiento de corto-circuito al primer fallo. Un doc por step (`cedar_step.rs`, `quota_step.rs`, `janus_step.rs`) con su única responsabilidad y su error dominante. `error_response.rs`: mapeo a respuestas gRPC. `sherlog.rs`: qué se loguea y qué nunca (datos sensibles).

### 4.12 `eda` — arquitectura orientada a eventos · P2

**Rol:** outbox y detección de fallas (EventBridge/SQS).

**Dentro de los archivos.** Es el módulo más pequeño (2 archivos) y el **peor documentado** (0 `///`). El outbox ya no es un patrón conceptual: la fila nace en `eav/writer/outbox.rs` dentro de la transacción EAV. El doc de módulo narra el ciclo completo del evento — nace `outbox_event` en la transacción → `moira.rs` lo drena con su semántica de reintento → routing a EventBridge/SQS — y por qué outbox y no publicación directa. Ejemplo doctest del sobre `eda`.

### 4.13 `grpc` — superficie de API · P2

**Rol:** server Tonic, interceptores, traducción proto↔dominio, handlers por RPC.

**Dentro de los archivos.** `server.rs`: puertos, reflection, TLS. `interceptors.rs`: **el orden de los interceptores documentado como secuencia obligatoria** (HMAC → sesión → Cedar fail-closed) — es un contrato de seguridad que hoy solo vive en código. `translator.rs`: mapeo bidireccional proto↔dominio y política ante campos desconocidos. `handlers/`: un doc por handler RPC con el flujo interno y los errores canónicos que puede devolver (referencia cruzada a `docs/reference/api-grpc.md`). `bootstrap.rs`: secuencia de arranque de servicios AWS.

### 4.14 `infrastructure` — adaptadores AWS · P2

**Rol:** adaptadores del SDK (DynamoDB, S3, Athena, Kinesis, SQS, EventBridge, Glue) + sesión, tenant guard, auditoría, bus de eventos, motor local de query S3.

**Dentro de los archivos.** Convención por adaptador: doc de módulo con el contrato (qué operación de dominio implementa), timeouts/reintentos, y cómo se emula en local (Docker/localstack según `docs/guides/desarrollo-local.md`). `local_s3_query_engine/` (recién refactorizado en `sql_parse.rs`/`pipeline.rs`/`lake_reader.rs`): documentar el pipeline en memoria puro y el corpus de caracterización. `domain_event_bus.rs`: quedó como el único mecanismo de eventos del motor tras eliminar el spawner de work orders — documentar el contrato productor (`eav/writer/outbox.rs`) → bus → consumidores externos (metri-cmms-plugin compone las OTs; ADR-007 SUPERADO), dejando claro que el motor es plano de datos. `tenant_guard.rs`: el mecanismo de aislamiento — doc con severidad máxima y ejemplo de bloqueo. `seeder.rs` y `session_store.rs`: propósito y ámbito (¿solo dev?).

### 4.15 `otel` — trazas · P2

**Rol:** OpenTelemetry OTLP.

**Dentro de los archivos.** Qué se traza (spans por RPC, atributos de tenant), dónde se exportan, y cómo desactivarlo en local.

### 4.16 `application` — puertos de aplicación (DIP) · P2

**Rol:** puertos de aplicación (DIP) — traits delimitados por la necesidad del consumidor; el subárbol `scheduling/` fue eliminado junto con el stack de materialización.

**Dentro de los archivos.** `mod.rs` ya trae una buena introducción en comentario `//` (DIP, composition roots, fakes sin AWS): migrarla a `//!`. `ports.rs`: cada trait puerto con su implementador esperado (EventBridge, outbox, DynamoDB — cableados en `grpc/bootstrap.rs` y `grpc/server.rs`) y ejemplo de implementación mínima (patrón C-TRAIT de las guías).

### 4.17 Binarios y tests · P2

- `src/bin/firehose-seeder.rs`: doc de cabecera con propósito, uso y ámbito (¿herramienta de desarrollo?).
- Cada archivo `*_tests.rs` con doc de módulo de una línea que diga **qué comportamiento caracteriza** (convención ligera: no se exige `///` en cada test).

---

## 5. Documentación `.md`

La estructura existente en `docs/` es sana y se conserva; el plan la completa y le añade índice.

```text
(raíz)
├── README.md                      # existe — español; enlaza su versión inglesa en la primera línea
├── README.en.md                   # NUEVO — traducción fiel del README (misma estructura y diagrama)

docs/
├── README.md                      # NUEVO — índice navegable de toda la documentación (bilingüe)
├── architecture/
│   ├── 00_OVERVIEW.md             # existe — revisar que refleje los módulos nuevos
│   ├── ADR-001…ADR-007            # existen — ADR-007 ya marcado SUPERADO
│   └── PLAN_*.md                  # existen — incluye PLAN_PATRON_RESULT.md (cumplido) y este plan
├── components/                    # NUEVO — un .md por componente (sección 5.2)
│   ├── eav.md, aegis.md, janus.md, … (15 archivos)
├── guides/
│   ├── desarrollo-local.md        # existe
│   └── deployment.md              # existe (en inglés — deploy por GitHub Actions + OIDC)
└── reference/
    ├── api-grpc.md                # existe — actualizar con RPCs nuevos
    ├── codigos-error.md           # existe — espejo del catálogo TOML (invariante 1:1 del auditor)
    ├── glosario.md                # NUEVO — equivalencias EN↔ES de los términos del dominio
    └── modelos-codice.md          # existe — regenerar de config/models/
```

### 5.1 `docs/README.md` — índice

Tabla con: documento → audiencia → cuándo consultarlo, redactada en ambas lenguas (es pieza bilingüe junto a `README.en.md`). Enlaces desde README.md raíz. Regla: ningún `.md` huérfano (todo documento debe ser alcanzable desde el índice).

### 5.2 `docs/components/<modulo>.md` — ficha por componente

Quince documentos cortos (1–3 páginas), con plantilla fija para que sean comparables entre sí. Cada ficha lleva H1 en inglés y abre con un resumen en inglés —bloque `> **English summary:**` de 3–5 frases—; el cuerpo y las descripciones de sección van en español:

1. **Purpose** — qué problema resuelve el componente (2–4 frases).
2. **Responsibilities / non-responsibilities** — qué NO hace (evita que crezca por absorción).
3. **Internal flow** — diagrama ASCII de etapas.
4. **Invariants** — lista numerada; cada invariante con el test que la protege.
5. **Entry points** — los 3–5 tipos/funciones públicos que un consumidor debe conocer (enlazando al rustdoc generado).
6. **Errors** — los errores canónicos que produce.
7. **Decisions** — enlaces a los ADR que lo respaldan.
8. **Known risks / TODO** — deudas documentadas.

El contenido de cada ficha sale de la Fase correspondiente de doc-comments: el `.md` se escribe cuando el módulo ya está documentado en código, extrayendo la vista arquitectónica (no duplicando la API).

### 5.3 `README.md` raíz

Ya cumple bien su rol (entrada con arquitectura en un vistazo). Cambios: enlazar `docs/README.md`, actualizar la tabla de módulos (`application` quedó reducido a puertos), purgar las referencias al stack de materialización eliminado, y crear `README.en.md` como traducción fiel (misma estructura, diagrama y tabla; enlace cruzado en la primera línea de ambos). Regla permanente: el README nunca explica internas — eso es de `docs/components/`.

---

## 6. Fases de ejecución

Orden bottom-up por dependencias: primero lo que todos consumen, al final la superficie.

| Fase | Alcance | Entregable | Criterio de aceptación |
|---|---|---|---|
| **0** | Estándar + plantillas (secciones 2–3) | Este plan aprobado; lints `broken_intra_doc_links = deny` en `Cargo.toml`; auditor `check_docs.py` con ratchet en el workflow de CI | `cargo doc` compila sin enlaces rotos; auditor de cabeceras en verde con el baseline |
| **1** | Raíces: `lib.rs`, `main.rs` + `docs/README.md` | Doc de crate + índice + `README.en.md` | Diagrama y tabla de módulos en el rustdoc del crate |
| **2** | P0: `domain`, `codice`, `eav` | Doc-comments completos | 100% ítems públicos con `///`; ≥1 doctest por módulo público; `# Errors` en toda fn con `Result` |
| **3** | P1: `aegis`, `janus`, `temporal` | Ídem + gramática de fórmulas + contrato FlatBuffers | Ídem; `compile_fail` en los ejemplos prohibidos de `aegis/sql` |
| **4** | P1: `janus_router`, `cedar`, `quota`, `iop`, `eda` | Ídem; `eda` pasa de 0 doc a estándar completo | Orden de interceptores y saga documentados |
| **5** | P2: `grpc`, `infrastructure`, `otel`, `application`, bins | Ídem | 100 % de archivos `.rs` con cabecera `//!` (auditor en verde); secuencia de interceptores en `grpc::interceptors` documentada |
| **6** | `.md`: 15 fichas en `docs/components/` (con resumen inglés) + `reference/` + `glosario.md` + README | Documentación md cerrada | Todo documento alcanzable desde índice; glosario y resúmenes EN en su sitio; `missing_docs = "deny"` en `Cargo.toml` |

El orden coincide con las fases que el `PLAN_PATRON_RESULT.md` ya cerró (P0: `domain`/`codice`/`eav` → P1: resto del núcleo → P2: superficie), así que cada módulo se documenta cuando su deuda Result está en cero y sus invariantes de allowlist están frescas. La migración de títulos ya existentes (H1 de los `.md` actuales al inglés) viaja en el PR de la fase que toca cada capa; los ADR reciben el título en inglés sin tocar su contenido de decisión. El ratchet del auditor de docs (§2.7) arranca en la Fase 0 desde el baseline y solo admite que la cuenta de archivos sin cabecera baje. En cada fase: `cargo doc --no-deps` sin warnings de rustdoc, `cargo test --doc` en verde, chequeo de docs integrado a `.github/workflows/rust.yml`, y un PR por módulo (revisable, sin PRs monolíticos de doc).

---

## 7. Mantenimiento (reglas permanentes)

1. **Definición de hecho** de todo PR futuro: si añade un ítem público, trae su `///` con secciones completas — `missing_docs = deny` lo hace cumplir en CI; si añade un archivo `.rs`, nace con su cabecera `//!` — el auditor de docs (§2.7) lo bloquea.
2. **Regla de las dos capas**: un cambio de arquitectura actualiza su `docs/components/<modulo>.md` y, si cambia la decisión, genera ADR nuevo; un cambio de API actualiza solo doc-comments.
3. **Los ADR son inmutables**: una decisión revertida genera un ADR nuevo que la invalida, nunca se edita el original.
4. Los doc-tests son tests de CI: un ejemplo que deja de compilar rompe el build — la documentación no puede podrirse en silencio.
5. **El catálogo y la allowlist son documentación**: una variante nueva de `ErrorCode` exige su entrada en `config/errors/error_catalog.toml` (gate 1:1 del auditor) y su sección `# Errors`; una invariante nueva exige su entrada justificada en `scripts/dev/result_pattern_allowlist.json`. Ambos archivos se tratan como documentación normativa, no como configuración.
6. **Anti-drift bilingüe**: el PR que modifica un documento de la capa bilingüe completa (`README`, índice, overview) actualiza su `.en.md` en el mismo PR; todo término nuevo del dominio entra al glosario antes de mergear.
7. **Títulos en inglés, sin excepción**: todo `.md` nuevo y todo doc-comment nuevo abren con título en inglés (H1 o primera línea); si el título introduce un término nuevo del dominio, entra al glosario en el mismo PR.
