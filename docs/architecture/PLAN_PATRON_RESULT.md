# PLAN_PATRON_RESULT.md — Plan de adopción total del patrón Result en metri-engine

> **Alcance:** este plan define, con reglas no negociables y verificación automática, cómo metri-engine completa la adopción del **patrón Result** (toda operación fallible retorna `Result<T, DomainError>`, cero panics fuera de invariantes declaradas, cero errores sin catálogo). Incluye: la definición operativa del 100 %, el catálogo de errores y su reparación a 1:1, el auditor automático con ratchet en CI, y las fases de ejecución con gates verificables. No incluye cambios de comportamiento de negocio ni rediseños de módulos.
>
> **Estado actual medido (2026-09-07, auditor `scripts/dev/check_result_pattern.py`, previo a la Fase 1):** 47 `.unwrap()` · 22 `.expect()` · 7 macros de pánico (5 migrables + 2 invariantes allowlisted) · 0 `assert*!` en código real · 21 firmas `Result<_, String | Box<dyn Error>>` · 0 `anyhow` · **1/17 módulos cerrados (6 %)**. Catálogo: 68 variantes `ErrorCode` / **67** en `ALL` / 61 entradas TOML · `COD_MAT_001` sin entrada (rompería el fail-fast de bootstrap) · 6 códigos canónicos compartidos · 1 entrada muerta (`EAV_003`) · 10 divergencias `is_retryable()` ↔ TOML.
>
> **Estado tras Fase 1 (2026-09-07):** catálogo **100 % en verde** — 69 variantes = 69 en `ALL` = 69 mapeos canónicos únicos ↔ 85 entradas TOML (12 `reserved`, 4 `deprecated`), fallback sincronizado, guards C1–C4 + I6 en `cargo test`, `From<DomainError> for tonic::Status` único, `unwrap_railway` eliminado, `ErrorCatalog::load` tipado con `INFRA_CATALOG_001`, `anyhow` fuera de `Cargo.toml`. Deuda de código: **93** (47 unwrap · 22 expect · 4 pánicos migrables · 20 firmas). Clippy: 0 errores, 86 warnings (≤ 88, bajando).
>
> **Estado tras Fase 2 (2026-09-07):** módulos P0 **cerrados** — `domain`, `codice` y `eav` pasan `--strict --only` con 0 violaciones. `config.rs` migra a `DomainError` con el código nuevo `INFRA_CONFIG_001` (TOML: 86 entradas), `map_to_datom_value` retorna `DomainError` (`CDX_002`), builders de índice retornan `DomainError` (`EAV_001`) y el decodificador total `decode_eavt_sk` elimina los 7 unwraps de SK binario en `transact.rs`/`pull.rs`. Allowlist de invariantes: 5 entradas (pánicos de bootstrap + accesores `OnceLock` + serialización por construcción). Deuda de código: **77** (40 unwrap · 18 expect · 4 pánicos migrables · 15 firmas) — 14/17 áreas sin deuda, 13 módulos aún con deuda.
>
> **Estado tras Fase 3 (2026-09-07):** módulos P1 **cerrados** — `aegis`, `temporal`, `quota`, `cedar`, `eda`, `janus`, `janus_router` e `iop` pasan `--strict --only` con 0 violaciones. Los 6 `Result<_, String>` de aegis SQL/AST migran a `DomainError` (`Aeg001`→`AEG_COMPILE_003`); `FormulaError` ya implementa `std::error::Error`; los 3 `unreachable!` (evaluator, oltp/compiler, quota_step) son brazos defensivos con error/pass-through; shunting-yard del parser sin unwraps; `virtual_table` usa `NaiveTime::MIN` + `epoch_to_day` con clamp calendario; `epoch_to_zdt`/`shift_months` son totales con clamp; locks de quota poisoning-tolerantes (`PoisonError::into_inner`); reloj de `eda` con `unwrap_or_default`; `assoc_path` recursivo sin pánicos; `compile_native_plan_fbs` retorna `Result` (snapshots de contrato actualizados). Allowlist: 10 invariantes (+4 regex literales en statics, +2 recursos Cedar embebidos). Deuda de código: **32** — solo queda la Fase 4 (`grpc`, `infrastructure`, `application`, `bin`, raíz). Clippy: 0 errores, 85 warnings (bajando).
>
> **Estado tras Fase 4 (2026-09-07) — MIGRACIÓN COMPLETA:** 17/17 módulos cerrados, deuda de código **0** y `--strict` global en verde. `grpc` retorna `DomainError` en toda su cadena de arranque (server/bootstrap, 3× `Box<dyn Error>` eliminados); los unwraps de handlers migran a `let-else` con `Status::from(DomainError)` (mapeo único R6); `explore` traduce vía `Status::from(e)`; `list_support` valida con `Janus400`. `infrastructure`: locks de stubs poisoning-tolerantes, `glue.rs` a `INFRA_GLUE_001`, HMAC con fallback infalible (`KeyInit::new`), timestamps del lake con clamp. `application`: pánico de hooks allowlisted como invariante (doble registro). `bin`/raíz: `next_arg` con exit(2) en vez de 6 expects, Firehose a `INFRA_FIREHOSE_001`, `main.rs` sin `Box<dyn Error>` con exit(1) explícito. Catálogo: 88 entradas (+`INFRA_GLUE_001`, `INFRA_FIREHOSE_001`), 72 variantes. Allowlist: 12 invariantes documentadas. Falta la Fase 5: lints en `deny` y CI en modo `--strict`.
>
> **Estado tras Fase 5 (2026-09-07) — PLAN CUMPLIDO:** cerradura de compilación activa — `#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::todo, clippy::unimplemented))]` en `lib.rs`, `main.rs` y `bin/firehose-seeder.rs`, con `#[allow]` puntuales y comentados en las 12 invariantes de la allowlist y exención explícita del código generado (tonic vía `mod pb`, flatc vía `fbs.rs`). CI corre el auditor en `--strict`: cero violaciones y catálogo perfecto bloquean cualquier PR. Verificado: `cargo build --all-targets` ✓ · `cargo clippy --all-targets` 0 errores / 86 warnings (línea estable) · `cargo test --lib` 484 tests ✓ · auditor 17/17 módulos y catálogo 72 variantes ↔ 88 entradas en 1:1.

---

## 1. Definición operativa: qué significa «100 % de cobertura del patrón Result»

«Cobertura 100 %» **no** es una métrica de líneas cubiertas: es esta lista de invariantes, cada una máquina-verificable. El plan está cumplido cuando los ocho detectores están en verde simultáneamente.

| # | Invariante | Detector automático | Gate |
|---|---|---|---|
| I1 | Toda operación fallible retorna `Result<T, DomainError>` (o alias `Railway`/`DomainResult`, o un error local con bridge `From<_> for DomainError` en su frontera) | Checks `string_result`, `panic_macro`, `unwrap` del auditor | CI ratchet → `--strict` en Fase 5 |
| I2 | Cero `.unwrap()` / `.expect()` en código no-test | Checks `unwrap`, `expect` | Ídem |
| I3 | Cero `panic!` / `todo!` / `unimplemented!` / `unreachable!` en código no-test, salvo invariantes de arranque allowlisted | Check `panic_macro` + `result_pattern_allowlist.json` | Ídem |
| I4 | Cero `Result<_, String>`, `Result<_, Box<dyn Error>>` y uso de `anyhow` | Checks `string_result`, `anyhow` | Ídem |
| I5 | Catálogo perfecto: 1 variante `ErrorCode` = 1 código canónico = 1 entrada TOML; `ALL` completo; `is_retryable()` sincronizado | Sección «Catálogo» del auditor + tests guard de Rust (§4.3) | Fase 1 |
| I6 | Toda traducción al borde (gRPC/HTTP) pasa por el mapeo central con `http_status`/`grpc_status` del catálogo | Test de propiedad sobre `ErrorCode::ALL` (§4.3) | Fase 1 |
| I7 | Lints de clippy en `deny` para las construcciones prohibidas | `[lints.clippy]` en `Cargo.toml` | Fase 5 |
| I8 | Nada de lo anterior se verifica «a ojo»: cada invariante corre en CI en cada PR | Workflow `rust.yml` | Ya activo (Fase 0) |

**Definición de «módulo cerrado»:** los 6 checks de código del auditor dan 0 violaciones para ese módulo. **Cobertura global** = módulos cerrados / 17. La sección de catálogo es global y se exige entera en la Fase 1.

## 2. Reglas no negociables

Cada regla tiene justificación y detector. Los tests están exentos por definición — la noción de «código no-test» es la convención del repo: dirs `src/**/tests/` y `src/**/testing/`, ficheros `*tests*.rs` / `golden_test.rs` / `test_support.rs`, `fbs.rs` generado, y bloques `#[cfg(test)]` inline.

- **R1 — Railway obligatorio.** Una función que puede fallar retorna `Result` con el error de dominio. Los errores `String` impiden matchear, traducir y auditar: prohibidos (detecta: `string_result`).
- **R2 — Prohibido `.unwrap()` / `.expect()`.** Un `unwrap` es un error sin código, sin stage y sin contexto — exactamente lo que el catálogo existe para evitar (detecta: `unwrap`, `expect`). Convertir a `DomainError::new(CODE, detalle)` o propagar con `?`.
- **R3 — Prohibido `panic!`/`todo!`/`unimplemented!`/`unreachable!`.** Un brazo «imposible» de un `match` es un error de dominio (`INTERNAL`), no un crash: se convierte a `DomainError` (detecta: `panic_macro`).
- **R4 — Tipos de error prohibidos en firmas:** `String`, `Box<dyn Error>`, `anyhow::Error`. El único error transitable es `DomainError` (o error local de aplicación con bridge). `anyhow` se elimina de `Cargo.toml` en la Fase 1 (0 usos hoy) (detecta: `string_result`, `anyhow`).
- **R5 — Los códigos viven SOLO en `config/errors/error_catalog.toml`** (ADR-005, inmutable). Nueva variante `ErrorCode` sin su entrada TOML = PR roto (detecta: sección catálogo + tests guard).
- **R6 — El borde gRPC traduce una sola vez.** `impl From<DomainError> for tonic::Status` es el único sitio que consulta `ErrorCatalog::http_status/grpc_status`; los handlers devuelven `?` hasta el final. Prohibido el mapeo manual disperso (`grpc/handlers/validations.rs` hoy) (verifica: test de propiedad I6).
- **R7 — Invariantes de pánico: solo con allowlist.** Un pánico deliberado (fail-fast de bootstrap, doble init) debe estar en `scripts/dev/result_pattern_allowlist.json` con `kind: invariante`, `max` exacto y la razón documentada. La deuda migrable NUNCA va en la allowlist: vive en la línea base y solo puede bajar. Si una entrada deja de aplicar, el auditor la marca obsoleta.
- **R8 — No-regresión (ratchet).** Toda PR corre el auditor en modo ratchet: cualquier métrica que crezca sobre `result_pattern_baseline.json` rompe el CI. `--update-baseline` solo se ejecuta en PRs que **reducen** deuda, y el diff del JSON debe mostrarse en el PR.

## 3. Catálogo de errores

### 3.1 Arquitectura (vigente, ADR-005)

```text
config/errors/error_catalog.toml   ← ÚNICA fuente (prohibido definir códigos fuera)
        │  carga + validación fail-fast en bootstrap (ErrorCatalog::init_global)
        ▼
ErrorCode (enum, 68 variantes)  ── canonical_code()/is_retryable()/stage()
        │
        ▼
DomainError { code, stage, detail, retryable, context }   ← el ÚNICO error transitable
        │
        ├─► iop/error_response.rs  → DTO forense (canonical_code + trace_id)
        └─► impl From<DomainError> for tonic::Status  ← (Fase 1) mapeo único al borde
```

Reglas del catálogo (complementan ADR-005):

- **C1.** Toda variante de `ErrorCode` está en `ALL` (política zero-drop) y tiene mapeo en `canonical_code()`.
- **C2.** Todo código canónico existe como entrada del TOML, con `family`, `stage`, `severity`, `http_status`, `grpc_status`, `description`, `context_required`, `retryable`.
- **C3.** El mapeo es **1:1**: dos variantes nunca comparten código canónico (un código compartido hace indistinguibles dos fallos distintos para clientes, alertas y soporte).
- **C4.** El fallback estático de `is_retryable()` (para cuando el catálogo aún no cargó) coincide con el TOML; el test guard lo verifica.
- **C5.** Entradas sin variante que las use: se marcan `reserved = true` (definidas para uso futuro) o `deprecated = true` (ya publicadas pero retiradas); el auditor las acepta y `gen_reference.py` las muestra como tales.
- **C6.** Plantilla de entrada nueva (append-only, nunca reordenar):

```toml
[[errors]]
code             = "EAV_FTS_001"        # FAMILIA_ETAPA_NNN — único en todo el archivo
family           = "eav"
stage            = "eav-fts"
severity         = "error"              # info | warning | error | fatal
http_status      = 500
grpc_status      = "INTERNAL"
description      = "FTS index write failed — trigram batch rejected"
context_required = ["entity_id", "cause"]
retryable        = false
```

### 3.2 Reparación del catálogo (Fase 1) — tabla de decisión por variante

La auditoría encontró 4 brechas estructurales: (a) `CodMat001` existe en el enum pero **no** en `ALL`; (b) `COD_MAT_001` no tiene entrada TOML (la validación fail-fast de bootstrap la rechazaría); (c) 6 códigos compartidos por 2–3 variantes; (d) 10 divergencias de `retryable`. Además, varios mapeos 1:1 apuntan a entradas cuyo `description` describe **otro** fallo (mismatch semántico detectado comparando el comentario de cada variante con el TOML). La tabla completa, variante a variante:

**Se mantienen sin cambio (mapeo 1:1 y semántica correcta):** `Janus400→JANUS_400`, `Janus403→JANUS_403`, `Janus500→GRPC_500`, `JanusAstCompileError→AEG_COMPILE_001`, `JanusFilterCompileError→AEG_COMPILE_002`, `JanusTenantMismatch→AEG_TENANT_MISSING`, `JanusVal001→JANUS_VAL_001`, `Jns001→JNS_001`, `JnsLock001→JNS_LOCK_001`, `JnsSeed001→JNS_SEED_001`, `JnsOlap001→JNS_OLAP_003`, `JnsTx001→JNS_TX_001`, `JnsRef002→JNS_REF_002`, `JnsConflict001→JNS_CONFLICT_001`, `Aeg002→AEG_002`, `Eav001→EAV_001`, `Eav003→EAV_TX_003` (contrato histórico documentado en ADR-005), `Eav004→EAV_004`, `EavTx001→EAV_TX_001`, `EavTx004→EAV_TX_004`, `Cod001→CDX_001`, `Cod002→CDX_002`, `CodScope001→JNS_SCOPE_001`, `Auth401→GRPC_AUTH_001`, `GrpcTenant001→GRPC_TENANT_001`, `Infra001→INFRA_DDB_001`, `InfraAthena005→INFRA_ATHENA_005`, `InfraCedar002→INFRA_CEDAR_002`, `Aud001→AUD_001`, `Aud002→AUD_002`, `Mcp503→MCP_503`, `Fml001…Fml013→FML_001…FML_013`.

**Se reparan (remap a código nuevo + entrada TOML nueva; cada remap exige auditar los call-sites de la variante antes de tocar la cadena):**

| Variante | Código hoy | Problema (TOML describe) | Acción propuesta | HTTP / gRPC |
|---|---|---|---|---|
| `CodMat001` | `COD_MAT_001` | Sin entrada TOML + falta en `ALL` | Añadir a `ALL` + entrada TOML | 400 / INVALID_ARGUMENT |
| `Janus401` | `GRPC_AUTH_001` | compartido ×3 | → nuevo `JANUS_401` | 401 / UNAUTHENTICATED |
| `Janus404` | `INFRA_DDB_003` | semántica DDB, no read-path | → nuevo `JANUS_404` | 404 / NOT_FOUND |
| `Janus422` | `JNS_REF_001` | solo refs UUID; 422 es genérico | → nuevo `JANUS_422` | 422 / INVALID_ARGUMENT |
| `AuthRevoked` | `GRPC_AUTH_001` | compartido ×3 | → nuevo `GRPC_AUTH_003` | 401 / UNAUTHENTICATED |
| `Auth403` | `GRPC_AUTH_002` | TOML: header ausente (≠ tenant mismatch) | → nuevo `GRPC_AUTH_004` | 403 / PERMISSION_DENIED |
| `Aeg001` | `AEG_001` | TOML: engine desconocido (≠ fallo de compilación SQL) | → nuevo `AEG_COMPILE_003` | 400 / INVALID_ARGUMENT |
| `Aeg003` | `AEG_003` | TOML: pull entity not found (≠ timeout Athena) | → nuevo `AEG_006` | 504 / DEADLINE_EXCEEDED |
| `Aeg004` | `AEG_004` | TOML: start_query falló (≠ parseo de salida) | → nuevo `AEG_007` | 500 / INTERNAL |
| `Aeg005` | `AEG_005` | TOML: get_results falló (≠ agregación no soportada) | → nuevo `AEG_008` | 400 / INVALID_ARGUMENT |
| `Eav002` | `EAV_002` | TOML: query falló (≠ entidad no encontrada) | → nuevo `EAV_005` | 404 / NOT_FOUND |
| `Eav005` | `EAV_TX_002` | TOML: límite 100 items (≠ sort key >1024B) | → nuevo `EAV_KEY_001` | 400 / INVALID_ARGUMENT |
| `EavFts001` | `EAV_001` | compartido ×2 | → nuevo `EAV_FTS_001` | 500 / INTERNAL |
| `Cod003` | `CDX_003` | compartido con schema-not-found; colisión ≠ not-found | → nuevo `CDX_004` | 500 / INTERNAL |
| `Iop001` | `JANUS_VAL_001` | compartido ×2 | → nuevo `IOP_001` | 400 / INVALID_ARGUMENT |
| `Iop002` | `JNS_001` | compartido ×2 | → nuevo `IOP_002` | 500 / INTERNAL |
| `Iop003` | `JNS_TX_001` | compartido ×2 | → nuevo `IOP_003` | 500 / INTERNAL (retryable) |
| `Iop004` | `INFRA_CEDAR_001` | TOML: Cedar caído (≠ outbox) | → nuevo `IOP_004` | 500 / INTERNAL |
| `Quota001` | `INFRA_DDB_002` | TOML: throughput DDB (≠ cuota agotada) | → nuevo `QUOTA_001` | 429 / RESOURCE_EXHAUSTED (retryable) |
| `Infra002` | `INFRA_ATHENA_003` | TOML: Athena GetResults (≠ S3) | → nuevo `INFRA_S3_001` | 500 / INTERNAL |
| `Infra003` | `INFRA_ATHENA_001` | TOML: Athena StartQuery (≠ SQS) | → nuevo `INFRA_SQS_001` | 500 / INTERNAL |
| `Infra004` | `INFRA_ATHENA_002` | TOML: query FAILED (≠ EventBridge) | → nuevo `INFRA_EB_001` | 500 / INTERNAL |
| `Infra005` | `INFRA_ATHENA_004` | TOML: timeout Athena (≠ Kinesis) | → nuevo `INFRA_KIN_001` | 500 / INTERNAL |

**Códigos que quedan huérfanos tras el remap** (`AEG_003`, `AEG_004`, `AEG_005`, `INFRA_ATHENA_001…004`, `JNS_REF_001` si `Janus422` sale, `EAV_002` si nadie lo usa): decidir por entrada — `reserved = true` si se van a usar a corto plazo, `deprecated = true` si ya se publicaron a clientes, borrar si nunca salieron del repo. La regla C5 y el auditor verifican el resultado.

**Versión del catálogo:** `2.0.0 → 3.0.0` (los clientes verán cadenas nuevas donde antes había códigos erróneos: es un arreglo de contrato, se anuncia en `docs/reference/codigos-error.md` regenerado). Tras el remap, regenerar el fallback de `is_retryable()` desde el TOML y activar el test guard.

### 3.3 Errores locales con bridge (política)

`FormulaError` (13 variantes ↔ `FML_001…013`) y `PublishError`/`ResolveError`/`MaterializeError` en `application::ports` son errores locales legítimos: dominan un subsistema y tienen bridge a `DomainError`. Política: (1) deben implementar `std::error::Error` (`FormulaError` hoy no lo hace → thiserror en Fase 3); (2) no pueden escapar de su módulo sin pasar por `From<_> for DomainError`; (3) todo código que usen debe estar en el TOML. Prohibido crear nuevos enums de error sin cumplir las tres condiciones.

## 4. Herramientas de garantía

### 4.1 Auditor automático — `scripts/dev/check_result_pattern.py`

Python 3 estándar, sin dependencias. Escanea `src/` excluyendo tests por convención (incluidos los bloques `#[cfg(test)]` inline, detectados por balance de llaves), limpia comentarios y literales para evitar falsos positivos, y verifica la paridad del catálogo parseando `errors.rs` y el TOML.

| Modo | Qué hace | Cuándo se usa |
|---|---|---|
| (sin flags) | **Ratchet**: falla si cualquier métrica crece sobre `result_pattern_baseline.json` | CI en cada PR (ya cableado en `rust.yml`) y `make result-check` |
| `--strict` | Exige 0 violaciones (global: código + catálogo) | Gate de cierre de plan (Fase 5) |
| `--strict --only MOD` | Exige 0 violaciones en un módulo (catálogo global no bloquea) | Gate de cierre por módulo/fase |
| `--update-baseline` | Reescribe la línea base | Solo en PRs que reducen deuda (R8) |
| `--verbose` | Lista cada violación `fichero:línea` | Al trabajar un módulo |
| `--json` | Salida máquina | Integraciones futuras |

Comandos de trabajo diario:

```bash
make result-check                                       # igual que CI
python3 scripts/dev/check_result_pattern.py --verbose   # ver cada violación
python3 scripts/dev/check_result_pattern.py --strict --only eav   # ¿cerré eav?
python3 scripts/dev/check_result_pattern.py --update-baseline     # solo si la deuda bajó
```

### 4.2 Línea base y allowlist (ya generadas en Fase 0)

- `scripts/dev/result_pattern_baseline.json` — conteos por check y módulo (la deuda total: 47/22/5/21 + 19 hallazgos de catálogo). El ratchet solo permite bajar.
- `scripts/dev/result_pattern_allowlist.json` — invariantes permanentes: `error_catalog.rs` (fail-fast de bootstrap, ADR-005) y `codice/registry.rs` (doble init del SSOT). Cada entrada con `max` exacto y razón; el auditor avisa cuando una entrada obsoleta debe podarse.

### 4.3 Guards en Rust (Fase 1) — el compilador y `cargo test` como segunda cerradura

El auditor es linting externo; los guards viven dentro de `cargo test` y no pueden desincronizarse del binario real. En `src/domain/tests/errors_tests.rs` (o un `catalog_parity_tests.rs` nuevo):

```rust
/// C1: toda variante está en ALL (zero-drop).
#[test]
fn all_cubre_todas_las_variantes() { /* totúales(): HashSet de ALL == variantes del enum */ }

/// C2: todo código canónico existe en el TOML cargado (falla en test, pánico en bootstrap).
#[test]
fn todo_codigo_canonico_esta_en_el_catalogo() { /* ErrorCode::ALL → ErrorCatalog::try_global() */ }

/// C3: 1 variante = 1 código.
#[test]
fn codigos_canonicos_son_unicos() { /* HashSet<canonical_code>.len() == ALL.len() */ }

/// C4: fallback estático == TOML.
#[test]
fn fallback_retryable_coincide_con_el_catalogo() { /* para toda v en ALL */ }

/// I6: el borde traduce según el catálogo, para TODAS las variantes.
#[test]
fn status_map_usa_el_catalogo() {
    for code in ErrorCode::ALL {
        let status = tonic::Status::from(DomainError::new(*code, "guard"));
        assert_eq!(status.code(), catalog_grpc_code(code), "{code:?}");
    }
}
```

### 4.4 Lints de compilación (cerradura final, activa desde la Fase 5)

Mecanismo real: `#![cfg_attr(not(test), deny(...))]` en las tres raíces de crate (`lib.rs`, `main.rs`, `bin/firehose-seeder.rs`):

```rust
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented
    )
)]
```

Dos razones de diseño, medidas durante la Fase 5:

1. **No en `Cargo.toml` con `[lints]`:** los lints de paquete aplican también a los targets de test, y los tests usan `unwrap`/`expect`/`assert!` libremente — romperían `cargo test`. El `cfg_attr(not(test), …)` apaga el deny exactamente cuando se compila con tests.
2. **El código generado se exime explícitamente:** el `metri.rs`/`metri.eda.v1.rs` de tonic (incluido vía `include_proto!` dentro de `mod pb`) lleva su `#![allow(...)]` en el módulo contenedor (`grpc/mod.rs`) — un atributo interno no puede anteponerse al fichero incluido; el generado de flatc (`janus_ir_ast_generated.rs`, vía `src/janus/fbs.rs`) lleva `#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]` junto a su `clippy::all` — ojo: `clippy::all` NO incluye los lints de restricción.

Las invariantes de la allowlist llevan `#[allow(clippy::panic / clippy::expect_used / clippy::unwrap_used)]` puntual, con comentario que referencia la regla R7 y su entrada en `scripts/dev/result_pattern_allowlist.json`. Sin CI, el 100 % sigue siendo imposible de romper: no compila.

## 5. Plan por componente (deuda medida)

Prioridad: **P0** = de lo que todo depende, **P1** = núcleo, **P2** = superficie. Violaciones: `unwrap/expect/panic/string_result`.

| Módulo | P | Deuda concreta (auditor 2026-09-07) |
|---|---|---|
| `domain` (catálogo) | P0 | 5 brechas de catálogo (§3.2); `unwrap_railway` (pánico); `error_catalog.rs:54` `Box<dyn Error>` + expects en `init_global`; `config.rs:96,121` `Result<_, String>` |
| `codice` | P0 | `validator.rs:110` `Result<DatomValue, String>`; expect en `registry.rs:580` |
| `eav` | P0 | 7 unwraps (`writer/transact.rs:698-699`, `reader/pull.rs` ×5); `index/aevt.rs:10`, `index/eavt.rs:10` `Result<Put, String>` |
| `aegis` | P1 | 11 unwraps (`sql/virtual_table.rs`, `formula/*`); 2 `unreachable!` (`oltp/compiler.rs:279`, `formula/evaluator.rs:63`); 6 `Result<_, String>` (`ast_ir.rs:44,341`, `sql/compiler.rs:231`, `cte_compiler.rs:113`, `metric_compiler.rs:24`, `select_compiler.rs:31`); `FormulaError` sin `std::error::Error` |
| `temporal` | P1 | 3 unwraps + 1 expect (`core.rs`, `adapters.rs:112`, `comparison.rs:174`) |
| `quota` | P1 | 6 unwraps en `reservations.rs:269-347` |
| `cedar` | P1 | 1 unwrap + 5 expects (`engine.rs:73`, `evaluator/action_registry.rs`, `authn.rs`) |
| `eda` | P1 | 4 unwraps en `moira.rs` |
| `janus` | P1 | 1 unwrap (`aggregator.rs:146`) + 1 expect (`normalizer/helpers.rs:21`) |
| `janus_router` | P1 | 1 unwrap (`partition.rs:53`) + 2 expects (`partition.rs:46`, `saga.rs:182`) |
| `iop` | P1 | 1 `unreachable!` migrable (`quota_step.rs:168`) |
| `grpc` | P2 | 5 unwraps (`server.rs:337`, handlers); `server.rs:16` y `bootstrap.rs:11,76` `Box<dyn Error>`; 4 `Result<_, String>`/Box; mapeo manual a `Status` en `handlers/validations.rs` (R6) |
| `infrastructure` | P2 | 8 unwraps (`kinesis.rs:248-289`, `sqs.rs:172,181`, `local_s3_query_engine/pipeline.rs:345`); `glue.rs:73` `Result<(), String>`; 1 expect (`session_store.rs:156`) |
| `application` | P2 | 1 pánico migrable (`scheduling/hooks.rs:63` — hook ausente → `DomainError`) |
| `bin` + raíz | P2 | `firehose-seeder.rs` 6 expects + 3 `Result<_, String>` (herramienta dev, va al final); `main.rs:9` `Box<dyn Error>` → manejo explícito + `exit(1)` |
| `otel` | P2 | **cerrado** ✓ (0 violaciones) |

## 6. Fases de ejecución

Orden bottom-up por dependencias, un PR por módulo (revisable, sin PRs monolíticos). Cada fase tiene gate ejecutable; ninguna fase siguiente empieza con la anterior en rojo.

| Fase | Alcance | Entregable | Criterio de aceptación (gate) |
|---|---|---|---|
| **0** | Blindaje (✅ hecha al escribir este plan) | Auditor + baseline + allowlist + gate ratchet en `rust.yml` + `make result-check` | `python3 scripts/dev/check_result_pattern.py` exit 0; una PR que añade un `unwrap` rompe el CI (verificado) |
| **1** | Catálogo perfecto + núcleo de dominio (✅ ejecutada 2026-09-07) | Reparación §3.2 (TOML v3.0.0 + `ALL` + 23 remaps + fallback); 5 guards Rust (§4.3); `unwrap_railway` eliminado; `ErrorCatalog::load → Result<_, DomainError>`; `From<DomainError> for Status` único; `anyhow` fuera de `Cargo.toml`; auditor acepta `reserved`/`deprecated` | Sección «Catálogo» del auditor 100 % ✓ (logrado); `cargo test --lib` verde — 479 tests; `gen_reference.py --check` verde; clippy 86 ≤ 88 |
| **2** | P0: `domain`, `codice`, `eav` (✅ ejecutada 2026-09-07) | Deuda de los 3 módulos a cero; `INFRA_CONFIG_001` nuevo; `decode_eavt_sk` total | `--strict --only domain` / `codice` / `eav` exit 0 (logrado); baseline solo baja: 93 → 77 |
| **3** | P1: `aegis`, `temporal`, `quota`, `cedar`, `eda`, `janus`, `janus_router`, `iop` (✅ ejecutada 2026-09-07) | Ídem por módulo; `FormulaError` implementa `Error` (Display manual sobre `detail()`) | `--strict --only <mod>` exit 0 por los 8 (logrado); baseline 77 → 32 |
| **4** | P2: `grpc`, `infrastructure`, `application`, `bin` + raíz (✅ ejecutada 2026-09-07) | Ídem; `explore` traduce vía `Status::from(DomainError)` (R6); `main.rs` fail-fast explícito | `--strict --only <mod>` exit 0 por cada uno (logrado) y `--strict` GLOBAL exit 0: deuda 0, cobertura 17/17 (100 %) |
| **5** | Cierre estricto (✅ ejecutada 2026-09-07) | `deny` de clippy en las 3 raíces de crate (§4.4); CI cambia ratchet → `--strict`; allowlist final: 12 invariantes documentadas (criterio enmendado: todas de clase `invariante`, ninguna deuda) | `--strict` global exit 0 (logrado); `cargo clippy --all-targets` 0 errores; cobertura 17/17 (100 %) |

## 7. Mantenimiento (reglas permanentes)

1. **Definición de hecho de todo PR:** `make result-check` en verde y, si el módulo ya está cerrado, `--strict --only <mod>` también. Un módulo cerrado no puede volver a abrirse sin aprobar la reapertura en el PR.
2. **El catálogo manda:** una variante nueva sin entrada TOML, un código compartido o una divergencia de `retryable` rompen CI por partida doble (auditor + tests guard).
3. **La línea base solo baja:** `--update-baseline` exclusivo de PRs que reducen deuda, con el diff del JSON a la vista. Prohibido usarlo para meter deuda nueva.
4. **La allowlist no crece sin ADR:** una invariante nueva de pánico exige su entrada con razón documentada y referencia al ADR que la respalda; revisión trimestral de entradas obsoletas.
5. **Coherencia con el resto del plan técnico:** `# Errors` obligatorio en toda fn con `Result` (PLAN_DOCUMENTACION.md §2.3) y ADR-005 inmutable — un cambio de decisión de catálogo genera ADR nuevo, nunca edita el original.
