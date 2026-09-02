# Metri Engine — Informe de estado y plan de refactorización

> **Componente:** `metri-engine` (Rust) · motor de datos multitenant OLTP/OLAP
> **Estado verificado:** 2 de septiembre de 2026 — contra el código, no contra la documentación
> **Versión:** v4 (2 de septiembre de 2026). v2 re-verificó el plan del 26 de agosto; la v3 añadió la purga del stack Clojure y la estrategia de documentación; la v4 decide **eliminar la carpeta `docs/` completa y diseñar la documentación desde cero**, actualizada a la realidad — y trae el `README.md` ya reescrito.
> **Naturaleza:** el sistema está en producción y funcionando; el plan no puede romperlo en ningún commit intermedio

---

## Verificación de partida

Todas las cifras de este documento salen de ejecutar las herramientas sobre el árbol de trabajo actual, no de estimaciones.

```bash
cargo check --all-targets    # exit 0, 75 warnings
cargo test --lib             # 339 passed, 0 failed, 16 ignored — 5,20 s
cargo clippy --all-targets   # 348 hallazgos
cargo fmt --check            # 31.356 líneas de diff pendiente
```

| Métrica | 26 ago (v1) | 2 sep (v2) | Δ |
|---|---|---|---|
| Archivos Rust | 208 | 224 | +16 |
| Líneas de código | 37.367 | 43.344 | +5.977 |
| Tests unitarios | 238 en verde | 339 en verde | **+101** |
| Tests `#[ignore]` (integración) | 4 | 16 | +12 |
| Bloques `unsafe` | 0 | 0 | = |
| `unwrap()` en código productivo | 40 | ~46 | +6 |
| Warnings de rustc | 70 | 75 | +5 |
| Hallazgos de clippy | 327 | 348 | +21 |
| Modelos en el Códice | 56 | 60 | +4 |
| Documentos de arquitectura | 51 | 48 | −3 |

**Lo que cambió desde la v1:** aterrizaron FTS omnisearch nativo (trigram en memoria + Damerau-Levenshtein, con sus snapshots de contrato), la pipeline OLAP Parquet/Snappy vía Glue Data Catalog con `tenant_id` como primera partición obligatoria, el esquema genérico multientidad con payload JSON, y se reescribió `DomainError` como struct compacto. Creció el código, creció la red de tests. Lo que **no** creció: la disciplina de repo (Parte II) ni el CI.

---

# Parte I — Informe de estado

## Calificación: 7 / 10

La misma nota que la v1, y por las mismas razones: el código mejora mes a mes, el proceso sigue quieto. La lista «para llegar a 8-9» de la v1 tenía cinco ítems; uno se resolvió (borrar `quota_guard.rs`), cuatro siguen intactos.

### Lo que sube la nota

**Arquitectura real, no aspiracional.** EAV schema-driven con 60 modelos JSON en `config/models/`, doble canal OLTP/OLAP, pipeline IOP por pasos, compilador de fórmulas propio (lexer → parser → resolver → evaluator), Cedar ABAC embebido y primitivas temporales. A esto se suma lo nuevo: FTS nativo sobre la tabla EAV y el data lake Parquet con partitioning por tenant — implementado, con tests, no solo documentado.

**Seguridad por encima del promedio.** Fail-closed en el interceptor gRPC, `constant_time_eq` para comparar HMAC, fail-fast en arranque si el secreto es débil o falta en producción (`src/grpc/server.rs:104`), FLS que oculta `password_hash` a quien no es `system-bff`, tenant guard, y **cero `unsafe`** en todo el crate.

**Testing con criterio y en crecimiento.** 339 tests (+101 desde la v1), snapshots `insta` para contratos Janus —ahora también para point lookup y FTS—, `proptest`, e inyección de dependencias por traits que hace mockeable el quota step y el writer EAV.

**Disciplina de errores.** ~46 `unwrap()` fuera de bloques de test sobre 43.344 líneas. `DomainError` se reescribió como struct compacto (`src/domain/errors.rs:361`): clippy ya no reporta ni un `result_large_err`. El ítem 3 de la Fase 4 de la v1 queda saldado.

### Lo que la baja

**Disciplina de repo mala — y sin cambiar desde la v1.** 369 archivos sin commitear (145 modificados, **137 borrados** —los `.clj` del port—, 85 sin rastrear) sobre un historial de 10 commits. Ver Parte II: sigue siendo el riesgo dominante del proyecto.

**El árbol sigue hablando de un stack que ya no existe.** El motor es 100% Rust, pero carga **312 comentarios `[PORTED_FROM: ...]` y ~140 líneas más del stack viejo en `src/`**, 48 documentos heredados en `docs/` (34 con menciones a Clojure), un CI de contratos que **ejecuta Clojure**, y 98 archivos `.clj`/`.edn` aún rastreados en HEAD. Decisión del propietario (v3→v4): nada se marca como histórico — se **purga**, la carpeta `docs/` se elimina entera y la documentación se rediseña desde cero (Fase 5). El `README.md` boilerplate de AWS SAM **ya fue reescrito** con el stack real (2 de septiembre).

**CI casi decorativo.** El único workflow valida contratos con Clojure y vigila la copia muerta del proto. Sigue **sin correr `cargo build`, `cargo test` ni `clippy`**.

**Funciones gigantes — y aparecieron otras nuevas.** `run_oltp_query_fbs` no cambió (579 líneas), pero la Fase 3 de la v1 no veía a `execute_single_query` (428 líneas en `local_s3_query_engine.rs`), `step4_mutational` (324 en `authorizer.rs`) ni `validate_role_grants` (287 en `service.rs`). Ver Parte IV.

**Higiene de secretos empeoró.** `.env.local`, que la v1 describía con credenciales AWS falsas, **hoy contiene un `HMAC_SECRET` real de 64 caracteres** y sigue rastreado en git. Las credenciales AWS siguen siendo placeholders, pero el secreto HMAC ya no es juguete: ver Fase 0, paso 5.

---

# Parte II — El riesgo que manda sobre todo lo demás

## El motor que está funcionando existe solo en el árbol de trabajo

Siete días después de la v1, la situación es esencialmente la misma, con más superficie expuesta:

- `HEAD` contiene **94 archivos `.clj`** y **121 de los 224 `.rs`** que hay en disco. Los **103 archivos Rust restantes no están commiteados**.
- `git status` suma **369 entradas**: 145 modificados, 137 borrados (el árbol Clojure, eliminado del disco pero vivo en HEAD), 85 sin rastrear.
- El diff sin commitear: **283 archivos, 20.329 inserciones, 29.853 borrados**.
- Sin tags. Sin `.git-blame-ignore-revs`. Rama `main`.

Desde la v1 se commitearon las features de FTS y OLAP (sobre archivos ya rastreados), lo que confirma que **sí se está trabajando contra este árbol** — pero el grueso del port y de las modificaciones recientes sigue viviendo únicamente en el disco. Un `git checkout .`, un `git stash` mal aplicado o un merge equivocado siguen pudiendo destruir medio producto. La Fase 0 sigue siendo veinte minutos que no se han dado.

---

# Parte III — Reglas que no se negocian

Cuatro invariantes que aplican a todos los commits del refactor. Si una tarea obliga a romper alguna, la tarea está mal descompuesta.

| # | Regla | Por qué |
|---|---|---|
| 01 | **Separación.** Un commit mueve código o cambia comportamiento, nunca las dos cosas. | Un diff que hace ambas es imposible de revisar y de revertir. |
| 02 | **Red primero.** Nada se refactoriza sin un test que falle si el comportamiento cambia. | Si no existe, escribirlo es parte de la tarea, no una tarea aparte. |
| 03 | **Verde continuo.** El binario compila y los tests pasan al final de *cada commit*. | Nada de ramas que quedan rotas tres días. |
| 04 | **Golden justificado.** Si un snapshot cambia, el commit explica por qué el nuevo valor es correcto. | Sin justificación, se revierte. Un golden que se actualiza «porque falló» convierte la red en decoración. |

---

# Parte IV — Las cinco fases

Cada fase termina en una puerta: una condición verificable con un comando. No se avanza hasta que la puerta pasa. El orden tiene dependencias reales — la Fase 3 sin la Fase 1 es refactorizar a ciegas sobre un sistema en producción.

---

## Fase 0 — Preservar lo que ya funciona

**Bloqueante · ~20 minutos · no es refactor, es rescate · ⚠️ SIGUE PENDIENTE (7 días después de la v1)**

1. **No intentes armar commits bonitos todavía.** Un solo `git add -A && git commit` que preserve todo; la historia se parte después con `git reset --soft`.
2. **Excluir basura antes del `add` — falta un patrón.** `.gitignore` ya cubre `__pycache__/`, `.DS_Store`, `.venv/` y `.aws-sam/`, pero **no `scratch/`** (a hoy ya no está en el disco, pero reaparece con cada ronda de experimentos locales — así pasó esta misma semana). Añadir `scratch/` al `.gitignore` es la única edición previa necesaria.
3. **Etiquetar y empujar.** `git tag v0-pre-refactor` y push del tag. Sigue siendo el punto de retorno de todo el plan.
4. **Verificar desde un clon limpio.** Clonar el repo en otro directorio y correr `cargo test --lib`. Si no pasa desde el clon, falta algo por commitear — típicamente `config/` o los `.fbs`.
5. **`.env.local`: ahora sí hay un secreto que proteger.** La v1 lo dejaba pasar porque solo llevaba credenciales falsas. Hoy contiene un `HMAC_SECRET` de 64 caracteres. Antes del commit de rescate: `git rm --cached .env.local` (el `.gitignore` ya lo lista, pero el tracking lo anula) y **rotar el secreto** en el entorno que lo consuma. El valor ya viajó en working trees, copias y posibles pushes; tratarlo como comprometido.

### Puerta 0

- [ ] `git status --short` devuelve vacío
- [ ] El tag `v0-pre-refactor` existe en el remoto
- [ ] `.env.local` deja de estar rastreado y el secreto fue rotado
- [ ] `cargo test --lib` pasa en verde **desde un clon fresco**, no desde el directorio de trabajo

---

## Fase 1 — Construir la red de seguridad

**Prerrequisito de las fases 2 a 4 · ⚠️ PENDIENTE**

1. **Unificar el contrato — y ojo: la sincronización manual ya falló una vez.** Las tres copias siguen ahí (Parte VI). Novedad de la v2: la copia raíz **sí fue editada el 31 de agosto** (40.403 → 40.900 bytes) y aun así **sigue sin contener `ListEntities`**. Alguien intentó sincronizarla a mano y lo hizo mal, probablemente desde un estado intermedio del contrato. Eso es exactamente el modo de fallo que esta tarea existe para matar: la copia viva es `proto/metri.proto`, las otras dos se borran, y el CI falla si reaparece un `.proto` fuera de `proto/`.
2. **Revivir la suite de 500 casos — sin el emisor `.edn`.** Sigue muerta al arrancar: `resources/schema/` **no existe**. La v4 cambia el arreglo: **no se regenera el `.edn`** — es formato de datos del stack muerto y su emisor vive en `tools/`, condenado por la purga de archivos (Fase 2, anexo, fila 1). La suite pasa a consumir el contrato directamente: parsear `proto/metri.proto` desde Python (`grpcio-tools`/`protoc`) o consumir un export JSON emitido por un binario Rust del propio engine. Y corregir la ruta obsoleta de su mensaje de error.
3. **Capturar un corpus golden.** Progreso parcial: `src/janus/snapshots/` tiene 4 snapshots — `point_lookup_native/plan` y `fts_search_native/plan` (los FTS son nuevos de la semana pasada). Sigue sin haber **ningún golden por viz type**: la meta de `KPI`, `TIMESERIES`, `TABLE`, `PIE`, `BUBBLE`, `CSV_EXPORT` en `tests/golden/` está intacta. El golden de `ListEntities` es prerrequisito de L4.
4. **Tapar el punto ciego del EAV.** Avance marginal: 7 archivos de `src/eav/` tienen módulos `#[cfg(test)]` inline, pero siguen sin haber **archivos de test dedicados** y el writer sigue siendo la capa con menos red por línea. Cubrir `writer/transact_with_projections`, `writer/transact_bulk_deferred` y `reader/pull.rs` antes de tocarlos. Nota v2: la función grande del writer ya no es `transact` sino `transact_with_projections` (`src/eav/writer/transact.rs:148`); actualizar los apuntes de cobertura.
5. **Poner el CI a hacer su trabajo.** No hay `rust.yml`; el único workflow sigue siendo `contract-conformance.yml`, que **ejecuta Clojure** (`setup-clojure`, `validate_contracts.clj`, `validate_traceability.clj`) y vigila `metri.proto` **raíz** — la copia muerta. Añadir `rust.yml` con `cargo build`, `cargo test` y `cargo clippy` (sin `-D warnings` todavía: 348 es la línea base) y reapuntar el workflow de contratos a `proto/`. Apuesta mínima: reapuntar. Apuesta definitiva, ordenada por la v3: **reescribir la conformidad de contratos en Rust** (dos tests que parseen `proto/` y validen trazabilidad) y ejecutar todo el workflow sin Clojure — va en la Fase 5A, una vez que este ítem exista, porque borrar los validadores antes de que haya reemplazo deja los contratos sin vigilancia.

### Puerta 1

- [ ] Existe una única copia viva del contrato, y el CI vigila esa
- [ ] La suite de 500 casos corre y produce un informe reproducible dos veces seguidas
- [ ] El `.edn` regenerado incluye `ListEntities`
- [ ] Existe al menos un golden por cada viz type del contrato
- [ ] `cargo test` corre en CI sobre cada push
- [ ] La línea base de clippy queda registrada en el repo

---

## Fase 2 — Limpiar el ruido, sin tocar semántica

**Riesgo bajo · alto retorno · ~½ completada desde la v1**

1. **`cargo fmt` en un commit aislado y en ningún otro.** El diff creció: **31.356 líneas** (eran 28.655). Mismo plan: commit propio, hash en `.git-blame-ignore-revs` (que no existe todavía).
2. **Eliminar los 75 warnings de rustc.** Eran 70; crecieron con las features nuevas. `cargo fix` resuelve la mayoría de forma automática.
3. **Borrar el código muerto confirmado — re-verificado hoy.** Sigue muerto y sin llamadas: `QueryConfig` (`src/aegis/sql/query_config.rs`, solo definición y `Default`), `get_key_date_range` (`src/infrastructure/local_s3_query_engine.rs:1424`), `is_system_ts_field` y `coerce_epoch` (`src/janus/filter_compiler.rs:10` y `:24`), `saga::kind`. **Cambio desde la v1:** `registry.by_hash` ahora sí se lee (`src/codice/registry.rs:313`) — ese ítem se descarta, el código resucitó legítimamente.
4. ~~**Borrar `quota_guard.rs`.**~~ ✅ **HECHO desde la v1.** El archivo ya no existe y nadie lo referencia. Queda su hermano: **`src/eda/outbox.rs` sigue siendo el stub peligroso** — `save_for_retry` devuelve `Ok(())` sin persistir nada, ahora con un test de 10 líneas que consagra el no-hacer-nada. O se implementa o se borra del `mod.rs`; cablearlo de buena fe sigue significando pérdida silenciosa de eventos.
5. **Aplicar las correcciones mecánicas de clippy.** Línea base hoy: **348 hallazgos** (eran 327). Mismo criterio: lo mecánico automatizado, lo dudoso a mano.
6. **Revisar a mano —no automatizar— los `if` con bloques idénticos.** Un condicional cuyas dos ramas hacen lo mismo suele ser una rama que se escribió mal. Candidatos a bug real.
7. **Unificar la regla de indexabilidad AVET.** La divergencia en `Null` **persiste verificada hoy**: `is_avet_indexable()` (`src/eav/types/value_type.rs:48`) excluye `Bytes | Array | Null`; `validate_list_filters()` (`src/grpc/service.rs:453`) excluye solo `Bytes | Array`. La validación acepta filtros que el writer rechaza. Extracción pequeña, sigue esperando.
8. **NUEVO: sacar las políticas Cedar de `docs/` — prerrequisito duro del borrado de `docs/` (Fase 5A).** El código de producción compila políticas con `include_str!("../../docs/architecture/cedar/metri.cedar")` (`src/grpc/service.rs:80`) y el schema con `include_str!("../../docs/architecture/cedar/cedar-schema.json")` (`src/cedar/authorizer.rs:671`). Son artefactos de seguridad vivos viviendo en un directorio de documentación — nada garantiza que una edición «documental» no compile una política distinta. Moverlos a `config/policies/` y referenciar desde ahí. **Sin este movimiento primero, el `rm -rf docs/` de la Fase 5A rompe el build.**

### Fase 2 · Anexo — Purga de archivos innecesarios (inventario verificado hoy)

Más allá del código muerto y los restos de la era Clojure, el árbol arrastra archivos y carpetas completos que ya no sirven a nada. Inventario con estado de git y referencias comprobadas:

| # | Archivo / carpeta | Estado en git | Por qué sobra | Acción |
|---|---|---|---|---|
| 1 | **`tools/`** — schema engine en Python (~15 archivos, 484 KB; `schema_engine/` en disco, plano en HEAD, **con `__pycache__/*.pyc` commiteados**) | Trackeado | Generador de esquemas + emisor `.edn`: redundante con el Códice (SSOT en `config/models/`) y con el contrato en `proto/`. Su único consumido vivo era la suite de 500 casos | Borrar **después** de redefinir su input (Fase 1, ítem 2): la suite consume `proto/` directo o un JSON exportado por un binario Rust. Nada vuelve a generar `.edn` |
| 2 | `check_builder.rs` (raíz) | Solo local | 37 bytes con un `println!("Hello")`; no está en `build.rs` ni en `Cargo.toml`, nadie lo importa | Borrar |
| 3 | `Dockerfile.builder` | Trackeado | Imagen de build de la era Clojure (`clojure:tools-deps` + plugin grpc-java) | Borrar |
| 4 | `docker-build.sh` | Trackeado | Sin referencias en Makefile, scripts ni CI; `make docker-build` ya lo cubre | Borrar |
| 5 | `run_server.sh` | Trackeado | 133 bytes, sin referencias; `make dev` / `make engine` lo cubren | Borrar |
| 6 | `template-draft.yaml` | Trackeado | Borrador abandonado; el template vivo es `template.yaml` | Borrar |
| 7 | `response.json` | Trackeado | Residuo de una invocación local de SAM de mayo | Borrar |
| 8 | `env.json`, `event.json`, `events/` | Solo local (ignorados) | Residuos de `sam local invoke` | Borrar del disco |
| 9 | `bin/smoke_test.sh` | Trackeado | Sin referencias; `make smoke` usa `scripts/local/grpcurl-test.sh`. Ojo: `bin/` en sí se queda — `.gitignore` lo usa como salida de build (`bin/bootstrap`) | Borrar el script |
| 10 | `src/bin/` | — | Directorio vacío | Borrar |
| 11 | `metri.proto` (raíz) y `src/grpc/gen/metri.rs` | Trackeados | Copias muertas del contrato | Ya tienen su propio plan: Parte VI / Fase 1 |
| 12 | `scratch/`, `__pycache__/`, `.DS_Store` | Solo local | Ruido de experimentos; `scratch/` ya no está en el disco a hoy | Patrón defensivo en `.gitignore` (Fase 0, paso 2) |

**Regla de la purga:** referencia verificada antes de borrar (este inventario la trae), cada archivo en commit propio o en un único commit mecánico de borrados — nunca mezclado con cambios semánticos — y `cargo test` en verde después. Borrar archivos no toca `src/`, salvo los módulos muertos ya listados arriba.

### Puerta 2

- [ ] Cero warnings de rustc en `cargo check --all-targets`
- [ ] Clippy por debajo de 150 hallazgos (desde 348)
- [ ] La suite golden produce salida **byte a byte idéntica** a la de la Puerta 1
- [ ] El commit de `fmt` está en `.git-blame-ignore-revs`
- [ ] Ningún `include_str!` de producción apunta a `docs/`
- [ ] Los archivos del anexo de purga ya no están en el disco ni en HEAD (`tools/`, `Dockerfile.builder`, `template-draft.yaml`, `response.json`, `check_builder.rs`, scripts huérfanos) y `cargo test` sigue en verde

---

## Fase 3 — Descomponer las funciones gigantes

**Una función por PR · ⚠️ SIN EMPEZAR · el blanco se movió**

Aquí está el trabajo real y todo el riesgo de regresión. Las costuras siguen existiendo en `run_oltp_query_fbs` (los banners de comentario que marcan bloques con responsabilidad única). Pero la v2 trae una novedad importante: **mientras la Fase 3 dormía, el código creció por otros sitios**, y la v1 no veía a tres de los gigantes actuales.

### Técnica: extraer y delegar

La función original conserva su firma y el orden de su cuerpo; los bloques se mueven a funciones privadas y las llamadas quedan donde estaba el código. **Nunca reordenar sentencias en el mismo commit que una extracción.** Si al extraer descubres que un bloque depende de estado mutable de otro, el hallazgo se documenta y se aborda en un commit posterior.

### Prioridad cero: el preámbulo Cedar triplicado — sigue triplicado

Re-verificado hoy: `explore` (`src/grpc/service.rs:593`), `query` (`:814`) y `list_entities` (`:1110`) contienen **el mismo bloque de autorización copiado**: construir el request, inyectar los metadatos y llamar a `cedar::authorizer::intercept`, seguido de `check_tenant_isolation` y `check_crud_authorization`. Tres copias de código de seguridad que nada sincroniza. Extraerlo a un único helper es la extracción de mayor valor del refactor y desbloquea la sub-fase L3 de `ListEntities`.

### Objetivos, por riesgo ajustado (medidos hoy)

| # | Función | Ubicación | Líneas | Nota v2 |
|---|---|---|---|---|
| 1 | `run_oltp_query_fbs` | `src/aegis/oltp/executor.rs:343` | 579 | Igual que la v1 (577). Sigue siendo el objetivo #1 |
| 2 | `execute_single_query` | `src/infrastructure/local_s3_query_engine.rs:180` | **428** | **NUEVO.** El motor OLAP local creció sin que la v1 lo viera |
| 3 | `step4_mutational` | `src/cedar/authorizer.rs:741` | **324** | Es la función que la v1 llamaba `authorize`; el paso mutacional del PDP |
| 4 | `transact_with_projections` | `src/eav/writer/transact.rs:148` | 290 | El writer se reorganizó: `transact` quedó en wrapper de 6 líneas y la lógica real vive aquí (más `transact_bulk_deferred`, 171 líneas en `:442`) |
| 5 | `validate_role_grants` | `src/grpc/service.rs:155` | **287** | **NUEVO.** Validación de roles con su propio peso |
| 6 | `query` | `src/grpc/service.rs:814` | 287 | Medio |
| 7 | `explore` | `src/grpc/service.rs:593` | 220 | Bajo |
| 8 | `bulk_ingest` | `src/grpc/service.rs:1458` | 192 | Bajo (creció desde 170) |
| 9 | `transact` (gRPC) | `src/grpc/service.rs:1268` | 189 | Medio |
| 10 | `list_entities` | `src/grpc/service.rs:1110` | 157 | Bajo — y congelado mientras dure la extracción (ver Parte V) |

**Baja del radar:** `match_routing_rules_batch` pasó de 272 a **150 líneas** — alguien la descompuso por su cuenta. Funciona así, no al revés: las reglas de la Fase 3 con la mitad del trabajo hecho de gratis. `assemble_principal_graph` (191 líneas en `authorizer.rs:1245`) entra en cola cuando toque tocar `authorizer.rs` por el ítem 3.

### Cómo partir `run_oltp_query_fbs`

Seis extracciones, en este orden, cada una su propio PR. Las dos primeras son las más fáciles porque son caminos de salida temprana ya aislados:

1. El camino de *time-travel* (`EavReader::history`) — retorna antes que todo lo demás.
2. El camino de *as-of snapshot* — igual, salida temprana independiente.
3. Extracción y resolución de atributos requeridos: hoy son seis bloques casi idénticos que recorren `metrics`, `dimensions`, `filters`, `sort`, `hierarchy` y `select_tree`. Candidatos evidentes a una función con un parámetro.
4. El cierre de filtrado en streaming: filtro de entidad, filtros de negocio, jerárquico y fuzzy. El más delicado, porque captura estado por movimiento — y **acaba de crecer**: el fallback FTS de la semana pasada vive aquí dentro.
5. Ventana temporal en memoria, ordenación y paginación.
6. *Output cast*, resolución de relaciones, `has_children` y construcción de la paginación.

### Puerta 3 — se evalúa en cada PR, no al final

- [ ] El diff muestra únicamente movimiento de código: nada añadido, nada eliminado, nada reordenado
- [ ] La suite golden sigue byte a byte idéntica
- [ ] `cargo test` en verde
- [ ] Ninguna función del PR supera las 80 líneas

---

## Fase 4 — Saldar la deuda de diseño

**Cambia estructura, con la red puesta · 1 de 5 ítems resuelto desde la v1**

1. **Configuración única, leída una sola vez — re-verificado, sigue pendiente.** `METRI_MASTER_TENANT_ID` se lee con `env::var` **dentro del camino caliente**, en cada consulta: `src/aegis/oltp/executor.rs:197` y `:349` (una en `run_oltp_query`, otra en `run_oltp_query_fbs`). 59 llamadas a `env::var` fuera de tests en total. Bonus de incoherencia que el `EngineConfig` arregla de paso: el mismo concepto tiene **dos nombres** — `MASTER_TENANT_ID` (lo lee `server.rs`) y `METRI_MASTER_TENANT_ID` (lo lee el executor). Construir un `EngineConfig` en el arranque, guardarlo en un `OnceLock` e inyectarlo. Arregla cuatro cosas: deriva de configuración, doble nombre del maestro, una llamada al sistema por consulta, y tests que hoy no pueden fijar configuración sin manipular el entorno global.
2. **Cerrar el fallback del secreto HMAC — sin cambios.** `src/grpc/server.rs:104` sigue leyendo `ENVIRONMENT` con default `"development"` y solo aborta si el valor es explícitamente `production`, `prod` o `staging`. Cualquier entorno mal nombrado arranca con el secreto de desarrollo hardcodeado. Invertir la lógica: valor desconocido o ausente se trata como producción y falla cerrado.
3. ~~**Adelgazar `DomainError`.**~~ ✅ **RESUELTO desde la v1.** El enum con variantes de 128+ bytes se reescribió como struct compacto `{ code, stage, detail, retryable, context }` (`src/domain/errors.rs:361`). `cargo clippy --all-targets` reporta **cero** `result_large_err`. Queda la nota menor de que `Option<serde_json::Value>` sigue siendo el campo más pesado del struct, pero ya no cruza el umbral de clippy.
4. **Aislar la raíz de composición.** `src/grpc/server.rs` sigue concentrando el cableado de 6 interruptores stub/real (Kinesis, Athena, SQS, EventBridge, S3, DynamoDB). Es cableado, no lógica de servidor. Novedad: el crecimiento de `service.rs` (ver Fase 3) hace que el corte raíz-lógica sea cada vez más rentable.
5. **Mover `MAX_LIMIT` al `EngineConfig`.** Sigue siendo una `const` hardcodeada, ahora en `src/grpc/service.rs:1142`.

### Puerta 4

- [ ] Cero `env::var` fuera del arranque y de la raíz de composición
- [ ] Existe un test que verifica que un `ENVIRONMENT` ausente o desconocido **impide** arrancar con el secreto por defecto
- [ ] Los tests corren en paralelo sin `set_var` sobre el entorno global

---

## Fase 5 — Purgar Clojure y dotar al motor de una estrategia de documentación

**Cierre · Alcance cambiado en la v3: de «marcar como histórico» a «purga total»**

La v2 proponía poner cabeceras de «documento histórico» a los documentos de la era Clojure. Decisión del propietario: no. **El motor es 100% Rust** — el stack viejo no es historia que documentar, es ruido que confunde a quien llega y a las herramientas. Todo se elimina o se reescribe; el historial de git conserva lo que algún día haga falta recuperar.

La Fase 5 tiene dos mitades: **5A** mata toda referencia al stack Clojure (inventario verificado abajo); **5B** instala una metodología de documentación para que el árbol no vuelva a pudrirse.

### 5A — Purga del stack Clojure (inventario verificado el 2 de septiembre)

| # | Categoría | Volumen | Acción |
|---|---|---|---|
| 1 | Comentarios `[PORTED_FROM: src/metri/…]` en `src/` | **312** | Eliminar. La trazabilidad cumplió su función: el port está completo y en verde (339 tests). El origen de cada módulo vive en el historial de git |
| 2 | Menciones de Clojure/Datomic/Malli/HoneySQL/`.edn` en `src/` | ~140 líneas | Reescribir el comentario, no borrarlo: la mayoría justifican decisiones de diseño («reemplaza X del stack JVM») — se conserva el *porqué* Rust y se cae el nombre del muerto. Ej.: `src/domain/errors.rs:13` |
| 3 | Workflow de contratos con Clojure (`setup-clojure`, `validate_contracts.clj`, `validate_traceability.clj`) | 1 workflow | **Reescribir la conformidad en Rust** (parsear `proto/`, validar trazabilidad) y solo entonces borrar los validadores. Va al final de 5A porque depende de la Fase 1 — sin CI de Rust previo, la purga dejaría los contratos sin vigilancia |
| 4 | **La carpeta `docs/` completa** — 48 documentos heredados, 34 con menciones al stack viejo | ~1 directorio | **Eliminarla entera** (decisión v4): no se clasifica ni se reescribe nada de lo heredado; la documentación se diseña desde cero en 5B. Antes del `rm -rf docs/`, tres dependencias duras: (a) mover las políticas Cedar a `config/policies/` — Fase 2, ítem 8, sin eso el build se rompe; (b) tener el reemplazo en Rust del CI de contratos (ítem 3); (c) reubicar los documentos vivos — este plan entre ellos — a la nueva estructura |
| 5 | Archivos `.clj`/`.cljc`/`.edn` rastreados en HEAD | 98 | Ya borrados del disco; desaparecen de HEAD con el primer commit de esta fase |
| 6 | `.edn` sueltos en disco (`docs/architecture/poc/deps.edn`, `docs/architecture/cedar/cedar-section.edn`, `cedar-policy-codice.edn`) | 3 | Borrar — verificado: ningún `include_str!` ni build los referencia. El esquema Cedar vive en `cedar-schema.json` y las políticas en `metri.cedar` |
| 7 | Emisor `.edn` — vive en `tools/` | 1 herramienta | Resuelto por la purga de archivos de la Fase 2 (anexo, fila 1): `tools/` se elimina completo **después** de que la Fase 1 migre el input de la suite de 500 casos a JSON o proto directo. En `.edn` no queda nada — es formato de datos del stack muerto |
| 8 | Menciones en `scripts/`, `tools/`, `config/`, `README.md`, `Makefile`, `template.yaml` | 7 | Reescribir en el pase final |
| 9 | `.gitignore` con `.cpcache/` (caché de Clojure) y README de SAM con Java/Maven | triviales | Cae con el rewrite del README y el pase de `.gitignore` |

**Orden de la purga:** CI primero (ítem 3, con la Fase 1 como prerrequisito) → `src/` después (ítems 1-2, en commits mecánicos separados — Regla 01: mueven texto, no semántica; hacerlos **antes** de la Fase 3 ensuciaría los diffs de extracción, así que su sitio natural es inmediatamente después de la Fase 3 o en paralelo sin solaparse con los archivos de la Fase 3) → `docs/` al final, y no es un `rm` más: políticas Cedar movidas (Fase 2.8), conformidad de contratos reescrita en Rust (ítem 3) y documentos vivos reubicados a la nueva estructura, **en ese orden**, antes de borrar.

### 5B — Documentación diseñada desde cero (metodología: docs-as-code + Diátaxis + ADRs)

Decisión v4: nada de lo heredado se migra ni se recicla como texto — **la carpeta `docs/` se borra entera y la documentación se diseña desde cero, actualizada a la realidad del motor Rust**. Los documentos viejos valen como materia prima de *lectura* para quien escriba (hay decisiones de diseño válidas en ellos), pero ningún texto entra a la nueva estructura sin reescribirse y llevar cabecera de vigencia. La metodología, en seis reglas:

**1. Docs-as-code.** La documentación vive en el repo, se revisa en PR y miente en CI si está desactualizada (la puerta 5 lo verifica). Sin documento fuera del repo, sin wiki paralela.

**2. Clasificación Diátaxis — cada documento declara a cuál de las cuatro intenciones sirve, y las carpetas lo reflejan:**

```
README.md               # Puerta de entrada: qué es el motor en 5 líneas + quickstart real (<15 min)
docs/
  architecture/         # EXPLICACIÓN: cómo está diseñado y por qué (C4 nivel 1-2)
    adr/                #   ADRs numerados: una decisión, un archivo, estado explícito
  reference/            # REFERENCIA: verdad mecánica 1:1 con el código — generada, nunca a mano
    api-grpc.md         #   generada desde proto/metri.proto
    codigos-error.md    #   generada desde config/errors/error_catalog.toml
    modelos-codice.md   #   generada desde config/models/*.json
  guides/               # HOW-TO: tareas operativas (deploy, debug, ingesta masiva, rotar secretos)
  archive/              # Documentos que no se reescriben pero pesan: fuera del camino, sin vigencia
```

El contenido técnico de `docs/rust/` (`JANUS - Rust.md`, `AEGIS - SQL.md`, `Metri EAV - OLPT.md`) es la materia prima para escribir los nuevos documentos de `architecture/` — como insumo de lectura, no como texto heredado.

**3. ADRs para lo que hoy son comentarios de 30 líneas.** Las decisiones importantes están escondidas en el código: el `Eav003` que se deja «por contrato» aunque nada implementa el lock (`src/domain/errors.rs`), el `by_hash` que resucitó, el outbox que no persiste. Cada una se convierte en un ADR corto (contexto → decisión → consecuencias → fecha); el comentario en el código queda en dos líneas que enlazan al ADR. Es lo que evita que la purga del ítem 2 de 5A tire conocimiento junto con el ruido.

**4. Regla de vigencia.** Todo documento lleva cabecera: qué es, contra qué estado del código se verificó, y última fecha de verificación. Un doc sin cabecera es huérfano y candidato a borrado en la revisión trimestral. Es la institucionalización de la convención que ya usa este plan: *verificado contra el código, no contra la documentación*.

**5. Generación sobre curación.** Lo que el código ya sabe no se escribe a mano: los tres `reference/*.md` se regeneran con un script en `scripts/docs/`, y el CI falla si el generado está desactualizado respecto a proto, catálogo o modelos. (Este motor ya tiene el catálogo de errores como TOML canónico y 60 modelos JSON — la referencia se genera sola; hoy solo falta el script.)

**6. Una fuente única por tema.** La API se documenta en el proto, los errores en el catálogo, los esquemas en el Códice — todo lo demás **enlaza**, no duplica. Es la regla que mata definitivamente el patrón que produjo las tres copias de `metri.proto` y los tres preámbulos Cedar.

**El `README.md` ya está reescrito** (2 de septiembre de 2026, tarea hecha adelantada): qué es el motor, mapa de módulos, API gRPC, quickstart con los comandos reales del Makefile, tests, estructura del repo, configuración por variables de entorno y despliegue SAM. Queda pendiente solo su verificación desde un clon fresco.

### Puerta 5

- [ ] `git grep -i "clojure\|datomic\|honey-sql\|malli"` sobre archivos rastreados devuelve **0**
- [ ] No queda ningún `.clj`/`.cljc`/`.edn` en el disco ni en HEAD
- [ ] La carpeta `docs/` heredada **ya no existe**; la nueva estructura (architecture/reference/guides/archive) está creada según 5B, con este plan reubicado dentro
- [ ] El CI de contratos corre **sin Clojure** (validadores reescritos en Rust, vigilando `proto/`)
- [ ] Los tres documentos de `reference/` se regeneran y el diff es vacío
- [ ] El README quickstart verificado desde un clon fresco
- [ ] Todo documento nuevo lleva cabecera de vigencia; los ADRs cubren las decisiones que hoy viven en comentarios

---

# Parte V — ListEntities, que está a medio camino

No es legado que haya que ordenar: es una capacidad nueva, en construcción activa, con su plan propio en [PLAN_IMPLEMENTACION_LIST_ENTITIES.md](PLAN_IMPLEMENTACION_LIST_ENTITIES.md). El refactor tiene que absorberla sin frenarla, y ella tiene que terminar sin esquivar las reglas del refactor.

Estado verificado contra el código hoy.

| Sub-fase | Entregable | Estado |
|---|---|---|
| **L1** | Contrato | **Hecho** |
| **L2** | Tope duro, sin paginación | **Hecho** |
| **L3** | Autorización | **Parcial** |
| **L4** | Paginación real | Pendiente |
| **L5** | Cliente Go y reconciliador | Pendiente |

**L1 — Contrato.** `rpc ListEntities` y sus mensajes viven en `proto/metri.proto:26`. Devuelve solo ids, con `truncated` explícito. Recuerda: está en la copia viva y ausente en las otras dos (Parte VI).

**L2 — Tope duro.** Implementado y estable: `limit` obligatorio, máximo 5.000, `AvetIntersect` con `entity_type` como primer filtro, validación contra el Códice. El orden determinista que fija el `sort()` sigue siendo la base del cursor de L4.

**L3 — Autorización (parcial — sin cambios desde la v1).** Cedar completo: `intercept`, `check_tenant_isolation`, `check_crud_authorization`, denegación explícita. **Sigue faltando `quota_step`**: re-verificado hoy, el bloque de `list_entities` (`src/grpc/service.rs:1110`) no llama cuota; el `QuotaGuardStep` se construye en `service.rs:58` pero lo consume el pipeline de escritura, y `quota_service.rs` documenta explícitamente qué rutas **saltan** el control. Listar es barato por consulta y caro en bucle. Se resuelve solo cuando el preámbulo Cedar de la Fase 3 se extraiga a un helper: ahí cae el quota step en el mismo sitio.

**L4 — Paginación real (pendiente).** `next_page_token` se devuelve siempre vacío (`src/grpc/service.rs:1263`). Y el riesgo de fondo sigue vivo: el `limit` recorta **después** de materializar la intersección completa — acota la respuesta, no el trabajo ni la memoria.

**L5 — Cliente Go (pendiente).** Fuera del motor. Con L2 estable ya desbloquea al reconciliador de Schedulers, siempre que respete `truncated` y aborte en lugar de borrar.

## Cómo encaja en las fases

- **Fase 0** — Sigue siendo el trabajo más reciente sin preservar: nada de la semana del 31 de agosto está respaldada en tags, y los commits de FTS/OLAP tocaron el árbol sin resolver la deuda de fondo.
- **Fase 1** — El `.edn` debe regenerarse desde `proto/metri.proto`. Y hace falta un golden de `ListEntities` antes de que L4 toque la paginación.
- **Fase 3** — El helper Cedar compartido es lo que permite completar L3 en un solo sitio.
- **Fase 4** — `MAX_LIMIT` sigue hardcodeado en `src/grpc/service.rs:1142`: va al `EngineConfig`.

## Una regla duplicada que sigue divergiendo

Qué se puede indexar en AVET está decidido en dos sitios, y **la divergencia en `Null` persiste verificada hoy**:

| Ubicación | Excluye |
|---|---|
| `is_avet_indexable()` — `src/eav/types/value_type.rs:48` | `Bytes`, `Array`, `Null` |
| `validate_list_filters()` — `src/grpc/service.rs:453` | `Bytes`, `Array` |

La validación acepta filtros por atributos `Null`-tipados que el writer va a rechazar. El camino de validación debe consultar al de escritura, no reimplementarlo: es una extracción pequeña para la Fase 2 (ítem 7).

---

# Parte VI — El contrato existe tres veces

Re-verificado hoy. Novedad: la copia muerta **fue editada el 31 de agosto** y la edición no la acercó al contrato real — es la prueba de que la sincronización manual no es una hipótesis de riesgo, es un hecho ocurrido.

| Copia | Papel | Tamaño y fecha | `ListEntities` |
|---|---|---|---|
| `proto/metri.proto` | **La viva.** La que compila `build.rs` vía `tonic_build` | 42.608 B · 31 ago 12:08 | Presente (3 menciones) |
| `metri.proto` (raíz) | **Obsoleta, editada y sigue sin el RPC.** El workflow de conformidad valida este archivo | 40.900 B · 31 ago 12:08 | Ausente |
| `src/grpc/gen/metri.rs` | **Salida generada, obsoleta y commiteada.** El módulo `pb` hace `include_proto!` desde `OUT_DIR`; nadie la usa. 106.788 B · 19 may | 2.391 líneas de ruido | Ausente |

**Resolución, en la Fase 1:** borrar las dos copias muertas, reapuntar el workflow de contratos y el generador de esquemas a `proto/`, y añadir una comprobación que falle si vuelve a aparecer un `.proto` fuera de ese directorio. Mientras haya tres copias —y una de ellas con ediciones recientes—, «el contrato» no significa nada.

---

# Parte VII — Qué no tocar

Un plan de refactor también define su perímetro. Estos módulos están sanos y bien cubiertos: moverlos solo añade riesgo sin comprar nada.

- **El motor de fórmulas** — `src/aegis/formula/`, con lexer, parser, resolver, evaluador y capa de seguridad separados, y sus archivos de test dedicados. Ya está descompuesto como debe estar; es el modelo a imitar en la Fase 3, no un objetivo.
- **Códice** (`src/codice/`) y **temporal** (`src/temporal/`) — funciones puras, cobertura densa, sin dependencias de infraestructura. Los `unwrap` que aparecen ahí son sobre parseos infalibles.
- **Los ~46 `unwrap` de código productivo** repartidos en 43.344 líneas. Es una cifra buena para Rust y ninguno está en un camino de petición sin validación previa. Convertirlos a `Result` en masa es mucho diff a cambio de poco.

---

# Parte VIII — Riesgos del plan

| Riesgo | Mitigación | Fase |
|---|---|---|
| **Pérdida del árbol de trabajo.** 103 archivos Rust + el estado de la semana del 31 de agosto existen solo en disco; 369 entradas sucias y sin tag siete días después de identificarlo | Commit de preservación inmediato, antes de organizar nada. Tag empujado al remoto. Verificación desde clon limpio | 0 |
| **Red de seguridad ciega.** El workflow vigila la copia raíz, que fue editada el 31 de agosto sin absorber `ListEntities`: la validación de contratos corre contra un API que ya no es el real | Unificar el contrato como primera tarea de la Fase 1, con comprobación en CI de que solo existe una copia | 1 |
| **`fmt` mezclado con lógica.** 31.356 líneas de reindentado enterrarían cualquier cambio semántico en el mismo diff | Commit aislado, registrado en `.git-blame-ignore-revs`, ejecutado antes de empezar a descomponer | 2 |
| **Regresión silenciosa en escritura EAV.** El writer creció (`transact_with_projections` + `transact_bulk_deferred`); un fallo ahí corrompe datos sin lanzar error visible | Cobertura del writer antes de tocarlo. El writer no se descompone hasta que sus tests existan | 1 → 3 |
| **Refactorizar un blanco móvil.** La semana del 31 de agosto aterrizó FTS y OLAP sobre los mismos archivos de la Fase 3, y `match_routing_rules_batch` se tocó fuera del plan | Terminar L3 *antes* de descomponer `service.rs`. El helper Cedar se hace primero. Y registrar los movimientos fuera de plan en este documento, no solo en commits | 3 |
| **Políticas de seguridad en `docs/`.** Una edición documental puede cambiar la política que compila el binario en producción — y el borrado de `docs/` (Fase 5A) rompería el build si se ejecuta antes | Mover `cedar/metri.cedar` y `cedar-schema.json` a `config/policies/` **antes** del `rm -rf docs/` (Fase 2, ítem 8 = prerrequisito duro de 5A) | 2 → 5A |
| **Documentación sin hogar.** El propio plan y las decisiones vivas viven en `docs/architecture/`; borrar la carpeta los elimina con todo lo demás | Reubicar el plan y los documentos vivos a la nueva estructura en el mismo commit del borrado (Fase 5A, fila 4c) | 5B |
| **Deriva del golden.** Un snapshot que se actualiza «porque falló» convierte la red en decoración | Regla 04: todo cambio de golden se justifica en el commit o se revierte | 3 |
| **Secreto HMAC rastreado.** `.env.local` pasó de contener placeholders a un secreto real, y sigue en git | `git rm --cached` + rotación antes del commit de rescate (Fase 0, paso 5) | 0 |
| **Purga que deja los contratos sin vigilancia.** Borrar los validadores Clojure del CI antes de que exista el reemplazo en Rust deja el API sin comprobación | Orden inverso: primero el CI de Rust (Fase 1, ítem 5), después reescribir la conformidad en Rust y solo entonces eliminar los `.clj` | 5A |
| **Purga que tira conocimiento.** Los 312 `[PORTED_FROM]` y los comentarios «reemplaza X de Clojure» son la única pista del origen y del *porqué* de muchas decisiones | El historial de git conserva el origen; las decisiones vivas se recogen en ADRs (Fase 5B, ítem 3) **antes** de purgar `src/` | 5A → 5B |
| **Suite de 500 casos huérfana.** Su input es un `.edn` — formato del stack muerto — generado por `tools/`, que la purga de archivos condena (Fase 2, anexo) | Migrar el input de la suite a JSON o proto directo como parte de la Fase 1, **antes** de borrar `tools/` | 1 → 2 |
| **Purga con daño colateral.** Un archivo «que sobra» puede tener consumidores fuera del repo (CI externo, otros servicios del monorepo, flujos de un desarrollador) | El inventario del anexo verifica referencias antes de borrar; cada borrado va en commit propio y revertible | 2 |

---

# Si solo hay tiempo para una cosa

Sigue siendo la **Fase 0**. Son veinte minutos que la v1 ya pedía hace siete días, y desde entonces el árbol se movió (FTS, OLAP, y un secreto real dentro de un archivo rastreado). Todo lo demás en este documento es deuda: molesta, se puede planificar, y sigue ahí mañana. El árbol de trabajo sin commitear, no — y cada semana que pasa, el «medio producto» que vive solo en el disco es más grande.
