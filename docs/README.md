# metri-engine Documentation Index

> Índice navegable de toda la documentación del repo. **Cada documento es alcanzable desde aquí** (regla: ningún `.md` huérfano, PLAN_DOCUMENTACION §5.1).
>
> Navegable index of every document in the repo. **Every document is reachable from here** (rule: no orphan `.md`).

## Documentación / Documentation

### Architecture (`docs/architecture/`)

| Documento | Audiencia | Cuándo consultarlo / When to read it |
|---|---|---|
| [00_OVERVIEW.md](architecture/00_OVERVIEW.md) | Todo el equipo | Visión general de la arquitectura / system overview |
| [PLAN_DOCUMENTACION.md](architecture/PLAN_DOCUMENTACION.md) | Equipo + contribuidores | Reglas de documentación (idioma, títulos, cobertura por archivo) y fases |
| [PLAN_PATRON_RESULT.md](architecture/PLAN_PATRON_RESULT.md) | Equipo + contribuidores | Patrón Result: reglas, catálogo de errores y auditor (cumplido) |
| [PLAN_REFACTORIZACION.md](architecture/PLAN_REFACTORIZACION.md) | Equipo | Plan vivo de rediseño de la arquitectura |
| [PLAN_REFACTORIZACION_MODELO.md](architecture/PLAN_REFACTORIZACION_MODELO.md) | Equipo | Simplificación del catálogo Códice: fases, deudas de modelado y contratos metri-app |
| [PLAN_REFACTORIZACION_CEDAR.md](architecture/PLAN_REFACTORIZACION_CEDAR.md) | Equipo | Rediseño del módulo de autorización |
| [PLAN_CORRECCIONES_PENDIENTES.md](architecture/PLAN_CORRECCIONES_PENDIENTES.md) | Equipo | Correcciones planificadas y su estado |
| [PLAN_COSTO_OLAP.md](architecture/PLAN_COSTO_OLAP.md) · [MEDICION_COSTO_OLAP.md](architecture/MEDICION_COSTO_OLAP.md) | Equipo | Costos y medición del canal OLAP |
| [PLAN_IMPLEMENTACION_LIST_ENTITIES.md](architecture/PLAN_IMPLEMENTACION_LIST_ENTITIES.md) | Equipo | RPC `ListEntities` |
| [PLAN_CACHE_JANUS_DYNAMODB.md](architecture/PLAN_CACHE_JANUS_DYNAMODB.md) | Equipo | Caché KV distribuida con TTL de DynamoDB para la capa de consulta Janus (diseño + fases) |

#### ADRs (`docs/architecture/adr/`) — decisiones inmutables

| ADR | Decisión / Decision |
|---|---|
| [ADR-001](architecture/adr/ADR-001-motor-eav-sobre-dynamodb.md) | Motor EAV sobre DynamoDB single-table |
| [ADR-002](architecture/adr/ADR-002-zero-trust-cedar.md) | Zero-trust Cedar: fail-closed y censura del tenant maestro |
| [ADR-003](architecture/adr/ADR-003-contrato-unico.md) | Contrato único (proto + FlatBuffers + delta) |
| [ADR-004](architecture/adr/ADR-004-configuracion-en-arranque.md) | Configuración y validación fail-fast en arranque |
| [ADR-005](architecture/adr/ADR-005-codigos-de-error-canonicos.md) | Códigos de error canónicos |
| [ADR-006](architecture/adr/ADR-006-identidad-de-entidades.md) | Identidad de entidades |
| [ADR-007](architecture/adr/ADR-007-materializacion-generica.md) | Materialización genérica — **SUPERADO**: la composición de OTs vive en metri-cmms-plugin |
| [ADR-008](architecture/adr/ADR-008-cache-janus-dynamodb.md) | Caché de consultas Janus sobre DynamoDB con TTL (tabla exclusiva, clave = AST post-ABAC) |
| [ADR-009](architecture/adr/ADR-009-motor-binario-unificado-mbf.md) | Motor Binario Unificado HTAP Metri Vortex (`.mbf`) sobre AWS S3 y retiro de DynamoDB/Athena |

### Metri Vortex Unified Binary Engine (`docs/mbf/`) — especificación del motor `.mbf`

| Documento | Audiencia | Cuándo consultarlo / When to read it |
|---|---|---|
| [mbf/README.md](mbf/README.md) | Todo el equipo | Índice maestro de la especificación del motor binario unificado `.mbf` |
| [01_ARCHITECTURE_AND_DESIGN.md](mbf/01_ARCHITECTURE_AND_DESIGN.md) | Todo el equipo | Arquitectura HTAP, sinergia IoT/CMMS/Financiero/BI y comparativa vs. IBM Maximo, Oracle, SAP y CMMS SaaS |
| [02_ARCHITECTURAL_PRINCIPLES_AND_CLEAN_CODE.md](mbf/02_ARCHITECTURAL_PRINCIPLES_AND_CLEAN_CODE.md) | Equipo + contribuidores | Clean Architecture, SOLID, DRY desde Códice, cero `unwrap`/`unsafe` y estándares `rustdoc` |
| [03_MBF_BINARY_FORMAT_SPEC.md](mbf/03_MBF_BINARY_FORMAT_SPEC.md) | Ingeniería de motor | Layout físico a nivel de byte de `.mbf` v1, `HOT_HEAD.mbf`, `.mbf.idx` y las 5 zonas alineadas a 64 bytes |
| [04_ALGORITHMS_AND_PAPERS.md](mbf/04_ALGORITHMS_AND_PAPERS.md) | Ingeniería de motor + BI | Papers científicos y ecuaciones: Welford/Chan, Pébay, KLL, Chimp128, Elias-Fano, FSST, CSR Euler, Fenwick y Hamilton |
| [05_SERVERLESS_LAMBDA_AND_S3_INFRASTRUCTURE.md](mbf/05_SERVERLESS_LAMBDA_AND_S3_INFRASTRUCTURE.md) | Infraestructura + Cloud | Optimización AWS Lambda Graviton ARM64, Cold Start `< 2 ms`, caché `/tmp` 512 MB, CAS S3 y ahorro de costos |
| [06_FOLDERS_FILES_AND_MODULE_MAP.md](mbf/06_FOLDERS_FILES_AND_MODULE_MAP.md) | Equipo + contribuidores | Árbol exacto de carpetas, archivos nuevos/modificados/eliminados y contratos de módulos Rust |
| [07_TESTING_AND_ORACLE_STRATEGY.md](mbf/07_TESTING_AND_ORACLE_STRATEGY.md) | QA + Ingeniería | Estrategia `proptest`, fuzzing de corrupción de bits, pruebas contables, concurrencia CAS y Oráculo en Modo Sombra |
| [08_IMPLEMENTATION_AND_LEGACY_DECOMMISSIONING.md](mbf/08_IMPLEMENTATION_AND_LEGACY_DECOMMISSIONING.md) | Equipo | Plan de implementación en 6 fases, migrador CLI y protocolo de eliminación del motor OLTP/OLAP antiguo |
| [09_CODICE_METADATA_AND_FUZZY_MATH_ENGINE.md](mbf/09_CODICE_METADATA_AND_FUZZY_MATH_ENGINE.md) | Equipo | Auditoría de `config/models/*.json`, expansión de metadatos sin límite (`MbfHyperSynopsis512`), confiabilidad Weibull/FFT/PCA y motor de búsqueda híbrido (Fuzzy FST + SymSpell + Fonético + BM25F + Vector RaBitQ HNSW + Uber H3) |
| [10_TEN_OUT_OF_TEN_ARCHITECTURAL_PERFECTION.md](mbf/10_TEN_OUT_OF_TEN_ARCHITECTURAL_PERFECTION.md) | Equipo + Arquitectos | Cierre de brechas al 10/10: Concurrencia OLTP extrema en S3 (Group Commit + 8-Cell Sharding + Auto-Rebase `< 150 µs`), evolución de esquema zero-downtime, sello criptográfico `Keyed BLAKE3` por tenant y compactación Hilbert |
| [11_COMPREHENSIVE_TESTING_AND_LOCAL_INFRA_PLAN.md](mbf/11_COMPREHENSIVE_TESTING_AND_LOCAL_INFRA_PLAN.md) | QA + Equipo | Plan maestro de pruebas con cobertura del 100% (`cargo-llvm-cov` + `cargo-mutants`), dataset realista de los 60 modelos del Códice e infraestructura local (`Zero-Docker` + `MinIO S3` + `Jaeger OTel`) |
| [12_STEP_BY_STEP_IMPLEMENTATION_AND_LOCAL_EXECUTION_PLAN.md](mbf/12_STEP_BY_STEP_IMPLEMENTATION_AND_LOCAL_EXECUTION_PLAN.md) | Equipo | Plan de implementación paso a paso en 6 etapas y ejecución verificada de todas las suites de prueba en local |

### Components (`docs/components/`) — fichas por módulo

| Ficha | Componente |
|---|---|
| [eav.md](components/eav.md) | Motor de almacenamiento EAV inmutable |
| [codice.md](components/codice.md) | Registro SSOT de esquemas |
| [domain.md](components/domain.md) | Contratos fundamentales: errores, eventos, puertos |
| [aegis.md](components/aegis.md) | Compiladores: SQL OLAP, executor OLTP, fórmulas |
| [janus.md](components/janus.md) | Read path |
| [janus_router.md](components/janus_router.md) | Write path |
| [cedar.md](components/cedar.md) | Autorización ABAC Zero-Trust |
| [quota.md](components/quota.md) | Cuotas por tenant |
| [iop.md](components/iop.md) | Pipeline de ingesta |
| [eda.md](components/eda.md) | Eventos: outbox y fallas |
| [temporal.md](components/temporal.md) | Primitivas temporales |
| [grpc.md](components/grpc.md) | Superficie de API gRPC |
| [infrastructure.md](components/infrastructure.md) | Adaptadores AWS y emulaciones locales |
| [application.md](components/application.md) | Puertos DIP |
| [otel.md](components/otel.md) | Trazas OpenTelemetry |

### Guides (`docs/guides/`)

| Documento | Cuándo consultarlo / When to read it |
|---|---|
| [desarrollo-local.md](guides/desarrollo-local.md) | Levantar el entorno local (DynamoDB Local, engine, seed) |
| [deployment.md](guides/deployment.md) | Deploy to AWS: GitHub Actions + OIDC (manual SAM deploy as emergency) |

### Reference (`docs/reference/`)

| Documento | Cuándo consultarlo / When to read it |
|---|---|
| [api-grpc.md](reference/api-grpc.md) | Contrato gRPC para consumidores (metri-app, bff, agentes) |
| [codigos-error.md](reference/codigos-error.md) | Catálogo canónico de errores y su mapeo gRPC |
| [diagrama-modelo-datos.md](reference/diagrama-modelo-datos.md) | Mapa visual de dominios, relaciones y deudas del modelo Códice |
| [glosario.md](reference/glosario.md) | Equivalencias EN↔ES de los términos del dominio |
| [modelos-codice.md](reference/modelos-codice.md) | Esquemas de los modelos del Códice |

### Code documentation (rustdoc)

La referencia de API vive dentro del código y se genera con:

```bash
cargo doc --no-deps --open    # referencia API (reglas: PLAN_DOCUMENTACION.md)
cargo test --doc              # los ejemplos de los doc-comments se ejecutan
python3 scripts/dev/check_docs.py   # auditor de cabeceras //! (ratchet)
```

Política de idioma: títulos y encabezados siempre en inglés; cuerpos en español (ver §1 del plan). Language policy: titles and headings always in English; bodies in Spanish.
