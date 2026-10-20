# Referencia de Suites de Pruebas (`tests/`) y Scripts (`scripts/`)

Este documento cataloga el 100% de las suites de verificación en `tests/` y las herramientas de auditoría, generación y operación en `scripts/`.

---

## 1. Suites de Integración y Guardianes en Rust (`tests/*.rs`)

Ejecutadas mediante `cargo test` o `make test-integration`:

| Archivo en `tests/` | Tipo de Suite | Invariantes que Verifica |
| :--- | :--- | :--- |
| `tests/architecture_solid_guard.rs` | Guardián de Arquitectura | Verifica 4 reglas estructurales duras: (1) `0` bloques `unsafe` en `src/`; (2) ISP $\le 5$ métodos por trait en `domain/protocols.rs` y `application/ports.rs`; (3) DIP (0 acoplamiento a `aws_sdk_*`, `tonic`, `lambda_runtime`, `crate::grpc` o `crate::infrastructure` en el núcleo saneado); (4) ejecución en verde de `scripts/dev/check_clean_code.py`. |
| `tests/mbf_comprehensive_verification.rs` | Verificación Integral `.mbf` | Prueba de extremo a extremo del motor **Metri Vortex (`.mbf`)**: cabecera alineada a 64B, protocolo CAS con inyección de conflictos `HTTP 412` (`InMemoryChaosS3Store`), reloj determinista (`DeterministicClock`), las 8 zonas físicas (`pax_mvcc`, `csr_graph`, `iot_timeseries`, `financial_ledger`, `bi_synopses`, `hybrid_search`, `fuzzy_math_catalog`, `cross_covariance`), paridad con los 60 modelos del Códice y el ejecutor `aegis::mbf`. |
| `tests/contract_conformance.rs` | Conformidad de Contrato gRPC | Valida el cumplimiento estricto de los contratos gRPC (`Discovery`, `Explore`, `Query`, `ListEntities`, `Transact`, `CompositeTransact`, `BulkIngest`, `ListSequenceConfigs`, `UpsertSequenceConfig`, `MatchRoutingRulesBatch`) sobre el servidor en memoria. |
| `tests/audit_e2e_test.rs` | Auditoría E2E | Verifica la captura e intercepción automática de eventos de auditoría (`audit_log`) en mutaciones y accesos a través de `src/infrastructure/audit/interceptor.rs`. |
| `tests/pm_vocabulary_guard.rs` | Guardián de Vocabulario CMMS | Garantiza que los modelos de mantenimiento preventivo (`preventive_maintenance`), procedimientos (`procedure_template`, `work_order_procedure`) y tareas (`work_order_task`) mantengan consistencia semántica sin reintroducir campos deprecados. |
| `tests/sherlog_test.rs` | Telemetría Forense de Fallos | Verifica el despacho de fallos de dominio (`domain_fault`) en el pipeline `iop::sherlog`, incluyendo la censura automática de campos marcados como `sensitive: true` en el Códice. |

---

## 2. Suites de Pruebas E2E, Fiabilidad y Contrato en Python (`tests/{e2e,reliability,unit}/`)

Estas suites permiten validar instancias locales o remotas del servidor gRPC/gRPC-Web (`localhost:9090` o entornos desplegados):

### 2.1 Pruebas End-to-End (`tests/e2e/`)

| Archivo | Propósito |
| :--- | :--- |
| `tests/e2e/integration_e2e.py` | Suite de integración completa cliente-servidor para ciclos de vida de entidades CMMS, transacciones y consultas analíticas. |
| `tests/e2e/production_smoke_test.py` | Prueba de humo no destructiva post-despliegue (`Discovery`, salud de servicios gRPC y latencia de respuesta). |
| `tests/e2e/cache_verify.py` | Verificación de consistencia de caché de lectura en `janus` e invalidación inmediata tras mutaciones en `eav::writer`. |

### 2.2 Pruebas de Fiabilidad y Escala (`tests/reliability/`)

| Archivo | Propósito |
| :--- | :--- |
| `tests/reliability/formula_reliability_test.py` | Pruebas de estrés y casos borde sobre el motor de fórmulas matemáticas de Aegis (división por cero, `NULLIF`, `CLAMP`, anidamiento profundo y rechazo de funciones prohibidas). |
| `tests/reliability/janus_ast_reliability_test.py` | Validación exhaustiva de compilación de consultas complejas hacia el AST IR en FlatBuffers. |
| `tests/reliability/scale_reliability_test.py` | Pruebas de concurrencia y rendimiento bajo ráfagas de lectura/escritura simultáneas. |
| `tests/reliability/scale_vizmeta_test.py` | Verificación de estabilidad de metadatos de visualización (`VizMeta`) en agregaciones multiserie de alto volumen. |
| `tests/reliability/grpc_web_client.py` | Cliente de transporte HTTP/gRPC-Web compartido por las suites de fiabilidad. |

### 2.3 Pruebas Funcionales y de Analítica (`tests/unit/`)

| Archivo | Propósito |
| :--- | :--- |
| `tests/unit/grpc_web_client.py` | Cliente ligero gRPC-Web para invocación desde scripts de prueba. |
| `tests/unit/validate_contract.py` | Validador de esquemas y contratos de respuesta gRPC. |
| `tests/unit/validate_hierarchy.py` | Verificación de consultas jerárquicas recursivas sobre ubicaciones (`location`) y activos (`asset`). |
| `tests/unit/verify_analytics.py` | Verificación de agregaciones analíticas (`SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, cuantiles). |
| `tests/unit/verify_assets_complete.py` | Validación del ciclo completo de activos, especificaciones técnicas y relaciones. |
| `tests/unit/verify_production_analytics.py` | Suite de validación de consultas analíticas sobre indicadores de producción y mantenimiento. |
| `tests/unit/verify_production_analytics_complete.py` | Batería extendida de cruce multi-entidad y series temporales en analítica. |
| `tests/unit/test_bench_assets.py` / `test_bench_production.py` | Benchmarks de latencia para consultas de activos y tableros analíticos. |
| `tests/unit/test_comparison.py` | Pruebas de comparación temporal (`YoY`, `MoM`, `PoP`) sobre `temporal::comparison`. |
| `tests/unit/test_discovery.py` | Pruebas del RPC `Discovery` y enriquecimiento con `config/locales/`. |
| `tests/unit/test_eda_prod.py` | Pruebas del servicio `MoiraRoutingService` y reglas de eventos. |
| `tests/unit/test_explain.py` | Verificación de planes de ejecución seleccionados por `janus::plan_selector`. |
| `tests/unit/test_filter_criteria.py` | Pruebas de compilación de predicados y filtros compuestos (`AND`, `OR`, `IN`, `BETWEEN`). |
| `tests/unit/test_interval_debug.py` | Pruebas de resolución de intervalos y truncamiento horario con zonas IANA. |
| `tests/unit/test_line_chart.py` | Pruebas de generación de series temporales para gráficos de líneas. |
| `tests/unit/test_search.py` | Pruebas de búsqueda full-text (`fts`) y búsqueda difusa sobre atributos indexados. |
| `tests/unit/test_single_bulk.py` | Pruebas de ingesta por lotes (`BulkIngest`) y validación de cuotas. |
| `tests/unit/test_vizmeta.py` | Pruebas de formatos de salida para KPIs, tablas, gráficos circulares y de burbujas. |

---

## 3. Scripts de Auditoría CI, Generación y Operación (`scripts/`)

### 3.1 Generadores y Auditores de Calidad (`scripts/docs/` y `scripts/dev/`)

| Script / Archivo | Propósito | Comando de Ejecución |
| :--- | :--- | :--- |
| `scripts/docs/gen_reference.py` | Autogenera `api-grpc.md`, `codigos-error.md` y `modelos-codice.md` en `docs/05-referencia/` desde las fuentes SSOT. | `python3 scripts/docs/gen_reference.py [--check]` |
| `scripts/dev/check_docs.py` | Audita el 100% de cobertura documental: cabeceras `//!` en todos los `.rs`, fichas de los 16 módulos en `docs/04-componentes/`, 0 documentos `.md` huérfanos y 0 enlaces rotos. | `python3 scripts/dev/check_docs.py [--strict]` |
| `scripts/dev/check_result_pattern.py` | Audita el cumplimiento estricto del patrón `Result` (0 `unwrap`/`expect`/`panic` fuera de tests, paridad 1:1 entre `ErrorCode` y `error_catalog.toml`). | `python3 scripts/dev/check_result_pattern.py --strict` |
| `scripts/dev/result_pattern_allowlist.json` | Lista blanca justificada de invariantes estáticas permitidas por el auditor del patrón `Result`. | Leído por `check_result_pattern.py` |
| `scripts/dev/result_pattern_baseline.json` | Línea base histórica (en 0 violaciones) del patrón `Result`. | Leído por `check_result_pattern.py` |
| `scripts/dev/check_clean_code.py` | Audita SOLID (`srp_file_lines`, `dip_layer_violation`, `isp_fat_trait`), DRY y Clean Code por archivo en Rust y Python. | `python3 scripts/dev/check_clean_code.py [--strict]` |
| `scripts/dev/clean_code_baseline.json` | Línea base de no-regresión por archivo para el ratchet de `check_clean_code.py`. | Leído por `check_clean_code.py` |
| `scripts/dev/docs_header_allowlist.json` | Lista blanca (vacía: 0 exenciones) del auditor de cabeceras `check_docs.py`. | Leído por `check_docs.py` |
| `scripts/dev/hooks/pre-commit` | Hook de Git que ejecuta `cargo fmt --check`, `gen_reference.py --check`, `check_result_pattern.py --strict`, `check_clean_code.py` y el guardián de `.gitignore`. | Instalado vía `make hooks` |

### 3.2 Utilidades de Desarrollo y Semillas (`scripts/dev/`)

| Script | Propósito |
| :--- | :--- |
| `scripts/dev/query_client.py` | Cliente interactivo de línea de comandos para enviar consultas gRPC al motor local. |
| `scripts/dev/fts_tool.py` | Utilidad de inspección y prueba de búsquedas por trigramas y similitud difusa. |
| `scripts/dev/seed_base.py` | Semilla base de tenant, roles, grupos y usuarios para pruebas locales. |
| `scripts/dev/seed_locations.py` / `seed_assets.py` / `seed_work_orders.py` / `seed_meter_reading.py` | Generadores de datos sintéticos para ubicaciones ISA-95, activos, órdenes de trabajo y lecturas IoT. |
| `scripts/dev/seed_quotas.py` / `seed_system_quotas.py` | Inicialización de cuotas (`domain_quota`) por tenant en entorno de desarrollo. |
| `scripts/dev/clean_locations.py` / `clean_system_quotas.py` | Scripts de limpieza de datos de prueba en entornos locales. |
| `scripts/dev/debug_analytics_error.py` / `debug_created_at.py` / `diagnose_db.py` / `shift_timestamps.py` | Herramientas de diagnóstico puntual y ajuste de marcas temporales en datasets de prueba. |

### 3.3 Herramientas Operativas y de Plataforma (`scripts/ops/`, `scripts/common/`, `scripts/proto/`)

| Script / Directorio | Propósito |
| :--- | :--- |
| `scripts/ops/bootstrap_github_oidc.py` | Configura de forma idempotente el proveedor OIDC de GitHub Actions en AWS IAM, el rol `metri-engine-github-deploy` de mínimo privilegio y el bucket de artefactos de despliegue. |
| `scripts/ops/bootstrap_production.py` | Inicialización de entidades maestras (`system` tenant, roles base y cuotas iniciales) en un entorno nuevo. |
| `scripts/ops/seed_quotas_production.py` / `seed_entity_quotas.py` | Aprovisionamiento y actualización de cuotas operativas por tenant. |
| `scripts/ops/verify_auth.py` / `check_production_data.py` / `list_production_users.py` / `query_system_assets.py` | Utilidades de verificación de autenticación HMAC/Cedar e inspección de estado en entornos remotos. |
| `scripts/ops/invalidate_lambda_caches.py` | Utilidad operativa para forzar recarga de estado en instancias calientes de AWS Lambda. |
| `scripts/ops/create_system_locations.py` / `ingest_production.py` / `dump_unique_ap.py` / `purge_production.py` / `purge_tenant_scan.py` / `reingesta_iceberg_failed.py` / `scan_production_ddb.py` / `sync_kinesis_s3.py` | Scripts operativos de administración y migración histórica de datos. |
| `scripts/common/{auth.py,grpc_client.py}` | Librería compartida de firma de tokens HMAC y cliente gRPC para todos los scripts Python. |
| `scripts/proto/{metri_pb2.py,metri_pb2_grpc.py}` | Stubs Python generados desde `proto/metri.proto` para los scripts de prueba y operación. |
| `scripts/local_suite/grpc_web_client.py` | Cliente auxiliar para ejecución de suites locales sobre HTTP/gRPC-Web. |
| `scripts/{assign_user_email.py,inspect_system_roles.py,list_python_scripts.py,purge_extra_roles.py,purge_system_roles.py,reassign_master_email.py}` | Utilidades administrativas de mantenimiento de usuarios y roles del tenant maestro. |
