# Índice Maestro de Documentación — `metri-engine`

> **Cobertura Documental del 100 % Verificada por CI (`scripts/dev/check_docs.py --strict` y `missing_docs = "deny"`).**
> Toda la documentación de `metri-engine` está estructurada bajo una metodología híbrida que combina **Diátaxis** (taxonomía en 5 áreas), **arc42** (arquitectura del sistema), **Modelo C4** (vistas de Contexto C1, Contenedores C2, Componentes C3 y Código C4), **MADR** (decisiones arquitectónicas inmutables) y **Docs-as-Code / RFC 1574** (generación automática SSOT y validación en compilación).

---

## 1. Inicio Rápido y Tutoriales (`docs/01-inicio/` · *Diátaxis: Tutorials*)

| Documento | Propósito y Alcance |
| :--- | :--- |
| [01-quickstart-local.md](01-inicio/01-quickstart-local.md) | Requisitos de herramientas, levantamiento del motor en memoria (`.mbf` stub), exploración con `grpcurl` y tabla completa de variables de entorno. |
| [02-recorrido-lectura-y-escritura.md](01-inicio/02-recorrido-lectura-y-escritura.md) | Recorrido guiado paso a paso de una transacción ACID (`Transact` / `CompositeTransact`) y de una consulta analítica (`Query`) a través de los 16 módulos. |

---

## 2. Guías Operativas y de Ingeniería (`docs/02-guias/` · *Diátaxis: How-To Guides*)

| Documento | Propósito y Alcance |
| :--- | :--- |
| [01-desarrollo-calidad-y-pruebas.md](02-guias/01-desarrollo-calidad-y-pruebas.md) | Flujo de trabajo diario, comandos del `Makefile`, ejecución de pruebas y cumplimiento de los guardianes de arquitectura, Clean Code, patrón `Result` y documentación. |
| [02-modelado-entidades-codice.md](02-guias/02-modelado-entidades-codice.md) | Guía completa para definir y evolucionar los 60 modelos JSON en `config/models/`: tipos, patrón monetario en céntimos (`*_cents`), banderas, secuencias, constraints, reglas EDA y procedimientos. |
| [03-despliegue-aws-y-ci-cd.md](02-guias/03-despliegue-aws-y-ci-cd.md) | Pipelines de GitHub Actions (`rust.yml`, `deploy-production.yml`), federación OIDC sin llaves estáticas, recursos del stack SAM (`template.yaml`) y procedimiento de rollback. |
| [04-operacion-scripts-y-observabilidad.md](02-guias/04-operacion-scripts-y-observabilidad.md) | Operación con scripts de `scripts/dev/` y `scripts/ops/`, trazas distribuidas OpenTelemetry (`OTLP`), logs estructurados en CloudWatch y telemetría forense (`sherlog`). |

---

## 3. Arquitectura General del Sistema (`docs/03-arquitectura/` · *arc42 + Modelo C4 + MADR*)

| Documento | Propósito y Alcance |
| :--- | :--- |
| [01-vision-general-y-contexto-c4.md](03-arquitectura/01-vision-general-y-contexto-c4.md) | Visión arquitectónica bajo plantilla **arc42** con diagramas **Modelo C4** de Nivel C1 (Contexto), Nivel C2 (Contenedores Serverless) y Nivel C3 (Mapa de los 16 Módulos). |
| [02-motor-binario-mbf-especificacion.md](03-arquitectura/02-motor-binario-mbf-especificacion.md) | Especificación física del motor HTAP **Metri Vortex (`.mbf`)**: cabecera alineada a 64 bytes, manifiesto `HOT_HEAD.mbf`, control MVCC/CAS y las 8 zonas especializadas. |
| [03-algoritmos-y-fundamentos-matematicos.md](03-arquitectura/03-algoritmos-y-fundamentos-matematicos.md) | Formulación matemática y complejidad algorítmica de Welford/Chan, momentos de Pébay, cuantiles KLL, Chimp128, grafos CSR con Euler `[tin, tout]`, Fenwick `FixedDecimal128` y búsqueda híbrida. |
| [04-seguridad-zero-trust-y-multitenancy.md](03-arquitectura/04-seguridad-zero-trust-y-multitenancy.md) | Defensa en profundidad: autenticación HMAC *constant-time*, `TenantGuard`, autorización ABAC con Cedar Policy 3, censura del tenant maestro y sandbox de fórmulas. |
| [05-calidad-clean-code-y-patron-result.md](03-arquitectura/05-calidad-clean-code-y-patron-result.md) | Arquitectura Hexagonal (DIP/ISP), invariantes SOLID/DRY/Clean Code (`architecture_solid_guard.rs`) y cerradura estricta del patrón `Result` (cero `unwrap`/`unsafe`). |
| [06-infraestructura-serverless-arm64.md](03-arquitectura/06-infraestructura-serverless-arm64.md) | Topología AWS Serverless: Lambda ARM64 Graviton (`1769 MB`), almacenamiento efímero NVMe `/tmp` (`512 MB`), protocolo S3 Conditional Writes, CloudFront, WAFv2, KMS y SQS FIFO. |

### 3.1 Registro de Decisiones de Arquitectura Vigentes (`docs/03-arquitectura/adr/`)

| ADR | Decisión Arquitectónica Vigente |
| :--- | :--- |
| [ADR-001](03-arquitectura/adr/ADR-001-motor-htap-binario-mbf-sobre-s3.md) | Motor binario unificado HTAP **Metri Vortex (`.mbf`)** sobre Amazon S3 y caché NVMe `/tmp`. |
| [ADR-002](03-arquitectura/adr/ADR-002-modelo-datoms-inmutables-y-mvcc.md) | Modelo de hechos atómicos inmutables (Datoms EAVT/AEVT/AVET/VAET) y concurrencia MVCC sin `UPDATE` in-place. |
| [ADR-003](03-arquitectura/adr/ADR-003-seguridad-zero-trust-cedar-y-hmac.md) | Seguridad Zero-Trust *fail-closed* con Cedar Policy 3, HMAC y censura del tenant maestro (`system`). |
| [ADR-004](03-arquitectura/adr/ADR-004-contratos-tipados-grpc-flatbuffers-y-deltas.md) | Contratos tipados de extremo a extremo: Protobuf (gRPC), FlatBuffers zero-copy (AST IR) y sobre canónico de eventos/deltas. |
| [ADR-005](03-arquitectura/adr/ADR-005-registro-ssot-codice-y-validacion-arranque.md) | Códice (`config/models/*.json`) como única fuente de verdad (SSOT) compilada *fail-fast* en el arranque. |
| [ADR-006](03-arquitectura/adr/ADR-006-catalogo-canonico-errores-y-patron-result.md) | Catálogo canónico de errores (`error_catalog.toml`) en paridad 1:1 con `ErrorCode` y patrón `Result` estricto. |
| [ADR-007](03-arquitectura/adr/ADR-007-identidad-entidades-ulid-y-secuencias.md) | Identidad híbrida de entidades: ULIDs monotónicos para claves técnicas y secuencias ACID / Base36 para códigos humanos. |

---

## 4. Componentes Detallados (`docs/04-componentes/` · *Modelo C4 Nivel C3/C4*)

Cada ficha documenta el 100 % de los submódulos, archivos `.rs`, contratos públicos, diagramas de flujo, invariantes, errores y pruebas de su respectivo módulo en `src/`:

| Ficha de Componente | Módulo en `src/` | Archivos `.rs` | Síntesis de Responsabilidad |
| :--- | :--- | :---: | :--- |
| [01-mbf.md](04-componentes/01-mbf.md) | `src/mbf/` | 13 | Formato binario `.mbf` v1 alineado a 64B, `MbfStorageClient`, manifiesto `tx` y las 8 zonas físicas (`pax_mvcc`, `csr_graph`, `iot_timeseries`, `financial_ledger`, `bi_synopses`, `hybrid_search`, `fuzzy_math_catalog`, `cross_covariance`). |
| [02-eav.md](04-componentes/02-eav.md) | `src/eav/` | 21 | Almacenamiento de datoms inmutables sobre `.mbf`: codificación de tipos (`types`), escritor transaccional ACID (`writer`), lector (`reader`) y búsqueda por trigramas (`fts`). |
| [03-codice.md](04-componentes/03-codice.md) | `src/codice/` | 14 | Compilador e índice `O(1)` de los 60 esquemas JSON (`registry`), perfiles matemáticos/difusos (`profiles`), coerción, validación y generación de secuencias/Base36. |
| [04-domain.md](04-componentes/04-domain.md) | `src/domain/` | 19 | Tipos y puertos fundamentales sin dependencias de infraestructura: `DomainError`, `ErrorCatalog`, `events` (`envelope`, `delta`), `protocols`, `pipeline` (`Railway`), `config` y `audit`. |
| [05-application.md](04-componentes/05-application.md) | `src/application/` | 2 | Puertos de aplicación (`ports.rs`) diseñados bajo Segregación de Interfaces (ISP) e Inversión de Dependencias (DIP). |
| [06-janus.md](04-componentes/06-janus.md) | `src/janus/` | 34 | Camino de lectura (*Read Path*): normalizadores por operación, validador, selector de planes, compilador AST IR (FlatBuffers), cláusulas ABAC, agregador, multiseries y caché. |
| [07-janus-router.md](04-componentes/07-janus-router.md) | `src/janus_router/` | 6 | Camino de escritura (*Write Path*): `JanusRouter`, canal transaccional `OltpChannel`, proyector de sagas declarativas (`saga.rs`) y generador ULID monotónico (`ulid.rs`). |
| [08-aegis.md](04-componentes/08-aegis.md) | `src/aegis/` | 42 | Ejecutores analíticos y transaccionales sobre AST IR (`mbf`, `oltp`) y motor de fórmulas matemáticas sandboxeado (`formula`), más `pagination`, `label_template` y `temporal_bridge`. |
| [09-cedar.md](04-componentes/09-cedar.md) | `src/cedar/` | 26 | Autorización ABAC Zero-Trust: verificación HMAC (`authn`), pipeline de 4 pasos, grafo de principal, evaluadores por tipo de acción, motor Cedar y cachés con invalidación. |
| [10-quota.md](04-componentes/10-quota.md) | `src/quota/` | 12 | Control atómico de cuotas por tenant: `QuotaResolver`, libro mayor condicional `QuotaLedger`, tickets de reserva (`reservations`), superposición de lectura (`projection`) y `sweeper`. |
| [11-iop.md](04-componentes/11-iop.md) | `src/iop/` | 14 | Pipeline de orquestación de ingesta por pasos (`CedarStep` $\rightarrow$ `QuotaStep` $\rightarrow$ `JanusStep`), traducción de errores (`error_response`) y bus forense de fallos (`sherlog`). |
| [12-eda.md](04-componentes/12-eda.md) | `src/eda/` | 3 | Arquitectura orientada a eventos: drenaje del *Transactional Outbox*, evaluación de reglas de ruteo y recuperación de eventos huérfanos en `moira.rs`. |
| [13-temporal.md](04-componentes/13-temporal.md) | `src/temporal/` | 5 | Primitivas temporales puras: aritmética de calendario con zonas horarias (`core.rs`), resolución de 29 ventanas temporales (`time_frame.rs`), comparaciones (`comparison.rs`) y adaptadores. |
| [14-grpc.md](04-componentes/14-grpc.md) | `src/grpc/` | 25 | Transporte gRPC/Tonic-Web: arranque (`bootstrap`, `server`), interceptores de seguridad, 4 servicios gRPC, traductor Protobuf y los 11 módulos de `handlers/`. |
| [15-infrastructure.md](04-componentes/15-infrastructure.md) | `src/infrastructure/` | 9 | Adaptadores AWS SDK (`s3_export`, `sqs`, `eventbridge`, `domain_event_bus`), aislamiento `TenantGuard`, almacén de sesiones HMAC (`session_store`) e interceptor de auditoría (`audit`). |
| [16-otel.md](04-componentes/16-otel.md) | `src/otel/` | 2 | Configuración e inicialización del exportador OTLP de OpenTelemetry y trazas distribuidas (`tracer.rs`). |

---

## 5. Referencia Canónica (`docs/05-referencia/` · *Diátaxis: Reference*)

| Documento | Fuente de Verdad (SSOT) | Propósito y Alcance |
| :--- | :--- | :--- |
| [api-grpc.md](05-referencia/api-grpc.md) | `proto/metri.proto` + `proto/eda.proto` | **[Autogenerado]** Catálogo completo de servicios, RPCs, mensajes y enumeraciones gRPC. |
| [codigos-error.md](05-referencia/codigos-error.md) | `config/errors/error_catalog.toml` | **[Autogenerado]** Catálogo normativo de códigos de error por familia, etapa, HTTP/gRPC status y reintentabilidad. |
| [modelos-codice.md](05-referencia/modelos-codice.md) | `config/models/*.json` | **[Autogenerado]** Tabla completa de las 60 entidades del Códice con sus banderas, constraints, reglas EDA y atributos. |
| [diagrama-modelo-datos.md](05-referencia/diagrama-modelo-datos.md) | `config/models/*.json` | Diagramas Mermaid de relaciones entre dominios (CMMS, Planificación, Plantillas, Activos ISA-95, IoT, Inventario/Contabilidad, Identidad y Plataforma). |
| [politicas-prompts-y-localizacion.md](05-referencia/politicas-prompts-y-localizacion.md) | `config/{policies,prompts,locales}/` | Especificación de las reglas Cedar (`P00`–`P07`), clasificador y plantillas de prompts de **Metri Q Assistant** y diccionarios i18n (`es`/`en`). |
| [suites-pruebas-y-scripts.md](05-referencia/suites-pruebas-y-scripts.md) | `tests/` + `scripts/` | Inventario del 100 % de las suites de verificación en Rust/Python y de los scripts de CI, desarrollo y operaciones. |
| [glosario.md](05-referencia/glosario.md) | Todo el repositorio | Definiciones precisas de todos los conceptos técnicos, siglas y estructuras del motor. |

---

## 6. Documentación en Código (`rustdoc`)

Además de esta suite Markdown, el 100 % de los archivos `.rs` de `src/` posee cabecera de módulo (`//!`) y el 100 % de los ítems públicos cuenta con comentarios de documentación (`///`), verificado en compilación mediante `missing_docs = "deny"` y `broken_intra_doc_links = "deny"`:

```bash
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --lib   # Genera y valida la referencia API Rust
cargo test --doc                                       # Ejecuta los doctests del crate
python3 scripts/docs/gen_reference.py --check          # Verifica sincronía de docs/05-referencia/
python3 scripts/dev/check_docs.py --strict             # Audita cobertura 100% en src/ y docs/
```
