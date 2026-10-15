# Metri Vortex (`.mbf`) — Unified HTAP Binary Engine Documentation Suite

> **English summary:** This directory contains the complete architectural, mathematical, binary-format, infrastructure, testing, and migration specifications for **Metri Vortex (`.mbf` — Metri Binary Format)**. Metri Vortex replaces both the DynamoDB OLTP EAV engine and the Athena/Firehose/Iceberg OLAP SQL pipeline with a single zero-copy, SIMD-accelerated binary engine built natively on AWS S3 and AWS Lambda ARM64 (Graviton).

---

## Propósito de esta Carpeta

Esta carpeta documenta de extremo a extremo el salto tecnológico de `metri-engine`: la transición desde una arquitectura dual (DynamoDB para OLTP + Kinesis Firehose/Glue/Athena SQL para OLAP) hacia un **Motor Único HTAP (Hybrid Transactional/Analytical Processing)** basado exclusivamente en el formato de archivo binario propio **`.mbf` (Metri Binary Format)** sobre **AWS S3**.

Cada documento cubre una dimensión crítica del diseño para garantizar una implementación determinista, auditable y alineada con las reglas de calidad del repositorio ([`PLAN_REFACTORIZACION.md`](../architecture/PLAN_REFACTORIZACION.md) y [`PLAN_DOCUMENTACION.md`](../architecture/PLAN_DOCUMENTACION.md)).

---

## Mapa Navegable de Documentos (`docs/mbf/`)

| # | Documento | Alcance Técnico Principal |
| :--- | :--- | :--- |
| **01** | [01_ARCHITECTURE_AND_DESIGN.md](01_ARCHITECTURE_AND_DESIGN.md) | **Arquitectura y Diseño General HTAP:** Visión global del motor unificado, flujos de lectura/escritura gRPC, integración de dominios (IoT, CMMS ISA-95, Financiero/Contable, BI) y benchmarking competitivo frente a **IBM Maximo, Oracle EAM, SAP S/4HANA y CMMS SaaS**. |
| **02** | [02_ARCHITECTURAL_PRINCIPLES_AND_CLEAN_CODE.md](02_ARCHITECTURAL_PRINCIPLES_AND_CLEAN_CODE.md) | **Principios Arquitectónicos, SOLID, DRY y Clean Code:** Arquitectura Hexagonal, segregación de puertos en `src/domain/protocols.rs`, derivación DRY del layout binario desde los 60 modelos del Códice, cero `unwrap()`, cero `unsafe` y estándares `rustdoc`. |
| **03** | [03_MBF_BINARY_FORMAT_SPEC.md](03_MBF_BINARY_FORMAT_SPEC.md) | **Especificación Física del Formato `.mbf` v1:** Layout a nivel de byte alineado a 64 bytes (`MbfHeader`, `HotHeadManifest`, `.mbf.idx`, Zona 1 PAX+MVCC, Zona 2 Grafo CSR+ISA-95, Zona 3 IoT Chimp128, Zona 4 Contabilidad Exacta `FixedDecimal128` FIFO/PMP/FX/Merkle y Zona 5 Sinopsis BI). |
| **04** | [04_ALGORITHMS_AND_PAPERS.md](04_ALGORITHMS_AND_PAPERS.md) | **Algoritmos, Fórmulas Matemáticas y Papers Científicos:** Fundamentos teóricos y ecuaciones de Welford/Chan, KLL Quantile Sketches, Chimp128/Gorilla, Elias-Fano, FSST, Roaring Bitmaps, Recorrido de Euler para árboles ISA-95, Sumas Prefijas de Fenwick, Método de Hamilton e IRLS Logístico SIMD. |
| **05** | [05_SERVERLESS_LAMBDA_AND_S3_INFRASTRUCTURE.md](05_SERVERLESS_LAMBDA_AND_S3_INFRASTRUCTURE.md) | **Infraestructura AWS Serverless, Lambda ARM64 y S3:** Optimización para Graviton (`neoverse-n1`), Cold Start `< 2 ms` vía `.rodata`, binding directo `lambda_http` sobre `RESPONSE_STREAM`, uso de los 512 MB gratuitos de `/tmp` NVMe, protocolo CAS (`If-Match`/`If-None-Match`) y modelo de ahorro de costos (>92 %). |
| **06** | [06_FOLDERS_FILES_AND_MODULE_MAP.md](06_FOLDERS_FILES_AND_MODULE_MAP.md) | **Estructura de Carpetas, Archivos y Módulos:** Mapa completo del árbol de directorios en `src/mbf/`, `src/aegis/mbf/`, `src/infrastructure/`, responsabilidades por archivo y contratos públicos de Rust. |
| **07** | [07_TESTING_AND_ORACLE_STRATEGY.md](07_TESTING_AND_ORACLE_STRATEGY.md) | **Estrategia de Testing y Arnés del Oráculo:** Pruebas basadas en propiedades (`proptest`), inyección de fallos binarios, pruebas de concurrencia ACID, validación contable al centavo, Modo Sombra (`MBF_ENGINE_MODE=shadow`) y suite de 500 casos de contrato. |
| **08** | [08_IMPLEMENTATION_AND_LEGACY_DECOMMISSIONING.md](08_IMPLEMENTATION_AND_LEGACY_DECOMMISSIONING.md) | **Plan de Implementación y Eliminación del Motor OLAP/OLTP Antiguo:** Las 6 fases incrementales con puertas de salida, herramienta de migración `mbf-migrator` y protocolo de desmantelamiento seguro de DynamoDB, Kinesis Firehose, Glue, Athena y compilador SQL. |
| **09** | [09_CODICE_METADATA_AND_FUZZY_MATH_ENGINE.md](09_CODICE_METADATA_AND_FUZZY_MATH_ENGINE.md) | **Expansión de Metadatos del Códice (`config/models/`), Matemática de Orden Superior y Motor Híbrido Fuzzy/Vector:** Auditoría de los 60 modelos JSON, super-bloque `MbfHyperSynopsis512` (512B), momentos de Pébay ($M_3$ Skewness, $M_4$ Kurtosis, Jarque-Bera), confiabilidad Weibull/MTBF/MTTR, FFT/PSD IoT, matriz multivariada PCA/Mahalanobis y búsqueda multimodal de 6 capas (FST Levenshtein DFA + SymSpell + BM25F + Metaphone Fonético + RaBitQ HNSW + Uber H3). |
| **10** | [10_TEN_OUT_OF_TEN_ARCHITECTURAL_PERFECTION.md](10_TEN_OUT_OF_TEN_ARCHITECTURAL_PERFECTION.md) | **Perfección Arquitectónica (10/10):** Protocolo OLTP de alta concurrencia sin contención en S3 (`GroupCommitCoalescer` + `8-Cell Domain Sharding` + `CommutativeDeltaAutoRebase` en $< 150\ \mu\text{s}$ ante HTTP `412`), evolución de esquema *Zero-Downtime* sobre Petabytes históricos (`LazyDefaultProjection`), sellado criptográfico Multi-Tenant (`Keyed BLAKE3` + `AES-256-GCM-SIV`) y compactación por Curvas de Hilbert. |
| **11** | [11_COMPREHENSIVE_TESTING_AND_LOCAL_INFRA_PLAN.md](11_COMPREHENSIVE_TESTING_AND_LOCAL_INFRA_PLAN.md) | **Plan Maestro de Pruebas (100% Cobertura), Dataset Realista e Infraestructura Local:** Estrategia de testabilidad total (`IClock`, `FaultInjectingObjectStore`, `cargo-llvm-cov` 100%, `cargo-mutants`), generador del gemelo digital realista (*Centro Comercial Mayor Plaza Bogotá* sobre las 60 entidades), suites completas de BI/Matemática/Fuzzy Search y stack local (`LocalFsObjectStore` Zero-Docker + `docker-compose.yml` con MinIO S3, DynamoDB y Jaeger OTel). |
| **12** | [12_STEP_BY_STEP_IMPLEMENTATION_AND_LOCAL_EXECUTION_PLAN.md](12_STEP_BY_STEP_IMPLEMENTATION_AND_LOCAL_EXECUTION_PLAN.md) | **Plan de Implementación Paso a Paso y Ejecución de Pruebas en Local:** Hoja de ruta en 6 etapas atómicas para implementar `src/mbf/` y `src/aegis/mbf/`, construir el dataset realista *Mayor Plaza Bogotá* y verificar en local el 100% de las suites (`cargo test`, `check_docs.py`, `check_result_pattern.py --strict`) sin deploys ni commits. |

---

## Decisiones de Arquitectura Relacionadas

- [ADR-009: Motor Binario Unificado HTAP (`.mbf`) sobre AWS S3](../architecture/adr/ADR-009-motor-binario-unificado-mbf.md)
- [00_OVERVIEW.md — Visión General de `metri-engine`](../architecture/00_OVERVIEW.md)
- [Índice Principal de Documentación (`docs/README.md`)](../README.md)
