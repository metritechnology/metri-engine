# metri-engine — Visión de arquitectura

> **Verificado contra el código:** 2 de septiembre de 2026 · motor 100% Rust
> **Fuente única del API:** [`proto/metri.proto`](../../proto/metri.proto)

metri-engine es el motor de datos de la plataforma metri (CMMS/BI SaaS multitenant para el sector salud). Se despliega como **una única AWS Lambda ARM64** (`provided.al2023`, binario `bootstrap`) que sirve un **servidor gRPC Tonic** detrás de Function URL + CloudFront + WAF, y combina OLTP y OLAP en el mismo proceso.

## Los cuatro pilares

1. **OLTP — motor EAV inmutable propio** (`src/eav/`): single-table en DynamoDB, datoms con índices EAVT/AEVT/AVET/VAET, transacciones ACID (`TransactWriteItems`), FTS, jerarquías y cursors.
2. **OLAP — data lake** (`src/infrastructure/` + `src/aegis/sql/`): Parquet/Snappy en S3 con `tenant_id` como primera partición obligatoria, Glue Data Catalog, queries vía Athena. El compilador SQL (`aegis/sql/`, sea-query) hace la inyección imposible por construcción.
3. **Seguridad Zero-Trust** (`src/cedar/`): ABAC con Cedar Policy embebido, aislamiento por tenant fail-closed, censura del tenant maestro (`is_master_tenant` + `SystemSecurityRules`), guard HMAC fail-closed en el arranque (`domain::config::resolve_hmac_secret`).
4. **Cuotas** (`src/quota/`): ledger atómico por tenant con autoridad, reservas para consumo de IA (Bedrock) y sweeper.

## Mapa de módulos

| Módulo | Rol |
|---|---|
| `janus` | Read path: `QueryRequest` → AST IR en FlatBuffers (zero-copy), agregaciones, multi-series, FTS |
| `janus_router` | Write path: routing OLTP/OLAP, sagas, proyecciones madre-hija en una TX |
| `aegis` | Executor OLTP + compilador SQL Athena + motor de fórmulas (lexer→parser→resolver→evaluator) |
| `eav` | Motor de almacenamiento: writer ACID, reader con caché, constraints, chunker |
| `codice` | Registro SSOT de ~60 modelos JSON (`config/models/`), coerción, ULIDs |
| `cedar` | Autorización ABAC, cache de principals, reglas del sistema |
| `quota` | Cuotas atómicas (ledger, reservas, sweeper) |
| `iop` | Pipeline de ingesta por pasos: Cedar → Quota → Janus |
| `eda` | Eventos de dominio: Moira (routing), Sherlog (faults → EventBridge) |
| `temporal` | Primitivas temporales timezone-aware |
| `grpc` | Servidor, interceptors (HMAC + sesión), implementación de servicios |
| `domain` | Protocolos (traits) y errores — cero dependencia de infraestructura |
| `infrastructure` | Adaptadores AWS SDK: DynamoDB, S3, Athena, Kinesis, Firehose, SQS, EventBridge, Glue |

## Decisiones de diseño

Viven como ADRs numerados en [`adr/`](adr/). Los comentarios de código enlazan a ellos; las decisiones muertas viven en el historial de git, no en el árbol.

## Documentación

- **Referencia** (`reference/`): generada desde las fuentes únicas (proto, catálogo de errores, modelos). El CI falla si está desactualizada (`scripts/docs/gen_reference.py --check`).
- **Guías** (`guides/`): how-to operativos.
- **Plan vivo**: [`PLAN_REFACTORIZACION.md`](PLAN_REFACTORIZACION.md) — estado verificado del código y roadmap.
