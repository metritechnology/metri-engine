# Infrastructure — Outbound Adapters

> **English summary:** Outbound adapters: AWS SDK clients (DynamoDB, S3, Athena, Firehose, SQS, EventBridge, Glue), the tenant isolation guard, the HMAC session store, the domain event bus, the local S3 query engine (real data without Athena for local dev) and the OLAP infrastructure seeder. Each implements an application/domain port.

## Purpose

Aislar el SDK de AWS detrás de puertos: el dominio no sabe que existe DynamoDB; esta capa traduce contratos a llamadas concretas y provee emulaciones locales para desarrollar sin nube.

## Responsibilities / non-responsibilities

**Hace:** clientes AWS (`dynamodb`, `athena`, `kinesis`, `sqs`, `eventbridge`, `glue`, `s3_export`); `TenantGuard` (aislamiento en la capa EAV); `HmacTokenStore` (`session_store`); bus de eventos de dominio (EventBridge real + outbox); motor local de query S3 (`local_s3_query_engine`); seeder OLAP puro (`seeder`); auditoría (`audit`).
**No hace:** decidir nada de negocio; los interruptores `*_MODE` (stub/real) son configuración, no política.

## Internal flow

```text
puerto (application/domain) ◀── implementación ── adaptador AWS
   stub/real por servicio: ATHENA_MODE · KINESIS_MODE · EVENTBRIDGE_MODE · S3_MODE · SQS_MODE
local: Aegis SQL ─▶ sql_parse ─▶ pipeline (memoria) ─▶ lake_reader (S3 real, sin Athena)
```

## Invariants

1. **Aislamiento verificable** — `TenantGuard` valida que todo I/O lleve `tenant_id` en el PK; replica los guards del stack anterior.
2. **El engine levanta sin AWS** — todos los adaptadores tienen modo stub; el local `s3_query_engine` lee los datos reales escritos por Firehose.
3. **Puertos, no concreciones** — cada adaptador implementa un trait de `application::ports` o `domain::protocols`; los tests usan fakes.

## Entry points

- [`infrastructure::dynamodb`] — el cliente base.
- [`infrastructure::tenant_guard`] — aislamiento.
- [`infrastructure::domain_event_bus`] — publicadores de eventos.
- [`infrastructure::local_s3_query_engine`] — OLAP local.

## Errors

`INFRA_*` del catálogo (`INFRA_GLUE_001`, `INFRA_FIREHOSE_001`, `INFRA_CATALOG_001`, …).

## Decisions

- LocalStack no cubre Athena: por eso existe el motor local de query S3 (parse del SQL de Aegis + pipeline en memoria sobre JSON-lines del lake).

## Known risks / TODO

- Los adaptadores stub deben seguir la misma superficie que los reales; los tests de fake vs real viven por módulo consumidor.
