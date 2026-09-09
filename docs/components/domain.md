# Domain Contracts

> **English summary:** Dependency-free core types: the canonical error monad (`DomainError`/`ErrorCode` with a 1:1 TOML catalog), the domain event contract (envelope + delta, producer side), the domain ports (traits), and railway helpers. Everything else in the crate speaks in these types.

## Purpose

Tipos y contratos fundamentales sin ninguna dependencia de infraestructura: todos los módulos comunican errores, eventos y resultados en los tipos de este módulo.

## Responsibilities / non-responsibilities

**Hace:** `DomainError`/`ErrorCode` (catálogo 1:1 con TOML); loader del catálogo; sobre y delta de eventos (lado productor, espejo de metri-contracts); puertos de dominio (`ISessionStore`, `IQueryEngine`, `IStreamWriter`, `IEventBus`, …); helpers Railway; configuración del engine leída una vez; contrato del audit log.
**No hace:** I/O, políticas de negocio, validación de esquemas.

## Internal flow

```text
error_catalog.toml ──ErrorCatalog::load──▶ OnceLock global
  operación fallible ──▶ DomainError { code, context } ──▶ Status gRPC (único From, R6)
  mutación ──▶ Delta + Envelope ──▶ outbox_event ──▶ eda/moira ──▶ Hub
```

## Invariants

1. **Catálogo 1:1** — cada variante `ErrorCode` tiene exactamente una entrada canónica en `config/errors/error_catalog.toml` (72 ↔ 88 con `reserved`/`deprecated`); el auditor del patrón Result lo bloquea en CI.
2. **Un solo puente a gRPC** — `From<DomainError> for tonic::Status` es único (regla R6): consulta el catálogo con el `canonical_code`.
3. **Los nombres de campos del sobre SON el contrato** — renombrar uno rompe el parseo del Hub en producción; `golden_test.rs` compara contra fixtures dorados de metri-contracts por igualdad exacta.
4. **Fail-closed del sobre** — `from_attributes` rechaza sobres sin `job_id`/`tenant_id`/`ulid`/trigger: reintentar no lo arregla, es Poison.

## Entry points

- [`domain::DomainError`] y [`domain::ErrorCode`].
- [`domain::ErrorCatalog::load`] + [`domain::error_catalog`].
- [`domain::events::envelope`] / [`domain::events::delta`].
- [`domain::protocols`] — los puertos; [`domain::DomainResult`].
- [`domain::pipeline::Railway`].

## Errors

Este módulo los define: códigos canónicos por familia (`JANUS_400`, `EAV_*`, `QTA_*`, `ABAC_*`, `INFRA_*`, …), semántica HTTP/gRPC y `is_retryable` sincronizados con el TOML.

## Decisions

- ADR-005 — códigos de error canónicos.
- ADR-003 — contrato único (el sobre/delta espejo de metri-contracts).
- PLAN_PATRON_RESULT.md — adopción total del patrón Result (cumplido).

## Known risks / TODO

- Mantener la sincronía `is_retryable()` ↔ TOML al añadir variantes (el auditor vigila la parte estructural; la semántica es de revisión).
