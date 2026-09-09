# EAV Storage Engine

> **English summary:** Immutable EAV storage engine on a DynamoDB single table. Datoms are never updated or deleted (retract + assert); transactions are ACID via `TransactWriteItems` with optimistic uniqueness claims resolved inside the transaction. Every key carries `tenant_id`.

## Purpose

Almacenar el estado de todas las entidades como hechos atómicos (datoms) inmutables sobre una sola tabla DynamoDB, con cuatro índices que cubren todos los patrones de acceso del producto. Es el corazón OLTP del motor: todo lo demás compila hacia él o lee de él.

## Responsibilities / non-responsibilities

**Hace:** datoms y su encoding binario; transacción ACID; los 4 índices; pull y query; paginación por cursor; FTS por trigramas; jerarquías; catálogo de atributos; sharding de escritura; telemetría IoT.
**No hace:** autorización (cedar), validación de esquemas (codice), compilación de queries (aegis/janus), publicación de eventos externos (eda — solo escribe la fila outbox).

## Internal flow

```text
              transact (ACID)                     reader
payload ─▶ datom_plan ─▶ chunker ─▶ TransactWriteItems   pull ──▶ estado de entidad
                │                        │               query ─▶ 5 planes físicos
                ├─ enricher/system_attrs │
                ├─ constraints (claims)  ├─ DESPUÉS del commit:
                └─ outbox (evento)       │   fts_dispatch (BatchWrite)
                                         └─ cache_policy (invalidación)
```

## Invariants

1. **Inmutabilidad** — un datom persistido jamás se actualiza ni borra; retract + assert (los tests de writer fijan la semántica).
2. **ACID** — la transacción completa comete o no comete; los claims de unicidad (`constraints/unique_claim.rs`) resuelven colisiones concurrentes: exactamente una tx comete, la otra recibe `UniqueClaim`.
3. **Aislamiento por tenant** — `tenant_id` en cada PK; `TenantGuard` lo valida en la frontera (ver `infrastructure`).
4. **SK binario ordenable** — el encoding (`types/encoding.rs`) garantiza orden por `attr_id`, `tx` y `op`; es lo que permite as-of y última-versión en una Query.
5. **El evento nace con la mutación** — la fila `outbox_event` se escribe en la misma transacción (`writer/outbox.rs`); sin transacción no hay evento.
6. **FTS degradado a propósito** — el índice de trigramas se despacha después del commit (`fts_dispatch.rs`): política documentada, no defecto.

## Entry points

- [`eav::writer::transact`] — escritura transaccional (el camino caliente).
- [`eav::reader::pull::EavReader`] — estado e historial de una entidad.
- [`eav::reader::query::EavQueryExecutor`] — los 5 planes físicos (`PointLookup`, `AvetSingleFilter`, …).
- [`eav::cursor::CompositeCursor`] — paginación estable multi-índice.
- [`eav::types::datom::{Datom, DatomValue}`] — la unidad atómica.

## Errors

`EAV_*` del catálogo canónico (`config/errors/error_catalog.toml`): violaciones de unicidad, valores fuera de tipo, límites de transacción. Ver `docs/reference/codigos-error.md`.

## Decisions

- ADR-001 — motor EAV sobre DynamoDB single-table.
- ADR-006 — identidad de entidades (ULID + códigos Base36).

## Known risks / TODO

- La caché de lectura (`EAV_CACHE`/`AEVT_SCAN_CACHE`) depende de la invalidación correcta de `writer/cache_policy.rs` — se amplió a entidades proyectadas de saga; vigilar nuevos caminos de escritura.
