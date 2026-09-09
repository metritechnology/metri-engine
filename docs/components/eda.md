# EDA — Event-Driven Architecture

> **English summary:** Completes the transactional-outbox cycle: the `outbox_event` row is born inside the EAV transaction (in `eav::writer::outbox`), and Moira drains it with retry semantics toward EventBridge/SQS. Fault reporting (Sherlog) rides the same channel.

## Purpose

Desacoplar procesos externos (metri-schedulers/Hub, el event router en Go) del write path sin arriesgar pérdida de eventos: outbox transaccional para la salida, colas FIFO para el transporte.

## Responsibilities / non-responsibilities

**Hace:** el emisor Moira (despacho asíncrono); la semántica de reintento y el ruteo a EventBridge/SQS; el reporte de fallas ( Sherlog vive en `iop`, su transporte aquí).
**No hace:** generar el evento (nace en la transacción EAV), decidir el contenido del payload (contrato en `domain::events`).

## Internal flow

```text
transacción EAV ─▶ outbox_event (misma tx, eav::writer::outbox)
                        │ drenaje
                        ▼
        moira ── reintento ──▶ EventBridge / SQS FIFO ──▶ Hub / event router (Go)
```

## Invariants

1. **Outbox, no publicación directa** — el evento no puede perderse: nace en la misma transacción que la mutación que lo causa.
2. **El id del evento ES el ULID de la mutación** — llave de orden del ledger del Hub.
3. **Payload idéntico al delta de metri-contracts** — aplanado y compartido por todos los consumidores; los fixtures dorados lo candan.

## Entry points

- [`eda::moira`] — el emisor.
- [`crate::eav::writer::outbox`] — el productor de la fila (ver ficha EAV).

## Errors

Fallos de publicación se reintentan; tras agotar, el evento queda trazable (DLQ del stack SAM).

## Decisions

- SQS FIFO + EventBridge según consumidor; DLQ dedicada en el stack SAM.

## Known risks / TODO

- El drenaje depende de metri-schedulers o del bus de `infrastructure::domain_event_bus`; vigilar lag del outbox en producción.
