# Janus Router — Write Path

> **English summary:** The write path: routes mutations to the ACID OLTP channel (EAV) or the columnar OLAP channel (Firehose), projects declarative sagas into `scheduled_job`, handles S3/Hive partitioning and generates monotonic ULIDs. Invoked exclusively by the IOP after Cedar and Quota.

## Purpose

Ser la única puerta de escritura del motor: decidir canal, ejecutar la escritura y orquestar las proyecciones derivadas (sagas) con garantías de idempotencia.

## Responsibilities / non-responsibilities

**Hace:** ruteo OLTP/OLAP; escritura ACID vía EAV (`oltp_channel`); escritura columnar vía Firehose (`olap_channel`); particionado Hive (`partition`); sagas declarativas (`saga`); ULIDs monotónicos (`ulid`).
**No hace:** autorizar ni cobrar (eso ya ocurrió en el IOP), normalizar requests (janus).

## Internal flow

```text
IOP (Cedar ✓, Quota ✓) ─▶ JanusRouter (6 pasos)
     ├─ OLTP ─▶ oltp_channel ─▶ eav::writer::transact (ACID)
     └─ OLAP ─▶ olap_channel ─▶ Firehose ─▶ S3 (Parquet/Iceberg)
CREATE con shadow_sagas_mapping ─▶ saga ─▶ scheduled_job (proyección)
```

## Invariants

1. **Invocado solo por el IOP** — nunca hay escritura sin autorización y cuota previas.
2. **La saga es declarativa** — la proyección a `scheduled_job` se describe en el modelo (`shadow_sagas_mapping`), no en código de negocio.
3. **ULIDs monotónicos** — ordenables en el mismo milisegundo; base de la identidad de entidades (ADR-006).
4. **Particionado Hive puro** — `partition` es dominio puro sin I/O; el path S3 se evalúa en memoria.

## Entry points

- [`janus_router::router::JanusRouter`] — la puerta.
- [`janus_router::oltp_channel`] / [`janus_router::olap_channel`].
- [`janus_router::saga::SagaBuilder`].

## Errors

Errores de escritura EAV (`EAV_*`), validación de Códice (`COD_*`) y fallos de canal OLAP.

## Decisions

- Canal OLAP columnar nativo (sin Raw Zone genérica): cada entidad OLAP tiene su stream Firehose dedicado.

## Known risks / TODO

- La semántica de la saga está fijada por `janus_router/tests/saga_tests.rs` — ampliarla requiere actualizar los tests primero.
