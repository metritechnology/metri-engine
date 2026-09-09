# IOP — Ingest Pipeline

> **English summary:** The step-based ingest pipeline: Cedar authorization, then quota, then Janus routing — orchestrated by a railway engine that short-circuits on the first error. Each step has exactly one responsibility; errors come out as forensic gRPC responses.

## Purpose

Garantizar que toda escritura pase por autorización y cuota en orden, con un solo punto que coordina los pasos y traduce fallos a respuestas forenses (con trace id).

## Responsibilities / non-responsibilities

**Hace:** el motor Railway (`pipeline`); el orquestador raíz (`core`); los tres pasos (Cedar, Quota, Janus); el DTO forense de errores; Sherlog (reporte de fallas al canal EDA).
**No hace:** lógica de autorización, cálculo de cuotas ni ruteo — cada paso delega en su módulo dueño.

## Internal flow

```text
run(pasos):  cedar_step ─▶ quota_step ─▶ janus_step
             corto-circuito al primer Err; los pasos restantes no se ejecutan
error ─▶ error_response (sanitiza contexto + trace_id OTel) ─▶ Status gRPC
falta ─▶ sherlog ─▶ EventBridge (fault pipeline)
```

## Invariants

1. **Orden no negociable** — Cedar antes de Quota antes de Janus; nadie escribe sin autorización y cuota.
2. **Cortocircuito puro** — `pipeline::run` es función pura, cero I/O, cero estado; propaga el primer `Err` sin ejecutar lo demás.
3. **Errores forenses, no filtrados** — el DTO conserva lo necesario para depurar sin exfiltrar datos sensibles.

## Entry points

- [`iop::pipeline::run`] — el motor.
- [`iop::core::IopOrchestrator`] — composición real de pasos.
- [`iop::sherlog`] — fallas hacia EDA.

## Errors

Reexporta los errores canónicos de cada paso (`ABAC_*`, `QTA_*`, `JANUS_*`); sin errores propios de pipeline.

## Decisions

- Pipeline como `reduce` Railway — port directo del stack anterior, verificado por `iop/tests/pipeline_tests.rs`.

## Known risks / TODO

- Añadir un cuarto paso (p. ej. auditoría síncrona) exige tocar el orden en `core` — mantener la lista explícita.
