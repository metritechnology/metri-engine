# Janus — Read Path

> **English summary:** The read pipeline: normalizes raw gRPC bodies per RPC shape, validates against the AST IR contract, selects the optimal EAV index plan, and compiles to a zero-copy FlatBuffers AST IR. Includes aggregation, multi-series fusion and ABAC clause injection.

## Purpose

Convertir peticiones de lectura (Query, Discovery, Explore, BulkIngest responses, MatchRoutingRules) en planes ejecutables y resultados ensamblados, sin que el resto del sistema conozca la forma cruda de los requests.

## Responsibilities / non-responsibilities

**Hace:** normalización por forma de RPC; validación de contrato del AST IR; selección de plan (`PointLookup`, `AvetSingleFilter`, …); compilación a FlatBuffers (zero-copy); cláusulas ABAC desde el CedarCtx; agregaciones y OutputCast; fusión multi-series (full outer join asintótico); post-proceso de label templates.
**No hace:** ejecutar (aegis/eav/athena), autorizar (cedar), cobrar cuotas (quota).

## Internal flow

```text
request gRPC ─▶ normalizer (por RPC) ─▶ validator ─▶ abac_clauses
             ─▶ ast_compiler (IR FlatBuffers) ─▶ plan_selector
             ─▶ router::oltp | router::olap ─▶ aggregator (OutputCast)
             ─▶ post_processor (labels) ─▶ respuesta
multi-series: sub-queries por group-id ─▶ full outer join fusionado
```

## Invariants

1. **IR inmutable** — el AST IR compilado no se muta; los pasos producen nuevos valores.
2. **Un normalizador por forma de RPC** — el trait `NormalizerStrategy` mantiene el dispatch sin condicionales dispersos.
3. **Zero-copy en la frontera** — el contrato con eav/aegis viaja en FlatBuffers; el ownership del buffer está documentado en `janus::fbs`.
4. **Las cláusulas ABAC se inyectan antes de compilar** — ningún filtro de seguridad viaja por fuera del AST (ver `abac_clauses.rs`).

## Entry points

- [`janus::normalizer`] — uno por RPC (`query`, `bulk`, `discovery`, `explore`, `transaction`, `match_rules`).
- [`janus::ast_compiler`] / [`janus::validator`] / [`janus::plan_selector`].
- [`janus::aggregator`] / [`janus::multi_series`].

## Errors

`JANUS_400` y familia: request que no cumple contrato, plan imposible, series inconsistentes.

## Decisions

- ADR-003 — contrato único; FlatBuffers como IR de frontera.

## Known risks / TODO

- Los snapshots de contrato (`janus/testing/contract_tests.rs`) fijan la salida binaria: cambios de FBS requieren regenerarlos conscientemente.
