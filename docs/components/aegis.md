# Aegis — Compilers

> **English summary:** The three compilers of the engine: SQL for Athena/Postgres/MySQL via sea-query (injection impossible by construction), the OLTP in-memory executor over the EAV engine, and the sandboxed formula language with its own lexer/parser/evaluator and SQL compilation.

## Purpose

Traducir intenciones declarativas (AST IR) a ejecución concreta: SQL para el canal OLAP y planes físicos para el canal OLTP, más el lenguaje de fórmulas que ambos canales comparten.

## Responsibilities / non-responsibilities

**Hace:** compilación SQL por partes (select/metrics/where/CTE); gate Zero-Trust de seguridad; soporte de entidades híbridas (virtual tables); executor OLTP (hydrator, filter, 14 agregaciones, comparison, hierarchy, caster); fórmulas (sandbox + evaluación + SQL); paginación por cursor; label templates.
**No hace:** I/O (los compiladores son puros), ruteo (janus/janus_router), autorización (cedar — solo consume el contexto).

## Internal flow

```text
OLAP:  AstIr ─▶ compiler ─▶ select/metric/where/cte_compiler ─▶ SqlDialect ─▶ SQL
                       └─▶ security (aislamiento tenant) antes de renderizar
OLTP:  AstIr ─▶ compiler ─▶ plan EAV ─▶ executor:
        compile ▶ execute ▶ hydrator(cache) ▶ filter ▶ aggregation ▶
        comparison ▶ hierarchy ▶ caster
```

## Invariants

1. **Inyección imposible por construcción** — no hay concatenación de SQL: valores como parámetros e identificadores como alias de `sea-query`.
2. **Aislamiento por tenant verificado** — `sql::security` valida el gate Zero-Trust (ZT Invariant §7) antes de renderizar.
3. **Fórmulas sandboxeadas** — `formula/security` rechaza lo que el lenguaje no permite; el evaluador solo ve variables resueltas por el trait `VariableResolver`.
4. **Compiladores puros** — sin I/O ni estado; el I/O vive en ejecutores.

## Entry points

- [`aegis::sql::compiler`] — compilador OLAP principal.
- [`aegis::oltp::compiler`] + [`aegis::oltp::executor`] — camino OLTP.
- [`aegis::formula`] — lexer, parser, evaluator, `FunctionRegistry`.
- [`aegis::ast_ir`] — el IR tipado compartido.

## Errors

`AEG_*` (compilación) y errores de fórmula (`FormulaError` con bridge a `DomainError`).

## Decisions

- `sea-query` como backend SQL (MySQL/Postgres/Athena) — decisión del port.
- Arquitectura SOLID explícita en el motor de fórmulas (SRP/OCP/LSP/ISP/DIP).

## Known risks / TODO

- El dialecto Athena/Presto es el más probado; MySQL/Postgres tienen menos superficie cubierta por tests de contrato.
