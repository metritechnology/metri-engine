(ns metri.aegis.sql
  "Fachada pública del OLAP SQL compiler.
   Mantiene backward-compatibility: callers existentes no cambian su require.

   Namespaces especializados (Single Responsibility):
     sql/helpers.clj     → col-kw, table-str
     sql/time_frame.clj  → resolve-time-frame (28 tipos TimeFrameContext)
     sql/where.clj       → where-node->honey, time-frame->honey-clause, add-where
     sql/aggregation.clj → build-metric-expr (14 AggregationFunction + CASE WHEN)
     sql/select.clj      → build-select-exprs, GROUP BY, ORDER BY (6 OutputCastType)
     sql/hierarchy.clj   → build-hierarchy-parts (EXISTS correlated subquery)
     sql/comparison.clj  → build-comparison-cte-query (5 AnalyticalComparison types)
     sql/compiler.clj    → ast-contains-tenant?, compile-athena-sql
     sql/executor.clj    → run-olap-chunks (polling Athena con backoff)"
  (:require [metri.aegis.sql.compiler   :as compiler]
            [metri.aegis.sql.executor   :as executor]
            [metri.aegis.sql.time-frame :as time-frame]))

;; ── API pública — re-exportaciones ───────────────────────────────────────────

(def ast-contains-tenant?
  "Gate de seguridad ZT §7. Ver compiler/ast-contains-tenant?"
  compiler/ast-contains-tenant?)

(def compile-athena-sql
  "Compila AST IR → SQL string Athena. Ver compiler/compile-athena-sql"
  compiler/compile-athena-sql)

(def run-olap-chunks
  "Compila + ejecuta en Athena + polling. Ver executor/run-olap-chunks"
  executor/run-olap-chunks)

(def resolve-time-frame
  "Resuelve TimeFrameContext (28 tipos) → {start-ts end-ts}. Ver time-frame/resolve-time-frame"
  time-frame/resolve-time-frame)
