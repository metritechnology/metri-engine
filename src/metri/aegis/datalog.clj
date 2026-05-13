(ns metri.aegis.datalog
  "Fachada pública del compilador/ejecutor Datahike OLTP.
   Mantiene backward-compatibility: callers existentes no cambian su require.

   Namespaces especializados (Single Responsibility):
     datalog/where.clj      → where-node->parts (14 FilterOperator → Datalog clauses)
     datalog/pull.clj       → select->pull-pattern, merge-pull-additions
     datalog/sort.clj       → sort-oltp-result (SortDefinition → in-memory sort)
     datalog/time_frame.clj → time-frame->clauses (28 TimeFrameContext → :_timestamp range)
     datalog/hierarchy.clj  → build-hierarchy-parts, inject-has-children
     datalog/compiler.clj   → compile-oltp-query
     datalog/executor.clj   → run-oltp-query, run-oltp-chunks"
  (:require [metri.aegis.datalog.compiler :as compiler]
            [metri.aegis.datalog.executor :as executor]))

;; ── API pública — re-exportaciones ───────────────────────────────────────────

(def compile-oltp-query
  "Compila AST IR OLTP → mapa query Datahike. Ver compiler/compile-oltp-query"
  compiler/compile-oltp-query)

(def run-oltp-query
  "Ejecuta query Datahike y retorna [:ok ...] | [:error ...]. Ver executor/run-oltp-query"
  executor/run-oltp-query)

(def run-oltp-chunks
  "Compila + ejecuta + chunking de 100 filas. Ver executor/run-oltp-chunks"
  executor/run-oltp-chunks)
