;; [PORTED_TO_RUST: src/aegis/sql/compiler.rs]
;; NO MODIFICAR ESTE ARCHIVO.
;; La fuente de verdad para esta lógica ahora reside en Rust.
(ns metri.aegis.sql.compiler
  "Compilador principal: AST IR → SQL string para Athena via HoneySQL.
   SRP: orquestar los sub-compiladores y aplicar el security gate ZT.

   Rutas de compilación:
     Sin :comparisons (o solo BENCHMARK)     → compile-base-query (SELECT simple)
     Con TIME_SHIFT_* o SMART               → comparison/build-comparison-cte-query (WITH CTEs)

   INVARIANTE ZERO-TRUST:
     ast-contains-tenant? es el gate de seguridad cardinal.
     Ningún SQL se genera si el AST IR no contiene el nodo tenant_id."
  (:require [taoensso.timbre :as log]
            [clojure.string :as str]
            [honey.sql :as hsql]
            [metri.aegis.sql.helpers :as h]
            [metri.aegis.sql.where :as w]
            [metri.aegis.sql.select :as sel]
            [metri.aegis.sql.hierarchy :as hier]
            [metri.aegis.sql.comparison :as comp]
            [metri.aegis.sql.time-frame :as tf]
            [metri.domain.errors :as errors]))

;; ── Security Gate — ZT Invariant §7 ─────────────────────────────────────────

(defn ast-contains-tenant?
  "Verifica que el AST IR contiene :entity/tenant-id en el árbol :where.
   Gate de seguridad §7 — si retorna false, el SQL NO se genera nunca.
   Compatible con Janus Pool Model (:entity/tenant-id) y cualquier field
   cuyo nombre contenga la string 'tenant'.
   :ref-filter se omite en el scan (sus inner-nodes son de otra entidad)."
  [ast-ir]
  (letfn [(scan [node]
            (when (and node (vector? node))
              (let [[op & args] node]
                (case op
                  :=         (let [field (first args)]
                               (or (= field :entity/tenant-id)
                                   (= field :tenant/id)
                                   (str/includes? (name field) "tenant")
                                   (and (namespace field) (str/includes? (namespace field) "tenant"))))
                  :and       (some scan (filter vector? args))
                  :or        (some scan (filter vector? args))
                  :not       (scan (first (filter vector? args)))
                  ;; :ref-filter apunta a otra entidad — no puede contener tenant assert
                  :ref-filter false
                  false))))]
    (scan (:where ast-ir))))

;; ── Resolución dinámica del campo temporal ──────────────────────────────────

(defn- resolve-ts-col
  "Infiere el campo temporal (tipo 'epoch') desde el schema del AST IR.
   Estrategia (orden de prioridad):
     1. Atributo con :type 'epoch' en el schema → usa su :name
     2. Fallback conservador: :created_at (legacy, para entities sin schema cargado)

   Esto resuelve el bug de time-frame ignorado en entities como 'meter_reading'
   donde el campo temporal se llama 'timestamp' (no 'created_at')."
  [ast-ir]
  (let [attrs (get-in ast-ir [:schema :attributes])
        epoch-attr (when (seq attrs)
                     (first (filter #(= "epoch" (:type %)) attrs)))]
    (if epoch-attr
      (keyword (:name epoch-attr))
      :created_at)))

(defn- compile-base-query
  "Compila AST IR → mapa HoneySQL base sin CTEs.
   Maneja HierarchyContext con alias 't' cuando inject-has-children=true."
  [ast-ir time-frame tbl-str tenant-id output-cast database]
  (let [where-honey  (w/where-node->honey (:where ast-ir) {:database database})
        ts-col       (resolve-ts-col ast-ir)
        time-clause  (when time-frame (w/time-frame->honey-clause time-frame ts-col))
        hierarchy    (:hierarchy ast-ir)
        hparts       (hier/build-hierarchy-parts hierarchy tbl-str tenant-id)
        full-where   (cond-> where-honey
                       time-clause           (as-> wh (w/add-where wh time-clause))
                       (:extra-where hparts) (as-> wh (w/add-where wh (:extra-where hparts))))
        output-cast  (or output-cast :KPI)
        limit        (case output-cast
                       :CSV_EXPORT nil
                       :TABLE      10000
                       (or (:limit ast-ir) 1000))
        sel-exprs    (cond-> (sel/build-select-exprs ast-ir output-cast)
                       (:extra-select hparts) (conj (:extra-select hparts)))
        grp          (sel/build-group-by-exprs ast-ir output-cast)
        ord          (sel/build-order-by-exprs ast-ir output-cast)
        ;; Alias 't' en la tabla principal cuando has_children usa 't.id'
        tbl-expr     (if (:extra-select hparts)
                       [(keyword tbl-str) :t]
                       (keyword tbl-str))]
    (cond-> {:select sel-exprs
             :from   [tbl-expr]
             :where  full-where}
      (seq grp) (assoc :group-by grp)
      (seq ord) (assoc :order-by ord)
      limit     (assoc :limit limit))))

;; ── Entry point público ───────────────────────────────────────────────────────

(defn compile-athena-sql
  "Transpila AST IR OLAP → {:sql string :database string ...}.
   INVARIANTE: nunca genera SQL sin WHERE tenant_id (gate ast-contains-tenant?).

   Retorna:
     [:ok {:sql str :database str :entity str
           :output-cast kw :comparison? bool :benchmarks [...]}]
     [:error ...]"
  [ast-ir database tenant-id]
  (if-not (ast-contains-tenant? ast-ir)
    (do
      (log/error "[Aegis Compiler] AST IR sin tenant isolation — bloqueado!")
      (errors/error :AEG_TENANT_MISSING {:tenant-id tenant-id}))
    (try
      (let [entity       (or (:entity ast-ir) "events")
            tbl          (h/table-str entity database)
            output-cast  (or (:output-cast ast-ir) :KPI)
            time-frame   (tf/resolve-time-frame (:time-frame ast-ir))
            comparisons  (not-empty (:comparisons ast-ir))
            limit        (case output-cast
                           :CSV_EXPORT nil
                           :TABLE      10000
                           (or (:limit ast-ir) 1000))
            ;; Tipos que requieren CTE: TIME_SHIFT_* y SMART
            cte-comps    (when comparisons
                           (filter #(contains? #{:TIME_SHIFT_RELATIVE
                                                  :TIME_SHIFT_SHORTCUT
                                                  :TIME_SHIFT_ABSOLUTE
                                                  :SMART}
                                                (:type %))
                                   comparisons))
            query-map    (if (seq cte-comps)
                           (comp/build-comparison-cte-query
                             ast-ir tbl
                             (w/where-node->honey (:where ast-ir) {:database database})
                             (or time-frame {:start-ts nil :end-ts nil})
                             comparisons output-cast limit)
                           (compile-base-query ast-ir time-frame tbl tenant-id output-cast database))

            [sql]        (hsql/format query-map {:inline true :dialect :ansi})

            ;; BENCHMARK → metadata para que el translator construya IntelligenceSignal
            benchmarks   (when comparisons
                           (not-empty
                             (keep (fn [c]
                                     (when (= :BENCHMARK (:type c))
                                       {:type            :BENCHMARK
                                        :label           (:label c)
                                        :benchmark-value (:benchmark-value c)}))
                                   comparisons)))]

        (log/debug "[Aegis Compiler] SQL compilado | entity:" entity
                   "| output-cast:" output-cast
                   "| cte-comps:" (count cte-comps)
                   "| sql-len:" (count sql))

        [:ok {:sql          sql
              :database     database
              :entity       entity
              :output-cast  output-cast
              :comparison?  (boolean comparisons)
              :benchmarks   benchmarks}])

      (catch clojure.lang.ExceptionInfo e
        (log/warn "[Aegis Compiler] Compile error:" (ex-message e))
        (errors/error :AEG_COMPILE_002
                      {:detail    (ex-message e)
                       :tenant-id tenant-id}))
      (catch Exception e
        (log/error e "[Aegis Compiler] Error inesperado")
        (errors/error :AEG_COMPILE_002
                      {:detail    (ex-message e)
                       :tenant-id tenant-id})))))
