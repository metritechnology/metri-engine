;; [PORTED_TO_RUST: src/aegis/datalog/compiler.rs]
;; NO MODIFICAR ESTE ARCHIVO.
;; La fuente de verdad para esta lógica ahora reside en Rust.
(ns metri.aegis.datalog.compiler
  "Compilador OLTP: AST IR → mapa de query Datahike ejecutable.
   SRP: orquestar where + pull + time-frame + hierarchy — sin I/O, sin estado.

   Flujo:
     :where       → datalog.where/where-node->parts    → cláusulas + :in bindings
     :time-frame  → datalog.time-frame/time-frame->clauses → cláusulas :_created_at
     :hierarchy   → datalog.hierarchy/build-hierarchy-parts → cláusulas + pull-additions
     :select      → datalog.pull/select->pull-pattern  → pull spec
     :sort        → propagado al executor para sort en memoria
     :metrics     → propagado al executor para agregación in-memory (datalog.aggregation)
     :dimensions  → propagado al executor para GROUP BY / TIMESERIES bucketing
     :comparisons → propagado al executor para datalog.comparison
     :output-cast → propagado al executor para routing KPI/TIMESERIES/TABLE/PIE/BUBBLE/CSV_EXPORT

   Retorna:
     {:query        {:find [(pull ?e [...])] :in [$ ...] :where [...]}
      :args         [set-val ...]     ← nil si no hay :in bindings
      :limit        int
      :sort         [SortDefinition]
      :hier         {:has-children? bool :rev-key kw}
      :metrics      [MetricDefinition]       ← nil si no hay
      :dimensions   [DimensionDefinition]    ← nil si no hay
      :comparisons  [AnalyticalComparison]   ← nil si no hay
      :output-cast  keyword                  ← nil si no especificado
      ;; Expuestos para datalog.comparison (queries shifted):
      :base-clauses [clause ...]   ← WHERE + hierarchy (SIN time-frame)
      :in-sym->val  {sym val}
      :pull-pattern [...]
      :resolved-tf  {:start-ts long :end-ts long} | nil}"
  (:require [metri.aegis.datalog.where      :as w]
            [metri.aegis.datalog.pull       :as p]
            [metri.aegis.datalog.time-frame :as tf]
            [metri.aegis.datalog.hierarchy  :as h]
            [metri.aegis.time-frame         :as aegis-tf]))

(defn- resolve-created-at-field
  "Recupera dinámicamente el campo de tipo 'epoch' desde el esquema (inyectado en el AST IR por Janus).
   Si no existe o no tiene un campo epoch explícito, cae en el default :meta/created_at."
  [entity-type schema]
  (let [epoch-attr (first (filter #(= "epoch" (:type %)) (:attributes schema)))]
    (if epoch-attr
      (keyword entity-type (:name epoch-attr))
      :meta/created_at)))

(defn- infer-required-fields
  "Infiere el subconjunto mínimo de atributos a extraer (pull) basado en las
   dimensiones, métricas, y ordenamientos del AST IR.
   Esto evita ejecutar (pull ?e [*]) en consultas analíticas (KPI/PIE/TIMESERIES),
   reduciendo drásticamente la latencia al no tener que deserializar toda la entidad."
  [ast-ir ts-field]
  (let [extract-kw (fn [obj k fallback-k] 
                     (when-let [v (or (get obj k) (get obj fallback-k))] 
                       (keyword (name v))))
        metric-kws (mapcat (fn [m] 
                             [(extract-kw m :attribute :field) 
                              (extract-kw m :secondary-attribute :secondary-field)]) 
                           (:metrics ast-ir))
        dim-kws    (map (fn [d] (extract-kw d :attribute :field)) (:dimensions ast-ir))
        sort-kws   (map (fn [s] (extract-kw s :attribute :field)) (:order-by ast-ir))
        base-kws   (set (concat metric-kws dim-kws sort-kws [ts-field :entity/ulid :entity/type :tenant/id]))
        all-kws    (disj base-kws nil :id)]
    (vec all-kws)))

(defn compile-oltp-query
  "Compila el AST IR OLTP → mapa de query Datahike ejecutable.
   Lanza ExceptionInfo si el árbol :where es inválido."
  [ast-ir]
  (let [counter (atom 0)

        ;; ── WHERE → cláusulas Datalog + :in bindings ─────────────────────
        {:keys [clauses in-sym->val]}
        (w/where-node->parts (:where ast-ir) counter)

        ;; ── TimeFrameContext → cláusulas :meta/created_at ───────────────────
        ts-field    (resolve-created-at-field (:entity ast-ir) (:schema ast-ir))
        tf-clauses  (or (tf/time-frame->clauses (:time-frame ast-ir) counter ts-field) [])
        ;; Período resuelto — expuesto para comparison module
        resolved-tf (aegis-tf/resolve-time-frame (:time-frame ast-ir))

        ;; ── HierarchyContext → cláusulas extra + pull additions ──────────
        hparts       (h/build-hierarchy-parts (:hierarchy ast-ir) (:entity ast-ir))
        hier-clauses (or (:extra-clauses hparts) [])

        ;; base-clauses = WHERE + hierarchy (SIN time-frame) — para comparison
        ;; ENFORCE entity-type clause explicitly for Datahike
        type-clause  ['?e :entity/type (keyword (:entity ast-ir))]
        base-clauses (vec (concat [type-clause] clauses hier-clauses))
        all-clauses  (vec (concat base-clauses tf-clauses))

        ;; ── SELECT → pull pattern ─────────────────────────────────────────
        base-pull  (let [explicit-pull (p/select->pull-pattern (:select ast-ir))
                         cast          (:output-cast ast-ir)
                         is-analytical? (or (#{:KPI :PIE :TIMESERIES :BUBBLE} cast)
                                            (and (nil? cast) (seq (:metrics ast-ir))))]
                     (if is-analytical?
                       (infer-required-fields ast-ir ts-field)
                       explicit-pull))
        full-pull  (p/merge-pull-additions base-pull (or (:pull-additions hparts) []))

        ;; ── Query map Datahike ────────────────────────────────────────────
        query (cond-> {:find  [(list 'pull '?e full-pull)]
                        :where all-clauses}
                (seq in-sym->val)
                (assoc :in (into ['$] (keys in-sym->val))))

        args (when (seq in-sym->val)
               (vals in-sym->val))]

    {:query        query
     :args         args
     :limit        (or (:limit ast-ir) 100)
     :sort         (or (:order-by ast-ir) [])
     :hier         (when (:has-children? hparts)
                     (select-keys hparts [:has-children? :rev-key]))
     :metrics      (seq (:metrics ast-ir))
     :dimensions   (seq (:dimensions ast-ir))
     :comparisons  (seq (:comparisons ast-ir))
     :output-cast  (:output-cast ast-ir)
     ;; Expuestos para comparison module
     :base-clauses  base-clauses
     :in-sym->val   in-sym->val
     :pull-pattern  full-pull
     :resolved-tf   resolved-tf
     :ts-field      ts-field}))
