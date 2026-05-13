(ns metri.janus.ast-specs
  "Specs Malli para el contrato Janus AST IR.
   Fuente de verdad: resources/schema/janus-ast-ir.edn
   Cubre: context-invariant, filter-node/group (recursivos), enums, ast-ir."
  (:require [malli.core :as m]
            [malli.error :as me]
            [taoensso.timbre :as log]
            [metri.janus.validator :as validator]))

;; ── §1: Enums (cerrados, con sentinel guard en el caller) ─────────────────────

(def aggregation-function
  [:enum "COUNT" "SUM" "AVG" "MIN" "MAX" "MEDIAN" "STD_DEV" "VARIANCE"
         "PERCENTILE_90" "PERCENTILE_95" "PERCENTILE_99"
         "CORRELATION" "LINEAR_REGRESSION" "LOGISTIC_REGRESSION"])

(def filter-operator
  [:enum "EQ" "NEQ" "GT" "GTE" "LT" "LTE" "IN" "NOT_IN"
         "BETWEEN" "LIKE" "IS_NULL" "IS_NOT_NULL" "MATCHES" "CONTAINS"])

(def output-cast-type
  [:enum "KPI" "TIMESERIES" "TABLE" "PIE" "BUBBLE" "CSV_EXPORT"])

(def query-scope
  [:enum "ALL" "OWN" "ASSIGNED" "OWN_OR_ASSIGNED" "NONE"])

;; ── §2: Schemas Recursivos (FilterNode ↔ FilterGroup) ────────────────────────

(def filter-value
  [:map
   [:string-val   {:optional true} :string]
   [:number-val   {:optional true} number?]
   [:bool-val     {:optional true} :boolean]
   [:timestamp-val {:optional true} :int]
   [:list-val     {:optional true} [:vector :any]]
   ;; §4 FilterValueList — rango para BETWEEN ([min max])
   [:range-values {:optional true} [:vector :any]]])

(def filter-value-list
  "§4 FilterValueList — wrapper proto para listas de FilterValue (BETWEEN / range-values).
   Usado por el operador BETWEEN: range-values[0]=min, range-values[1]=max."
  [:map [:values {:optional true} [:vector filter-value]]])

(def filter-criteria
  [:map
   [:field  :string]
   [:op-ref filter-operator]
   [:value  {:optional true} filter-value]])

;; Schemas recursivos: filter-node ↔ filter-group
;; Malli requiere el patrón [:schema {:registry {::N ...}} [:ref ::N]] para evitar stack overflow
(def filter-node
  [:schema
   {:registry
    {::fn [:map
           [:node {:optional true}
            [:or
             [:map [:criteria filter-criteria]]
             [:map [:group [:ref ::fg]]]]]]
     ::fg [:map
           [:conjunction [:enum "AND" "OR" "NOT"]]
           [:nodes [:vector [:ref ::fn]]]]}}
   [:ref ::fn]])



(defn context-invariant-validator
  "Valida el contexto Cedar usando el esquema generado en el registry dinámico."
  [cedar-ctx]
  (when-not @validator/ast-registry
    (validator/init-registry!))
  (let [valid? (m/validate :metri.cedar/context-invariant cedar-ctx {:registry @validator/ast-registry})]
    (when-not valid?
      (let [errs (me/humanize (m/explain :metri.cedar/context-invariant cedar-ctx {:registry @validator/ast-registry}))]
        (log/error "[Janus AST Specs] context-invariant-validator falló. Errores:" errs "| Contexto:" cedar-ctx)))
    valid?))

;; ── §AST IR: estructura del árbol compilado ──────────────────────────────────
;; Agnóstico a la base de datos. Solo keywords semánticos + operadores.

(def ast-where-op
  [:enum := :not= :> :< :>= :<= :in :not-in :and :or :not :fts
         ;; Extensión cross-entity: Janus genera este nodo desde dot-path fields
         ;; (e.g. "location_id.type"). Nunca proviene directamente del proto.
         :ref-filter])

;; Nodo :where puede ser un operador compuesto o una hoja de comparación
(def ast-where-node :any)  ;; fully recursive — validated structurally at compile time

(def ast-ir
  [:map
   [:select   {:optional true} [:vector :any]]
   [:where    :any]         ;; árbol [:and [:= ...] ...] — validado por ast-contains-tenant?
   [:limit    {:optional true} pos-int?]
   [:cursor   {:optional true} [:maybe :string]]
   [:order-by {:optional true} [:vector :any]]
   [:metrics  {:optional true} [:vector :any]]
   [:group-by {:optional true} [:vector :any]]
   [:entity   {:optional true} :string]])
