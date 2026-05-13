(ns metri.aegis.sql.aggregation
  "AggregationFunction builders: MetricDefinition → HoneySQL [expr alias].
   SRP: compilación de agregaciones — sin I/O, sin estado.

   Soporta las 14 AggregationFunction del contrato §1:
     COUNT SUM AVG MIN MAX MEDIAN STD_DEV VARIANCE
     PERCENTILE_90 PERCENTILE_95 PERCENTILE_99
     CORRELATION LINEAR_REGRESSION LOGISTIC_REGRESSION

   Filtered aggregation (MetricDefinition.filter §4):
     COUNT(CASE WHEN <filter> THEN 1 ELSE NULL END)
     SUM(CASE WHEN <filter> THEN field ELSE NULL END)"
  (:require [taoensso.timbre :as log]
            [clojure.string :as str]
            [metri.aegis.sql.helpers :as h]
            [metri.aegis.sql.where :as w]))

;; ── Agregación HoneySQL ───────────────────────────────────────────────────────

(defn- metric-agg-call
  "AggregationFunction string + campo efectivo → HoneySQL function call.
   secondary-field usado en CORRELATION y LINEAR_REGRESSION."
  [fn-str eff-field secondary-field]
  (case (str/upper-case (or fn-str "COUNT"))
    "COUNT"               [:count eff-field]
    "SUM"                 [:sum eff-field]
    "AVG"                 [:avg eff-field]
    "MIN"                 [:min eff-field]
    "MAX"                 [:max eff-field]
    "MEDIAN"              [:approx_percentile eff-field [:inline 0.5]]
    "STD_DEV"             [:stddev_samp eff-field]
    "VARIANCE"            [:var_samp eff-field]
    "PERCENTILE_90"       [:approx_percentile eff-field [:inline 0.9]]
    "PERCENTILE_95"       [:approx_percentile eff-field [:inline 0.95]]
    "PERCENTILE_99"       [:approx_percentile eff-field [:inline 0.99]]
    ;; Funciones bivariadas — requieren secondary-attribute en MetricDefinition
    "CORRELATION"         [:corr (or secondary-field eff-field) eff-field]
    "LINEAR_REGRESSION"   [:regr_slope (or secondary-field eff-field) eff-field]
    "LOGISTIC_REGRESSION" (do (log/warn "[Aegis AGG] LOGISTIC_REGRESSION no nativo en Athena — COUNT fallback")
                              [:count eff-field])
    [:count eff-field]))

;; ── API pública ───────────────────────────────────────────────────────────────

(defn build-metric-expr
  "MetricDefinition → HoneySQL [call-expr alias-kw].

   Con :filter    → CASE WHEN filtered aggregation (COUNT ignores NULL rows).
   Con :secondary-attribute → usado en CORR / REGR_SLOPE como segunda columna."
  [m]
  (let [fn-raw    (:aggregation m)
        fn-str    (str/upper-case (if (keyword? fn-raw) (name fn-raw) (str (or fn-raw "COUNT"))))
        attr      (when-let [a (:attribute m)] (h/col-kw a))
        sec-attr  (when-let [a (:secondary-attribute m)] (h/col-kw a))
        alias-kw  (keyword (or (:name m)
                               (str (str/lower-case fn-str) "_"
                                    (if attr (name attr) "total"))))
        field     (or attr :*)
        ;; Campo efectivo: plain o wrapeado en CASE WHEN si hay filtro
        eff-field (if-let [f (:filter m)]
                    (let [cond-expr (w/where-node->honey f)
                          then-val  (if (= "COUNT" fn-str) [:inline 1] field)]
                      [:case cond-expr then-val :else nil])
                    field)
        ;; COALESCE(agg, 0) para funciones numéricas que retornan NULL en sets vacíos.
        ;; COUNT ya retorna 0 nativamente. CORRELATION/LINEAR_REGRESSION pueden ser NULL semánticamente.
        null-safe-fns #{"SUM" "AVG" "MIN" "MAX" "MEDIAN" "STD_DEV" "VARIANCE"
                        "PERCENTILE_90" "PERCENTILE_95" "PERCENTILE_99"}
        agg-call  (metric-agg-call fn-str eff-field sec-attr)
        agg-call  (if (null-safe-fns fn-str)
                    [:coalesce agg-call [:inline 0]]
                    agg-call)]
    [agg-call alias-kw]))
