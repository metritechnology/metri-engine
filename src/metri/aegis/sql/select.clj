(ns metri.aegis.sql.select
  "SELECT / GROUP-BY / ORDER-BY builders con routing por OutputCastType.
   SRP: construcción de proyecciones — sin I/O, sin estado.

   OutputCastType routing (6 tipos del contrato §1):
     :KPI        → AGGs + FormulaEntry + SemanticMetricRef
     :TIMESERIES → date_trunc(interval, col) bucket + AGGs
     :TABLE      → columnas de :select
     :PIE        → AGGs agrupados por dimensión categoría
     :BUBBLE     → AGGs (x, y, size por dimensiones)
     :CSV_EXPORT → SELECT * sin LIMIT"
  (:require [taoensso.timbre :as log]
            [clojure.string :as str]
            [metri.aegis.sql.helpers :as h]
            [metri.aegis.sql.aggregation :as agg]))

;; ── DimensionDefinition → date_trunc ────────────────────────────────────────

(defn dim->bucket-expr
  "DimensionDefinition → expresión HoneySQL date_trunc para TIMESERIES.
   Con :interval  → [:date_trunc [:inline interval] col]
   Sin :interval  → keyword de columna directa (para PIE/BUBBLE groupings)."
  [d]
  (let [attr-name (name (h/col-kw (or (:attribute d) "event_ts")))
        interval (or (:interval d) "")]
    (if (seq interval)
      ;; Normalizar epoch a segundos via IF() nativo de Presto/Athena.
      ;; Si el valor > 1e11 viene en ms, dividir entre 1000 antes de from_unixtime.
      (let [norm-raw (str "IF(" attr-name " > 100000000000, "
                          attr-name " / 1000.0, CAST(" attr-name " AS DOUBLE))")
            norm-expr [[:raw norm-raw]]]
        [:date_trunc [:inline interval] [:from_unixtime norm-expr]])
      (h/col-kw (or (:attribute d) "event_ts")))))

;; ── SELECT expressions ────────────────────────────────────────────────────────

(defn build-select-exprs
  "Lista de expresiones HoneySQL para :select según output-cast.
   Retorna vector de: keyword | [expr alias] | [:*]."
  [ast-ir output-cast]
  (case (or output-cast :KPI)

    :TIMESERIES
    ;; date_trunc bucket(s) + métricas agregadas
    (let [dims    (or (:group-by ast-ir) [])
          metrics (or (:metrics ast-ir) [])]
      (into (mapv (fn [d] [(dim->bucket-expr d) (keyword (or (:attribute d) "bucket"))]) dims)
            (mapv agg/build-metric-expr metrics)))

    (:KPI :PIE :BUBBLE)
    ;; Dimensiones (group-by) + Métricas + FormulaEntry (:measures) + SemanticMetricRef (:semantic-measures)
    (let [dims     (or (:group-by ast-ir) [])
          metrics  (or (:metrics ast-ir) [])
          measures (or (:measures ast-ir) [])
          sem-ms   (or (:semantic-measures ast-ir) [])]
      (vec
        (concat
          (mapv dim->bucket-expr dims)
          (mapv agg/build-metric-expr metrics)
          ;; FormulaEntry → raw SQL expression (e.g. "SUM(revenue)/COUNT(orders)")
          (mapv (fn [fe] [[:raw (:formula fe)] (keyword (:name fe))]) measures)
          ;; SemanticMetricRef → requiere resolver externo; NULL placeholder en SQL
          (mapv (fn [sm]
                  (log/warn "[Aegis SELECT] SemanticMetricRef sin resolver externo — NULL:"
                            (:metric-key sm))
                  [[:raw (str "NULL /* semantic:" (:metric-key sm) " */")]
                   (keyword (:metric-key sm))])
                sem-ms))))

    :TABLE
    (let [;; Flatten nested objects to top-level keys for OLAP (Athena is flat)
          sel (when (:select ast-ir)
                (mapv #(if (map? %) (first (keys %)) %) (:select ast-ir)))
          dims (or (:group-by ast-ir) [])
          metrics (or (:metrics ast-ir) [])]
      (if (and (or (nil? sel) (= sel ['*]))
               (or (seq dims) (seq metrics)))
        (vec (concat (mapv dim->bucket-expr dims)
                     (mapv agg/build-metric-expr metrics)))
        (if (or (nil? sel) (= sel ['*])) [:*] (mapv h/col-kw sel))))

    :CSV_EXPORT
    [:*]

    ;; Default (unrecognized cast) → proyección directa
    (let [sel (when (:select ast-ir)
                (mapv #(if (map? %) (first (keys %)) %) (:select ast-ir)))]
      (if (or (nil? sel) (= sel ['*])) [:*] (mapv h/col-kw sel)))))

;; ── GROUP BY ─────────────────────────────────────────────────────────────────

(defn build-group-by-exprs
  "GROUP BY expressions para HoneySQL según output-cast.
   Retorna nil para output-casts que no agregan (TABLE, CSV_EXPORT)."
  [ast-ir output-cast]
  (let [dims (not-empty (:group-by ast-ir))]
    (case (or output-cast :KPI)
      :TIMESERIES
      (when dims (mapv dim->bucket-expr dims))

      (:PIE :BUBBLE)
      (when dims (mapv (fn [d] (h/col-kw (or (:attribute d) d))) dims))

      :KPI
      ;; GROUP BY solo si hay dimensiones extra (breakdown KPI)
      (when dims (mapv (fn [d] (h/col-kw (or (:attribute d) d))) dims))

      :TABLE
      (when (and dims (seq (:metrics ast-ir)))
        (mapv (fn [d] (h/col-kw (or (:attribute d) d))) dims))

      nil)))

;; ── ORDER BY ─────────────────────────────────────────────────────────────────

(defn build-order-by-exprs
  "ORDER BY expressions para HoneySQL.
   TIMESERIES → bucket (posición 1) siempre primero."
  [ast-ir output-cast]
  (let [order-by (not-empty (:order-by ast-ir))]
    (case (or output-cast :KPI)
      :TIMESERIES
      ;; Bucket en posición 1 del SELECT → ORDER BY 1 ASC primero
      (into [[[:raw "1"] :asc]]
            (mapv (fn [{:keys [field descending]}]
                    [(h/col-kw field) (if descending :desc :asc)])
                  (or order-by [])))

      (when order-by
        (mapv (fn [{:keys [field descending]}]
                [(h/col-kw field) (if descending :desc :asc)])
              order-by)))))
