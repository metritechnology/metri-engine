(ns metri.aegis.sql.comparison
  "AnalyticalComparison: 5 tipos del contrato §4 → CTEs HoneySQL con ventanas temporales.
   SRP: construcción de CTEs de comparación — sin I/O, sin estado.

   Delega la aritmética temporal a metri.temporal.comparison (bisiesto-safe, DST-aware).

   Estrategias por tipo:
     TIME_SHIFT_RELATIVE  → desplazar período por N granularidades reales (java.time)
     TIME_SHIFT_SHORTCUT  → atajos nombrados tz-aware (PREVIOUS_PERIOD, SAME_PERIOD_LAST_YEAR, etc.)
     TIME_SHIFT_ABSOLUTE  → ventana explícita con absolute-start-ts / absolute-end-ts
     SMART                → CTE histórico 90 días + AVG + STDDEV_SAMP → columnas z_score_*
     BENCHMARK            → columna [:inline value] en SELECT (sin CTE)

   Estructura SQL generada:
     KPI        → FROM current_data c, prev_0, ... (implicit CROSS JOIN)
     TIMESERIES → FROM current_data c LEFT JOIN prev_0 ON c.bucket = prev_0.bucket ..."
  (:require [taoensso.timbre :as log]
            [clojure.string :as str]
            [metri.aegis.sql.where :as w]
            [metri.aegis.sql.aggregation :as agg]
            [metri.aegis.sql.select :as sel]
            [metri.temporal.comparison :as tc])
  (:import [java.time Instant]))

;; ── Resolución dinámica del campo temporal (mismo patrón que compiler.clj) ────────

(defn- resolve-ts-col
  "Infiere el campo temporal (tipo 'epoch') desde el schema del AST IR.
   Fallback: :created_at (legacy)."
  [ast-ir]
  (let [attrs (get-in ast-ir [:schema :attributes])
        epoch-attr (when (seq attrs)
                     (first (filter #(= "epoch" (:type %)) attrs)))]
    (if epoch-attr
      (keyword (:name epoch-attr))
      :created_at)))


;; ── Delegación a temporal.comparison ───────────────────────────────────────
;; La aritmética temporal ahora vive en metri.temporal.comparison.
;; Estas funciones privadas son wrappers de compatibilidad interna.

(defn- comparison->period
  "AnalyticalComparison × {:start-ts :end-ts} → {:prev-start :prev-end} | nil.
   Delega a temporal.comparison/resolve-comparison-period (bisiesto-safe)."
  [comp time-range]
  (when-let [p (tc/resolve-comparison-period comp time-range "UTC")]
    {:prev-start (:prev-start p)
     :prev-end   (:prev-end p)}))

;; ── CTE builder principal ─────────────────────────────────────────────────────

(defn build-comparison-cte-query
  "Genera query HoneySQL con :with CTEs para AnalyticalComparison.

   Retorna mapa HoneySQL listo para (hsql/format q {:inline true :dialect :ansi}).

   CTEs generados:
     current_data  → período actual con WHERE temporal
     prev_N        → TIME_SHIFT comparisons (una CTE por comparison)
     smart_N       → SMART: ventana histórica 90d → AVG + STDDEV_SAMP por métrica

   SELECT final:
     TIMESERIES → c.bucket + c.metrics + prev_N.metrics (LEFT JOIN en bucket)
     KPI        → c.metrics + prev_N.metrics + z_score_* (CROSS JOIN implícito en FROM)"
  [ast-ir tbl-str where-honey time-frame comparisons output-cast limit]
  (let [tbl          (keyword tbl-str)
        ts-col       (resolve-ts-col ast-ir)
        cs           (or (:start-ts time-frame) 0)
        ce           (or (:end-ts time-frame) (.getEpochSecond (Instant/now)))
        curr-clause  (w/time-frame->honey-clause time-frame ts-col)
        curr-where   (w/add-where where-honey curr-clause)

        ;; Metric expressions para los CTEs internos
        metric-exprs (vec (mapv agg/build-metric-expr (or (:metrics ast-ir) [])))
        aliases      (mapv second metric-exprs)

        ;; SELECT del CTE: añade date_trunc bucket para TIMESERIES
        bucket-exprs (when (= :TIMESERIES output-cast)
                       (mapv (fn [d] [(sel/dim->bucket-expr d) (keyword (or (:attribute d) "bucket"))])
                             (or (:group-by ast-ir) [])))
        sel-for-cte  (let [base (if (seq bucket-exprs)
                                  (into (vec bucket-exprs) metric-exprs)
                                  metric-exprs)]
                       (if (empty? base)
                         (sel/build-select-exprs ast-ir output-cast)
                         base))
        grp-for-cte  (when (seq bucket-exprs)
                       (mapv sel/dim->bucket-expr (or (:group-by ast-ir) [])))

        ;; ── CTE: current_data ─────────────────────────────────────────────
        current-cte  [:current_data
                      (cond-> {:select sel-for-cte :from [tbl] :where curr-where}
                        (seq grp-for-cte) (assoc :group-by grp-for-cte))]

        ;; ── CTEs: TIME_SHIFT_* → prev_0, prev_1, ... ─────────────────────
        time-shift-ctes
        (vec
          (keep-indexed
            (fn [i comp]
              (when-let [period (comparison->period comp {:start-ts cs :end-ts ce})]
                (log/debug "[Aegis Comparison] Generated period for CTE:" i period)
                (let [prev-clause (w/time-frame->honey-clause
                                    {:start-ts (:prev-start period) :end-ts (:prev-end period)}
                                    ts-col)
                      prev-where  (w/add-where where-honey prev-clause)]
                  [(keyword (str "prev_" i))
                   (cond-> {:select sel-for-cte :from [tbl] :where prev-where}
                     (seq grp-for-cte) (assoc :group-by grp-for-cte))])))
            comparisons))
        
        _ (log/debug "[Aegis Comparison] Computed time-shift-ctes:" time-shift-ctes)

        ;; ── CTEs: SMART → smart_N (AVG + STDDEV histórico 90 días) ───────
        smart-ctes
        (vec
          (keep-indexed
            (fn [i comp]
              (when (= :SMART (:type comp))
                (let [hist-range  (tc/smart-history-window {:start-ts cs})
                      hist-clause (w/time-frame->honey-clause hist-range ts-col)
                      hist-where  (w/add-where where-honey hist-clause)
                      stat-exprs  (vec
                                    (apply concat
                                      (map (fn [[call alias]]
                                             (let [f (if (= :count (first call)) :* (second call))]
                                               [[[:avg f]        (keyword (str "mean_" (name alias)))]
                                                [[:stddev_samp f] (keyword (str "std_"  (name alias)))]]))
                                           metric-exprs)))]
                  [(keyword (str "smart_" i))
                   {:select stat-exprs :from [tbl] :where hist-where}])))
            comparisons))

        ;; ── BENCHMARK → columna inline (sin CTE) ─────────────────────────
        benchmark-cols
        (vec
          (keep (fn [comp]
                  (when (= :BENCHMARK (:type comp))
                    [[:inline (double (or (:benchmark-value comp) 0.0))]
                     (keyword (str "benchmark_" (or (:label comp) "value")))]))
                comparisons))

        all-ctes (vec (concat [current-cte] time-shift-ctes smart-ctes))

        ;; ── SELECT final ──────────────────────────────────────────────────
        final-sel
        (if (= :TIMESERIES output-cast)
          (let [b (name (or (:attribute (first (:group-by ast-ir))) "bucket"))]
            (vec
              (concat
                (if (empty? aliases) [[:raw "c.*"]] [[:raw (str "c." b)]])
                (mapv (fn [a] [[:raw (str "c." (name a))] a]) aliases)
              (mapcat (fn [[cte-name _]]
                        (mapv (fn [a]
                                [[:raw (str (name cte-name) "." (name a))]
                                 (keyword (str (name cte-name) "_" (name a)))])
                              aliases))
                      time-shift-ctes)
              benchmark-cols)))
          ;; KPI: current_X + prev_N_X + z_score_X (SMART)
          (vec
            (concat
              (if (empty? aliases) [[:raw "c.*"]] [])
              (mapv (fn [a] [[:raw (str "c." (name a))] (keyword (str "current_" (name a)))]) aliases)
              (mapcat (fn [[cte-name _]]
                        (mapv (fn [a]
                                [[:raw (str (name cte-name) "." (name a))]
                                 (keyword (str (name cte-name) "_" (name a)))])
                              aliases))
                      time-shift-ctes)
              benchmark-cols
              ;; z_score = (current - mean) / NULLIF(std, 0)
              (mapcat (fn [[smart-name _]]
                        (mapv (fn [a]
                                [[:/ [:- [:raw (str "c." (name a))]
                                         [:raw (str (name smart-name) ".mean_" (name a))]]
                                     [:nullif [:raw (str (name smart-name) ".std_" (name a))]
                                              [:inline 0]]]
                                 (keyword (str "z_score_" (name a)))])
                              aliases))
                      smart-ctes))))

        ;; ── FROM / JOINs ──────────────────────────────────────────────────
        final-from
        [[:current_data :c]]

        ;; LEFT JOINs para TIMESERIES y KPI
        left-joins
        (if (= :TIMESERIES output-cast)
          (let [b (name (or (:attribute (first (:group-by ast-ir))) "bucket"))]
            (vec
              (apply concat
                (map (fn [[cte-name _]]
                       [cte-name [:= [:raw (str "c." b)] [:raw (str (name cte-name) "." b)]]])
                     time-shift-ctes))))
          (vec
            (apply concat
              (map (fn [[cte-name _]]
                     [cte-name [:= 1 1]])
                   (concat time-shift-ctes smart-ctes)))))]

    (cond-> {:with   all-ctes
             :select final-sel
             :from   final-from}
      (seq left-joins) (assoc :left-join left-joins)
      limit            (assoc :limit limit))))
