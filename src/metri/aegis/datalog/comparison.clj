(ns metri.aegis.datalog.comparison
  "AnalyticalComparison: 5 tipos del contrato §4 → queries Datahike shifted + métricas in-memory.
   SRP: computar comparaciones temporales — sin estado.

   Estrategias (paralelo al sql/comparison.clj con CTEs):
     TIME_SHIFT_RELATIVE  → desplazo período actual por N×granularidad
     TIME_SHIFT_SHORTCUT  → 12 atajos nombrados del contrato
     TIME_SHIFT_ABSOLUTE  → ventana explícita absolute-start-ts / absolute-end-ts
     SMART                → ventana histórica 90d → mean/std → z-score por métrica
     BENCHMARK            → valor inline en resultado (sin query Datahike adicional)

   Diferencia respecto al path OLAP (SQL):
     SQL   → CTEs WITH prev_0 / smart_N + CROSS JOIN / LEFT JOIN en bucket
     Datalog → N queries d/q independientes con :_timestamp shiftado + apply-metrics in-memory

   Resultado: mapa plano fusionable con el resultado de métricas actuales.
     {:current_sum_revenue 1234
      :prev_0_sum_revenue  1100
      :z_score_sum_revenue 1.2
      :benchmark_target    1000.0}"
  (:require [clojure.string :as str]
            [datahike.api :as d]
            [taoensso.timbre :as log]
            [metri.aegis.datalog.aggregation :as agg])
  (:import [java.time Instant]))

;; ── Resolución de período de comparación ─────────────────────────────────────
;; Misma lógica que sql/comparison.clj — separada para no crear dependencia cruzada

(defn- shortcut->period
  "ShiftShortcut keyword + [cs ce] epoch-seg → {:prev-start :prev-end} | nil."
  [shortcut cs ce]
  (let [duration (- ce cs)
        shift    (fn [s] {:prev-start (- cs s) :prev-end (- ce s)})]
    (case shortcut
      :PREVIOUS_PERIOD          {:prev-start (- cs duration) :prev-end cs}
      :SAME_PERIOD_LAST_YEAR    (shift (* 365 86400))
      :SAME_PERIOD_LAST_QUARTER (shift (* 91 86400))
      :SAME_PERIOD_LAST_MONTH   (shift (* 30 86400))
      :SAME_DAY_LAST_WEEK       (shift (* 7 86400))
      :SAME_DAY_LAST_MONTH      (shift (* 30 86400))
      :SAME_DAY_LAST_YEAR       (shift (* 365 86400))
      :YESTERDAY_LAST_YEAR      {:prev-start (- cs (* 366 86400)) :prev-end (- cs (* 365 86400))}
      :YESTERDAY_LAST_MONTH     {:prev-start (- cs (* 31 86400))  :prev-end (- cs (* 30 86400))}
      :YESTERDAY_LAST_WEEK      {:prev-start (- cs (* 8 86400))   :prev-end (- cs (* 7 86400))}
      :TODAY_LAST_YEAR          (shift (* 365 86400))
      :TODAY_LAST_MONTH         (shift (* 30 86400))
      :SHIFT_SHORTCUT_UNSPECIFIED nil
      nil)))

(defn- comparison->period
  "AnalyticalComparison × {:start-ts :end-ts} → {:prev-start :prev-end} | nil.
   Retorna nil para BENCHMARK y SMART (no generan query shifted)."
  [comp {:keys [start-ts end-ts]}]
  (let [cs (or start-ts 0)
        ce (or end-ts (.getEpochSecond (Instant/now)))]
    (case (:type comp)
      :TIME_SHIFT_RELATIVE
      (let [gran   (or (:relative-granularity comp) "day")
            amount (long (or (:relative-amount comp) 1))
            secs   (case gran
                     "minute" 60
                     "hour"   3600
                     "day"    86400
                     "week"   (* 7 86400)
                     "month"  (* 30 86400)
                     "year"   (* 365 86400)
                     86400)]
        {:prev-start (- cs (* amount secs))
         :prev-end   (- ce (* amount secs))})

      :TIME_SHIFT_SHORTCUT (shortcut->period (:shortcut comp) cs ce)
      :TIME_SHIFT_ABSOLUTE {:prev-start (:absolute-start-ts comp)
                            :prev-end   (:absolute-end-ts comp)}
      :BENCHMARK nil
      :SMART     nil
      :COMPARISON_TYPE_UNSPECIFIED nil
      nil)))

;; ── Query helper ──────────────────────────────────────────────────────────────

(defn- run-shifted-query
  "Ejecuta d/q con base-clauses + ts-field filtrado para [prev-start prev-end].
   Retorna vector de row maps (pull results)."
  [db base-clauses in-sym->val pull-pattern prev-start prev-end ts-field]
  (let [;; Variable única para no colisionar con posibles ?ts1 del base-clauses
        ts-sym      '?ts_cmp
        tf-clauses  (cond-> [['?e (or ts-field :created_at) ts-sym]]
                      prev-start (conj [(list '>= ts-sym (* prev-start 1000))])
                      prev-end   (conj [(list '<= ts-sym (* prev-end 1000))]))
        all-clauses (vec (concat base-clauses tf-clauses))
        query       (cond-> {:find  [(list 'pull '?e pull-pattern)]
                              :where all-clauses}
                      (seq in-sym->val)
                      (assoc :in (into ['$] (keys in-sym->val))))
        raw         (if (seq in-sym->val)
                      (apply d/q query db (vals in-sym->val))
                      (d/q query db))]
    (mapv (fn [r]
            (into {}
                  (keep (fn [[k v]]
                          (when-not (#{:db/id :tenant/id :entity/type} k)
                            [(keyword (if (= k :entity/ulid) "id" (name k))) v]))
                        (first r))))
          raw)))
;; ── SMART: estadísticas históricas ───────────────────────────────────────────

(defn- compute-smart-stats
  "Dado rows históricos y métricas actuales, retorna mapa plano con
   mean_X, std_X y z_score_X para cada alias de métrica."
  [hist-rows metrics current-metric-map]
  (reduce
   (fn [acc m]
     (let [fn-kw    (or (:aggregation m) :COUNT)
           attr-kw  (when-let [a (:attribute m)] (keyword a))
           alias    (keyword (or (:name m)
                                 (str (str/lower-case (name fn-kw)) "_"
                                      (if attr-kw (name attr-kw) "total"))))
           nums     (filterv number?
                     (if attr-kw (mapv #(get % attr-kw) hist-rows) []))
           n        (count nums)
           mean-v   (when (pos? n) (/ (double (reduce + nums)) n))
           var-v    (when (and mean-v (> n 1))
                      (/ (reduce (fn [a x] (+ a (Math/pow (- x mean-v) 2))) 0.0 nums)
                         (dec n)))
           std-v    (when var-v (Math/sqrt var-v))
           curr-v   (get current-metric-map alias)
           z-score  (when (and curr-v mean-v std-v (pos? std-v))
                      (/ (- (double curr-v) mean-v) std-v))]
       (cond-> acc
         mean-v  (assoc (keyword (str "mean_" (name alias))) mean-v)
         std-v   (assoc (keyword (str "std_"  (name alias))) std-v)
         z-score (assoc (keyword (str "z_score_" (name alias))) z-score))))
   {}
   (or metrics [])))

;; ── API pública ───────────────────────────────────────────────────────────────

(defn run-comparisons
  "Ejecuta todas las AnalyticalComparison y retorna un mapa plano fusionable
   con las métricas actuales — sin modificar las filas originales.

   Parámetros:
     db              — snapshot Datahike (@conn)
     base-clauses    — WHERE + hierarchy clauses (SIN time-frame)
     in-sym->val     — bindings :in para sets/listas
     pull-pattern    — pull spec (mismo que query principal)
     metrics         — [MetricDefinition] (igual que en query principal)
     comparisons     — [AnalyticalComparison] del AST IR (contrato §4)
     resolved-tf     — {:start-ts :end-ts} | nil — período actual
     current-metrics — {:alias value} — apply-metrics del período actual

   Retorna mapa plano (merge-able con current-metrics):
     {:current_sum_revenue 1234      ← métrica actual renombrada (si hay comparaciones)
      :prev_0_sum_revenue  1100      ← TIME_SHIFT comparison 0
      :prev_1_sum_revenue  950       ← TIME_SHIFT comparison 1
      :benchmark_target    1000.0    ← BENCHMARK
      :mean_sum_revenue    1050.0    ← SMART
      :std_sum_revenue     80.0
      :z_score_sum_revenue 2.3}"
  [db base-clauses in-sym->val pull-pattern metrics comparisons resolved-tf current-metrics ts-field]
  (let [tf (or resolved-tf {})]
    (reduce
     (fn [result-map [idx comp]]
       (let [lbl (or (:label comp) (str "comp_" idx))]
         (case (:type comp)

           ;; ── TIME_SHIFT → query con período desplazado ──────────────────
           (:TIME_SHIFT_RELATIVE :TIME_SHIFT_SHORTCUT :TIME_SHIFT_ABSOLUTE)
           (let [period (comparison->period comp tf)]
             (if period
               (let [prev-rows (run-shifted-query db base-clauses in-sym->val pull-pattern
                                                  (:prev-start period) (:prev-end period) ts-field)
                     prev-m    (agg/apply-metrics prev-rows metrics)
                     prefix    (str "prev_" idx "_")]
                 (into result-map
                       (map (fn [[k v]] [(keyword (str prefix (name k))) v]) prev-m)))
               (do (log/warn "[Aegis Cmp] No se pudo resolver período para:" (:type comp) lbl)
                   result-map)))

           ;; ── BENCHMARK → columna inline (sin query) ─────────────────────
           :BENCHMARK
           (let [bk-key (keyword (str "benchmark_" (or lbl "value")))]
             (assoc result-map bk-key (double (or (:benchmark-value comp) 0.0))))

           ;; ── SMART → histórico 90d → z-score ───────────────────────────
           :SMART
           (let [cs         (or (:start-ts tf) 0)
                 hist-start (- cs (* 90 86400))
                 hist-rows  (run-shifted-query db base-clauses in-sym->val pull-pattern
                                               hist-start cs ts-field)
                 stats      (compute-smart-stats hist-rows metrics current-metrics)]
             (into result-map stats))

           ;; Tipo desconocido
           (do (log/warn "[Aegis Cmp] AnalyticalComparison tipo desconocido:" (:type comp))
               result-map))))
     ;; Si hay comparaciones TIME_SHIFT, renombramos las métricas actuales a current_X
     (if (some #(#{:TIME_SHIFT_RELATIVE :TIME_SHIFT_SHORTCUT :TIME_SHIFT_ABSOLUTE} (:type %))
               comparisons)
       (into {} (map (fn [[k v]] [(keyword (str "current_" (name k))) v]) current-metrics))
       current-metrics)
     (map-indexed vector comparisons))))
