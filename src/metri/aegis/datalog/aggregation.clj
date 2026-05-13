(ns metri.aegis.datalog.aggregation
  "MetricDefinition → agregación en memoria sobre resultados pull Datahike.
   SRP: computar métricas sobre rows — sin I/O, sin estado.

   14 AggregationFunction del contrato §1:
     COUNT SUM AVG MIN MAX          → reducción directa sobre colección
     MEDIAN PERCENTILE_90/95/99     → sort + índice percentílico (nearest-rank)
     STD_DEV VARIANCE               → varianza muestral (denominador n-1)
     CORRELATION                    → Pearson r sobre dos columnas
     LINEAR_REGRESSION              → slope (regr_slope) mínimos cuadrados
     LOGISTIC_REGRESSION            → COUNT fallback (no nativo en JVM sin deps ML)

   Filtered aggregation (MetricDefinition.filter §4):
     Si MetricDefinition tiene :filter, solo los rows que pasan el predicado
     in-memory contribuyen al cómputo. El predicado se evalúa directamente
     sobre el mapa de row pull — sin consulta Datahike adicional.

   Diseño:
     - node->pred  : FilterNode AST → (fn [row-map] bool)  ← evaluador in-memory
     - compute-agg : keyword × vals × sec-vals → number
     - apply-metrics : rows × [MetricDefinition] → {:alias value ...}"
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]))

;; ── Predicado in-memory para filtered aggregation ────────────────────────────
;; Evalúa el FilterNode AST sobre un mapa de row (salida pull Datahike).
;; Soporta los operadores escalares y booleanos suficientes para métricas
;; condicionales. Los operadores regex/FTS se degradan a pass (log warn).

(declare node->pred)

(defn- field-kw [f] (keyword f))

(defn- node->pred
  "FilterNode AST IR → predicado (fn [row-map] → bool).
   Retorna (constantly true) para operadores no soportados en memoria."
  [node]
  (when node
    (let [[op & args] node]
      (case op
        :=      (let [[field val] args]
                  #(= (get % (field-kw field)) val))

        :not=   (let [[field val] args]
                  #(not= (get % (field-kw field)) val))

        :>      (let [[field val] args]
                  #(when-let [v (get % (field-kw field))] (> v val)))

        :<      (let [[field val] args]
                  #(when-let [v (get % (field-kw field))] (< v val)))

        :>=     (let [[field val] args]
                  #(when-let [v (get % (field-kw field))] (>= v val)))

        :<=     (let [[field val] args]
                  #(when-let [v (get % (field-kw field))] (<= v val)))

        :in     (let [[field vals] args
                      s (set vals)]
                  #(contains? s (get % (field-kw field))))

        :not-in (let [[field vals] args
                      s (set vals)]
                  #(not (contains? s (get % (field-kw field)))))

        :between (let [[field [lo hi]] args]
                   #(when-let [v (get % (field-kw field))]
                      (and (>= v lo) (<= v hi))))

        :contains (let [[field val] args]
                    #(when-let [v (get % (field-kw field))]
                       (str/includes? (str v) val)))

        :like   (let [[field pattern] args
                      re (re-pattern
                          (str "^"
                               (-> pattern
                                   (str/replace #"([\.\+\*\^\$\{\}\(\)\|\[\]])" "\\\\$1")
                                   (str/replace "%" ".*")
                                   (str/replace "_" "."))
                               "$"))]
                  #(when-let [v (get % (field-kw field))]
                     (boolean (re-find re (str v)))))

        :is-null     (let [[field _] args]
                       #(nil? (get % (field-kw field))))

        :is-not-null (let [[field _] args]
                       #(some? (get % (field-kw field))))

        :and (let [preds (mapv node->pred args)]
               #(every? (fn [p] (p %)) preds))

        :or  (let [preds (mapv node->pred args)]
               #(boolean (some (fn [p] (p %)) preds)))

        :not (let [p (node->pred (first args))]
               #(not (p %)))

        ;; Operadores no evaluables in-memory (regex/FTS) → pass con warn
        (do (log/warn "[Aegis Agg] Operador no soportado en filtered-agg in-memory:"
                      op "— rows no filtradas para esta métrica")
            (constantly true))))))

;; ── Funciones estadísticas auxiliares ────────────────────────────────────────

(defn- safe-mean
  "Media aritmética; nil si colección vacía."
  [nums]
  (when (seq nums)
    (/ (double (reduce + nums)) (count nums))))

(defn- variance-sample
  "Varianza muestral (denominador n-1); nil si < 2 elementos."
  [nums]
  (when (> (count nums) 1)
    (let [m (safe-mean nums)]
      (/ (reduce (fn [acc x] (+ acc (Math/pow (- x m) 2))) 0.0 nums)
         (dec (count nums))))))

(defn- percentile-nearest-rank
  "Percentil nearest-rank sobre colección ya ordenada [0-1]; nil si vacía."
  [sorted-nums p]
  (when (seq sorted-nums)
    (let [n   (count sorted-nums)
          idx (min (dec n) (int (Math/floor (* p n))))]
      (nth sorted-nums idx))))

(defn- pearson-r
  "Coeficiente de correlación de Pearson entre xs e ys (misma longitud).
   Retorna nil si desviación estándar de alguna serie es 0 o < 2 puntos."
  [xs ys]
  (when (and (> (count xs) 1) (= (count xs) (count ys)))
    (let [mx   (safe-mean xs)
          my   (safe-mean ys)
          n    (count xs)
          cov  (/ (reduce + (map (fn [x y] (* (- x mx) (- y my))) xs ys)) (dec n))
          vx   (variance-sample xs)
          vy   (variance-sample ys)]
      (when (and vx vy (pos? vx) (pos? vy))
        (/ cov (Math/sqrt (* vx vy)))))))

(defn- regr-slope
  "Slope de regresión lineal OLS (ys ~ xs). Retorna nil si SS_xx = 0."
  [xs ys]
  (when (and (> (count xs) 1) (= (count xs) (count ys)))
    (let [mx    (safe-mean xs)
          my    (safe-mean ys)
          ss-xy (reduce + (map (fn [x y] (* (- x mx) (- y my))) xs ys))
          ss-xx (reduce + (map (fn [x] (Math/pow (- x mx) 2)) xs))]
      (when (pos? ss-xx)
        (/ ss-xy ss-xx)))))

;; ── Dispatcher de agregación ──────────────────────────────────────────────────

(defn- compute-agg
  "Aplica fn-kw sobre vals (+ sec-vals para bivariadas).
   vals / sec-vals: colecciones de valores raw del row (pueden contener nil).
   Retorna el valor numérico computado o nil si no hay datos suficientes."
  [fn-kw vals sec-vals]
  (let [nums     (filterv number? vals)
        sec-nums (filterv number? (or sec-vals []))
        sorted   (sort nums)]
    (case fn-kw
      :COUNT               (count vals)
      :SUM                 (when (seq nums) (reduce + 0.0 nums))
      :AVG                 (safe-mean nums)
      :MIN                 (first sorted)
      :MAX                 (last sorted)
      :MEDIAN              (percentile-nearest-rank sorted 0.5)
      :PERCENTILE_90       (percentile-nearest-rank sorted 0.9)
      :PERCENTILE_95       (percentile-nearest-rank sorted 0.95)
      :PERCENTILE_99       (percentile-nearest-rank sorted 0.99)
      :STD_DEV             (when-let [v (variance-sample nums)] (Math/sqrt v))
      :VARIANCE            (variance-sample nums)
      :CORRELATION         (pearson-r nums sec-nums)
      :LINEAR_REGRESSION   (regr-slope sec-nums nums)
      :LOGISTIC_REGRESSION (do (log/warn "[Aegis Agg] LOGISTIC_REGRESSION no nativo — COUNT fallback")
                               (count vals))
      ;; Función desconocida → COUNT defensivo
      (do (log/warn "[Aegis Agg] AggregationFunction desconocida:" fn-kw "— COUNT fallback")
          (count vals)))))

;; ── API pública ───────────────────────────────────────────────────────────────

(defn apply-metrics
  "Aplica vector de MetricDefinition sobre rows Datahike ya procesados.

   Parámetros:
     rows    — [{:attr val ...}] salida del executor (mapas keyword)
     metrics — [{:attribute :aggregation :name :filter :secondary-attribute}]

   Retorna mapa {:alias-kw computed-value ...} con una entrada por métrica.

   Filtered aggregation:
     Si MetricDefinition contiene :filter (FilterNode AST), el predicado
     in-memory se evalúa sobre cada row. Solo los rows que pasan contribuyen
     al cómputo de la métrica.

   Alias de la métrica:
     - Usa :name si está presente.
     - Genera «fn_attr» (e.g. «sum_revenue») si no hay :name."
  [rows metrics]
  (reduce
   (fn [acc m]
     (let [fn-kw    (or (:aggregation m) :COUNT)
           attr-kw  (when-let [a (:attribute m)] (keyword a))
           sec-kw   (when-let [a (:secondary-attribute m)] (keyword a))
           alias-kw (keyword (or (:name m)
                                 (str (str/lower-case (name fn-kw))
                                      "_"
                                      (if attr-kw (name attr-kw) "total"))))

           ;; Filtered aggregation — predicado in-memory
           eff-rows (if-let [f (:filter m)]
                      (let [pred (node->pred f)]
                        (filterv pred rows))
                      rows)

           vals     (if attr-kw
                      (mapv #(get % attr-kw) eff-rows)
                      ;; COUNT(*) — usamos los rows como unidad de cuenta
                      eff-rows)
           sec-vals (when sec-kw (mapv #(get % sec-kw) eff-rows))
           result   (compute-agg fn-kw vals sec-vals)]
       (assoc acc alias-kw result)))
   {}
   metrics))
