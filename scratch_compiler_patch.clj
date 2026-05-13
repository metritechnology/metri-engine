(defn infer-required-fields
  "Infiere el subconjunto mínimo de atributos a extraer (pull) basado en las
   dimensiones, métricas, y ordenamientos del AST IR.
   Esto evita ejecutar (pull ?e [*]) en consultas analíticas (KPI/PIE/TIMESERIES),
   reduciendo la latencia P50 de ~900ms a ~50ms al no tener que deserializar toda la entidad."
  [ast-ir ts-field]
  (let [extract-kw (fn [obj k] (when-let [v (get obj k)] (keyword (name v))))
        metric-kws (mapcat (fn [m] [(extract-kw m :attribute) (extract-kw m :secondary-attribute)]) (:metrics ast-ir))
        dim-kws    (map (fn [d] (extract-kw d :attribute)) (:dimensions ast-ir))
        sort-kws   (map (fn [s] (extract-kw s :attribute)) (:order-by ast-ir))
        base-kws   [:db/id :entity/type :tenant/id :entity/ulid :meta/created_at ts-field]
        all-kws    (set (remove nil? (concat metric-kws dim-kws sort-kws base-kws)))]
    (vec all-kws)))
