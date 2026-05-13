(ns metri.janus.batch-enricher
  "Enriquecedor de queries pre-compilación.
   SRP: aplica BatchContext.common-filters y DashboardCrossFilterContext
        al mapa de sub-queries antes de que el AST compiler los procese.

   Principios:
     • Pura transformación de datos — sin I/O, sin estado.
     • Guardia Dominial (cross-filter): best-effort — errores por campo
       desconocido se encapsulan al nivel del sub-query en el pipeline."
  (:require [clojure.string :as str]))

;; ═══════════════════════════════════════════════════════════════════════════
;; BatchContext — common-filters + common-entity
;; ═══════════════════════════════════════════════════════════════════════════

(defn apply-batch-context
  "Mergea el BatchContext en cada sub-query del mapa.

   common-filters → prepended a los :filters de cada sub-query
                    (los filtros globales toman precedencia de orden).
   common-entity  → heredado si la sub-query no tiene :entity propia.

   No hace nada si batch-ctx es nil."
  [queries batch-ctx]
  (if (nil? batch-ctx)
    queries
    (let [common-filters (or (:common_filters batch-ctx) [])
          common-entity  (:common_entity batch-ctx)]
      (reduce-kv
        (fn [m qk qm]
          (assoc m qk
                 (cond-> qm
                   (seq common-filters)
                   (update :filters #(into (vec common-filters) (or % [])))
                   (and common-entity (str/blank? (str (:entity qm))))
                   (assoc :entity common-entity))))
        {}
        queries))))

;; ═══════════════════════════════════════════════════════════════════════════
;; DashboardCrossFilterContext — inyección cross-chart best-effort
;; ═══════════════════════════════════════════════════════════════════════════

(defn apply-cross-filter
  "Inyecta los cross-filters del DashboardCrossFilterContext en cada sub-query.

   Guardia Dominial: la inyección es best-effort — si el ast-compiler rechaza
   un filtro por unknown-attribute (la entidad no tiene esa columna transaccional),
   el error queda encapsulado como chunk de error solo para ese query-key;
   los demás sub-queries del batch continúan sin interrupción.

   No hace nada si cross-filter es nil o no tiene :cross-filters."
  [queries cross-filter]
  (if (or (nil? cross-filter) (empty? (:cross_filters cross-filter)))
    queries
    (let [cross-filters (:cross_filters cross-filter)]
      (reduce-kv
        (fn [m qk qm]
          (assoc m qk
                 (update qm :filters #(into (vec (or % [])) cross-filters))))
        {}
        queries))))
