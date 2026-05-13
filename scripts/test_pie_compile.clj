(ns test-pie-compile
  (:require [metri.aegis.datalog.compiler :as compiler]
            [metri.aegis.datalog.executor :as executor]
            [clojure.pprint :refer [pprint]]))

(def mock-ast-ir
  {:entity "work_order"
   :schema {:attributes [{:name "total_cost" :type "decimal"}
                         {:name "status" :type "string"}]}
   :select [:status]
   :where [:and]
   :limit 100
   :order-by []
   :metrics [{:aggregation :SUM :attribute "total_cost"}]
   :dimensions [{:entity "work_order" :attribute "status"}]
   :output-cast :PIE})

;; Simulación de respuesta de Datahike, simulando "rows" devueltos ANTES de apply-output-cast.
;; Recordemos que el executor.clj quita los namespaces, pero simularemos que la query devuelve:
(def mock-datahike-result
  [
   [#:work_order{:status "OPEN", :total_cost 100}]
   [#:work_order{:status "CLOSED", :total_cost 50}]
   [#:work_order{:status "OPEN", :total_cost 25}]
  ])

;; El executor de datahike hace map first y limpia namespaces:
(def raw-rows
  (mapv (fn [r]
          (into {}
                (keep (fn [[k v]]
                        (when-not (#{:db/id :tenant/id :entity/type} k)
                          [(keyword (if (= k :entity/ulid) "id" (name k))) v]))
                      (first r))))
        mock-datahike-result))

(println "==== RAW ROWS (Post-limpieza de namespaces) ====")
(pprint raw-rows)

(println "\n==== APLICANDO OUTPUT CAST (PIE) ====")
;; executor/apply-output-cast
(def final-result
  (@#'executor/apply-output-cast raw-rows (:metrics mock-ast-ir) (:dimensions mock-ast-ir) (:output-cast mock-ast-ir)))

(pprint final-result)
