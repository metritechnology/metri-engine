(ns janus-tester-test
  (:require [clojure.test :refer [deftest is testing]]
            [janus-tester :as jt]))

(defn base-query
  []
  {:tenant-id "tenant_a"
   :queries
   {:q1 {:ast {:entity :work_order
               :metrics [{:metric/attribute :total_cost
                          :metric/aggregation :sum}]
               :dimensions [{:dimension/attribute :status}]
               :time-frame {:time/type :last-n-days
                            :time/n-value 7
                            :time/timezone "America/Bogota"}}
         :meta {:viz "table" :output-cast "table"}}}})

(deftest output-cast-guards
  (testing "PIE sin dimension debe fallar"
    (let [payload (assoc-in (base-query) [:queries :q1 :ast :dimensions] [])
          result (jt/validate-request-guards (assoc-in payload [:queries :q1 :meta :output-cast] "pie"))]
      (is (false? (:ok? result)))
      (is (= :OUTPUT_CAST_VIOLATION (:error-code result)))))

  (testing "TIMESERIES con contexto temporal pasa"
    (let [result (jt/validate-request-guards (assoc-in (base-query) [:queries :q1 :meta :output-cast] "timeseries"))]
      (is (true? (:ok? result)))))

  (testing "KPI con dimensiones debe fallar"
    (let [result (jt/validate-request-guards (assoc-in (base-query) [:queries :q1 :meta :output-cast] "kpi"))]
      (is (false? (:ok? result)))
      (is (= :OUTPUT_CAST_VIOLATION (:error-code result))))))

(deftest security-and-depth-guards
  (testing "Formula demasiado larga dispara DSL_DEPTH_LIMIT"
    (let [long-formula (apply str (repeat 600 "A"))
          payload (assoc-in (base-query) [:queries :q1 :ast :measures]
                            [{:name "bad" :formula long-formula}])
          result (jt/validate-request-guards payload)]
      (is (false? (:ok? result)))
      (is (= :DSL_DEPTH_LIMIT (:error-code result)))))

  (testing "Patron peligroso en formula dispara DSL_SECURITY_VIOLATION"
    (let [payload (assoc-in (base-query) [:queries :q1 :ast :measures]
                            [{:name "bad" :formula "SUM(cost); DROP TABLE users"}])
          result (jt/validate-request-guards payload)]
      (is (false? (:ok? result)))
      (is (= :DSL_SECURITY_VIOLATION (:error-code result))))))

(deftest canonical-error-codes-are-declared
  (is (contains? jt/canonical-dsl-error-codes :DSL_SYNTAX_ERROR))
  (is (contains? jt/canonical-dsl-error-codes :TYPE_MISMATCH))
  (is (contains? jt/canonical-dsl-error-codes :DSL_SECURITY_VIOLATION))
  (is (contains? jt/canonical-dsl-error-codes :DSL_DEPTH_LIMIT))
  (is (contains? jt/canonical-dsl-error-codes :OUTPUT_CAST_VIOLATION)))
