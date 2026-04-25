(ns metri.domain.result-test
  "Tests del módulo domain/pipeline/result.clj
   Funciones puras Railway: ok? error? unwrap — sin I/O, sin dependencias externas."
  (:require [clojure.test :refer [deftest is testing are]]
            [metri.domain.pipeline.result :as r]))

(deftest result-ok?
  "ok? retorna true para [:ok …], false para [:error …]"
  (are [input expected] (= expected (r/ok? input))
    [:ok {:data 1}]        true
    [:ok nil]              true
    [:ok]                  true
    [:error {:code :E001}] false
    [:error]               false
    nil                    false))

(deftest result-error?
  "error? retorna true para [:error …], false para [:ok …]"
  (are [input expected] (= expected (r/error? input))
    [:error {:code :E001}] true
    [:error]               true
    [:ok {:data 1}]        false
    [:ok]                  false
    nil                    false))

(deftest result-unwrap-ok
  "unwrap retorna el body de [:ok body]"
  (is (= {:ulid "U1"} (r/unwrap [:ok {:ulid "U1"}])))
  (is (= nil          (r/unwrap [:ok nil])))
  (is (= 42           (r/unwrap [:ok 42]))))

(deftest result-unwrap-error-lanza
  "unwrap en [:error …] lanza ExceptionInfo"
  (is (thrown? clojure.lang.ExceptionInfo
               (r/unwrap [:error {:code :E001}]))))

(deftest result-railway-composicion
  "Los helpers permiten composición Railway idiomática"
  (let [step1 (fn [x] (if (pos? x) [:ok (* x 2)] [:error {:code :NEGATIVE}]))
        step2 (fn [x] (if (< x 100) [:ok (str "result=" x)] [:error {:code :TOO_LARGE}]))]
    (let [r1 (step1 5)]
      (is (r/ok? r1))
      (let [r2 (step2 (r/unwrap r1))]
        (is (r/ok? r2))
        (is (= "result=10" (r/unwrap r2)))))
    (let [r1 (step1 -1)]
      (is (r/error? r1))
      (is (= :NEGATIVE (:code (second r1)))))))
