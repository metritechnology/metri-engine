(ns metri.infrastructure.athena-test
  "Matriz TDD — ATH-01..10
   Tests unitarios con InMemoryQueryEngine stub. Cero I/O real.
   Valida:
     - Contratos Railway start-query! / get-query-results ([:ok] / [:error {:code ...}])
     - Clasificación de códigos INFRA_ATHENA_001..005
     - Retryability y context invariants"
  (:require [clojure.test :refer [deftest is testing are]]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]))

;; ─── Stub canónico — InMemoryQueryEngine ─────────────────────────────────────

(defrecord InMemoryQueryEngine [queries-atom]
  proto/IQueryEngine
  (start-query! [_ sql _database]
    (let [id (str "exec-" (hash sql))]
      (swap! queries-atom assoc id {:sql sql :status :SUCCEEDED})
      [:ok {:execution-id id}]))
  (get-query-results [_ execution-id]
    (if-let [q (get @queries-atom execution-id)]
      [:ok {:execution-id execution-id
            :columns      ["col_a" "col_b"]
            :rows         [["val1" "val2"]]}]
      (errors/error :INFRA_ATHENA_002 {:execution_id execution-id
                                        :cause        "execution-id not found in stub"}))))

(defn make-stub [] (->InMemoryQueryEngine (atom {})))

;; ─── ATH-01: start-query! happy path ────────────────────────────────────────

(deftest ath-01-start-query-ok
  "ATH-01 — start-query! exitoso retorna [:ok {:execution-id str}]"
  (let [engine     (make-stub)
        [tag body]  (proto/start-query! engine "SELECT 1" "analytics")]
    (is (= :ok tag))
    (is (string? (:execution-id body)))))

;; ─── ATH-02: get-query-results happy path ────────────────────────────────────

(deftest ath-02-get-results-ok
  "ATH-02 — get-query-results exitoso retorna [:ok {:columns [...] :rows [...]}]"
  (let [engine     (make-stub)
        [_ {:keys [execution-id]}] (proto/start-query! engine "SELECT 1" "db")
        [tag body]  (proto/get-query-results engine execution-id)]
    (is (= :ok tag))
    (is (vector? (:columns body)))
    (is (vector? (:rows body)))))

;; ─── ATH-03: start-query! → INFRA_ATHENA_001 ────────────────────────────────

(deftest ath-03-start-query-fails
  "ATH-03 — StartQueryExecution SDK exception → [:error {:code :INFRA_ATHENA_001}]"
  (let [failing (reify proto/IQueryEngine
                  (start-query! [_ _ _]
                    (errors/error :INFRA_ATHENA_001
                                  {:workgroup "metri-analytics"
                                   :database  "default"
                                   :cause     "ConnectionRefused"}))
                  (get-query-results [_ _] [:ok {}]))
        [tag body] (proto/start-query! failing "SELECT 1" "db")]
    (is (= :error tag))
    (is (= :INFRA_ATHENA_001 (:code body)))
    (is (true? (:retryable? body)))))

;; ─── ATH-04: get-query-results → INFRA_ATHENA_002 ───────────────────────────

(deftest ath-04-get-results-fails
  "ATH-04 — GetQueryResults SDK exception → [:error {:code :INFRA_ATHENA_002}]"
  (let [[tag body] (errors/error :INFRA_ATHENA_002
                                  {:execution_id "exec-abc"
                                   :cause        "S3 output bucket not accessible"})]
    (is (= :error tag))
    (is (= :INFRA_ATHENA_002 (:code body)))
    (is (true? (:retryable? body)))))

;; ─── ATH-05: FAILED query state → INFRA_ATHENA_003 ──────────────────────────

(deftest ath-05-query-failed-state
  "ATH-05 — Query en estado FAILED → [:error {:code :INFRA_ATHENA_003}]"
  (let [[tag body] (errors/error :INFRA_ATHENA_003
                                  {:execution_id        "exec-abc"
                                   :state_change_reason "Table 'metrics' does not exist"})]
    (is (= :error tag))
    (is (= :INFRA_ATHENA_003 (:code body)))
    (is (false? (:retryable? body)) "SQL FAILED no es reintentable (fix the query)")))

;; ─── ATH-06: Timeout → INFRA_ATHENA_004 ─────────────────────────────────────

(deftest ath-06-query-timeout
  "ATH-06 — Polling timeout → [:error {:code :INFRA_ATHENA_004}]"
  (let [[tag body] (errors/error :INFRA_ATHENA_004
                                  {:execution_id "exec-long"
                                   :timeout_ms   10000})]
    (is (= :error tag))
    (is (= :INFRA_ATHENA_004 (:code body)))
    (is (false? (:retryable? body)) "Timeout no es reintentable inmediatamente (async reconciliation)")))

;; ─── ATH-07: AccessDenied → INFRA_ATHENA_005 ────────────────────────────────

(deftest ath-07-access-denied
  "ATH-07 — AccessDeniedException → [:error {:code :INFRA_ATHENA_005}]"
  (let [[tag body] (errors/error :INFRA_ATHENA_005
                                  {:workgroup "metri-analytics"
                                   :iam_arn   "arn:aws:sts::982592308819:assumed-role/LambdaRole/fn"})]
    (is (= :error tag))
    (is (= :INFRA_ATHENA_005 (:code body)))
    (is (false? (:retryable? body)) "IAM AccessDenied NO es reintentable")))

;; ─── ATH-08: Liskov ──────────────────────────────────────────────────────────

(deftest ath-08-satisfies-protocol
  "ATH-08 — Liskov: InMemoryQueryEngine satisfies IQueryEngine"
  (is (satisfies? proto/IQueryEngine (make-stub))))

;; ─── ATH-09: Retryability invariants ─────────────────────────────────────────

(deftest ath-09-retryability-invariants
  "ATH-09 — Invariantes de retryability para todos los INFRA_ATHENA_*"
  (are [code ctx retryable?] (= retryable? (:retryable? (second (errors/error code ctx))))
    :INFRA_ATHENA_001 {:workgroup "w" :database "d" :cause "err"}      true
    :INFRA_ATHENA_002 {:execution_id "e" :cause "err"}                 true
    :INFRA_ATHENA_003 {:execution_id "e" :state_change_reason "err"}   false
    :INFRA_ATHENA_004 {:execution_id "e" :timeout_ms 10000}            false
    :INFRA_ATHENA_005 {:workgroup "w" :iam_arn "arn:x"}                false))

;; ─── ATH-10: Railway shape all codes ─────────────────────────────────────────

(deftest ath-10-railway-shape-all-codes
  "ATH-10 — Todos los INFRA_ATHENA_* retornan [:error {:code ...}] Railway"
  (are [code ctx] (let [[tag body] (errors/error code ctx)]
                    (and (= :error tag) (= code (:code body))))
    :INFRA_ATHENA_001 {:workgroup "w" :database "d" :cause "x"}
    :INFRA_ATHENA_002 {:execution_id "e" :cause "x"}
    :INFRA_ATHENA_003 {:execution_id "e" :state_change_reason "x"}
    :INFRA_ATHENA_004 {:execution_id "e" :timeout_ms 5000}
    :INFRA_ATHENA_005 {:workgroup "w" :iam_arn "arn:x"}))
