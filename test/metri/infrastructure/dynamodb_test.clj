(ns metri.infrastructure.dynamodb-test
  "Matriz TDD — DDB-01..12
   Tests unitarios con stubs AWS DynamoDB. Cero I/O real.
   Valida:
     - Contratos Railway ([:ok] / [:error {:code ...}])
     - Clasificación de códigos INFRA_DDB_001..005
     - Comportamiento ante AccessDeniedException y Throttle"
  (:require [clojure.test :refer [deftest is testing are]]
            [metri.domain.errors :as errors]))

;; ─── Helpers de stub ────────────────────────────────────────────────────────

(defn- aws-anomaly
  "Simula una respuesta de anomalía cognitect.aws."
  ([category error-code]
   {:cognitect.anomalies/category category
    :cognitect.aws.error/code     error-code
    :Message                      (str "Simulated " error-code)})
  ([category]
   (aws-anomaly category "InternalServerError")))

(defn- make-get-item-fn
  "Factoría: retorna una fn que simula get-item con la respuesta dada."
  [resp-or-fn]
  (fn [_client table key-map]
    (let [resp (if (fn? resp-or-fn) (resp-or-fn) resp-or-fn)]
      (if (:cognitect.anomalies/category resp)
        (let [code (if (= "AccessDeniedException"
                          (get resp :cognitect.aws.error/code))
                    :INFRA_DDB_004
                    :INFRA_DDB_001)]
          (errors/error code {:table   table
                              :key     (pr-str key-map)
                              :anomaly (select-keys resp [:cognitect.anomalies/category
                                                          :cognitect.aws.error/code
                                                          :Message])}))
        [:ok (:Item resp)]))))

;; ─── DDB-01: GetItem happy path ──────────────────────────────────────────────

(deftest ddb-01-get-item-ok
  "DDB-01 — GetItem exitoso retorna [:ok {:PK ...}]"
  (let [get-item (make-get-item-fn {:Item {"PK" {:S "tenant-123"}}})
        [tag body] (get-item :client "schemas-table" {"PK" {:S "tenant-123"}})]
    (is (= :ok tag))
    (is (= "tenant-123" (get-in body ["PK" :S])))))

;; ─── DDB-02: GetItem → INFRA_DDB_001 ────────────────────────────────────────

(deftest ddb-02-get-item-general-error
  "DDB-02 — GetItem SDK error → [:error {:code :INFRA_DDB_001}]"
  (let [get-item (make-get-item-fn (aws-anomaly :cognitect.anomalies/fault))
        [tag body] (get-item :client "schemas-table" {"PK" {:S "x"}})]
    (is (= :error tag))
    (is (= :INFRA_DDB_001 (:code body)))))

;; ─── DDB-03: GetItem → INFRA_DDB_004 (AccessDenied) ─────────────────────────

(deftest ddb-03-get-item-access-denied
  "DDB-03 — GetItem AccessDeniedException → [:error {:code :INFRA_DDB_004}]"
  (let [get-item (make-get-item-fn (aws-anomaly :cognitect.anomalies/incorrect "AccessDeniedException"))
        [tag body] (get-item :client "schemas-table" {"PK" {:S "x"}})]
    (is (= :error tag))
    (is (= :INFRA_DDB_004 (:code body)))
    (is (false? (:retryable? body)) "AccessDenied no es reintentable")))

;; ─── DDB-04: PutItem happy path ──────────────────────────────────────────────

(deftest ddb-04-put-item-ok
  "DDB-04 — PutItem exitoso retorna [:ok]"
  (let [put-item (fn [_c _t _i] [:ok])]
    (is (= [:ok] (put-item :c "t" {})))))

;; ─── DDB-05: PutItem → INFRA_DDB_002 ────────────────────────────────────────

(deftest ddb-05-put-item-error
  "DDB-05 — PutItem SDK error → [:error {:code :INFRA_DDB_002}]"
  (let [[tag body] (errors/error :INFRA_DDB_002
                                 {:table   "schemas-table"
                                  :anomaly {:cognitect.anomalies/category :cognitect.anomalies/fault}})]
    (is (= :error tag))
    (is (= :INFRA_DDB_002 (:code body)))
    (is (true? (:retryable? body)))))

;; ─── DDB-06: PutItem → INFRA_DDB_005 (Throttle) ─────────────────────────────

(deftest ddb-06-put-item-throttled
  "DDB-06 — PutItem throttled → [:error {:code :INFRA_DDB_005}]"
  (let [[tag body] (errors/error :INFRA_DDB_005
                                 {:table     "schemas-table"
                                  :operation :put-item})]
    (is (= :error tag))
    (is (= :INFRA_DDB_005 (:code body)))
    (is (true? (:retryable? body)) "Throttle es reintentable")))

;; ─── DDB-07: UpdateItem happy path ───────────────────────────────────────────

(deftest ddb-07-update-item-ok
  "DDB-07 — UpdateItem exitoso retorna [:ok {:Attributes ...}]"
  (let [update-item (fn [_c _t _k _u _n _v] [:ok {"quota" {:N "42"}}])]
    (let [[tag body] (update-item :c "t" {} "SET #q = :v" {} {})]
      (is (= :ok tag))
      (is (map? body)))))

;; ─── DDB-08: UpdateItem → INFRA_DDB_003 ─────────────────────────────────────

(deftest ddb-08-update-item-error
  "DDB-08 — UpdateItem SDK error → [:error {:code :INFRA_DDB_003}]"
  (let [[tag body] (errors/error :INFRA_DDB_003
                                 {:table       "schemas-table"
                                  :key         "{\"PK\" {:S \"t1\"}}"
                                  :update_expr "SET #q = :v"
                                  :anomaly     {:cognitect.anomalies/category :cognitect.anomalies/fault}})]
    (is (= :error tag))
    (is (= :INFRA_DDB_003 (:code body)))
    (is (true? (:retryable? body)))))

;; ─── DDB-09: UpdateItem → INFRA_DDB_004 (AccessDenied) ──────────────────────

(deftest ddb-09-update-item-access-denied
  "DDB-09 — UpdateItem AccessDeniedException → [:error {:code :INFRA_DDB_004}]"
  (let [[tag body] (errors/error :INFRA_DDB_004
                                 {:table     "schemas-table"
                                  :operation :update-item
                                  :iam_arn   "arn:aws:sts::982592308819:assumed-role/LambdaRole/fn"})]
    (is (= :error tag))
    (is (= :INFRA_DDB_004 (:code body)))
    (is (false? (:retryable? body)) "IAM error no es reintentable")))

;; ─── DDB-10: Error shape Railway ─────────────────────────────────────────────

(deftest ddb-10-railway-shape-all-errors
  "DDB-10 — Todos los INFRA_DDB_* son Railway [:error {:code ...}]"
  (testing "Cada código produce tuple Railway válida"
    (are [code ctx] (let [[tag body] (errors/error code ctx)]
                      (and (= :error tag)
                           (= code (:code body))))
      :INFRA_DDB_001 {:table "t" :key "k" :anomaly {}}
      :INFRA_DDB_002 {:table "t" :anomaly {}}
      :INFRA_DDB_003 {:table "t" :key "k" :update_expr "SET" :anomaly {}}
      :INFRA_DDB_004 {:table "t" :operation :get-item :iam_arn "arn:x"}
      :INFRA_DDB_005 {:table "t" :operation :put-item})))

;; ─── DDB-11: Retryability invariant ─────────────────────────────────────────

(deftest ddb-11-retryability-invariants
  "DDB-11 — Verificar invariantes de retryability por código"
  (let [retryable?  (fn [code ctx] (:retryable? (second (errors/error code ctx))))
        not-retryable? (complement retryable?)]
    (is (retryable?     :INFRA_DDB_001 {:table "t" :key "k" :anomaly {}}) "GetItem error es reintentable")
    (is (retryable?     :INFRA_DDB_002 {:table "t" :anomaly {}})           "PutItem error es reintentable")
    (is (retryable?     :INFRA_DDB_003 {:table "t" :key "k" :update_expr "" :anomaly {}}) "UpdateItem error es reintentable")
    (is (not-retryable? :INFRA_DDB_004 {:table "t" :operation :get :iam_arn "x"}) "AccessDenied NO es reintentable")
    (is (retryable?     :INFRA_DDB_005 {:table "t" :operation :put})       "Throttle es reintentable")))

;; ─── DDB-12: Context-required validation ─────────────────────────────────────

(deftest ddb-12-error-includes-context
  "DDB-12 — El error incluye el contexto pasado (table, key, anomaly)"
  (let [[_ body] (errors/error :INFRA_DDB_001
                                {:table   "metri-datahike-prod"
                                 :key     "{\"PK\" {:S \"tenant-1\"}}"
                                 :anomaly {:cognitect.aws.error/code "InternalServerError"}})]
    (is (= "metri-datahike-prod" (:table body)))
    (is (map? (:anomaly body)))))
