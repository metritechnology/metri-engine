(ns metri.infrastructure.kinesis-test
  "Matriz TDD — KNS-01..09
   Tests unitarios con InMemoryStreamWriter stub. Cero I/O real.
   Valida:
     - Contratos Railway put-record! ([:ok] / [:error {:code ...}])
     - Clasificación de códigos INFRA_KINESIS_001..003
     - Retryability invariants"
  (:require [clojure.test :refer [deftest is testing are]]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]))

;; ─── Stub canónico — InMemoryStreamWriter ────────────────────────────────────

(defrecord InMemoryStreamWriter [records-atom]
  proto/IStreamWriter
  (put-record! [_ stream-name _partition-key data]
    (let [id (str "seq-" (swap! records-atom conj {:stream stream-name :data data})
                         (count @records-atom))]
      [:ok {:sequence-number id}])))

(defn make-stub [] (->InMemoryStreamWriter (atom [])))

;; ─── KNS-01: put-record! happy path ─────────────────────────────────────────

(deftest kns-01-put-record-ok
  "KNS-01 — put-record! exitoso retorna [:ok {:sequence-number str}]"
  (let [w          (make-stub)
        [tag body]  (proto/put-record! w "metri-olap-events" "pk-1" {:event-type :ENTITY_CREATED})]
    (is (= :ok tag))
    (is (string? (:sequence-number body)))))

;; ─── KNS-02: put-record! acumula ─────────────────────────────────────────────

(deftest kns-02-records-stored-in-stub
  "KNS-02 — put-record! acumula los registros enviados"
  (let [w (make-stub)]
    (proto/put-record! w "stream-a" "pk" {:n 1})
    (proto/put-record! w "stream-a" "pk" {:n 2})
    (is (= 2 (count @(:records-atom w))))))

;; ─── KNS-03: put-record! → INFRA_KINESIS_001 ─────────────────────────────────

(deftest kns-03-put-record-general-error
  "KNS-03 — SDK error genérico → [:error {:code :INFRA_KINESIS_001}]"
  (let [failing (reify proto/IStreamWriter
                  (put-record! [_ _ _ _]
                    (errors/error :INFRA_KINESIS_001
                                  {:stream_name "metri-olap-events"
                                   :anomaly     {:cognitect.anomalies/category :cognitect.anomalies/fault
                                                 :cognitect.aws.error/code     "InternalServerError"}})))
        [tag body] (proto/put-record! failing "metri-olap-events" "pk" {:x 1})]
    (is (= :error tag))
    (is (= :INFRA_KINESIS_001 (:code body)))
    (is (true? (:retryable? body)) "Error general ES reintentable")))

;; ─── KNS-04: put-record! → INFRA_KINESIS_002 (ServiceUnavailable) ────────────

(deftest kns-04-put-record-service-unavailable
  "KNS-04 — ServiceUnavailableException → [:error {:code :INFRA_KINESIS_002}]"
  (let [[tag body] (errors/error :INFRA_KINESIS_002 {:stream_name "metri-olap-events"})]
    (is (= :error tag))
    (is (= :INFRA_KINESIS_002 (:code body)))
    (is (true? (:retryable? body)) "Service unavailable ES reintentable")))

;; ─── KNS-05: put-record! → INFRA_KINESIS_003 (AccessDenied) ─────────────────

(deftest kns-05-put-record-access-denied
  "KNS-05 — AccessDeniedException → [:error {:code :INFRA_KINESIS_003}]"
  (let [[tag body] (errors/error :INFRA_KINESIS_003
                                  {:stream_name "metri-olap-events"
                                   :iam_arn     "arn:aws:sts::982592308819:assumed-role/LambdaRole/fn"})]
    (is (= :error tag))
    (is (= :INFRA_KINESIS_003 (:code body)))
    (is (false? (:retryable? body)) "AccessDenied NO es reintentable")))

;; ─── KNS-06: Liskov ──────────────────────────────────────────────────────────

(deftest kns-06-satisfies-protocol
  "KNS-06 — Liskov: InMemoryStreamWriter satisfies IStreamWriter"
  (is (satisfies? proto/IStreamWriter (make-stub))))

;; ─── KNS-07: Railway shape ───────────────────────────────────────────────────

(deftest kns-07-railway-shape
  "KNS-07 — put-record! retorna tuple Railway válida"
  (let [w      (make-stub)
        result  (proto/put-record! w "s" "pk" {:data "x"})]
    (is (vector? result))
    (is (#{:ok :error} (first result)))))

;; ─── KNS-08: Retryability invariants ─────────────────────────────────────────

(deftest kns-08-retryability-invariants
  "KNS-08 — INFRA_KINESIS_001/002 reintentables, 003 no"
  (are [code ctx retryable?] (= retryable? (:retryable? (second (errors/error code ctx))))
    :INFRA_KINESIS_001 {:stream_name "s" :anomaly {}}              true
    :INFRA_KINESIS_002 {:stream_name "s"}                          true
    :INFRA_KINESIS_003 {:stream_name "s" :iam_arn "arn:x"}         false))

;; ─── KNS-09: Context incluido en el error ────────────────────────────────────

(deftest kns-09-error-includes-stream-name
  "KNS-09 — Todos los errores INFRA_KINESIS_* incluyen :stream_name en contexto"
  (doseq [code [:INFRA_KINESIS_001 :INFRA_KINESIS_002]]
    (let [[_ body] (errors/error code {:stream_name "metri-olap-events" :anomaly {}})]
      (is (= "metri-olap-events" (:stream_name body))
          (str code " debe incluir :stream_name")))))
