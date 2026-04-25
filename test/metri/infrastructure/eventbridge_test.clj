(ns metri.infrastructure.eventbridge-test
  "Matriz TDD — EVB-01..09 (01.02 Módulo XIII)
   Tests unitarios con InMemoryEventBus stub. Cero I/O."
  (:require [clojure.test :refer [deftest is testing are]]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]))

;; ─── Stub canónico — InMemoryEventBus ────────────────────────────────────────

(defrecord InMemoryEventBus [events-atom]
  proto/IEventBus
  (put-event! [_ bus-name source detail-type detail]
    (swap! events-atom conj {:bus-name    bus-name
                             :source      source
                             :detail-type detail-type
                             :detail      detail})
    [:ok {:event-id (str "stub-evt-" (count @events-atom))}]))

(defn make-stub [] (->InMemoryEventBus (atom [])))

;; ─── EVB-01..05 ──────────────────────────────────────────────────────────────

(deftest evb-01-put-event-ok
  "EVB-01 — put-event! happy path retorna [:ok {:event-id ...}]"
  (let [bus        (make-stub)
        [tag body]  (proto/put-event! bus "metri-faults" "metri-engine" "DOMAIN_FAULT_DETECTED" {:code :JNS_001})]
    (is (= :ok tag))
    (is (string? (:event-id body)))))

(deftest evb-02-records-events
  "EVB-02 — put-event! acumula los eventos en el stub"
  (let [bus (make-stub)]
    (proto/put-event! bus "bus" "src" "EVT_A" {:a 1})
    (proto/put-event! bus "bus" "src" "EVT_B" {:b 2})
    (is (= 2 (count @(:events-atom bus))))))

(deftest evb-03-put-event-fails
  "EVB-03 — SDK lanza → [:error {:code :INFRA_EVENTBRIDGE_001}]"
  (let [failing-bus (reify proto/IEventBus
                      (put-event! [_ _ _ _ _]
                        [:error {:code :INFRA_EVENTBRIDGE_001 :detail "Timeout"}]))
        [tag body]  (proto/put-event! failing-bus "bus" "src" "FAULT" {:x 1})]
    (is (= :error tag))
    (is (= :INFRA_EVENTBRIDGE_001 (:code body)))))

(deftest evb-04-satisfies-protocol
  "EVB-04 — Liskov: InMemoryEventBus satisfies IEventBus"
  (is (satisfies? proto/IEventBus (make-stub))))

(deftest evb-05-railway-shape
  "EVB-05 — Respuesta es tuple Railway [:ok|:error ...]"
  (let [bus    (make-stub)
        result  (proto/put-event! bus "bus" "src" "FAULT" {:msg "test"})]
    (is (vector? result))
    (is (#{:ok :error} (first result)))))

;; ─── EVB-06: Entry-level reject → INFRA_EVENTBRIDGE_002 ─────────────────────

(deftest evb-06-entry-level-error
  "EVB-06 — PutEvents OK pero entry ErrorCode → [:error {:code :INFRA_EVENTBRIDGE_002}]"
  (let [[tag body] (errors/error :INFRA_EVENTBRIDGE_002
                                  {:bus_name      "metri-faults"
                                   :error_code    "InternalFailure"
                                   :error_message "EventBridge rejected the entry"})]
    (is (= :error tag))
    (is (= :INFRA_EVENTBRIDGE_002 (:code body)))
    (is (true? (:retryable? body)) "Entry-level reject ES reintentable")))

;; ─── EVB-07: AccessDenied → INFRA_EVENTBRIDGE_003 ───────────────────────────

(deftest evb-07-access-denied
  "EVB-07 — AccessDeniedException → [:error {:code :INFRA_EVENTBRIDGE_003}] (no reintentable)"
  (let [[tag body] (errors/error :INFRA_EVENTBRIDGE_003
                                  {:bus_name "metri-faults"
                                   :iam_arn  "arn:aws:sts::982592308819:assumed-role/LambdaRole/fn"})]
    (is (= :error tag))
    (is (= :INFRA_EVENTBRIDGE_003 (:code body)))
    (is (false? (:retryable? body)) "IAM AccessDenied NO es reintentable")))

;; ─── EVB-08: Retryability invariants ─────────────────────────────────────────

(deftest evb-08-retryability-invariants
  "EVB-08 — Invariantes de retryability para todos los INFRA_EVENTBRIDGE_*"
  (are [code ctx expected] (= expected (:retryable? (second (errors/error code ctx))))
    :INFRA_EVENTBRIDGE_001 {:bus_name "b" :source "s" :detail_type "d" :anomaly {}} true
    :INFRA_EVENTBRIDGE_002 {:bus_name "b" :error_code "x" :error_message "y"}        true
    :INFRA_EVENTBRIDGE_003 {:bus_name "b" :iam_arn "arn:x"}                          false))

;; ─── EVB-09: Context incluido en error ───────────────────────────────────────

(deftest evb-09-error-includes-bus-name
  "EVB-09 — Todos los INFRA_EVENTBRIDGE_* incluyen :bus_name en contexto"
  (doseq [[code ctx] [[:INFRA_EVENTBRIDGE_001 {:bus_name "b" :source "s" :detail_type "d" :anomaly {}}]
                      [:INFRA_EVENTBRIDGE_002 {:bus_name "b" :error_code "x" :error_message "y"}]
                      [:INFRA_EVENTBRIDGE_003 {:bus_name "b" :iam_arn "arn:x"}]]]
    (let [[_ body] (errors/error code ctx)]
      (is (= "b" (:bus_name body)) (str code " debe incluir :bus_name")))))
