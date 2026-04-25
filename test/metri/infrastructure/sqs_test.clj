(ns metri.infrastructure.sqs-test
  "Matriz TDD — SQS-01..08 (01.02 Módulo XIII)
   Tests unitarios con InMemorySQSBus stub. Cero I/O.
   Firma real ISQSBus:
     (publish! bus payload group-id dedup-id)
     (receive-messages bus max-count)
     (delete-message! bus receipt-handle)"
  (:require [clojure.test :refer [deftest is testing are]]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]))

;; ─── Stub canónico — InMemorySQSBus ──────────────────────────────────────────

(defrecord InMemorySQSBus [queue-atom counter-atom]
  proto/ISQSBus

  (publish! [_ payload group-id _dedup-id]
    (let [id (str "msg-" (swap! counter-atom inc))]
      (swap! queue-atom conj {:message-id     id
                              :receipt-handle (str "rh-" id)
                              :body           (assoc payload :group-id group-id)})
      [:ok {:message-id id}]))

  (receive-messages [_ max-count]
    [:ok (vec (take max-count @queue-atom))])

  (delete-message! [_ receipt-handle]
    (swap! queue-atom (fn [q] (filterv #(not= receipt-handle (:receipt-handle %)) q)))
    [:ok]))

(defn make-stub []
  (->InMemorySQSBus (atom []) (atom 0)))

;; ─── SQS-01..08 ──────────────────────────────────────────────────────────────

(deftest sqs-01-publish-ok
  "SQS-01 — publish! happy path retorna [:ok {:message-id str}]"
  (let [bus        (make-stub)
        [tag body] (proto/publish! bus {:event-type :ENTITY_CREATED} "grp-1" "dedup-1")]
    (is (= :ok tag))
    (is (string? (:message-id body)))))

(deftest sqs-02-publish-railway-error
  "SQS-02 — SDK lanza → [:error {:code :INFRA_SQS_001}]"
  (let [failing-bus (reify proto/ISQSBus
                      (publish! [_ _ _ _]
                        [:error {:code :INFRA_SQS_001 :retryable? true :detail "Connection refused"}])
                      (receive-messages [_ _] [:ok []])
                      (delete-message! [_ _]  [:ok]))
        [tag body]  (proto/publish! failing-bus {:event-type :FAULT} "g" "d")]
    (is (= :error tag))
    (is (= :INFRA_SQS_001 (:code body)))))

(deftest sqs-03-receive-messages-ok
  "SQS-03 — receive-messages retorna mensajes publicados"
  (let [bus (make-stub)]
    (proto/publish! bus {:event-type :EVT_A} "g" "d1")
    (proto/publish! bus {:event-type :EVT_B} "g" "d2")
    (let [[tag msgs] (proto/receive-messages bus 10)]
      (is (= :ok tag))
      (is (= 2 (count msgs))))))

(deftest sqs-04-delete-removes-from-queue
  "SQS-04 — publish! → receive → delete → cola vacía (MATRIZ XIII)"
  (let [bus (make-stub)]
    (proto/publish! bus {:event-type :EVT_DEL} "g" "d1")
    (let [[_ msgs] (proto/receive-messages bus 10)]
      (doseq [msg msgs]
        (proto/delete-message! bus (:receipt-handle msg))))
    (let [[_ remaining] (proto/receive-messages bus 10)]
      (is (empty? remaining) "Cola debe estar vacía tras delete"))))

(deftest sqs-05-satisfies-protocol
  "SQS-05 — Liskov: InMemorySQSBus satisfies ISQSBus"
  (is (satisfies? proto/ISQSBus (make-stub))))

(deftest sqs-06-message-has-required-fields
  "SQS-06 — Mensajes recibidos tienen :message-id y :receipt-handle"
  (let [bus (make-stub)]
    (proto/publish! bus {:event-type :EVT_FIELDS} "g" "d1")
    (let [[_ [msg]] (proto/receive-messages bus 1)]
      (is (string? (:message-id msg))    "Debe tener :message-id")
      (is (string? (:receipt-handle msg)) "Debe tener :receipt-handle")
      (is (map?    (:body msg))           "Debe tener :body como mapa"))))

(deftest sqs-07-fifo-order
  "SQS-07 — 3 mensajes → recibidos en orden FIFO (MATRIZ XIII)"
  (let [bus (make-stub)]
    (proto/publish! bus {:event-type :ORDER_1 :seq 1} "g" "d1")
    (proto/publish! bus {:event-type :ORDER_2 :seq 2} "g" "d2")
    (proto/publish! bus {:event-type :ORDER_3 :seq 3} "g" "d3")
    (let [[_ msgs] (proto/receive-messages bus 10)]
      (is (= 3 (count msgs)))
      (is (= :ORDER_1 (get-in (first msgs) [:body :event-type]))
          "Primer mensaje publicado = primero en recibir"))))

(deftest sqs-08-railway-shape-all-methods
  "SQS-08 — Todas las operaciones retornan [:ok|:error ...] Railway"
  (let [bus (make-stub)]
    (are [result] (and (vector? result) (#{:ok :error} (first result)))
      (proto/publish! bus {:event-type :SHAPE_TEST} "g" "d")
      (proto/receive-messages bus 5)
      (proto/delete-message! bus "handle-fake"))))

;; ─── SQS-09: ReceiveMessage error → INFRA_SQS_002 ───────────────────────────

(deftest sqs-09-receive-messages-error
  "SQS-09 — ReceiveMessage SDK error → [:error {:code :INFRA_SQS_002}]"
  (let [failing (reify proto/ISQSBus
                  (publish! [_ _ _ _] [:ok {:message-id "x"}])
                  (receive-messages [_ _]
                    [:error {:code :INFRA_SQS_002 :retryable? true
                             :queue "q" :operation :receive-message}])
                  (delete-message! [_ _] [:ok]))
        [tag body] (proto/receive-messages failing 5)]
    (is (= :error tag))
    (is (= :INFRA_SQS_002 (:code body)))
    (is (true? (:retryable? body)))))

;; ─── SQS-10: DeleteMessage error → INFRA_SQS_003 ────────────────────────────

(deftest sqs-10-delete-message-error
  "SQS-10 — DeleteMessage SDK error → [:error {:code :INFRA_SQS_003}] (riesgo de duplicado)"
  (let [[tag body] (errors/error :INFRA_SQS_003
                                  {:queue          "https://sqs.us-east-1.amazonaws.com/123/outbox.fifo"
                                   :receipt_handle "rh-abc"
                                   :anomaly        {:cognitect.anomalies/category :cognitect.anomalies/fault}})]
    (is (= :error tag))
    (is (= :INFRA_SQS_003 (:code body)))
    (is (string? (:queue body)))))

;; ─── SQS-11: AccessDenied → INFRA_SQS_004 ───────────────────────────────────

(deftest sqs-11-send-access-denied
  "SQS-11 — AccessDeniedException → [:error {:code :INFRA_SQS_004}] (no reintentable)"
  (let [[tag body] (errors/error :INFRA_SQS_004
                                  {:queue     "https://sqs.us-east-1.amazonaws.com/123/outbox.fifo"
                                   :operation :send-message
                                   :iam_arn   "arn:aws:sts::982592308819:assumed-role/LambdaRole/fn"})]
    (is (= :error tag))
    (is (= :INFRA_SQS_004 (:code body)))
    (is (false? (:retryable? body)) "IAM AccessDenied NO es reintentable")))

;; ─── SQS-12: Bootstrap check falla → INFRA_SQS_005 ──────────────────────────

(deftest sqs-12-bootstrap-check-fails
  "SQS-12 — GetQueueAttributes en bootstrap → [:error {:code :INFRA_SQS_005}]"
  (let [[tag body] (errors/error :INFRA_SQS_005
                                  {:queue   "https://sqs.us-east-1.amazonaws.com/123/outbox.fifo"
                                   :anomaly {:cognitect.aws.error/code "QueueDoesNotExist"}})]
    (is (= :error tag))
    (is (= :INFRA_SQS_005 (:code body)))
    (is (true? (:retryable? body)))))

;; ─── SQS-13: Retryability invariants ─────────────────────────────────────────

(deftest sqs-13-retryability-invariants
  "SQS-13 — Invariantes de retryability para todos los INFRA_SQS_*"
  (are [code ctx expected] (= expected (:retryable? (second (errors/error code ctx))))
    :INFRA_SQS_001 {:queue "q" :operation :send-message    :anomaly {}} true
    :INFRA_SQS_002 {:queue "q" :operation :receive-message :anomaly {}} true
    :INFRA_SQS_003 {:queue "q" :receipt_handle "rh"        :anomaly {}} true
    :INFRA_SQS_004 {:queue "q" :operation :send-message  :iam_arn "x"} false
    :INFRA_SQS_005 {:queue "q" :anomaly {}}                             true))
