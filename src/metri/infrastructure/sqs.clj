;; [PORTED_TO_RUST: src/infrastructure/sqs.rs]
;; NO MODIFICAR — fuente de verdad en Rust
(ns metri.infrastructure.sqs
  "Cliente SQS FIFO para el Outbox Pattern (MoiraEmitter).
   Compatible con ElasticMQ localmente y AWS SQS en producción.
   Implementa ISQSBus. Ciclo de vida gestionado por Integrant."
  (:require [integrant.core :as ig]
            [cognitect.aws.client.api :as aws]
            [taoensso.timbre :as log]
            [cheshire.core :as json]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]))

;; ── Implementación ISQSBus ────────────────────────────────────────────────────

(defrecord SQSFifoBus [client queue-url]
  proto/ISQSBus

  (publish! [_ payload group-id dedup-id]
    (let [resp (aws/invoke client
                  {:op      :SendMessage
                   :request {:QueueUrl               queue-url
                             :MessageBody            (json/generate-string payload)
                             :MessageGroupId         group-id
                             :MessageDeduplicationId dedup-id}})]
      (if (:cognitect.anomalies/category resp)
        (let [code (if (= "AccessDeniedException" (get-in resp [:cognitect.aws.error/code]))
                    :INFRA_SQS_004
                    :INFRA_SQS_001)]
          (errors/error code {:queue     queue-url
                               :operation :send-message
                               :anomaly   (select-keys resp [:cognitect.anomalies/category
                                                             :cognitect.aws.error/code
                                                             :Message])}))
        [:ok {:message-id (:MessageId resp)}])))

  (receive-messages [_ max-count]
    (let [resp (aws/invoke client
                  {:op      :ReceiveMessage
                   :request {:QueueUrl            queue-url
                             :MaxNumberOfMessages (min max-count 10) ;; límite AWS
                             :WaitTimeSeconds     5}})]
      (if (:cognitect.anomalies/category resp)
        (let [code (if (= "AccessDeniedException" (get-in resp [:cognitect.aws.error/code]))
                    :INFRA_SQS_004
                    :INFRA_SQS_002)]
          (errors/error code {:queue     queue-url
                               :operation :receive-message
                               :anomaly   (select-keys resp [:cognitect.anomalies/category
                                                             :cognitect.aws.error/code
                                                             :Message])}))
        [:ok (mapv (fn [m] {:receipt-handle (:ReceiptHandle m)
                            :body           (json/parse-string (:Body m) true)})
                   (:Messages resp))])))

  (delete-message! [_ receipt-handle]
    (let [resp (aws/invoke client
                 {:op      :DeleteMessage
                  :request {:QueueUrl      queue-url
                            :ReceiptHandle receipt-handle}})]
      (if (:cognitect.anomalies/category resp)
        (errors/error :INFRA_SQS_003
                      {:queue          queue-url
                       :receipt_handle receipt-handle
                       :anomaly        (select-keys resp [:cognitect.anomalies/category
                                                          :cognitect.aws.error/code
                                                          :Message])})
        [:ok]))))

;; ── Integrant Lifecycle ───────────────────────────────────────────────────────

(defn- parse-endpoint [endpoint-str]
  "Parsea 'http://host:port' usando java.net.URI."
  (when endpoint-str
    (let [uri (java.net.URI. endpoint-str)]
      {:protocol (keyword (.getScheme uri))
       :hostname (.getHost uri)
       :port     (.getPort uri)})))

(defmethod ig/init-key :moira/sqs-bus
  [_ {:keys [region endpoint queue-url]}]
  (log/info "  -> [SQS] Inicializando cliente FIFO | queue:" queue-url
            (when endpoint (str "| endpoint: " endpoint)))
  (let [client-cfg (cond-> {:api :sqs :region region}
                     endpoint (assoc :endpoint-override (parse-endpoint endpoint)))
        client (aws/client client-cfg)]
    ;; Verificar que la cola existe
    (let [resp (aws/invoke client {:op :GetQueueAttributes
                                   :request {:QueueUrl       queue-url
                                             :AttributeNames ["ApproximateNumberOfMessages"]}})]
      (if (:cognitect.anomalies/category resp)
        (log/warn "  -> [SQS] GetQueueAttributes falló (puede ser normal en inicio):" resp)
        (log/info "  -> [SQS] Cola FIFO activa | msgs ~"
                  (get-in resp [:Attributes "ApproximateNumberOfMessages"]))))
    (->SQSFifoBus client queue-url)))

(defmethod ig/halt-key! :moira/sqs-bus
  [_ bus]
  (log/info "  -> [SQS] Cerrando cliente")
  nil)
