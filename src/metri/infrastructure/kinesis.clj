(ns metri.infrastructure.kinesis
  "Implementación del IStreamWriter para AWS Kinesis Firehose.
   Desacoplado de la lógica de dominio (SOLID).
   Toda excepción o anomalía de red se traduce a un [:error ...]."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [cognitect.aws.client.api :as aws]
            [metri.domain.protocols :as proto]
            [cheshire.core :as json]
            [metri.domain.errors :as errors]))

(defrecord KinesisFirehoseWriter [client]
  proto/IStreamWriter
  (put-record! [_ stream-name partition-key data]
    (try
      (let [payload (str (json/generate-string data) "\n")
            _       (log/info "Kinesis payload:" payload)
            req     {:op      :PutRecord
                     :request {:DeliveryStreamName stream-name
                               :Record {:Data (.getBytes payload "UTF-8")}}}
            resp    (aws/invoke client req)]
        (if (:cognitect.anomalies/category resp)
          (let [code (cond
                      (= "AccessDeniedException" (get-in resp [:cognitect.aws.error/code])) :INFRA_KINESIS_003
                      (= "ServiceUnavailableException" (get-in resp [:cognitect.aws.error/code])) :INFRA_KINESIS_002
                      :else :INFRA_KINESIS_001)]
            (log/error "Fallo en Firehose PutRecord:" resp)
            (errors/error code {:stream_name stream-name
                                :anomaly     (select-keys resp [:cognitect.anomalies/category
                                                                :cognitect.aws.error/code
                                                                :Message])}))
          [:ok {:sequence-number (:RecordId resp)}]))
      (catch Exception e
        (log/error e "Exception al publicar en Firehose")
        (errors/error :INFRA_KINESIS_001
                      {:stream_name stream-name
                       :anomaly     (ex-message e)})))))

(defmethod ig/init-key :infra/kinesis
  [_ {:keys [endpoint region] :as opts}]
  (log/info "  -> [Kinesis] Inicializando cliente AWS Firehose | region:" region)
  (let [aws-opts (cond-> {:api :firehose :region region}
                   endpoint (assoc :endpoint-override
                                   (let [uri (java.net.URI. endpoint)]
                                     {:protocol (keyword (.getScheme uri))
                                      :hostname (.getHost uri)
                                      :port     (.getPort uri)})))]
    (->KinesisFirehoseWriter (aws/client aws-opts))))

(defmethod ig/halt-key! :infra/kinesis
  [_ {:keys [client]}]
  (log/info "  <- [Kinesis] Cerrando cliente AWS Firehose")
  (when client
    ;; cognitect/aws api releases resources under the hood, but doesn't have an explicit stop
    nil))
