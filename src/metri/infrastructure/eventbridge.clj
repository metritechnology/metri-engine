(ns metri.infrastructure.eventbridge
  "Implementación del IEventBus para AWS EventBridge.
   Desacoplado de la lógica de dominio (SOLID).
   Toda excepción o anomalía de red se traduce a un [:error ...]."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [cognitect.aws.client.api :as aws]
            [metri.domain.protocols :as proto]
            [cheshire.core :as json]
            [metri.domain.errors :as errors]))

(defrecord EventBridgeClient [client]
  proto/IEventBus
  (put-event! [_ bus-name source detail-type detail]
    (try
      (let [payload (json/generate-string detail)
            req     {:op      :PutEvents
                     :request {:Entries [{:EventBusName bus-name
                                          :Source       source
                                          :DetailType   detail-type
                                          :Detail       payload}]}}
            resp    (aws/invoke client req)]
        (if (:cognitect.anomalies/category resp)
          (let [code (if (= "AccessDeniedException" (get-in resp [:cognitect.aws.error/code]))
                      :INFRA_EVENTBRIDGE_003
                      :INFRA_EVENTBRIDGE_001)]
            (log/error "Fallo en EventBridge PutEvents:" resp)
            (errors/error code {:bus_name    bus-name
                                :source      source
                                :detail_type detail-type
                                :anomaly     (select-keys resp [:cognitect.anomalies/category
                                                                :cognitect.aws.error/code
                                                                :Message])}))
          (let [entry-resp (-> resp :Entries first)]
            (if (:ErrorCode entry-resp)
              (errors/error :INFRA_EVENTBRIDGE_002
                            {:bus_name      bus-name
                             :error_code    (:ErrorCode entry-resp)
                             :error_message (:ErrorMessage entry-resp)})
              [:ok {:event-id (:EventId entry-resp)}]))))
      (catch Exception e
        (log/error e "Exception al publicar en EventBridge")
        (errors/error :INFRA_EVENTBRIDGE_001
                      {:bus_name    bus-name
                       :source      source
                       :detail_type detail-type
                       :anomaly     (ex-message e)})))))

(defmethod ig/init-key :infra/eventbridge
  [_ {:keys [endpoint region] :as opts}]
  (log/info "  -> [EventBridge] Inicializando cliente AWS Events | region:" region)
  (let [aws-opts (cond-> {:api :events :region region}
                   endpoint (assoc :endpoint-override
                                   (let [uri (java.net.URI. endpoint)]
                                     {:protocol (keyword (.getScheme uri))
                                      :hostname (.getHost uri)
                                      :port     (.getPort uri)})))]
    (->EventBridgeClient (aws/client aws-opts))))

(defmethod ig/halt-key! :infra/eventbridge
  [_ {:keys [client]}]
  (log/info "  <- [EventBridge] Cerrando cliente AWS Events")
  (when client
    nil))
