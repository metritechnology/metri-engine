(ns metri.infrastructure.tracer
  "Cliente de Infraestructura para Observabilidad y Telemetría.
   Inicializa OpenTelemetry SDK (OTel) para exportar spans a ADOT (AWS Distro for OpenTelemetry)
   o a la consola en entorno local.
   Cumple el principio de inicialización única administrada por Integrant."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [steffan-westcott.clj-otel.sdk.otel-sdk :as sdk]
            [steffan-westcott.clj-otel.exporter.otlp.http.trace :as otlp-trace]))

(defmethod ig/init-key :infra/tracer
  [_ {:keys [endpoint service-name] :or {service-name "metri-engine"}}]
  (if (or (empty? endpoint) (= endpoint ""))
    (do
      (log/info "  -> [Tracer] OTLP Endpoint vacío. OTel SDK inicializado en modo NoOp/Local")
      ;; Configura un tracer en memoria que no exporta (ideal para tests/local sin docker)
      (sdk/init-otel-sdk!)
      :noop)
    (do
      (log/info "  -> [Tracer] Inicializando OTel SDK | service:" service-name "| endpoint:" endpoint)
      (let [;; El exportador OTLP enviará los spans por HTTP al ADOT Collector
            exporter (otlp-trace/span-exporter {:endpoint endpoint})]
        (sdk/init-otel-sdk!
         {:tracer-provider {:resource     {:attributes {"service.name" service-name}}
                            :span-processors [{:exporters [exporter]}]}})
        exporter))))

(defmethod ig/halt-key! :infra/tracer
  [_ exporter]
  (log/info "  <- [Tracer] Cerrando OTel SDK")
  (sdk/close-otel-sdk!)
  ;; Si tenemos un exportador activo, lo cerramos explícitamente para flushear spans pendientes
  (when (not= exporter :noop)
    (try
      (.close ^java.io.Closeable exporter)
      (catch Exception e
        (log/warn "Error cerrando OTel exporter:" (ex-message e))))))
