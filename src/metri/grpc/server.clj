;; [PORTED_TO_RUST: src/grpc/server.rs]
;; NO MODIFICAR — fuente de verdad en Rust
(ns metri.grpc.server
  "Servidor gRPC Netty — ciclo de vida Integrant.
   Levanta en el puerto configurado, registra el servicio impl,
   y realiza graceful shutdown al recibir ig/halt-key!."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.grpc.interceptors :as interceptors])
  (:import [io.grpc Server ServerBuilder]
           [io.grpc.protobuf.services ProtoReflectionService HealthStatusManager]
           [io.grpc.health.v1 HealthCheckResponse$ServingStatus]))

;; ── Integrant Lifecycle ───────────────────────────────────────────────────────

(defmethod ig/init-key :grpc/health-manager [_ _]
  (HealthStatusManager.))

(defmethod ig/halt-key! :grpc/health-manager [_ _] nil)

(defmethod ig/init-key :grpc/server
  [_ {:keys [service-impl port enable-reflection health-manager
             max-inbound-msg-size-mb deadline-ms]
      :or   {port 9090 enable-reflection false
             max-inbound-msg-size-mb 16 deadline-ms 30000}}]
  (log/info "  -> [gRPC] Construyendo servidor Netty en puerto" port)
  (let [max-bytes (* max-inbound-msg-size-mb 1024 1024)
        builder   (-> (ServerBuilder/forPort port)
                      (.maxInboundMessageSize max-bytes)
                      ;; Servicio principal
                      (.addService service-impl)
                      ;; Health protocol (grpc.health.v1.Health)
                      (.addService (.getHealthService ^HealthStatusManager health-manager))
                      ;; Interceptores (outer → inner)
                      (.intercept (interceptors/logging-interceptor))
                      (.intercept (interceptors/deadline-enforcement-interceptor deadline-ms))
                      (.intercept (interceptors/otel-server-interceptor)))
        ;; Reflection — permite grpcurl list/describe en local
        builder   (if enable-reflection
                    (.addService builder (ProtoReflectionService/newInstance))
                    builder)
        ^Server server (.build builder)]
    (.start server)
    ;; Marcar servicio como SERVING en el health manager
    (.setStatus ^HealthStatusManager health-manager
                "" HealthCheckResponse$ServingStatus/SERVING)
    (log/info (str "\n✓ Metri Engine READY — gRPC listening on port " port
                   (when enable-reflection " [reflection=ON]")))
    {:server server :port port :health-manager health-manager}))

(defmethod ig/halt-key! :grpc/server
  [_ {:keys [^Server server ^HealthStatusManager health-manager port]}]
  (log/info "  -> [gRPC] Iniciando graceful shutdown (30s max)...")
  ;; 1. Señalizar NOT_SERVING para que el load balancer deje de enrutar
  (.enterTerminalState health-manager)
  ;; 2. Dejar de aceptar nuevas conexiones
  (.shutdown server)
  ;; 3. Esperar hasta 30s para que los RPCs en vuelo terminen
  (when-not (.awaitTermination server 30 java.util.concurrent.TimeUnit/SECONDS)
    (log/warn "  -> [gRPC] Timeout — forzando shutdownNow")
    (.shutdownNow server))
  (log/info "  -> [gRPC] Servidor detenido en puerto" port))
