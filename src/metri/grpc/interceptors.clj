(ns metri.grpc.interceptors
  "Interceptores del servidor gRPC Netty.
   Orden de ejecución (más externo → más interno):
     otel-server-interceptor → deadline-enforcement → logging-interceptor → service
   
   NOTA: io.grpc.ServerInterceptor es una interfaz genérica <ReqT, RespT>.
   Clojure requiere proxy (no reify) para métodos genéricos de Java."
  (:require [taoensso.timbre :as log])
  (:import [io.grpc ServerInterceptor ServerCall ServerCall$Listener Metadata]))

;; ─────────────────────────────────────────────────────────────────────────────
;; OTel NoOp Interceptor (más externo)
;; En producción, ADOT Java agent inyecta el real automáticamente.
;; ─────────────────────────────────────────────────────────────────────────────

(defn otel-server-interceptor []
  (proxy [ServerInterceptor] []
    (interceptCall [call headers next]
      (.startCall next call headers))))

;; ─────────────────────────────────────────────────────────────────────────────
;; Deadline Enforcement Interceptor
;; ─────────────────────────────────────────────────────────────────────────────

(defn deadline-enforcement-interceptor [_default-ms]
  (proxy [ServerInterceptor] []
    (interceptCall [call headers next]
      (.startCall next call headers))))

;; ─────────────────────────────────────────────────────────────────────────────
;; Structured Logging Interceptor (más interno — más cerca del servicio)
;; Emite: method name + latencia en ms al completar cada RPC.
;; ─────────────────────────────────────────────────────────────────────────────

(defn logging-interceptor []
  (proxy [ServerInterceptor] []
    (interceptCall [call headers next]
      (let [method-name (.. call getMethodDescriptor getFullMethodName)
            start-ns    (System/nanoTime)
            listener    (.startCall next call headers)]
        ;; Wrap listener para interceptar onComplete/onCancel
        (proxy [ServerCall$Listener] []
          (onMessage   [msg] (.onMessage listener msg))
          (onHalfClose []    (.onHalfClose listener))
          (onReady     []    (.onReady listener))
          (onCancel    []
            (log/debug "gRPC CANCEL" method-name)
            (.onCancel listener))
          (onComplete  []
            (log/info (format "gRPC  %-50s %6.1fms"
                              method-name
                              (/ (- (System/nanoTime) start-ns) 1e6)))
            (.onComplete listener)))))))
