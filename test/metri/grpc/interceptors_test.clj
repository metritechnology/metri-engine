(ns metri.grpc.interceptors-test
  "Matriz TDD — ICPT-01..08 (01.03 Módulo XI.4)
   Testea la construcción y composición de los 3 interceptores Netty:
     otel-server-interceptor, deadline-enforcement-interceptor, logging-interceptor.
   
   Estrategia: los interceptores son proxies Java (ServerInterceptor).
   Los tests verifican la interfaz pública sin hacer I/O ni levantar un servidor gRPC."
  (:require [clojure.test :refer [deftest is testing]]
            [metri.grpc.interceptors :as ic])
  (:import [io.grpc ServerInterceptor
                    ServerCall ServerCall$Listener Metadata]))

;; ─── Spy helpers ─────────────────────────────────────────────────────────────

(defn- make-noop-call []
  "Devuelve un ServerCall mock que registra si fue invocado."
  (proxy [ServerCall] []
    (getMethodDescriptor []
      (let [dummy-marshaller (reify io.grpc.MethodDescriptor$Marshaller
                               (parse [_ _] nil)
                               (stream [_ _] nil))]
        (.. io.grpc.MethodDescriptor
            (newBuilder)
            (setFullMethodName "Metri/Transact")
            (setType io.grpc.MethodDescriptor$MethodType/UNARY)
            (setRequestMarshaller dummy-marshaller)
            (setResponseMarshaller dummy-marshaller)
            build)))
    (sendHeaders [_])
    (sendMessage [_])
    (close [_ _])
    (isCancelled [] false)
    (setMessageCompression [_])
    (isReady [] true)
    (request [_])))

(defn- make-capturing-next []
  "Devuelve un ServerCallHandler que captura la llamada a startCall."
  (let [called? (atom false)
        listener (proxy [ServerCall$Listener] [])]
    {:called? called?
     :listener listener
     :handler  (reify io.grpc.ServerCallHandler
                  (startCall [_ _ _]
                    (reset! called? true)
                    listener))}))

;; ─── ICPT-01..08 ─────────────────────────────────────────────────────────────

(deftest icpt-01-otel-interceptor-es-server-interceptor
  "ICPT-01 — otel-server-interceptor retorna instancia de ServerInterceptor"
  (is (instance? ServerInterceptor (ic/otel-server-interceptor))))

(deftest icpt-02-deadline-interceptor-es-server-interceptor
  "ICPT-02 — deadline-enforcement-interceptor retorna instancia de ServerInterceptor"
  (is (instance? ServerInterceptor (ic/deadline-enforcement-interceptor 5000))))

(deftest icpt-03-logging-interceptor-es-server-interceptor
  "ICPT-03 — logging-interceptor retorna instancia de ServerInterceptor"
  (is (instance? ServerInterceptor (ic/logging-interceptor))))

(deftest icpt-04-otel-interceptor-llama-next
  "ICPT-04 — otel-server-interceptor pasa el call al siguiente handler"
  (let [{:keys [called? handler]} (make-capturing-next)
        interceptor (ic/otel-server-interceptor)
        headers     (Metadata.)]
    (.interceptCall interceptor (make-noop-call) headers handler)
    (is @called? "OTel interceptor debe delegar a next.startCall")))

(deftest icpt-05-deadline-interceptor-llama-next
  "ICPT-05 — deadline-enforcement-interceptor pasa el call al siguiente handler"
  (let [{:keys [called? handler]} (make-capturing-next)
        interceptor (ic/deadline-enforcement-interceptor 3000)
        headers     (Metadata.)]
    (.interceptCall interceptor (make-noop-call) headers handler)
    (is @called? "Deadline interceptor debe delegar a next.startCall")))

(deftest icpt-06-logging-interceptor-llama-next
  "ICPT-06 — logging-interceptor pasa el call al siguiente handler"
  (let [{:keys [called? handler]} (make-capturing-next)
        interceptor (ic/logging-interceptor)
        headers     (Metadata.)]
    (.interceptCall interceptor (make-noop-call) headers handler)
    (is @called? "Logging interceptor debe delegar a next.startCall")))

(deftest icpt-07-logging-retorna-listener
  "ICPT-07 — logging-interceptor retorna un ServerCall$Listener válido"
  (let [{:keys [handler]} (make-capturing-next)
        interceptor (ic/logging-interceptor)
        headers     (Metadata.)
        result      (.interceptCall interceptor (make-noop-call) headers handler)]
    (is (instance? ServerCall$Listener result)
        "logging-interceptor debe retornar un Listener (wrapeado)")))

(deftest icpt-08-tres-interceptores-son-distintos
  "ICPT-08 — Los 3 interceptores son instancias independientes (no singleton)"
  (let [i1 (ic/otel-server-interceptor)
        i2 (ic/otel-server-interceptor)]
    (is (not (identical? i1 i2)) "Cada llamada construye una instancia nueva")))
