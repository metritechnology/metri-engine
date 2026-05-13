(ns metri.grpc.service
  "MetriServiceImpl — adaptador gRPC → pipelines de dominio.
   Única responsabilidad: traducir Protobuf ↔ Clojure y despachar al pipeline.
   Toda la lógica de negocio vive en IOP/Aegis/Códice, no aquí."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.domain.errors :as errors])
  (:import [metri.data.grpc MetriServiceGrpc$MetriServiceImplBase
                            TransactionRequest TransactionResponse
                            BulkRequest BulkResponse
                            QueryRequest QueryResponse
                            DiscoveryRequest DiscoveryResponse
                            ExploreRequest ExploreResponse
                            MatchRoutingRulesBatchRequest MatchRoutingRulesBatchResponse]
           [io.grpc stub.StreamObserver Status StatusRuntimeException]))

;; ── Template: handle-unary ───────────────────────────────────────────────────

(defn- handle-unary
  "Template genérico para RPCs unarios usando el Dispatcher Universal."
  [method-name ^StreamObserver observer request dispatcher]
  (try
    (let [route  (get dispatcher method-name)
          ctx    ((:req->ctx route) request)
          result ((:pipeline route) ctx)
          resp   ((:res->proto route) result)]
      (.onNext observer resp)
      (.onCompleted observer))
    (catch StatusRuntimeException e
      (log/error "gRPC StatusRuntimeException en" method-name (ex-message e))
      (.onError observer e))
    (catch Exception e
      (log/error e "gRPC excepción no manejada en" method-name (ex-message e))
      (.onError observer
        (-> (Status/INTERNAL)
            (.withDescription (ex-message e))
            (.withCause e)
            (.asRuntimeException))))))

;; ── MetriServiceImpl ──────────────────────────────────────────────────────────

(defn build-service-impl
  "Construye la implementación del MetriServiceGrpc usando proxy.
   Delega todo el ruteo y traducción al Dispatcher inyectado."
  [{:keys [dispatcher]}]
  (proxy [MetriServiceGrpc$MetriServiceImplBase] []

    ;; ── Operations ────────────────────────────────────────────────────────
    (transact [^TransactionRequest req ^StreamObserver obs]
      (handle-unary "Transact" obs req dispatcher))

    (bulkIngest [^BulkRequest req ^StreamObserver obs]
      (handle-unary "BulkIngest" obs req dispatcher))

    ;; ── Metadata ──────────────────────────────────────────────────────────
    (discovery [^DiscoveryRequest req ^StreamObserver obs]
      (handle-unary "Discovery" obs req dispatcher))

    (explore [^ExploreRequest req ^StreamObserver obs]
      (handle-unary "Explore" obs req dispatcher))

    ;; ── Analytics: server-streaming ───────────────────────────────────────
    (query [^QueryRequest req ^StreamObserver obs]
      (try
        (let [route  (get dispatcher "Query")
              ctx    ((:req->ctx route) req)
              chunks ((:pipeline route) ctx)]   ;; retorna secuencia lazy de chunks
          (doseq [chunk chunks]
            (.onNext obs ((:res->proto route) chunk)))
          (.onCompleted obs))
        (catch Exception e
          (log/error "gRPC Query stream error:" (ex-message e))
          (.onError obs (-> (Status/INTERNAL)
                            (.withDescription (ex-message e))
                            (.asRuntimeException))))))

    ;; ── EDA ───────────────────────────────────────────────────────────────
    (matchRoutingRulesBatch [^MatchRoutingRulesBatchRequest req ^StreamObserver obs]
      (handle-unary "MatchRoutingRulesBatch" obs req dispatcher))))

;; ── Integrant Lifecycle ───────────────────────────────────────────────────────

(defmethod ig/init-key :grpc/service-impl
  [_ {:keys [dispatcher]}]
  (log/info "  -> [gRPC] ServiceImpl compilado (Dumb Adapter)")
  (build-service-impl {:dispatcher dispatcher}))

(defmethod ig/halt-key! :grpc/service-impl [_ _] nil)
