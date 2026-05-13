(ns metri.grpc.dispatcher
  "Command Dispatcher -- Directorio universal de endpoints gRPC.
   Vincula cada operacion de MetriService con sus traductores y su pipeline
   de negocio, actuando como la fuente de verdad para Netty y Lambda.

   NOTA ARQUITECTONICA: Este dispatcher es PURO ROUTING.
   No contiene logica de negocio ni transformaciones.
   El pipeline de normalizacion (Paso 7) vive en metres.janus.core,
   garantizando que todo chunk ya llega normalizado al translator."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.grpc.translator :as t])
  (:import [metri.data.grpc TransactionRequest BulkRequest
                            DiscoveryRequest ExploreRequest
                            QueryRequest MatchRoutingRulesBatchRequest]))

(defn build-routes
  "Construye el mapa de rutas inyectando los pipelines reales."
  [{:keys [iop-pipeline janus-query janus-discovery janus-explore janus-rule-matcher]}]
  {"Transact"
   {:parse-fn     #(TransactionRequest/parseFrom ^bytes %)
    :req->ctx     t/transaction-request->ctx
    :pipeline     iop-pipeline
    :res->proto   t/iop-result->transaction-response
    :type         :unary}

   "BulkIngest"
   {:parse-fn     #(BulkRequest/parseFrom ^bytes %)
    :req->ctx     t/bulk-request->ctx
    :pipeline     iop-pipeline
    :res->proto   t/iop-result->bulk-response
    :type         :unary}

   "Discovery"
   {:parse-fn     #(DiscoveryRequest/parseFrom ^bytes %)
    :req->ctx     t/discovery-request->ctx
    :pipeline     janus-discovery
    :res->proto   t/discovery-result->response
    :type         :unary}

   "Explore"
   {:parse-fn     #(ExploreRequest/parseFrom ^bytes %)
    :req->ctx     t/explore-request->ctx
    :pipeline     janus-explore
    :res->proto   t/explore-result->response
    :type         :unary}

   "MatchRoutingRulesBatch"
   {:parse-fn     #(MatchRoutingRulesBatchRequest/parseFrom ^bytes %)
    :req->ctx     t/match-batch-request->ctx
    :pipeline     janus-rule-matcher
    :res->proto   t/match-batch-result->response
    :type         :unary}

   "Query"
   {:parse-fn     #(QueryRequest/parseFrom ^bytes %)
    :req->ctx     t/query-request->ctx
    :pipeline     janus-query
    ;; Translator puro: el chunk ya llega normalizado desde metres.janus.core (Paso 7)
    :res->proto   t/aegis-chunk->query-response
    :type         :server-streaming}})

(defmethod ig/init-key :grpc/dispatcher
  [_ deps]
  (log/info "  -> [gRPC] Dispatcher universal compilado")
  (build-routes deps))

(defmethod ig/halt-key! :grpc/dispatcher [_ _] nil)
