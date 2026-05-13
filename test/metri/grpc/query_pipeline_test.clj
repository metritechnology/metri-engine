(ns metri.grpc.query-pipeline-test
  "Valida el flujo E2E del Read Path: gRPC -> Janus -> Janus AST IR -> Aegis -> gRPC
   Respetando los principios SOLID (DIP: inyectando dependencias fake)."
  (:require [clojure.test :refer [deftest is testing]]
            [metri.grpc.translator :as t]
            [metri.janus.core :as janus]
            [metri.domain.protocols :as proto])
  (:import [metri.data.grpc QueryRequest AnalyticsRequest MetricDefinition AggregationFunction
                            QueryResponse RowSet DataRowList DataRow]
           [com.google.protobuf Value Value$KindCase]))

;; ─── Stubs y Mocks para DIP (SOLID) ──────────────────────────────────────────

(defrecord FakeASTCompiler [expected-req-map returned-ir]
  proto/IASTCompiler
  (compile-ast [_ req-map cedar-ctx]
    (is (= "tnt-test" (:tenant-id cedar-ctx)))
    ;; Validamos que el request map se haya traducido correctamente
    (is (= expected-req-map req-map))
    [:ok returned-ir]))

(defrecord FakeAegisEngine [expected-ir returned-chunks]
  proto/IAegisEngine
  (transmute! [_ ast-ir]
    ;; Validamos que Aegis reciba exactamente el AST IR
    (is (= expected-ir ast-ir))
    returned-chunks))


;; ─────────────────────────────────────────────────────────────────────────────

(defn- build-query-request []
  (let [metric (-> (MetricDefinition/newBuilder)
                   (.setEntity "meter_reading")
                   (.setAttribute "value")
                   (.setAggregation AggregationFunction/SUM)
                   (.build))
        analytics-req (-> (AnalyticsRequest/newBuilder)
                          (.setEntity "meter_reading")
                          (.addMetrics metric)
                          (.build))]
    (-> (QueryRequest/newBuilder)
        (.setTenantId "tnt-test")
        (.putQueries "q1" analytics-req)
        (.build))))

(deftest grpc-to-janus-to-aegis-to-grpc
  (testing "1. Protobuf -> Janus Request Context (Translator)"
    (let [grpc-req (build-query-request)
          ctx      (t/query-request->ctx grpc-req)]
      
      (is (= "tnt-test" (:tenant-id ctx)))
      (is (contains? (:queries ctx) :q1))
      
      (let [q1-req (get-in ctx [:queries :q1])]
        (is (= "meter_reading" (:entity q1-req)))
        (is (= 1 (count (:metrics q1-req))))
        (is (= "SUM" (-> q1-req :metrics first :aggregation name)))
        
        (testing "2. Janus Core (Orchestrator) -> AST IR -> Aegis"
          (let [fake-ast-ir {:type :ast-ir :entity "meter_reading" :metrics [:sum]}
                
                fake-compiler (->FakeASTCompiler q1-req fake-ast-ir)
                
                ;; Aegis retorna un lazy sequence of chunks
                fake-aegis    (->FakeAegisEngine fake-ast-ir
                                                 (lazy-seq [[:ok {:data [[42.0]]
                                                                  :columns [{:key "sum_value" :type "number"}]
                                                                  :execution-time-ms 10
                                                                  :channel :olap}]]))
                
                stub-cedar    (fn [_ctx]
                                [:ok {:tenant-id "tnt-test"
                                      :user-id "test-user"
                                      :roles #{"admin"}}])
                
                ;; Pipeline execution
                chunks (janus/run-query-pipeline ctx stub-cedar fake-compiler fake-aegis)
                
                ;; Janus returns a lazy sequence
                _      (is (seq? chunks) "Janus debe retornar un lazy-seq")
                chunk1 (first chunks)]
            
            (is (= :ok (first chunk1)))
            (is (= :q1 (:query-key (second chunk1))) "Janus debe inyectar la llave :q1")
            
            (testing "3. Aegis Chunks -> Protobuf QueryResponse (Translator)"
              (let [grpc-res ^QueryResponse (t/aegis-chunk->query-response chunk1)
                    
                    ;; La multiplexación debe meterlo en batch_results bajo "q1"
                    batch-map (.getBatchResultsMap grpc-res)
                    q1-res    (.get batch-map "q1")]
                
                (is (some? q1-res) "El resultado debe estar multiplexado bajo 'q1'")
                
                (let [status   (.getStatus q1-res)
                      data     (.getData q1-res)
                      metadata (.getMetadata q1-res)
                      
                      ;; Extraer row
                      dr-list  (.getRowsJson data)
                      rows     (.getIterList dr-list)
                      dr       (first rows)
                      val      (first (.getValuesList dr))]
                  
                  (is (.getSuccess status))
                  (is (= 10 (.getExecutionTimeMs metadata)))
                  (is (= "olap" (.getEngine metadata)))
                  (is (= 1 (count rows)))
                  (is (= 42.0 (.getNumberValue val)) "El dato numérico transmutado debe llegar a gRPC intacto"))))))))))
