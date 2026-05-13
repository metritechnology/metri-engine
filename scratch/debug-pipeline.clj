(ns debug-pipeline
  (:require [metri.janus.core :as janus]
            [metri.domain.protocols :as proto]
            [metri.grpc.translator :as t]
            [clojure.test :refer [is]])
  (:import [metri.data.grpc QueryRequest AnalyticsRequest MetricDefinition AggregationFunction]))

(defrecord FakeASTCompiler [expected-req-map returned-ir]
  proto/IASTCompiler
  (compile-ast [_ req-map cedar-ctx]
    [:ok returned-ir]))

(defrecord FakeAegisEngine [expected-ir returned-chunks]
  proto/IAegisEngine
  (transmute! [_ ast-ir]
    (println "transmute returning type:" (type returned-chunks))
    returned-chunks))

(defn run-test []
  (let [metric (-> (MetricDefinition/newBuilder)
                   (.setEntity "meter_reading")
                   (.setAttribute "value")
                   (.setAggregation AggregationFunction/SUM)
                   (.build))
        analytics-req (-> (AnalyticsRequest/newBuilder)
                          (.setEntity "meter_reading")
                          (.addMetrics metric)
                          (.build))
        grpc-req (-> (QueryRequest/newBuilder)
                     (.setTenantId "tnt-test")
                     (.putQueries "q1" analytics-req)
                     (.build))
        ctx      (t/query-request->ctx grpc-req)
        q1-req   (get-in ctx [:queries :q1])
        fake-ast-ir {:type :ast-ir :entity "meter_reading" :metrics [:sum]}
        fake-compiler (->FakeASTCompiler q1-req fake-ast-ir)
        fake-aegis    (->FakeAegisEngine fake-ast-ir
                                         (lazy-seq [[:ok {:data [[42.0]]
                                                          :columns [{:key "sum_value" :type "number"}]
                                                          :execution-time-ms 10
                                                          :channel :olap}]]))
        stub-cedar    (fn [_ctx]
                        [:ok {:tenant-id "tnt-test"
                              :user-id "test-user"
                              :roles #{"admin"}}])
        psq-res (#'janus/process-single-query :q1 q1-req stub-cedar fake-compiler fake-aegis)]
    (println "psq type:" (type psq-res))
    (println "psq value:" psq-res)))

(run-test)
