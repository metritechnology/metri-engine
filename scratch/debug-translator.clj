(ns debug-translator
  (:require [metri.grpc.translator :as t]))

(def chunk [:ok {:data [[42.0]] :columns [{:key "sum_value" :type "number"}] :execution-time-ms 10 :channel :olap :query-key :q1}])

(println "chunk-body keys:" (keys (second chunk)))
(println "query-key is:" (:query-key (second chunk)))
(println "name qk is:" (name (:query-key (second chunk))))

(import [metri.data.grpc QueryResponse])

(def b (QueryResponse/newBuilder))
(def qr (QueryResponse/newBuilder))
(println "Putting batch result...")
(.putBatchResults b "q1" (.build qr))
(def built (.build b))
(println "Batch map size:" (.getBatchResultsCount built))
(println "Batch map:" (.getBatchResultsMap built))
