(ns test-translator
  (:require [metri.grpc.translator :as t]
            [clojure.pprint :refer [pprint]])
  (:import [metri.data.grpc QueryRequest AnalyticsRequest]))

(defn -main []
  (let [q (-> (AnalyticsRequest/newBuilder)
              (.setTenantId "tenant-1")
              (.setEntity "inventory_movement")
              (.build))
        req (-> (QueryRequest/newBuilder)
                (.setTenantId "tenant-1")
                (.putQueries "oltp_table" q)
                (.build))
        ctx (t/query-request->ctx req)]
    (println "--- Context ---")
    (pprint ctx)))

(-main)
