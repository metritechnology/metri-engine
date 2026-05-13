(ns scratch-test-signal
  (:require [metri.grpc.translator :as t]
            [com.google.protobuf.util JsonFormat])
  (:import [metri.data.grpc QueryResponse]))

(def body {:output-cast :KPI
           :data [["1426294.1840585014"]]
           :columns [{:key "sum_reading_value"}]
           :viz-ext {:type "indicator" :payload {:signal {:value 1426294.1840585014}}}})

(def resp-chunk (t/query-result->response [:ok body]))

(let [printer (-> (JsonFormat/printer) (.includingDefaultValueFields) (.preservingProtoFieldNames))]
  (println "JSON with defaults:")
  (println (.print printer resp-chunk)))

(let [printer (-> (JsonFormat/printer) (.preservingProtoFieldNames))]
  (println "\nJSON without defaults (like python test):")
  (println (.print printer resp-chunk)))
