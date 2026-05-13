(ns test-grpc-production
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io])
  (:import [metri.data.grpc BulkRequest BulkResponse QueryRequest QueryResponse AnalyticsRequest MetricDefinition AggregationFunction DataRowList]
           [java.util Base64]
           [java.nio ByteBuffer]))

(def endpoint "https://d21ik83yjpr5g6.cloudfront.net")

(defn encode-grpc-web [proto-msg]
  (let [proto-bytes (.toByteArray proto-msg)
        len (alength proto-bytes)
        bb (ByteBuffer/allocate (+ 5 len))]
    (.put bb (byte 0))
    (.putInt bb len)
    (.put bb proto-bytes)
    (.array bb)))

(defn decode-grpc-web [bytes msg-parser]
  (if (> (alength bytes) 5)
    (let [bb (ByteBuffer/wrap bytes)
          flag (.get bb)
          len (.getInt bb)
          proto-bytes (byte-array len)]
      (.get bb proto-bytes)
      (msg-parser proto-bytes))
    nil))

(defn send-grpc [path proto-msg]
  (let [payload (encode-grpc-web proto-msg)
        b64-payload (.encodeToString (Base64/getEncoder) payload)
        url (str endpoint path)
        client (-> (java.net.http.HttpClient/newBuilder) (.build))
        request (-> (java.net.http.HttpRequest/newBuilder)
                    (.uri (java.net.URI/create url))
                    (.header "Content-Type" "application/grpc-web-text")
                    (.header "Accept" "application/grpc-web-text")
                    (.POST (java.net.http.HttpRequest$BodyPublishers/ofString b64-payload))
                    (.build))
        resp (.send client request (java.net.http.HttpResponse$BodyHandlers/ofByteArray))]
    (if (= 200 (.statusCode resp))
      (.body resp)
      (do
        (println "Error HTTP:" (.statusCode resp))
        (println (String. (.body resp)))
        nil))))

(defn run []
  (println "---- INICIANDO TEST gRPC EN PRODUCCIÓN (Read Path) ----")
  
  ;; 2. Enviar Query
  (let [metric (-> (MetricDefinition/newBuilder)
                   (.setEntity "work_order")
                   (.setAttribute "total_cost")
                   (.setAggregation AggregationFunction/SUM)
                   (.build))
        analytics-req (-> (AnalyticsRequest/newBuilder)
                          (.setTenantId "golden-tenant")
                          (.setEntity "work_order")
                          (.addMetrics metric)
                          (.build))
        query-req (-> (QueryRequest/newBuilder)
                      (.setTenantId "golden-tenant")
                      (.putQueries "q1" analytics-req)
                      (.build))]
    (println "⏳ Enviando Query (SUM total_cost) a Lambda vía CloudFront...")
    (let [res-bytes (send-grpc "/metri.MetriService/Query" query-req)]
      (when res-bytes
        (let [resp ^QueryResponse (decode-grpc-web res-bytes #(QueryResponse/parseFrom %))]
          (println "Full QueryResponse:")
          (println (.toString resp))
          (let [batch-map (.getBatchResultsMap resp)
                q1-res (.get batch-map "q1")]
            (if q1-res
              (let [data (.getData q1-res)
                    rows (.getIterList (.getRowsJson data))]
                (println "✅ Query OK. Resultados recibidos vía gRPC:")
                (doseq [row rows]
                  (let [val (first (.getValuesList row))]
                    (println "   -> SUM(total_cost) =" (.getNumberValue val)))))
              (println "❌ Query Falló o sin resultados.")))))))
            
  (println "--------------------------------------------------------------"))

(run)
