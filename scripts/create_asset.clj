(ns create-asset
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io])
  (:import [metri.data.grpc TransactionRequest TransactionResponse OperationAction]
           [com.google.protobuf Struct Value ListValue]
           [java.util Base64]
           [java.nio ByteBuffer]))

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

(defn map->struct [m]
  (let [builder (Struct/newBuilder)]
    (doseq [[k v] m]
      (let [val-builder (Value/newBuilder)]
        (cond
          (string? v) (.setStringValue val-builder v)
          (number? v) (.setNumberValue val-builder (double v))
          (boolean? v) (.setBoolValue val-builder v)
          :else (.setStringValue val-builder (str v)))
        (.putFields builder (name k) (.build val-builder))))
    (.build builder)))

(defn send-grpc [path proto-msg]
  (let [payload (encode-grpc-web proto-msg)
        b64-payload (.encodeToString (Base64/getEncoder) payload)
        url (str "https://d21ik83yjpr5g6.cloudfront.net" path)
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
  (println "---- INICIANDO CREACIÓN DE ASSET ----")
  
  (let [payload-map {:name "Test Asset via gRPC"
                     :serial_number "SN-12345"
                     :status "ACTIVE"}
        req (-> (TransactionRequest/newBuilder)
                (.setTenantId "golden-tenant-123")
                (.setEntityType "asset")
                (.setAction OperationAction/CREATE)
                (.setPayload (map->struct payload-map))
                (.build))]
    (let [res-bytes (send-grpc "/metri.MetriService/Transact" req)]
      (when res-bytes
        (let [resp ^TransactionResponse (decode-grpc-web res-bytes #(TransactionResponse/parseFrom %))]
          (println "Respuesta recibida:")
          (println (.toString resp))))))
  (println "--------------------------------------------------------------"))

(run)
