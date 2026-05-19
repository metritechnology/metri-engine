;; [PORTED_TO_RUST: src/main.rs]
;; NO MODIFICAR ESTE ARCHIVO — la fuente de verdad ahora reside en Rust.
(ns metri.lambda.handler
  "Adaptador Lambda → pipelines del Metri Engine (Emulación gRPC-Web).
   Recibe el JSON de API Gateway, extrae el Base64 (grpc-web+proto),
   lo parsea, ejecuta el motor, y devuelve un JSON con isBase64Encoded=true
   que contiene la trama gRPC-Web perfectamente formateada.
   SnapStart compatible: ig/init se ejecuta en la fase de init (congelada)."
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler]
   :constructors {[] []}
   :init init)
  (:require [clojure.java.io :as io]
            [integrant.core :as ig]
            [taoensso.timbre :as log]
            [cheshire.core :as json]
            [metri.bootstrap :as bootstrap]
            [metri.config.readers :as readers]
            [metri.infrastructure.datahike]
            [metri.infrastructure.dynamodb]
            [metri.infrastructure.session-store]
            [metri.infrastructure.sqs]
            [metri.infrastructure.athena]
            [metri.infrastructure.eventbridge]
            [metri.infrastructure.kinesis]
            [metri.infrastructure.glue]
            ;; FASE 03 - IOP Real & Janus
            [metri.iop.core]
            [metri.janus-router.core]
            [metri.janus-router.channels.oltp]
            [metri.janus-router.channels.olap]
            [metri.janus.core]
            [metri.janus.ast-compiler]
            [metri.aegis.core]
            [metri.cedar.stub]
            [metri.quota.stub]
            [metri.moira.stub]
            [metri.infrastructure.audit.stub]
            ;; Stubs Residuales
            [metri.iop.sherlog]
            [metri.aegis.stub]
            [metri.eda.stub]
            [metri.codice.registry]
            [metri.codice.generator]
            [metri.grpc.dispatcher]
            [metri.grpc.server]
            [metri.grpc.service]
            [metri.grpc.translator :as t]
            [metri.domain.errors :as errors])
  (:import [java.io InputStream OutputStream]
           [java.util Base64]
           [java.nio ByteBuffer]
           [metri.data.grpc TransactionRequest TransactionResponse BulkRequest BulkResponse QueryRequest QueryResponse]
           [com.google.protobuf GeneratedMessageV3]))

;; ── Sistema Integrant — inicializado UNA VEZ (SnapStart lo congela) ──────────

(defn- load-config []
  (let [env  (or (System/getenv "ENVIRONMENT") "prod")
        file (if (= "local" env)
               "config/system.dev.edn"
               "config/system.lambda.edn")
        res  (io/resource file)]
    (if res
      (ig/read-string {:readers readers/all} (slurp res))
      (throw (ex-info (str "Config no encontrada: " file) {:file file})))))

(def ^:private system
  (delay
    (bootstrap/run-fail-fast!)
    (require 'metri.grpc.dispatcher)
    (try
      (ig/init (load-config))
      (catch Exception e
        (println "====== SYSTEM BOOT ERROR ======")
        (println (ex-message e))
        (println (ex-data e))
        (if-let [cause (ex-cause e)]
          (do
            (println "====== CAUSE ======")
            (println (ex-message cause))
            (println (ex-data cause))))
        (throw e)))))
        
(defn -init []
  (try
    (println "[SnapStart] Inicializando sistema en fase Init de Lambda...")
    @system
    (println "[SnapStart] Sistema inicializado exitosamente.")
    [[] nil]
    (catch Exception e
      (println "====== SNAPSTART INIT ERROR ======")
      (println (ex-message e))
      (println (ex-data e))
      (throw e))))

;; ── Enrutador de Endpoints gRPC-Web (SOLID & DRY) ──────────────────────────────

(def ^:private rpc-routes
  {"/metri.MetriService/Transact"
   {:parse-fn     #(TransactionRequest/parseFrom ^bytes %)
    :req->ctx     t/transaction-request->ctx
    :res->proto   t/iop-result->transaction-response}

   "/metri.MetriService/BulkIngest"
   {:parse-fn     #(BulkRequest/parseFrom ^bytes %)
    :req->ctx     t/bulk-request->ctx
    :res->proto   t/iop-result->bulk-response}
    
   "/metri.MetriService/Query"
   {:type         :server-streaming
    :parse-fn     #(QueryRequest/parseFrom ^bytes %)
    :req->ctx     t/query-request->ctx
    ;; Translator puro: chunk ya llega normalizado desde metres.janus.core (Paso 7)
    :res->proto   t/aegis-chunk->query-response}})

;; ── Utilidades gRPC-Web ────────────────────────────────────────────────────────

(defn- decode-grpc-web-request [^bytes body-bytes parse-fn]
  ;; Omitir primer byte (0=data, >128=trailer) y 4 bytes de longitud
  (if (> (alength body-bytes) 5)
    (let [bb (ByteBuffer/wrap body-bytes)
          flag    (.get bb)
          length  (.getInt bb)
          proto-bytes (byte-array length)]
      (try
        (.get bb proto-bytes)
        (parse-fn proto-bytes)
        (catch java.nio.BufferUnderflowException e
          (throw (ex-info "Buffer underflow parsing grpc-web"
                          {:decoded-length (alength body-bytes)
                           :flag flag
                           :expected-length length})))))
    (throw (ex-info "gRPC-Web frame demasiado corto" {}))))

(defn- encode-grpc-data-frame [^GeneratedMessageV3 msg]
  (let [proto-bytes (.toByteArray msg)
        proto-len   (alength proto-bytes)
        bb          (ByteBuffer/allocate (+ 5 proto-len))]
    (.put bb (byte 0x00))
    (.putInt bb proto-len)
    (.put bb proto-bytes)
    (.array bb)))

(defn- encode-grpc-trailer-frame [grpc-status]
  (let [trailer-str (str "grpc-status: " grpc-status "\r\n")
        trailer-bytes (.getBytes trailer-str "UTF-8")
        trailer-len (alength trailer-bytes)
        bb          (ByteBuffer/allocate (+ 5 trailer-len))]
    (.put bb (unchecked-byte 128))
    (.putInt bb trailer-len)
    (.put bb trailer-bytes)
    (.array bb)))

;; ── Handler Lambda (gRPC-Web Puro) ─────────────────────────────────────────────

(defn -handleRequest
  "Entry point Lambda (Function URL con RESPONSE_STREAM).
   Lee JSON de entrada, extrae gRPC-Web base64, procesa, y transmite
   la respuesta en bytes puros usando el stream HTTP de AWS."
  [_this ^InputStream input-stream ^OutputStream output-stream _context]
  (let [sys @system]
    (try
      (let [;; 1. Leer evento JSON de AWS (Petición de entrada)
            event (json/parse-stream (io/reader input-stream) true)
            headers (:headers event)
            
            ;; 1.5 Validar Token de CloudFront (Zero-Trust)
            expected-token (System/getenv "METRI_ORIGIN_TOKEN")
            client-token (get headers :x-metri-origin-token)
            
            _ (when (and expected-token (not= expected-token client-token))
                (log/warn "Bloqueo de seguridad: Token de CloudFront inválido o ausente"
                          {:client-token client-token})
                (throw (ex-info "Forbidden" {:status 403})))

            ;; 1.6 Resolver Ruta vía Dispatcher Universal
            path (or (:rawPath event)
                     (get-in event [:requestContext :http :path])
                     "/metri.MetriService/Transact")]
        
        (if (= path "/grpc.health.v1.Health/Check")
          (let [;; protobuf para SERVING = enum 1 -> [8 1]
                data-frame (byte-array [0 0 0 0 2 8 1]) ;; flag 0x00, length 2, data [8 1]
                trailer-frame (encode-grpc-trailer-frame 0)
                final-bytes (byte-array (concat data-frame trailer-frame))
                proxy-resp {:statusCode 200
                            :headers {"Content-Type" "application/grpc-web+proto"
                                      "Access-Control-Allow-Origin" "*"
                                      "Access-Control-Expose-Headers" "grpc-status, grpc-message"}
                            :isBase64Encoded true
                            :body (.encodeToString (Base64/getEncoder) final-bytes)}
                resp-bytes (.getBytes (json/generate-string proxy-resp) "UTF-8")]
            (.write output-stream resp-bytes)
            (.flush output-stream))
            
          (let [method-name (last (clojure.string/split path #"/"))
                dispatcher  (:grpc/dispatcher sys)
                route       (get dispatcher method-name)
                
                _ (when-not route
                    (throw (ex-info "Método gRPC no soportado o ruta inválida" {:status 404 :path path})))
                body-raw (:body event)
                is-base64? (:isBase64Encoded event)
                content-type (or (get headers :content-type) (get headers :Content-Type) "")
                
                ;; 2. Extraer bytes reales del body HTTP
                http-body-bytes (if is-base64?
                                  (.decode (Base64/getDecoder) ^String (or body-raw ""))
                                  (.getBytes ^String (or body-raw "") "UTF-8"))
                                  
                ;; 3. Decodificar gRPC-Web si es text (base64)
                grpc-frame-bytes (if (.startsWith ^String content-type "application/grpc-web-text")
                                   (.decode (Base64/getDecoder) http-body-bytes)
                                   http-body-bytes)
                                   
                req (decode-grpc-web-request grpc-frame-bytes (:parse-fn route))
                ctx ((:req->ctx route) req)
                
                ;; 3 & 4 & 5. Ejecutar Pipeline y construir Frames gRPC-Web
                final-bytes
                (if (= (:type route) :server-streaming)
                  ;; ────────── SERVER STREAMING ──────────
                  (let [chunks ((:pipeline route) ctx)
                        proto-responses (doall (map (:res->proto route) chunks))
                        merged-root (if (<= (count proto-responses) 1)
                                      (first proto-responses)
                                      (let [b (metri.data.grpc.QueryResponse/newBuilder)]
                                        (.setStatus b (-> (metri.data.grpc.Status/newBuilder) (.setSuccess true) .build))
                                        (doseq [^metri.data.grpc.QueryResponse qr proto-responses]
                                          (.putAllBatchResults b (.getBatchResultsMap qr)))
                                        (.build b)))
                        data-frame (encode-grpc-data-frame merged-root)
                        trailer-frame (encode-grpc-trailer-frame 0)
                        all-bytes (byte-array (concat data-frame trailer-frame))]
                    all-bytes)
                  ;; ────────── UNARY ──────────
                  (let [result ((:pipeline route) ctx)
                        tag    (first result)
                        resp   ((:res->proto route) result)
                        grpc-status (if (= tag :ok) 0 2)
                        data-frame (encode-grpc-data-frame resp)
                        trailer-frame (encode-grpc-trailer-frame grpc-status)
                        all-bytes (byte-array (concat data-frame trailer-frame))]
                    all-bytes))]
            
            ;; 6. Escribir JSON Proxy para Function URL
            (let [proxy-resp {:statusCode 200
                              :headers {"Content-Type" "application/grpc-web+proto"
                                        "Access-Control-Allow-Origin" "*"
                                        "Access-Control-Expose-Headers" "grpc-status, grpc-message"}
                              :isBase64Encoded true
                              :body (.encodeToString (Base64/getEncoder) final-bytes)}
                  resp-bytes (.getBytes (json/generate-string proxy-resp) "UTF-8")]
              (.write output-stream resp-bytes)
              (.flush output-stream)))))
        
      (catch Exception e
        (if (= 403 (:status (ex-data e)))
          (let [proxy-resp {:statusCode 403
                            :headers {"Content-Type" "application/json"}
                            :body "{\"message\":\"Forbidden by Zero-Trust Token\"}"}
                resp-bytes (.getBytes (json/generate-string proxy-resp) "UTF-8")]
            (.write output-stream resp-bytes)
            (.flush output-stream))
          (do
            (log/error e "Fallo catastrófico en gRPC-Web Lambda")
            ;; G1 FASE 10: excepción inesperada → Sherlog/SYS_000
            (try
              (require 'metri.iop.error-response)
              (require 'metri.iop.sherlog)
              (let [sys @system
                    dto ((resolve 'metri.iop.error-response/build-error-dto)
                         {:code :SYS_000 :stage :lambda :detail (ex-message e)}
                         {} nil)]
                ((resolve 'metri.iop.sherlog/process-fault!)
                 (:iop/sherlog sys)
                 {:code :SYS_000}
                 dto))
              (catch Exception inner
                (log/error inner "Fallo al escalar SYS_000 a Sherlog")))
            (let [proxy-resp {:statusCode 500
                              :headers {"Content-Type" "application/json"}
                              :body "{\"message\":\"Internal Server Error\"}"}
                  resp-bytes (.getBytes (json/generate-string proxy-resp) "UTF-8")]
              (.write output-stream resp-bytes)
              (.flush output-stream))))))))
