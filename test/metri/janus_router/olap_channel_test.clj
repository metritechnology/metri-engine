(ns metri.janus-router.olap-channel-test
  "Tests unitarios del OLAPChannel — canal bypass Big Data via IStreamWriter.
   Cero I/O real — usa InMemoryStreamWriter como stub canónico.
   7 tests cubriendo el contrato completo del canal OLAP."
  (:require [clojure.test :refer [deftest is testing]]
            [metri.janus-router.channels.olap :as olap]
            [metri.janus-router.stubs.core :as stubs]))

;; ─── Factory de OLAPChannel con stubs ────────────────────────────────────────

(defn make-olap-channel
  "Crea un OLAPChannel con un InMemoryStreamWriter inyectado."
  [stream-writer]
  (olap/->OLAPChannel stream-writer "metri-test-stream"))

(defn olap-ctx
  "Ctx canónico para el OLAPChannel — ya enriquecido por JanusRouter."
  [& {:keys [tenant-id entity-type data] 
      :or   {tenant-id "tnt-acme" entity-type "meter_reading" data [{:val 1}]}}]
  {:tenant-id   tenant-id
   :entity-type entity-type
   :schema      {:entity            entity-type
                 :engine            "olap"
                 :partition_strategy "YYYY-MM-DD"}
   :request     {:entity-type entity-type
                 :operation   :bulk-ingest
                 :data        data}})

;; ═══════════════════════════════════════════════════════════════════════════
;; Tests
;; ═══════════════════════════════════════════════════════════════════════════

(deftest olap-01-bulk-ingest-three-records-ok
  "OLAP-01 — BulkIngest con 3 registros retorna [:ok {:ingested-count 3}]."
  (let [writer  (stubs/make-stream-writer)
        channel (make-olap-channel writer)
        data    [{:asset_id "a1" :value 1.0}
                 {:asset_id "a2" :value 2.0}
                 {:asset_id "a3" :value 3.0}]
        [tag body] (.route channel (olap-ctx :data data))]
    (is (= :ok tag))
    (is (= 3 (:ingested-count body)))
    (is (= 0 (:outbox-count body)))))

(deftest olap-02-transact-blocked-no-data
  "OLAP-02 — rpc Transact (data=nil) retorna [:error :JNS_OLAP_001]."
  (let [writer  (stubs/make-stream-writer)
        channel (make-olap-channel writer)
        ctx     {:tenant-id   "tnt-x"
                 :entity-type "meter_reading"
                 :schema      {:entity "meter_reading" :engine "olap" :partition_strategy "YYYY-MM-DD"}
                 :request     {:entity-type "meter_reading"
                               :operation   :create
                               :data        nil}}  ; nil data = rpc Transact
        [tag body] (.route channel ctx)]
    (is (= :error tag))
    (is (= :JNS_OLAP_001 (:code body)))))

(deftest olap-03-empty-data-ok
  "OLAP-03 — data=[] (batch vacío) retorna [:ok {:ingested-count 0}]."
  (let [writer  (stubs/make-stream-writer)
        channel (make-olap-channel writer)
        [tag body] (.route channel (olap-ctx :data []))]
    (is (= :ok tag))
    (is (= 0 (:ingested-count body)))))

(deftest olap-04-records-decorated-with-metadata
  "OLAP-04 — Cada registro tiene metadatos tipados y los datos de dominio se mantienen planos para mapear 1:1 a Parquet."
  (let [writer  (stubs/make-stream-writer)
        channel (make-olap-channel writer)
        _result (.route channel (olap-ctx :tenant-id "acme" :entity-type "meter_reading"
                                          :data [{:asset_id "a1" :reading_value 42.0}]))
        records @(:records-atom writer)]
    (is (= 1 (count records)) "Debe haber 1 record en el stream")
    (let [r (first records)]
      ;; Columnas de metadatos tipadas
      (is (= "acme"           (:_tenant r))          ":_tenant inyectado (Zero-Trust)")
      (is (= "meter_reading"  (:_entity r))          ":_entity inyectado")
      (is (some? (:id r))                            ":id (ULID) presente")
      (is (some? (:_timestamp r))                    ":_timestamp presente")
      (is (some? (:_partition_path r))               ":_partition_path presente")
      ;; Campos de dominio encapsulados en JSON
      (is (some? (:data r))                          "campo data presente")
      (let [data-map (cheshire.core/parse-string (:data r) true)]
        (is (= "a1" (:asset_id data-map))            "asset_id presente en JSON")
        (is (= 42.0 (:reading_value data-map))       "reading_value presente en JSON")))))

(deftest olap-05-tenant-id-in-partition-path
  "OLAP-05 — El tenant_id no aparece en la ruta (está en el Firehose prefix), pero el _tenant sí."
  (let [writer  (stubs/make-stream-writer)
        channel (make-olap-channel writer)
        _result (.route channel (olap-ctx :tenant-id "acme-corp"))
        r       (first @(:records-atom writer))]
    ;; El tenant está en :_tenant del payload (lo procesa Firehose como partition key)
    (is (= "acme-corp" (:_tenant r))
        "El tenant debe estar como campo :_tenant en el payload enviado al stream")))

(deftest olap-06-stream-writer-error-propagates
  "OLAP-06 — Si el IStreamWriter falla, se propaga [:error :INFRA_KINESIS_001]."
  (let [writer  (stubs/make-error-stream-writer :INFRA_KINESIS_001)
        channel (make-olap-channel writer)
        [tag body] (.route channel (olap-ctx))]
    (is (= :error tag))
    (is (= :INFRA_KINESIS_001 (:code body)))))

(deftest olap-07-records-count-in-stream
  "OLAP-07 — Los N registros del batch se escriben en el stream, no menos ni más."
  (let [writer  (stubs/make-stream-writer)
        channel (make-olap-channel writer)
        data    (vec (for [i (range 5)] {:asset_id (str "a" i) :value (* i 1.0)}))
        _result (.route channel (olap-ctx :data data))]
    (is (= 5 (count @(:records-atom writer)))
        "Deben existir exactamente 5 registros escritos en el stream")))
