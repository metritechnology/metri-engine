(ns metri.janus-router.channels.olap
  "OLAPChannel — Canal de escritura bypass Big Data via IStreamWriter.
   Solo acepta rpc BulkIngest — bloquea rpc Transact con JNS_OLAP_001.
   Datos con engine:olap → IStreamWriter → S3 Parquet → Athena.

   SCHEMA DE ENTIDAD:
   El registro final se aplana combinando los metadatos y los campos del dominio.
   Esto permite mapeo 1:1 con las columnas Glue (ej. asset_id, reading_value)
   para aprovechar el predicate pushdown columnar en S3 Parquet."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [cheshire.core :as json]
            [metri.janus-router.channels.protocol :refer [IJanusWriteChannel]]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]
            [metri.janus-router.ulid :as ulid]
            [metri.janus-router.partition :as partition]))

(defn- decorate-record
  "Construye el registro inyectando los metadatos estructurales de la arquitectura Zero-Trust
   y la ruta dinámica de partición generada por el motor.
   Los campos de dominio se mantienen en el primer nivel para mapear a columnas Parquet."
  [record tenant-id entity-type timestamp ulid partition-path]
  (assoc record
         :id             ulid
         :_tenant        tenant-id
         :_entity        (name entity-type)
         :_timestamp     timestamp
         :_partition_path partition-path))

(defrecord OLAPChannel [stream-writer stream-prefix]
  IJanusWriteChannel
  (route [_ ctx]
    (let [{:keys [tenant-id entity-type operation]} ctx
          op   (or operation (get-in ctx [:request :operation]))
          data (get-in ctx [:request :data])] ;; data es ahora un sequence de native maps

      ;; rpc Transact es inválido para engine:olap — solo BulkIngest
      ;; Transact no envía :data (data = nil), BulkIngest envía :data (lista, puede ser vacía)
      (if (nil? data)
        (do
          (log/warn "[Janus OLAP] rpc Transact bloqueado | entity:" entity-type
                    "— use rpc BulkIngest")
          (errors/error :JNS_OLAP_001
                        {:stage     :janus
                         :detail    "rpc Transact is forbidden for engine:olap — use BulkIngest"
                         :entity_type entity-type
                         :tenant-id tenant-id}))

        ;; Bypass directo Stream Writer (Un solo stream genérico para todo OLAP)
        (let [stream-name stream-prefix ;; 'metri-olap-stream' inyectado
              records     (or data [])
              timestamp   (System/currentTimeMillis)
              schema      (:schema ctx)
              strategy    (:partition_strategy schema "YYYY-MM-DD")
              
              ;; OPTIMIZACIÓN CPU: Evaluar fechas UTC una sola vez por todo el batch (O(1))
              date-evaluated-strategy (partition/pre-evaluate-date-strategy strategy timestamp)
              
              ;; Procesar en batch cada registro
              results
              (mapv (fn [record]
                      (let [ulid (ulid/generate)
                            partition-path (partition/build-dynamic-path date-evaluated-strategy record)
                            ;; Schema genérico: metadatos tipados + payload serializado como JSON string
                            decorated (decorate-record record tenant-id entity-type timestamp ulid partition-path)]
                        (proto/put-record! stream-writer stream-name ulid decorated)))
                    records)
              
              ;; Encontrar posibles errores
              first-error (first (filter #(= (first %) :error) results))]
          
          (if first-error
            first-error ;; Escalar fallo de infraestructura (:INFRA_KINESIS_001, etc.)
            (do
              (log/info "[Janus OLAP] ✅ Bypass Stream Writer exitoso | stream:" stream-name
                        "| count:" (count records)
                        "| tenant:" tenant-id)
              ;; Retornamos el payload de BulkResponse esperado
              [:ok {:ingested-count (count records)
                    :outbox-count   0}])))))))

(defmethod ig/init-key :janus-router/olap-channel
  [_ {:keys [stream-writer stream-prefix]}]
  (log/info "  -> [Janus] OLAPChannel activo | Stream Writer backend | prefix:" stream-prefix)
  (->OLAPChannel stream-writer stream-prefix))

(defmethod ig/halt-key! :janus-router/olap-channel [_ _] nil)
