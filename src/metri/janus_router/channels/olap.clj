(ns metri.janus-router.channels.olap
  "OLAPChannel — Canal de escritura bypass Big Data via IStreamWriter.
   Solo acepta rpc BulkIngest — bloquea rpc Transact con JNS_OLAP_001.
   Datos con engine:olap → IStreamWriter → S3 Parquet → Athena.

   SCHEMA GENÉRICO MULTIENTIDAD:
   El registro final tiene SIEMPRE la misma estructura plana:
     - Columnas tipadas fijas:  id, _tenant, _entity, _timestamp, _partition_path
     - Columna 'payload':       JSON string con todos los campos de dominio
   Esto permite que el Glue schema sea estático e invariante para cualquier
   entidad del Códice sin requerir cambios de infra en FASE 05."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [cheshire.core :as json]
            [metri.janus-router.channels.protocol :refer [IJanusWriteChannel]]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]
            [metri.janus-router.ulid :as ulid]
            [metri.janus-router.partition :as partition]))

(defn- decorate-record
  "Construye el registro final para Firehose con schema genérico invariante.

   Estructura de salida (misma para CUALQUIER entidad del Códice):
   {
     :id             ULID del registro (monotonónico)
     :_tenant        tenant_id inyectado por CedarAuthorizer (Zero-Trust)
     :_entity        tipo de entidad (meter_reading, asset, energy_bill, ...)
     :_timestamp     epoch millis UTC del momento de ingestión
     :_partition_path ruta Hive evaluada por partition.clj
     :payload        JSON string con todos los campos de dominio
   }

   El campo :payload es opaco para el schema Glue — es un STRING tipado.
   Para queries Athena: json_extract_scalar(payload, '$.reading_value').
   En FASE 05 el Códice podrá generar tablas Glue por entidad con columnas
   tipadas si se requiere predicate pushdown sobre campos de dominio."
  [record tenant-id entity-type timestamp ulid partition-path]
  {:id             ulid
   :_tenant        tenant-id
   :_entity        (name entity-type)
   :_timestamp     timestamp
   :_partition_path partition-path
   ;; Serializar los campos de dominio como JSON string — schema-agnostic.
   ;; Se excluyen las claves de metadatos si el cliente las envió por error.
   :payload        (json/generate-string
                    (dissoc record :_tenant :_entity :_timestamp :_partition_path :id))})

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
