(ns metri.janus-router.channels.olap
  "OLAPChannel — Canal de escritura bypass Big Data via IStreamWriter.
   Solo acepta rpc BulkIngest — bloquea rpc Transact con JNS_OLAP_001.

   Arquitectura: Columnar Nativo (sin Raw Zone genérica)
   ─────────────────────────────────────────────────────
   Cada entidad OLAP escribe en su propio stream Firehose dedicado:
     meter_reading  → metri-olap-stream-meter-reading
     audit_log      → metri-olap-stream-audit-log
     domain_fault   → metri-olap-stream-domain-fault

   El registro se aplana combinando metadatos del sistema + campos del dominio.
   Esto permite mapeo 1:1 con las columnas Iceberg nativas para aprovechar
   predicate pushdown columnar en S3 Parquet/Athena."
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.janus-router.channels.protocol :refer [IJanusWriteChannel]]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]
            [metri.janus-router.ulid :as ulid]
            [metri.codice.api :as codice]))

;; ── Helpers ──────────────────────────────────────────────────────────────────

(defn- entity->stream-name
  "Construye el nombre del stream Firehose para una entidad.
   Convierte underscores a hyphens: meter_reading → <prefix>-meter-reading"
  [stream-prefix entity-type]
  (let [entity-str (-> entity-type name (str/replace #"_" "-"))]
    (str stream-prefix "-" entity-str)))

(defn- coerce-numeric-fields
  "Coerce campos numéricos del Códice que lleguen como strings.
   Crítico para Parquet/Iceberg donde los tipos deben coincidir exactamente."
  [record attributes]
  (let [numeric-types #{"double" "float" "decimal" "epoch" "long" "int" "bigint" "timestamp"}
        numeric-attrs (filter #(numeric-types (:type %)) attributes)]
    (reduce
     (fn [rec attr]
       (let [k (keyword (:name attr))
             v (get rec k)]
         (if (string? v)
           (assoc rec k (try (Double/parseDouble v) (catch Exception _ v)))
           rec)))
     record
     numeric-attrs)))

(defn- decorate-record
  "Aplana el payload con los metadatos del sistema como campos nativos.
   Produce un mapa plano compatible con el esquema Iceberg de la entidad.

   Columnas del sistema inyectadas:
     :id         → ULID del registro (clave única Iceberg)
     :_tenant    → ID del tenant (columna de partición)
     :created_at → epoch-ms del momento de ingestión (server-side)"
  [record tenant-id created-at ulid]
  (merge record
         {:id         ulid
          :_tenant    tenant-id
          :created_at created-at}))

;; ── OLAPChannel ──────────────────────────────────────────────────────────────

(defrecord OLAPChannel [stream-writer stream-prefix]
  IJanusWriteChannel
  (route [_ ctx]
    (let [{:keys [tenant-id entity-type operation]} ctx
          op   (or operation (get-in ctx [:request :operation]))
          data (get-in ctx [:request :data])]

      ;; rpc Transact es inválido para engine:olap — solo BulkIngest
      ;; Transact no envía :data (data = nil), BulkIngest envía :data (lista, puede ser vacía)
      (if (nil? data)
        (do
          (log/warn "[Janus OLAP] rpc Transact bloqueado | entity:" entity-type
                    "— use rpc BulkIngest")
          (errors/error :JNS_OLAP_001
                        {:stage       :janus
                         :detail      "rpc Transact is forbidden for engine:olap — use BulkIngest"
                         :entity_type entity-type
                         :tenant-id   tenant-id}))

        ;; Escritura directa en el stream Firehose de la entidad
        (let [stream-name (entity->stream-name stream-prefix entity-type)
              records     (or data [])
              created-at  (System/currentTimeMillis)

              ;; Recuperar atributos del Códice para coerción dinámica de tipos
              attrs-res  (codice/describe-attributes (name entity-type) ctx)
              attributes (if (= :ok (first attrs-res)) (second attrs-res) [])

              results
              (mapv (fn [record]
                      (let [record-ulid  (ulid/generate)
                            ;; 1. Coerción de tipos numéricos (string → double/long)
                            coerced      (coerce-numeric-fields record attributes)
                            ;; 2. Aplanado final: campos de dominio + metadatos del sistema
                            decorated    (decorate-record coerced tenant-id created-at record-ulid)]
                        (proto/put-record! stream-writer stream-name record-ulid decorated)))
                    records)

              first-error (first (filter #(= (first %) :error) results))]

          (if first-error
            first-error
            (do
              (log/info "[Janus OLAP] ✅ Ingestión exitosa"
                        "| stream:" stream-name
                        "| registros:" (count records)
                        "| tenant:" tenant-id)
              [:ok {:ingested-count (count records)
                    :outbox-count   0}])))))))

(defmethod ig/init-key :janus-router/olap-channel
  [_ {:keys [stream-writer stream-prefix]}]
  (log/info "  -> [Janus] OLAPChannel activo | Columnar Nativo | prefix:" stream-prefix)
  (->OLAPChannel stream-writer stream-prefix))

(defmethod ig/halt-key! :janus-router/olap-channel [_ _] nil)
