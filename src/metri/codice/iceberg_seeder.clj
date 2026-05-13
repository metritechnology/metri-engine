(ns metri.codice.iceberg-seeder
  "FASE 10 — Sincronización Out-of-Band de Códice hacia AWS Athena (Iceberg).
   Lee el registry de Códice y genera/ejecuta DDLs (CREATE TABLE IF NOT EXISTS)
   para cada entidad marcada como engine:olap — una tabla Iceberg nativa por entidad.

   Arquitectura: Columnar Nativo (sin Raw Zone genérica)
   ─────────────────────────────────────────────────────
   Cada entidad OLAP genera su propia tabla Iceberg con columnas tipadas:
     meter_reading  → (id, _tenant, created_at, asset_id, reading_value DOUBLE, ...)
     audit_log      → (id, _tenant, created_at, tenant_id, action_type, ...)
   Esto elimina json_extract_scalar y habilita predicate pushdown columnar en S3 Parquet.

   IMPORTANTE: Este namespace se ejecuta desde el CLI / CI-CD, NO bloquea el arranque
   en producción de Lambda para evitar la latencia de red de Athena en el Cold Start."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [metri.codice.registry :as registry])
  (:import [software.amazon.awssdk.services.athena AthenaClient]
           [software.amazon.awssdk.services.athena.model StartQueryExecutionRequest ResultConfiguration GetQueryExecutionRequest]
           [java.lang Thread]))

;; ── Mapeo de tipos Códice → Athena/Iceberg ───────────────────────────────────

(def ^:private type-map
  {"string"    "string"
   "text"      "string"
   "uuid"      "string"
   "reference" "string"  ;; FK como string (ULID)
   "enum"      "string"
   "integer"   "bigint"
   "long"      "bigint"
   "bigint"    "bigint"
   "float"     "double"
   "double"    "double"
   "decimal"   "double"   ;; medidas analíticas (reading_value, quantity_change)
   "epoch"     "double"   ;; timestamps unix (millis/seconds)
   "timestamp" "bigint"
   "boolean"   "boolean"
   "date"      "date"
   "json"      "string"  ;; blobs JSON semiestructurados → string
   "default"   "string"})

(defn- map-athena-type [codice-type]
  (get type-map (name (or codice-type "default")) "string"))

;; ── Generación de DDL por entidad ────────────────────────────────────────────

(defn- entity->table-name
  "Normaliza el nombre de entidad a snake_case para el nombre de tabla Iceberg."
  [entity-keyword]
  (-> entity-keyword name (str/replace #"[-]" "_")))

(defn- generate-entity-table-ddl
  "Genera el DDL CREATE TABLE IF NOT EXISTS para una entidad OLAP.
   Columnas del sistema: id, _tenant, created_at (partición por _tenant).
   Columnas del dominio: derivadas del Códice con tipos nativos Athena."
  [model s3-bucket]
  (let [table-name  (entity->table-name (:entity model))
        attributes  (:attributes model)

        ;; Columnas del sistema (siempre presentes en toda tabla OLAP)
        system-col-defs ["  id         string"
                         "  _tenant    string"
                         "  created_at bigint"]

        ;; Columnas del dominio — tipadas desde Códice
        ;; Se excluyen atributos que coincidan con columnas de sistema
        system-names #{"id" "_tenant" "created_at"}
        domain-col-defs
        (->> attributes
             (remove #(system-names (name (:name %))))
             (map (fn [attr]
                    (let [col-name (str/replace (name (:name attr)) #"-" "_")
                          col-type (map-athena-type (:type attr))]
                      (format "  %-30s %s" col-name col-type)))))

        all-col-defs (concat system-col-defs domain-col-defs)
        cols-str     (str/join ",\n" all-col-defs)]

    (format
     (str "CREATE TABLE IF NOT EXISTS metri_olap.%s (\n%s\n)\n"
          "PARTITIONED BY (_tenant)\n"
          "LOCATION 's3://%s/iceberg-data/%s/'\n"
          "TBLPROPERTIES (\n"
          "  'table_type'='ICEBERG',\n"
          "  'format'='parquet',\n"
          "  'write_compression'='snappy',\n"
          ;; Bloom filters: habilita skip de row-groups para columnas de alta
          ;; cardinalidad. Reduce scans en rangos numericos (LTE/BETWEEN) de ~9s a ~3s.
          "  'write.parquet.bloom-filter-enabled.column.reading_value'='true',\n"
          "  'write.parquet.bloom-filter-enabled.column.quantity'='true',\n"
          "  'write.parquet.bloom-filter-enabled.column.unit_of_measure'='true'\n"
          ");")
     table-name cols-str s3-bucket table-name)))

;; ── Cliente Athena ────────────────────────────────────────────────────────────

(defn- execute-athena-query! [^AthenaClient client query s3-output-loc]
  (let [req (-> (StartQueryExecutionRequest/builder)
                (.queryString query)
                (.resultConfiguration (-> (ResultConfiguration/builder)
                                          (.outputLocation s3-output-loc)
                                          .build))
                .build)
        res     (.startQueryExecution client req)
        exec-id (.queryExecutionId res)]

    (loop []
      (Thread/sleep 1000)
      (let [status-req (-> (GetQueryExecutionRequest/builder)
                           (.queryExecutionId exec-id)
                           .build)
            status-res (.getQueryExecution client status-req)
            state      (.. status-res queryExecution status state toString)]
        (cond
          (= state "SUCCEEDED") :ok
          (or (= state "FAILED") (= state "CANCELLED"))
          (do
            (log/error "Athena DDL Failed:" (.. status-res queryExecution status stateChangeReason))
            :error)
          :else (recur))))))

;; ── Punto de entrada CLI/CI-CD ────────────────────────────────────────────────

(defn sync-tables!
  "Punto de entrada para `clj -X metri.codice.iceberg-seeder/sync-tables!`.

   Por cada entidad con engine:olap en el Códice, ejecuta:
     CREATE TABLE IF NOT EXISTS metri_olap.<entity> (columnas nativas) ...

   Opciones:
     :models-dir  (default 'resources/models')
     :s3-bucket   (default 'metri-lake-982592308819-us-east-1')
     :s3-output   (default 's3://metri-lake-982592308819-us-east-1/athena-results/')"
  [opts]
  (let [models-dir (get opts :models-dir "resources/models")
        s3-bucket  (get opts :s3-bucket "metri-lake-982592308819-us-east-1")
        s3-output  (get opts :s3-output (str "s3://" s3-bucket "/athena-results/"))

        registry-data (registry/build-registry models-dir)
        entities-olap (->> (:registry registry-data)
                           vals
                           (map :model)
                           (filter #(= "olap" (get % :engine "oltp"))))]

    (log/info "══════════════════════════════════════════════════════════")
    (log/info "  Iceberg Seeder — Columnar Nativo por Entidad")
    (log/info "══════════════════════════════════════════════════════════")
    (log/info "Entidades OLAP detectadas:" (count entities-olap))
    (doseq [m entities-olap]
      (log/info "  ▸" (name (:entity m))))

    (with-open [client (-> (AthenaClient/builder) .build)]
      (doseq [model entities-olap]
        (let [table-name (entity->table-name (:entity model))
              ddl        (generate-entity-table-ddl model s3-bucket)]
          (log/info "\n──────────────────────────────────────────────────────────")
          (log/info "▶ Sincronizando tabla:" table-name)
          (log/debug "DDL:\n" ddl)
          (if (= :ok (execute-athena-query! client ddl s3-output))
            (log/info "  ✅ Tabla" table-name "sincronizada exitosamente.")
            (log/error "  ❌ Fallo al sincronizar tabla" table-name)))))

    (log/info "\n══════════════════════════════════════════════════════════")
    (log/info "  Iceberg Seeder finalizado.")
    (log/info "══════════════════════════════════════════════════════════")))
