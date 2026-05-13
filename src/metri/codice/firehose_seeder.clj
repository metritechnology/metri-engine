(ns metri.codice.firehose-seeder
  "FASE 10 — Sincronización Out-of-Band de Códice hacia AWS Kinesis Firehose.
   Lee el registry de Códice y crea/verifica (upsert idempotente) un DeliveryStream
   dedicado por cada entidad marcada como engine:olap.
   El ARN del FirehoseDeliveryRole se resuelve dinámicamente desde CloudFormation
   para evitar depender del sufijo aleatorio que genera CFN en el nombre físico.

   Patrón análogo a iceberg-seeder, pero gestiona infraestructura Firehose
   vía cognitect.aws.client en lugar de DDLs Athena.

   Naming convention:
     entity:  meter_reading   → stream: metri-olap-stream-meter-reading
     entity:  audit_log       → stream: metri-olap-stream-audit-log

   IMPORTANTE: Este namespace se ejecuta desde el CLI / CI-CD, NO desde Lambda.
   Los streams una vez creados persisten independientemente del stack CloudFormation."
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [cognitect.aws.client.api :as aws]
            [metri.codice.registry :as registry]))

;; ── Configuración ─────────────────────────────────────────────────────────────

(def ^:private defaults
  {:stream-prefix   "metri-olap-stream"
   :glue-db         "metri_olap"
   :glue-region     "us-east-1"
   :s3-bucket       "metri-lake-982592308819-us-east-1"
   :buffering-mb    64
   :buffering-secs  300
   :models-dir      "resources/models"
   :cf-stack-name   "metri-engine"
   :cf-role-logical "FirehoseDeliveryRole"})

;; ── Normalización de nombres ───────────────────────────────────────────────────

(defn- entity->stream-name
  "meter_reading → metri-olap-stream-meter-reading"
  [stream-prefix entity-kw]
  (str stream-prefix "-" (str/replace (name entity-kw) #"_" "-")))

(defn- entity->table-name
  "meter_reading → meter_reading  (snake_case para Glue)"
  [entity-kw]
  (str/replace (name entity-kw) #"-" "_"))

;; ── Construcción del request CreateDeliveryStream ──────────────────────────────

(defn- build-create-request
  "Construye el mapa de parámetros cognitect para CreateDeliveryStream.
   Configuración Iceberg: DirectPut → Firehose → S3 Parquet (Iceberg table).
   role-arn: ARN resuelto dinámicamente desde CloudFormation."
  [{:keys [stream-prefix glue-db glue-region s3-bucket
           buffering-mb buffering-secs]} role-arn model]
  (let [entity-kw   (:entity model)
        stream-name (entity->stream-name stream-prefix entity-kw)
        table-name  (entity->table-name entity-kw)
        entity-slug (str/replace (name entity-kw) #"_" "-")]
    {:DeliveryStreamName stream-name
     :DeliveryStreamType "DirectPut"
     :IcebergDestinationConfiguration
     {:RoleARN role-arn
      :CatalogConfiguration
      {:CatalogArn (str "arn:aws:glue:" glue-region ":" (second (str/split role-arn #":iam::")) ":catalog")}
      :DestinationTableConfigurationList
      [{:DestinationDatabaseName glue-db
        :DestinationTableName   table-name
        :UniqueKeys             ["id"]
        :S3ErrorOutputPrefix    (str "errors/firehose/" entity-slug
                                     "/!{firehose:error-output-type}/")}]
      :BufferingHints {:SizeInMBs          buffering-mb
                       :IntervalInSeconds  buffering-secs}
      :S3Configuration
      {:BucketARN       (str "arn:aws:s3:::" s3-bucket)
       :RoleARN         role-arn
       :Prefix          (str "firehose-backup/" entity-slug
                              "/year=!{timestamp:yyyy}/month=!{timestamp:MM}"
                              "/day=!{timestamp:dd}/")
       :ErrorOutputPrefix "errors/firehose/!{firehose:error-output-type}/"}}}))

;; ── Upsert idempotente de un stream ───────────────────────────────────────────

(defn- stream-exists?
  "Retorna true si el stream ya existe en Firehose (DescribeDeliveryStream)."
  [client stream-name]
  (let [resp (aws/invoke client {:op      :DescribeDeliveryStream
                                 :request {:DeliveryStreamName stream-name}})]
    (not (:cognitect.anomalies/category resp))))

(defn- ensure-stream!
  "Crea el DeliveryStream si no existe. Idempotente — no falla si ya existe."
  [client config role-arn model]
  (let [stream-name (entity->stream-name (:stream-prefix config) (:entity model))]
    (if (stream-exists? client stream-name)
      (do
        (log/info "  ✓ Stream ya existe:" stream-name)
        :already-exists)
      (let [req  (build-create-request config role-arn model)
            resp (aws/invoke client {:op :CreateDeliveryStream :request req})]
        (if (:cognitect.anomalies/category resp)
          (do
            (log/error "  ✗ Error creando stream:" stream-name resp)
            :error)
          (do
            (log/info "  ✅ Stream creado:" stream-name)
            :created))))))

;; ── Resolución dinámica del FirehoseDeliveryRole ARN ────────────────────────

(defn- get-firehose-role-arn
  "Resuelve el ARN físico del FirehoseDeliveryRole desde CloudFormation.
   Evita hardcodear el sufijo aleatorio que CFN añade al nombre del rol."
  [region stack-name logical-id]
  (let [cf   (aws/client {:api :cloudformation :region region})
        resp (aws/invoke cf {:op      :DescribeStackResource
                             :request {:StackName         stack-name
                                       :LogicalResourceId logical-id}})]
    (if (:cognitect.anomalies/category resp)
      (throw (ex-info "No se pudo resolver el FirehoseDeliveryRole ARN"
                      {:stack stack-name :logical logical-id :resp resp}))
      (let [physical-name (get-in resp [:StackResourceDetail :PhysicalResourceId])]
        (str "arn:aws:iam::" 
             (-> (aws/client {:api :sts :region region})
                 (aws/invoke {:op :GetCallerIdentity})
                 :Account)
             ":role/" physical-name)))))

;; ── Punto de entrada CLI/CI-CD ────────────────────────────────────────────────

(defn sync-streams!
  "Punto de entrada para `clj -X metri.codice.firehose-seeder/sync-streams!`.

   Por cada entidad con engine:olap en el Códice, crea (si no existe) un
   Kinesis Firehose DeliveryStream con destino Iceberg en S3.

   Opciones aceptadas (todas opcionales):
     :models-dir      (default 'resources/models')
     :stream-prefix   (default 'metri-olap-stream')
     :glue-db         (default 'metri_olap')
     :glue-region     (default 'us-east-1')
     :s3-bucket       (default 'metri-lake-982592308819-us-east-1')
     :buffering-mb    (default 64)
     :buffering-secs  (default 300)"
  [opts]
  (let [config        (merge defaults opts)
        region        (:glue-region config)
        models-dir    (:models-dir config)

        _             (log/info "══════════════════════════════════════════════════════════")
        _             (log/info "  Firehose Seeder — Streams Dinámicos por Entidad OLAP")
        _             (log/info "══════════════════════════════════════════════════════════")

        registry-data (registry/build-registry models-dir)
        entities-olap (->> (:registry registry-data)
                           vals
                           (map :model)
                           (filter #(= "olap" (get % :engine "oltp"))))

        _             (log/info "Entidades OLAP detectadas:" (count entities-olap))
        _             (doseq [m entities-olap] (log/info "  ▸" (name (:entity m))))

        role-arn      (get-firehose-role-arn region (:cf-stack-name config) (:cf-role-logical config))
        _             (log/info "FirehoseDeliveryRole ARN:" role-arn)

        client        (aws/client {:api :firehose :region region})

        results       (doall
                       (for [model entities-olap]
                         (let [stream-name (entity->stream-name (:stream-prefix config) (:entity model))]
                           (log/info "\n──────────────────────────────────────────────────────────")
                           (log/info "▶ Sincronizando stream:" stream-name)
                           [stream-name (ensure-stream! client config role-arn model)])))]

    (log/info "\n══════════════════════════════════════════════════════════")
    (log/info "  Resultados:")
    (doseq [[stream-name result] results]
      (case result
        :already-exists (log/info "  ✓" stream-name "(ya existía)")
        :created        (log/info "  ✅" stream-name "(creado)")
        :error          (log/error "  ✗" stream-name "(ERROR)")))

    (let [errors (filter #(= :error (second %)) results)]
      (if (seq errors)
        (do
          (log/error "  ❌ Firehose Seeder finalizó con" (count errors) "errores.")
          (System/exit 1))
        (do
          (log/info "══════════════════════════════════════════════════════════")
          (log/info "  ✅ Firehose Seeder finalizado exitosamente.")
          (log/info "══════════════════════════════════════════════════════════"))))))
