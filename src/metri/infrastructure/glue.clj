(ns metri.infrastructure.glue
  "AWS Glue Infrastructure Client — Dynamic Schema Management for OLAP.
   Sincroniza el Catálogo de Glue con los modelos del Códice.

   Arquitectura: Columnar Nativo (una tabla Iceberg por entidad OLAP)
   ──────────────────────────────────────────────────────────────────
   En lugar de actualizar una tabla genérica 'olap_events', este cliente
   sincroniza el esquema de CADA tabla individual de entidad OLAP con Glue.
   La tabla física la crea el iceberg-seeder (CI/CD); este cliente solo
   actualiza sus columnas si el modelo del Códice ha evolucionado."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [cheshire.core :as json]
            [integrant.core :as ig]
            [cognitect.aws.client.api :as aws]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]
            [taoensso.timbre :as log])
  (:import [java.util.jar JarFile]))

;; ── Mapeo de tipos Códice → Glue ─────────────────────────────────────────────

(def ^:private codice->glue-type
  {"double"    "double"
   "float"     "double"
   "decimal"   "double"
   "epoch"     "double"
   "long"      "bigint"
   "int"       "bigint"
   "bigint"    "bigint"
   "timestamp" "bigint"
   "uuid"      "string"
   "string"    "string"
   "text"      "string"
   "reference" "string"
   "enum"      "string"
   "json"      "string"
   "boolean"   "boolean"})

(defn- resolve-glue-type [attr]
  (get codice->glue-type (:type attr) "string"))

;; ── Resource Loader (JAR compatible) ─────────────────────────────────────────

(defn- list-model-resources [dir-name]
  (let [dir-path (if (.startsWith dir-name "resources/") (subs dir-name 10) dir-name)
        res      (io/resource dir-path)]
    (when res
      (let [proto (.getProtocol res)]
        (cond
          (= "file" proto)
          (->> (file-seq (io/file (.toURI res)))
               (filter #(and (.isFile %) (.endsWith (.getName %) ".json"))))

          (= "jar" proto)
          (let [jar-path (-> res .getPath (.split "!") first (subs 5))
                jar-file (JarFile. jar-path)
                prefix   (str dir-path "/")]
            (try
              (->> (enumeration-seq (.entries jar-file))
                   (filter #(and (.startsWith (.getName %) prefix)
                                 (.endsWith (.getName %) ".json")))
                   (mapv (fn [e] (io/resource (.getName e)))))
              (finally (.close jar-file))))
          :else [])))))

;; ── Carga de Modelos OLAP ────────────────────────────────────────────────────

(defn- load-olap-models []
  (log/info "[Glue Sync] 📂 Escaneando modelos en resources/models...")
  (let [resources (list-model-resources "models")]
    (->> resources
         (map #(json/parse-string (slurp %) true))
         (filter #(= "olap" (:engine %))))))

;; ── Generación de columnas por entidad ───────────────────────────────────────

(defn- entity->table-name [entity-keyword]
  (-> entity-keyword name (str/replace #"[-]" "_")))

(defn- build-entity-columns
  "Genera la lista de columnas Glue para una entidad OLAP.
   Columnas del sistema (id, _tenant, created_at) + columnas del dominio."
  [model]
  (let [attributes  (:attributes model)
        system-names #{"id" "_tenant" "created_at"}
        system-cols [{:Name "id"         :Type "string"}
                     {:Name "_tenant"    :Type "string"}
                     {:Name "created_at" :Type "bigint"}]
        domain-cols (->> attributes
                         (remove #(system-names (name (:name %))))
                         (map (fn [attr]
                                {:Name (str/replace (name (:name attr)) #"-" "_")
                                 :Type (resolve-glue-type attr)})))]
    (vec (concat system-cols domain-cols))))

;; ── Glue API Wrapper ──────────────────────────────────────────────────────────

(defn- get-table [client db table]
  (aws/invoke client {:op :GetTable :request {:DatabaseName db :Name table}}))

(defn- update-table-schema! [client db table-meta new-columns]
  (let [current-sd (get-in table-meta [:Table :StorageDescriptor])
        updated-sd (assoc current-sd :Columns new-columns)
        request    {:DatabaseName db
                    :TableInput   {:Name              (get-in table-meta [:Table :Name])
                                   :Description       (get-in table-meta [:Table :Description])
                                   :TableType         (get-in table-meta [:Table :TableType])
                                   :Parameters        (get-in table-meta [:Table :Parameters])
                                   :PartitionKeys     (get-in table-meta [:Table :PartitionKeys])
                                   :StorageDescriptor updated-sd}}]
    (aws/invoke client {:op :UpdateTable :request request})))

;; ── Sincronización por entidad ────────────────────────────────────────────────

(defn- sync-entity-table!
  "Sincroniza el esquema de una tabla Iceberg individual en Glue.
   Solo actualiza si hay diferencias de columnas respecto al Códice."
  [client db model]
  (let [table-name  (entity->table-name (:entity model))
        new-columns (build-entity-columns model)
        table-meta  (get-table client db table-name)]

    (if (:cognitect.anomalies/category table-meta)
      (log/warn "[Glue Sync] ⚠️ Tabla no encontrada en Glue (aún no creada por seeder):" table-name)
      (let [current-cols (get-in table-meta [:Table :StorageDescriptor :Columns])
            current-names (set (map :Name current-cols))
            new-names     (set (map :Name new-columns))]
        (if (= current-names new-names)
          (log/info "[Glue Sync] ✅" table-name "— esquema al día |" (count new-columns) "columnas")
          (do
            (log/info "[Glue Sync] ⚠️  Actualizando esquema de" table-name "| nuevas columnas:" new-columns)
            (let [res (update-table-schema! client db table-meta new-columns)]
              (if (:cognitect.anomalies/category res)
                (log/error "[Glue Sync] ❌ Error UpdateTable para" table-name ":" res)
                (log/info "[Glue Sync] 🚀" table-name "— sincronizado |" (count new-columns) "columnas")))))))))

;; ── API Pública ───────────────────────────────────────────────────────────────

(defn sync-all-entity-tables!
  "Sincroniza el esquema de TODAS las tablas OLAP en Glue.
   Itera sobre cada modelo con engine:olap y actualiza Glue si hay cambios."
  [client db]
  (log/info "[Glue Sync] 🔄 Iniciando sincronización de tablas OLAP en Glue | database:" db)
  (try
    (let [models (load-olap-models)]
      (log/info "[Glue Sync] 📄 Modelos OLAP encontrados:" (count models))
      (doseq [model models]
        (sync-entity-table! client db model)))
    (catch Exception e
      (log/error e "[Glue Sync] 💥 Fallo fatal en sincronización"))))

;; ── Integrant ────────────────────────────────────────────────────────────────

(defmethod ig/init-key :infra/glue
  [_ {:keys [endpoint region database sync-enabled?] :as opts}]
  (log/info "  -> [Glue] Inicializando cliente AWS Glue | database:" database)
  (let [aws-opts (cond-> {:api :glue :region region}
                   endpoint (assoc :endpoint-override
                                   (let [uri (java.net.URI. endpoint)]
                                     {:protocol (keyword (.getScheme uri))
                                      :hostname (.getHost uri)
                                      :port     (.getPort uri)})))
        client   (aws/client aws-opts)]

    (when sync-enabled?
      (sync-all-entity-tables! client database))

    {:client   client
     :database database}))

(defmethod ig/halt-key! :infra/glue
  [_ {:keys [client]}]
  (log/info "  <- [Glue] Cerrando cliente AWS Glue")
  (when client nil))
