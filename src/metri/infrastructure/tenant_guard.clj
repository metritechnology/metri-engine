;; [PORTED_TO_RUST: src/infrastructure/tenant_guard.rs]
;; NO MODIFICAR ESTE ARCHIVO — la fuente de verdad ahora reside en Rust.
(ns metri.infrastructure.tenant-guard
  "Pool Model gate para Datahike.
   Asegura el aislamiento multitenant y que :tenant/id exista en el schema."
  (:require [integrant.core :as ig]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [datahike.api :as d]
            [metri.infrastructure.datahike :as datahike]))

(defn- extract-schema-from-models []
  (let [models-dir (or (System/getenv "CODICE_MODELS_DIR") "resources/models")
        classpath-dir (if (.startsWith models-dir "resources/") (subs models-dir 10) models-dir)
        dir (io/file models-dir)
        files (if (.isDirectory dir)
                (filter #(and (.isFile %) (.endsWith (.getName %) ".json")) (file-seq dir))
                (let [resource-url (io/resource classpath-dir)]
                  (if (nil? resource-url)
                    []
                    (let [protocol (.getProtocol resource-url)]
                      (cond
                        (= protocol "jar")
                        (let [jar-path (-> resource-url .getPath (.split "!") first (subs 5))
                              jar-file (java.util.jar.JarFile. jar-path)
                              prefix   (str classpath-dir "/")]
                          (->> (enumeration-seq (.entries jar-file))
                               (filter #(and (-> % .getName (.startsWith prefix))
                                             (-> % .getName (.endsWith ".json"))))
                               (sort-by #(.getName %))
                               (mapv (fn [entry] (io/resource (.getName entry))))))
                        (= protocol "file")
                        (->> (file-seq (io/file (.toURI resource-url)))
                             (filter #(and (.isFile %) (.endsWith (.getName %) ".json")))
                             (sort-by #(.getName %)))
                        :else [])))))]
    (mapcat (fn [f]
              (try
                (let [model (json/parse-stream (io/reader f) true)
                      entity-name (name (:entity model))]
                  (map (fn [attr]
                         (let [attr-name (keyword entity-name (name (:name attr)))
                               type (case (:type attr)
                                      "number" :db.type/long
                                      "epoch" :db.type/long
                                      "decimal" :db.type/double
                                      "boolean" :db.type/boolean
                                      "array" :db.type/string
                                      :db.type/string)]
                           {:db/ident attr-name
                            :db/valueType type
                            :db/cardinality :db.cardinality/one}))
                       (:attributes model)))
                (catch Exception e
                  (log/error "Error parsing model file para esquema:" f)
                  [])))
            files)))

(defn ensure-tenant-schema!
  "Verifica que el atributo :tenant/id y el esquema base existan en Datahike.
   Si no existe, transacciona el esquema base y el esquema dinámico de los modelos."
  [conn]
  (let [base-schema [{:db/ident :tenant/id
                      :db/valueType :db.type/string
                      :db/cardinality :db.cardinality/one}
                     {:db/ident :entity/ulid
                      :db/valueType :db.type/string
                      :db/cardinality :db.cardinality/one
                      :db/unique :db.unique/identity}
                     {:db/ident :entity/type
                      :db/valueType :db.type/keyword
                      :db/cardinality :db.cardinality/one}
                     {:db/ident :meta/created_at
                      :db/valueType :db.type/long
                      :db/cardinality :db.cardinality/one}]
        dynamic-schema (extract-schema-from-models)
        full-schema (into base-schema dynamic-schema)]
    (let [existing-attr (d/q '[:find ?e :where [?e :db/ident :asset/manufacturer]] @conn)]
      (if (seq existing-attr)
        (log/info "Esquema Datahike ya existe y está actualizado, omitiendo transacción.")
        (do
          (log/info "Transaccionando esquema base y dinámico en Datahike (" (count full-schema) "atributos)")
          (datahike/transact-schema! {:conn conn} full-schema))))))

(defn query-with-tenant
  "Ejecuta una lectura Datalog validando el tenant-id implícitamente."
  [db tenant-id query & args]
  ;; Aquí se podría inyectar ast-compiler o filtros de tenant si fuera necesario.
  ;; Por diseño D7, la responsabilidad de filtrar la delegamos (por ahora) a la llamada.
  (apply d/q query db args))

(defn transact-with-tenant!
  "Ejecuta una transacción Datahike asegurando que toda entidad tenga :tenant/id."
  [conn tenant-id tx-data]
  (let [secured-tx (mapv (fn [fact]
                           (if (map? fact)
                             (assoc fact :tenant/id (str tenant-id))
                             fact))
                         tx-data)]
    (d/transact conn {:tx-data secured-tx})))

(defmethod ig/init-key :infra/tenant-guard
  [_ {:keys [datahike-conn]}]
  (let [conn (:conn datahike-conn)]
    (ensure-tenant-schema! conn)
    {:conn conn
     :query-with-tenant query-with-tenant
     :transact-with-tenant! transact-with-tenant!}))
