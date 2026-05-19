;; [PORTED_TO_RUST: src/infrastructure/athena.rs]
;; NO MODIFICAR ESTE ARCHIVO — la fuente de verdad ahora reside en Rust.
(ns metri.infrastructure.athena
  "Cliente AWS Athena para queries OLAP en producción.
   Implementa IQueryEngine. Ciclo de vida gestionado por Integrant.
   En local, se sustituye por InMemoryQueryEngine (stubs/athena.clj)."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.domain.errors :as errors]
            [metri.domain.protocols :as proto]
            [metri.temporal.core :as temporal]
            [clojure.string :as str])
  (:import [software.amazon.awssdk.services.athena AthenaClient]
           [software.amazon.awssdk.services.athena.model
            StartQueryExecutionRequest QueryExecutionContext
            ResultConfiguration GetQueryResultsRequest
            QueryExecutionStatus QueryExecutionState]
           [software.amazon.awssdk.regions Region]
           [java.net URI]))

;; ── Implementación IQueryEngine ────────────────────────────────────────────────

(defrecord AthenaQueryEngine [^AthenaClient client workgroup output-location]
  proto/IQueryEngine

  (start-query! [_ sql database]
    (try
      (let [ctx     (-> (QueryExecutionContext/builder)
                        (.database (or database "default"))
                        (.build))
            result  (-> (ResultConfiguration/builder)
                        (.outputLocation output-location)
                        (.build))
            req     (-> (StartQueryExecutionRequest/builder)
                        (.queryString sql)
                        (.queryExecutionContext ctx)
                        (.resultConfiguration result)
                        (.workGroup workgroup)
                        (.build))
            resp    (.startQueryExecution client req)]
        [:ok {:execution-id (.queryExecutionId resp)}])
      (catch Exception e
        (errors/error :INFRA_ATHENA_001
                      {:workgroup workgroup
                       :database  (or database "default")
                       :cause     (ex-message e)}))))

  (get-query-results [_ execution-id]
    (try
      (let [req  (-> (GetQueryResultsRequest/builder)
                     (.queryExecutionId execution-id)
                     (.build))
            resp (.getQueryResults client req)
            rs   (.resultSet resp)

            ;; Leer tipos de columna desde ResultSetMetadata
            ;; Athena retorna todos los valores como VarCharValue (strings).
            ;; ColumnInfo.type() devuelve: "integer" "bigint" "double" "float"
            ;; "boolean" "varchar" "date" "timestamp" etc.
            col-infos (.columnInfo (.resultSetMetadata rs))
            col-types (mapv #(str/lower-case (.type %)) col-infos)
            col-metas (mapv (fn [ci t]
                              {:key   (.name ci)
                               :label (.name ci)
                               :type  (case t
                                        ("integer" "int" "tinyint" "smallint" "bigint" "double" "float" "real" "decimal" "numeric") "number"
                                        ("timestamp" "date") "timestamp"
                                        ("boolean") "boolean"
                                        "string")})
                            col-infos col-types)

            ;; Coercionador: string → tipo Clojure según el tipo Athena
            coerce (fn [^String s col-type]
                     (if (or (nil? s) (= "" s))
                       nil
                       (case col-type
                         ("integer" "int" "tinyint" "smallint")
                         (try (Long/parseLong s) (catch Exception _ s))
                         ("bigint")
                         (try (Long/parseLong s) (catch Exception _ s))
                         ("double" "float" "real")
                         (try (Double/parseDouble s) (catch Exception _ s))
                         ("decimal" "numeric")
                         (try (Double/parseDouble s) (catch Exception _ s))
                         ("boolean")
                         (= "true" (str/lower-case s))
                         ;; Athena timestamp/date → epoch-segundos (Long) via temporal.core
                         ("timestamp" "date")
                         (or (temporal/parse-athena-ts s) s)
                         ;; varchar, char, json, array, etc → string
                         s)))

            ;; Parsear filas omitiendo el header (primera fila)
            data-rows (rest (.rows rs))
            parsed-rows (mapv (fn [row]
                                (mapv (fn [datum i]
                                        (coerce (.varCharValue datum) (nth col-types i "")))
                                      (.data row)
                                      (range)))
                              data-rows)]

        [:ok {:execution-id execution-id
              :columns      col-metas
              :rows         parsed-rows}])
      (catch Exception e
        (log/debug "[Athena] get-query-results polling status:" (ex-message e))
        (errors/error :INFRA_ATHENA_002
                      {:execution_id execution-id
                       :cause        (ex-message e)})))))

;; ── Integrant Lifecycle ────────────────────────────────────────────────────────

(defmethod ig/init-key :infra/athena
  [_ {:keys [region workgroup output-location endpoint]}]
  (log/info "  -> [Athena] Inicializando cliente | workgroup:" workgroup)
  (let [builder (-> (AthenaClient/builder)
                    (.region (Region/of region)))
        builder (if endpoint
                  (.endpointOverride builder (URI/create endpoint))
                  builder)
        client  (.build builder)]
    (->AthenaQueryEngine client
                          (or workgroup "primary")
                          (or output-location "s3://metri-athena-results/"))))

(defmethod ig/halt-key! :infra/athena
  [_ record]
  (log/info "  -> [Athena] Cerrando cliente")
  (when-let [client (:client record)]
    (.close ^AthenaClient client)))
