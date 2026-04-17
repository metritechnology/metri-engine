(ns metri.infrastructure.athena
  "Componente de conexión analítica OLAP Serverless conectada a AWS Athena."
  (:import [software.amazon.awssdk.services.athena AthenaClient]))

(def ^:private -client (atom nil))

(defn create-client
  "Inicializa y cachea el cliente AWS Athena.
   Usa el 'DefaultCredentialsProviderChain' automáticamente proveniente de Java V2."
  []
  (if-let [c @-client]
    c
    (let [builder (AthenaClient/builder)
          client  (.build builder)]
      (reset! -client client)
      client)))

(defn execute-query
  "Despacha SQL al Workgroup analítico de AWS Athena."
  [sql-string]
  (let [client    (create-client)
        workgroup (or (System/getenv "ATHENA_WORKGROUP") "primary")
        request   (-> (software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest/builder)
                      (.queryString sql-string)
                      (.workGroup workgroup)
                      (.build))
        response  (.startQueryExecution client request)]
    (if-let [exec-id (.queryExecutionId response)]
      (do
        (println "Consulta Athena lanzada exitosamente:" exec-id)
        {:QueryExecutionId exec-id})
      (do
        (println "Fallo de comunicación con Athena:" response)
        nil))))
