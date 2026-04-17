(ns metri.infrastructure.datahike
  "Componente de conexión transaccional OLTP embebida a AWS DynamoDB vía Datahike Serverless."
  (:require [datahike.api :as d]))

(def ^:private -connection (atom nil))

(defn get-connection
  "Inicializa, cachea y retorna la conexión activa de Datahike Serverless.
   Evalúa de forma estricta asegurando que no levante múltiples motores por invocación de Lambda."
  []
  (if-let [conn @-connection]
    conn
    (let [uri (or (System/getenv "DATAHIKE_STORE_URI") 
                  "datahike:mem://metri-dev-mem")]
      (println "[Datahike] Inicializando infraestructura en:" uri)
      (try
        ;; Con pre-vuelo aseguramos la auto-creación del catálogo
        (when-not (d/database-exists? {:uri uri})
          (println "[Datahike] Bootstrapping nuevo sistema de bases de datos Datalog...")
          (d/create-database {:uri uri}))
        (let [conn (d/connect {:uri uri})]
          (reset! -connection conn)
          (println "[Datahike] Link OLTP estable y cacheado en JVM.")
          conn)
        (catch Exception e
          (println "[Datahike_Error_Boot] Imposible inicializar servidor analítico embebido. Falla de Boot / DynamoDB."
                   "| Razón:" (.getMessage e))
          {:error {:code :DOMAIN_FAULT_ESCALATED
                   :message "Falla crítica al inicializar la base de datos Datahike en el contenedor."
                   :details (.getMessage e)}})))))
