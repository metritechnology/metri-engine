(ns metri.infrastructure.datahike
  "Componente OLTP embebido: Datahike Serverless sobre DynamoDB.
   Ciclo de vida gestionado por Integrant (ig/init-key / ig/halt-key!).
   Un solo componente por proceso — el Pool Model garantiza aislamiento
   multitenant vía tenant-guard, no por conexiones separadas."
  (:require [integrant.core :as ig]
            [datahike.api :as d]
            [datahike-dynamodb.core]
            [taoensso.timbre :as log]))

;; ── Integrant Lifecycle ───────────────────────────────────────────────────────

(defmethod ig/init-key :infra/datahike
  [_ config]
  (let [cfg (assoc config :keep-history? false :schema-flexibility :write :allow-unsafe-config true)]
    (log/info "  -> [Datahike] Inicializando | backend:" (get-in cfg [:store :backend]) "| table:" (get-in cfg [:store :table]))
    (try
      (when-not (d/database-exists? cfg)
        (log/info "  -> [Datahike] Bootstrapping nueva base de datos Datalog...")
        (d/create-database cfg))
      (let [conn (d/connect cfg)]
        (log/info "  -> [Datahike] Link OLTP estable")
        {:conn conn :cfg cfg})
      (catch Exception e
        (throw (ex-info "[Datahike] Imposible inicializar — fallo de Boot / DynamoDB"
                        {:cfg cfg :cause (ex-message e)}))))))

(defmethod ig/halt-key! :infra/datahike
  [_ {:keys [conn]}]
  (log/info "  -> [Datahike] Liberando conexión")
  (try (d/release conn) (catch Exception _ nil)))

;; ── Helpers para uso interno del bootstrap ────────────────────────────────────

(defn transact-schema!
  "Transacciona un schema de atributos Datahike (vector de attr maps).
   Idempotente — usa :db/ident como identidad."
  [{:keys [conn]} schema-attrs]
  (when (seq schema-attrs)
    (d/transact conn schema-attrs)
    (log/info "  -> [Datahike] Schema transaccionado:" (count schema-attrs) "atributos")))

(defn q
  "Ejecuta una query Datalog. Retorna el resultado o lanza."
  [{:keys [conn]} query & args]
  (apply d/q query @conn args))

(defn transact!
  "Ejecuta una transacción. Retorna el tx-report o lanza."
  [{:keys [conn]} tx-data]
  (d/transact conn tx-data))
