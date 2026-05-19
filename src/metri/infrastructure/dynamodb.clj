;; [PORTED_TO_RUST: src/infrastructure/dynamodb.rs]
;; NO MODIFICAR ESTE ARCHIVO — la fuente de verdad ahora reside en Rust.
(ns metri.infrastructure.dynamodb
  "Cliente DynamoDB directo para QuotaGuard y operaciones de baja latencia.
   Independiente de Datahike — SDK Cognitect puro.
   Ciclo de vida gestionado por Integrant."
  (:require [integrant.core :as ig]
            [cognitect.aws.client.api :as aws]
            [taoensso.timbre :as log]
            [metri.domain.errors :as errors]))

;; ── Integrant Lifecycle ───────────────────────────────────────────────────────

(defmethod ig/init-key :infra/dynamodb
  [_ {:keys [region endpoint]}]
  (log/info "  -> [DynamoDB] Inicializando cliente SDK | region:" region
            (when endpoint (str "| endpoint: " endpoint)))
  (let [client-cfg (cond-> {:api :dynamodb :region region}
                     endpoint (assoc :endpoint-override
                                     (let [uri (java.net.URI. endpoint)]
                                       {:protocol (keyword (.getScheme uri))
                                        :hostname (.getHost uri)
                                        :port     (.getPort uri)})))
        client (aws/client client-cfg)]
    ;; Health check — ListTables
    (let [resp (aws/invoke client {:op :ListTables :request {:Limit 1}})]
      (if (:cognitect.anomalies/category resp)
        (log/warn "  -> [DynamoDB] ListTables falló (puede ser normal en local):" resp)
        (log/info "  -> [DynamoDB] Cliente activo — tablas visibles:" (count (:TableNames resp)))))
    {:client client :region region}))

(defmethod ig/halt-key! :infra/dynamodb
  [_ {:keys [client]}]
  (log/info "  -> [DynamoDB] Cerrando cliente SDK")
  (.close ^java.io.Closeable client))

;; ── Operaciones helper (usadas por QuotaGuard) ───────────────────────────────

(defn get-item
  "Obtiene un item de DynamoDB por clave primaria.
   Retorna el mapa del item o nil si no existe."
  [{:keys [client]} table-name key-map]
  (let [resp (aws/invoke client {:op      :GetItem
                                 :request {:TableName table-name
                                           :Key       key-map}})]
    (if (:cognitect.anomalies/category resp)
      (let [code (if (= "AccessDeniedException" (get-in resp [:cognitect.aws.error/code]))
                   :INFRA_DDB_004
                   :INFRA_DDB_001)]
        (errors/error code {:table   table-name
                             :key     (pr-str key-map)
                             :anomaly (select-keys resp [:cognitect.anomalies/category
                                                         :cognitect.aws.error/code
                                                         :Message])}))
      [:ok (:Item resp)])))

(defn put-item!
  "Escribe un item en DynamoDB. Retorna :ok o lanza."
  [{:keys [client]} table-name item-map]
  (let [resp (aws/invoke client {:op      :PutItem
                                 :request {:TableName table-name
                                           :Item      item-map}})]
    (if (:cognitect.anomalies/category resp)
      (let [code (cond
                   (= "AccessDeniedException" (get-in resp [:cognitect.aws.error/code])) :INFRA_DDB_004
                   (= :cognitect.anomalies/throttled (:cognitect.anomalies/category resp))  :INFRA_DDB_005
                   :else :INFRA_DDB_002)]
        (errors/error code {:table   table-name
                             :anomaly (select-keys resp [:cognitect.anomalies/category
                                                         :cognitect.aws.error/code
                                                         :Message])}))
      [:ok])))

(defn update-item!
  "Actualiza atributos de un item vía UpdateExpression."
  [{:keys [client]} table-name key-map update-expr attr-names attr-values]
  (let [resp (aws/invoke client {:op      :UpdateItem
                                 :request {:TableName                table-name
                                           :Key                      key-map
                                           :UpdateExpression         update-expr
                                           :ExpressionAttributeNames  attr-names
                                           :ExpressionAttributeValues attr-values
                                           :ReturnValues             "UPDATED_NEW"}})]
    (if (:cognitect.anomalies/category resp)
      (let [code (cond
                   (= "AccessDeniedException" (get-in resp [:cognitect.aws.error/code])) :INFRA_DDB_004
                   (= :cognitect.anomalies/throttled (:cognitect.anomalies/category resp))  :INFRA_DDB_005
                   :else :INFRA_DDB_003)]
        (errors/error code {:table       table-name
                             :key         (pr-str key-map)
                             :update_expr update-expr
                             :anomaly     (select-keys resp [:cognitect.anomalies/category
                                                             :cognitect.aws.error/code
                                                             :Message])}))
      [:ok (:Attributes resp)])))

(defn delete-item!
  "Elimina un item de DynamoDB por clave primaria. Idempotente — retorna [:ok] aunque no existiera."
  [{:keys [client]} table-name key-map]
  (let [resp (aws/invoke client {:op      :DeleteItem
                                  :request {:TableName table-name
                                            :Key       key-map}})]
    (if (:cognitect.anomalies/category resp)
      (let [code (if (= "AccessDeniedException" (get-in resp [:cognitect.aws.error/code]))
                   :INFRA_DDB_004
                   :INFRA_DDB_001)]
        (errors/error code {:table   table-name
                             :key     (pr-str key-map)
                             :anomaly (select-keys resp [:cognitect.anomalies/category
                                                         :cognitect.aws.error/code
                                                         :Message])}))
      [:ok])))
