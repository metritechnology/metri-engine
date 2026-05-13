(ns metri.infrastructure.valkey
  "⚠️  DEPRECATED — Este namespace ya no está activo en producción.
   Fue eliminado junto con la VPC para reducir costos AWS (~$41-56/mes).
   REEMPLAZADO POR: metres.infrastructure.session-store (InMemorySessionStore + DynamoDBSessionStore)
   Ver: resources/config/system.lambda.edn → :infra/session-store {:strategy :memory}
   CONSERVADO SOLO como referencia histórica. NO utilizar en nuevos componentes."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]))

(defmacro ^:private wcar* [conn-opts & body]
  `(car/wcar ~conn-opts ~@body))

;; ── Implementación ISessionStore ─────────────────────────────────────────────

(defrecord ValkeySessionStore [conn-opts]
  proto/ISessionStore

  (get-session [_ token]
    (try
      (let [raw (wcar* conn-opts (car/get (str "session:" token)))]
        (when raw
          (json/parse-string raw true)))
      (catch Exception e
        (log/warn "Valkey get-session falló:" (ex-message e))
        (errors/error :INFRA_VALKEY_001
                      {:timeout_ms 1000
                       :detail     (ex-message e)}))))

  (put-session! [_ token session-map ttl-seconds]
    (try
      (wcar* conn-opts
        (car/setex (str "session:" token)
                   (int ttl-seconds)
                   (json/generate-string session-map)))
      :ok
      (catch Exception e
        (log/error "Valkey put-session! falló:" (ex-message e))
        (errors/error :INFRA_VALKEY_001
                      {:timeout_ms 1000
                       :detail     (ex-message e)}))))

  (del-session! [_ token]
    (try
      (wcar* conn-opts (car/del (str "session:" token)))
      :ok
      (catch Exception e
        (log/warn "Valkey del-session! falló (idempotente):" (ex-message e))
        (errors/error :INFRA_VALKEY_001
                      {:timeout_ms 1000
                       :detail     (ex-message e)})))))

;; ── Integrant Lifecycle ───────────────────────────────────────────────────────

(defmethod ig/init-key :infra/valkey
  [_ {:keys [host port password ssl?]
      :or   {port 6379 ssl? false password ""}}]
  (log/info "  -> [Valkey] Conectando a" host ":" port)
  (let [uri (if ssl?
              (str "rediss://" host ":" port)
              (str "redis://" host ":" port))
        conn-opts {:pool {}
                   :spec (cond-> {:uri uri}
                           (seq password) (assoc :password password))}]
    (let [pong (wcar* conn-opts (car/ping))]
      (if (= "PONG" pong)
        (log/info "  -> [Valkey] PONG recibido — session store activo")
        (throw (ex-info "Valkey PING falló"
                        {:host host :port port :response pong}))))
    (->ValkeySessionStore conn-opts)))

(defmethod ig/halt-key! :infra/valkey [_ _]
  (log/info "  -> [Valkey] Cerrando conexión"))
