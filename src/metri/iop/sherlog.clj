(ns metri.iop.sherlog
  "Módulo Sherlog — Pipeline EDA de Errores (Módulo V).
   Despacha errores críticos y advertencias al Fault Bus asíncrono."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]))

(defprotocol IFaultNotifier
  (notify! [this error-dto severity]
    "Envía el DTO al destino externo de forma asíncrona o fire-and-forget.
     Nunca lanza — siempre retorna :ok o :error."))

;; ── Implementación EventBridge ───────────────────────────────────────────────

(defrecord EventBridgeNotifier [eb-client bus-name]
  IFaultNotifier
  (notify! [_ error-dto severity]
    (try
      (let [result (proto/put-event! eb-client
                                     bus-name
                                     "metri.engine" ;; source
                                     (str "DOMAIN_FAULT_" (name severity)) ;; detail-type
                                     error-dto)]
        (if (= (first result) :error)
          (log/error "Sherlog falló al emitir evento a EventBridge:" (second result))
          (log/debug "Sherlog -> EventBridge OK:" (-> result second :event-id)))
        :ok)
      (catch Exception e
        ;; Jamás debemos interrumpir el flujo principal del usuario por un fallo en la telemetría.
        (log/error e "Sherlog atrapó una excepción crítica al notificar")
        :error))))

;; ── Dispatch Central (Process-Fault) ─────────────────────────────────────────

(defn process-fault!
  "Evalúa la mónada de error original frente al catálogo.
   Si la severidad es :warning, :error o :fatal, invoca al notifier.
   Debe inyectarse el DTO ya construido para enviarlo en el payload."
  [notifier error-map error-dto]
  (let [code      (:code error-map)
        cat-entry (errors/lookup code)
        severity  (:severity cat-entry :warning)]
    (cond
      (= severity :info)
      (log/info "Fault(info):" code)

      (contains? #{:warning :error :fatal} severity)
      (do
        (log/warn "Sherlog escalando fallo" severity "->" code)
        (notify! notifier error-dto severity))

      :else
      (log/debug "Fault (unknown severity):" code))))

;; ── Integrant Lifecycle ───────────────────────────────────────────────────────

(defmethod ig/init-key :iop/sherlog
  [_ {:keys [event-bus-client bus-name]}]
  (log/info "  -> [Sherlog] Inicializando EventBridgeNotifier | Bus:" bus-name)
  (->EventBridgeNotifier event-bus-client bus-name))

(defmethod ig/halt-key! :iop/sherlog [_ _]
  (log/info "  <- [Sherlog] Apagando notificador"))
