(ns metri.moira.stub
  "MoiraEventEmitter stub — fire-and-forget sin SQS.
   Reemplazado por la implementación real en FASE 04."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]))

(defmethod ig/init-key :moira/emitter [_ _]
  (log/info "  -> [Moira STUB] EventEmitter stub activo — sin SQS | FASE 04 pendiente")
  (fn [janus-result]
    (log/debug "[Moira STUB] fire-and-forget | ulid:" (:ulid janus-result)
               "| entity:" (:entity-type janus-result))))

(defmethod ig/halt-key! :moira/emitter [_ _] nil)
