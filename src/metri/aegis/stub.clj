(ns metri.aegis.stub
  "Pipeline Aegis stub para entorno local.
   Query retorna un stream de un único chunk vacío."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]))

(defmethod ig/init-key :aegis/pipeline [_ _]
  (log/info "  -> [Aegis STUB] Pipeline stub activo")
  (fn [_ctx]
    ;; Retorna una secuencia lazy de un chunk vacío (server-streaming)
    [[:ok {:data {} :metadata {:execution-time-ms 1 :stub? true}}]]))

(defmethod ig/halt-key! :aegis/pipeline [_ _] nil)
