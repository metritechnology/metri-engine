(ns metri.infrastructure.audit.stub
  "AuditInterceptor stub — NoOp sin Kinesis.
   Reemplazado por la implementación real en FASE 09."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.domain.audit.protocol :refer [IAuditInterceptor]]))

(defrecord NoOpAuditInterceptor []
  IAuditInterceptor
  (audit! [_ request result]
    (log/debug "[Audit STUB] audit! invocado | status:" (first result)
               "| entity:" (get-in request [:entity-type]))))

(defmethod ig/init-key :audit/interceptor [_ _]
  (log/info "  -> [Audit STUB] AuditInterceptor stub activo — sin Kinesis | FASE 09 pendiente")
  (->NoOpAuditInterceptor))

(defmethod ig/halt-key! :audit/interceptor [_ _] nil)
