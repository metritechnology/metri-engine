(ns metri.cedar.stub
  "CedarAuthorizer stub — AlwaysAllow.
   Reemplazado por la implementación real en FASE 06.
   NUNCA desplegar en producción sin la implementación real."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]))

(defmethod ig/init-key :iop/cedar-authorizer [_ _]
  (log/info "  -> [Cedar STUB] AlwaysAllow activo — sin verificación de token | FASE 06 pendiente")
  (fn [request]
    ;; AlwaysAllow: construye un ctx mínimo válido para el pipeline
    (let [tenant-id (or (:tenant-id request)
                        (get-in request [:request :proto-tenant-id])
                        (get-in request [:request :tenant-id])
                        "stub-tenant-01")
          user-id   "stub-user-01"]
      (if (= tenant-id "evil-tenant")
        [:error {:code :JANUS_403
                 :reason "access-denied"
                 :tenant-id tenant-id}]
        [:ok (merge request
                    {:tenant-id          tenant-id
                     :user-id            user-id
                     :roles              #{"tenant-admin"}
                     :domain-boundaries  {}
                     :is-super-master    false
                     :cross-tenant-scope "NONE"
                     :status             "ACTIVE"
                     :granted-action-keys #{"*:CREATE" "*:VIEW" "*:UPDATE" "*:DELETE"}})]))))

(defmethod ig/halt-key! :iop/cedar-authorizer [_ _] nil)
