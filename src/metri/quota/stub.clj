;; [PORTED_TO_RUST: src/cedar/quota_guard.rs]
;; NO MODIFICAR ESTE ARCHIVO.
;; La fuente de verdad para esta lógica ahora reside en Rust.
(ns metri.quota.stub
  "QuotaGuard stub — pass-through sin verificación de cuotas.
   Reemplazado por la implementación real en FASE 07."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]))

(defmethod ig/init-key :iop/quota-guard [_ _]
  (log/info "  -> [QuotaGuard STUB] Pass-through activo — sin verificación DynamoDB | FASE 07 pendiente")
  (fn [ctx]
    ;; Pass-through: propaga ctx heredado + una reserva stub
    [:ok (assoc ctx :quota-reservation
                {:id     (str "rsv-stub-" (System/currentTimeMillis))
                 :debit  1
                 :domain (get-in ctx [:request :entity-type] "unknown")
                 :status :pending})]))

(defmethod ig/halt-key! :iop/quota-guard [_ _] nil)
