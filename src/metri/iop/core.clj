;; [PORTED_TO_RUST: src/iop/core.rs]
;; NO MODIFICAR — fuente de verdad en Rust
(ns metri.iop.core
  "IOP -- Ingestion Orchestration Pipeline.
   Orquestador raiz del Metri Engine. No conoce dominios de negocio.
   Coordina la cadena de interceptores desacoplados en orden estricto:
     Paso 1: CedarAuthorizer  (autorizacion Zero-Trust)
     Paso 2: QuotaGuard       (control de recursos por tenant)
     Paso 3: JanusRouter      (validacion payload + ruteo canal de escritura)
     -> gRPC 200 OK al cliente
     -> (async) MoiraEmitter  (cierre EDA -- solo en [:ok])
     -> audit! SIEMPRE        ([:ok] y [:error])
     -> normalizer            (garantia de contrato de salida)"
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.iop.pipeline :as pipeline]
            [metri.janus.normalizer :as normalizer]
            [metri.domain.audit.protocol :refer [audit!]]))

(derive :iop/pipeline :iop/orchestrator)

(defmethod ig/init-key :iop/orchestrator
  [k {:keys [steps moira-emitter audit-interceptor]}]
  (log/info "  -> [IOP] Orchestrator real activo para" k "|" (count steps) "pasos sincronos")
  (fn run-iop [request]
    (let [start-ms (System/currentTimeMillis)
          result   (pipeline/run steps request)
          result+  (cond-> result
                     (= :ok (first result))
                     (update 1 assoc :execution-time-ms
                             (- (System/currentTimeMillis) start-ms)))]

      ;; Moira: fire-and-forget - solo en [:ok], fuera del hilo de respuesta gRPC
      (when (= :ok (first result+))
        (future
          (try
            (moira-emitter (second result+))
            (catch Exception e
              (log/error "[IOP] Moira fire-and-forget fallo:" (.getMessage e))))))

      ;; AuditInterceptor: SIEMPRE -- [:ok] y [:error]
      ;; Nunca altera result+ ni bloquea la respuesta al caller
      (try
        (audit! audit-interceptor request result+)
        (catch Exception e
          (log/error "[IOP] AuditInterceptor fallo:" (.getMessage e))))

      ;; Normalizer: garantia de contrato de salida (TransactionResponse / BulkResponse)
      ;; Infierre el tipo por estructura del body (entity-id -> :transaction, ingested-count -> :bulk)
      (normalizer/normalize-response result+))))

(defmethod ig/halt-key! :iop/orchestrator [_ _] nil)
