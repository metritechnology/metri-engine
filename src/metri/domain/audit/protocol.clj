(ns metri.domain.audit.protocol
  "Protocolo del AuditInterceptor.
   Se invoca SIEMPRE — [:ok] y [:error].
   Nunca bloquea. Nunca altera el result del pipeline.")

(defprotocol IAuditInterceptor
  (audit! [this request result]
    "request :: gRPC request completo
     result  :: [:ok ctx] | [:error error-map]
     Retorna nil — el resultado es ignorado por el IOP.
     NUNCA lanza — los errores internos son absorbidos y logueados."))
