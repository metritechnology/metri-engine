(ns metri.domain.protocols
  "SSOT de todos los protocolos de infraestructura del Metri Engine.
   Ningún namespace de infraestructura implementa lógica — solo satisface estos contratos.
   Los consumidores (IOP, Aegis, etc.) dependen de estos protocolos, nunca de las implementaciones.")

;; ── Session Store (HMAC Token) ─────────────────────────────────────────────
;; Usado por: CedarAuthorizer (verifica token → {tenant-id, user-id})
;; Implementación: HMACTokenStore — verifica HMAC-SHA256 local, sin red.
;;   get-session  : verifica firma + expiry + blacklist → identidad
;;   put-session! : REVOCAR token (escribe REVOKED#<jti> en blacklist DynamoDB)
;;   del-session! : quitar de blacklist (des-revocar)

(defprotocol ISessionStore
  "Verificador de tokens HMAC. Impl: HMACTokenStore (metres.infrastructure.session-store)."
  (get-session   [store token]
    "Verifica token 'mk_...' → {:tenant-id :user-id :jti :exp} o nil si inválido/expirado/revocado.")
  (put-session!  [store token session-map ttl-seconds]
    "REVOCAR: añade el token a la blacklist DynamoDB. En modo HMAC, session-map se ignora.")
  (del-session!  [store token-or-jti]
    "Quita un jti de la blacklist (des-revocar). Idempotente."))

;; ── SQS FIFO Bus (ElasticMQ / AWS SQS) ───────────────────────────────────────
;; Usado por: MoiraEmitter (Outbox Pattern → webhooks/EDA)

(defprotocol ISQSBus
  "Bus de mensajes FIFO. Implementaciones: SQS real / ElasticMQ local / atom (test)."
  (publish!         [bus payload group-id dedup-id]
    "Publica un mensaje en la cola FIFO. Retorna {:message-id ...} o lanza.")
  (receive-messages [bus max-count]
    "Recibe hasta max-count mensajes. Retorna vector de {:receipt-handle :body}.")
  (delete-message!  [bus receipt-handle]
    "Confirma el procesamiento eliminando el mensaje de la cola."))

;; ── Query Engine (Athena / stub) ─────────────────────────────────────────────
;; Usado por: AegisPipeline (OLAP queries)

(defprotocol IQueryEngine
  "Motor de consultas analíticas. Implementaciones: AthenaClient / InMemoryQueryEngine."
  (start-query!      [engine sql database]
    "Inicia una query asíncrona. Retorna [:ok {:execution-id id}] | [:error ...].")
  (get-query-results [engine execution-id]
    "Espera y retorna resultados. Retorna [:ok {:columns [] :rows []}] | [:error ...]."))

;; ── Stream Writer (Kinesis / stub) ───────────────────────────────────────────
;; Usado por: AuditInterceptor (bulk analytics events), BulkIngest

(defprotocol IStreamWriter
  "Escritura de registros a un stream analítico. Implementaciones: KinesisWriter / InMemoryStreamWriter."
  (put-record! [writer stream-name partition-key data]
    "Escribe un record al stream. Retorna [:ok {:sequence-number ...}] | [:error ...]."))

;; ── Event Bus (EventBridge / stub) ────────────────────────────────────────────
;; Usado por: SherlogNotifier (Fault Bus — escalaciones DOMAIN_FAULT_ESCALATED)

(defprotocol IEventBus
  "Bus de eventos de dominio. Implementaciones: EventBridgeClient / InMemoryEventBus."
  (put-event! [bus event-bus-name source detail-type detail]
    "Publica un evento de dominio. Retorna [:ok {:event-id ...}] | [:error ...]."))

;; ── Read Path — Janus Cerebro + Aegis ────────────────────────────────────────
;; Usados por: JanusCerebro (05.01) ↔ CedarAuthorizer (06) ↔ AegisTransmuter (05)

(defprotocol ICedarContext
  "Autorización Zero-Trust. Implementaciones: CedarAuthorizer (prod) / stub AlwaysAllow (dev).
   Retorna Railway: [:ok cedar-ctx] | [:error error-map]."
  (intercept [this request]
    "Evalúa el request contra políticas Cedar.
     cedar-ctx cumple :metri.cedar/context-invariant (tenant-id, user-id, roles, domain-boundaries)."))

(defprotocol IASTCompiler
  "Compilador puro de AST IR. Sin I/O. Sin estado.
   Implementaciones: JanusASTCompiler / InMemoryASTCompiler (tests)."
  (compile-ast [this query-descriptor cedar-ctx]
    "Transforma descriptor gRPC + cedar-ctx → AST IR inmutable.
     Retorna [:ok ast-ir-map] | [:error error-map].
     Invariante: ast-ir[:where] siempre incluye [:= :entity/tenant-id tenant-id]."))

(defprotocol IAegisEngine
  "Motor analítico ciego. Transpila AST IR → Datalog (OLTP) o SQL (OLAP) y ejecuta.
   Implementaciones: AegisTransmuter (prod) / InMemoryAegisEngine (tests)."
  (transmute! [this ast-ir]
    "Ejecuta el AST IR contra el motor de datos correcto.
     Retorna lazy-seq de [:ok chunk-map] | un único [:error error-map]."))
