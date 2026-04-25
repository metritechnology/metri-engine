(ns metri.janus-router.stubs.core
  "Stubs canónicos para tests de Janus Router, IOP, y canales OLTP/OLAP.
   Todos satisfacen los mismos Protocols que las implementaciones reales
   — Principio Liskov verificable via tests de contrato.

   STUBS DISPONIBLES:
     IStreamWriter:        InMemoryStreamWriter, ErrorStreamWriter
     IJanusWriteChannel:   NoOpWriteChannel, SpyWriteChannel, ErrorWriteChannel
     IAuditInterceptor:    SpyAuditInterceptor, ThrowingAuditInterceptor, NoOpAuditInterceptor
     IFaultNotifier:       SpyFaultNotifier, NoOpFaultNotifier
     Fns:                  make-spy-moira, make-throwing-moira"
  (:require [metri.janus-router.channels.protocol :refer [IJanusWriteChannel]]
            [metri.domain.protocols :as proto]
            [metri.domain.audit.protocol :refer [IAuditInterceptor]]))

;; ═══════════════════════════════════════════════════════════════════════════
;; IStreamWriter — InMemoryStreamWriter
;; ═══════════════════════════════════════════════════════════════════════════

(defrecord InMemoryStreamWriter [records-atom]
  proto/IStreamWriter
  (put-record! [_ _stream-name _partition-key data]
    (swap! records-atom conj data)
    [:ok {:sequence-number (str "stub-seq-" (count @records-atom))}]))

(defn make-stream-writer
  "Crea un InMemoryStreamWriter con un atom vacío.
   Acceder a los records guardados via @(:records-atom writer)."
  []
  (->InMemoryStreamWriter (atom [])))

;; ── ErrorStreamWriter: siempre retorna [:error] ──────────────────────────

(defrecord ErrorStreamWriter [error-code]
  proto/IStreamWriter
  (put-record! [_ _stream-name _partition-key _data]
    [:error {:code (or error-code :INFRA_KINESIS_001)
             :stage :infrastructure
             :detail "Simulated Kinesis failure"}]))

(defn make-error-stream-writer
  "Crea un ErrorStreamWriter que retorna el código de error dado."
  ([] (->ErrorStreamWriter :INFRA_KINESIS_001))
  ([code] (->ErrorStreamWriter code)))

;; ═══════════════════════════════════════════════════════════════════════════
;; IJanusWriteChannel — Stubs de canales
;; ═══════════════════════════════════════════════════════════════════════════

(defrecord NoOpWriteChannel []
  IJanusWriteChannel
  (route [_ _ctx]
    [:ok {:ulid "stub-ulid-noop" :channel :noop}]))

(defn make-noop-channel [] (->NoOpWriteChannel))

;; ── SpyWriteChannel: registra el ctx recibido ───────────────────────────

(defrecord SpyWriteChannel [calls-atom]
  IJanusWriteChannel
  (route [_ ctx]
    (swap! calls-atom conj ctx)
    [:ok {:ulid "spy-ulid" :channel :spy}]))

(defn make-spy-channel
  "Crea un SpyWriteChannel.
   Acceder a las llamadas via @(:calls-atom channel)."
  []
  (->SpyWriteChannel (atom [])))

;; ── ErrorWriteChannel: siempre retorna [:error] ─────────────────────────

(defrecord ErrorWriteChannel [error-code]
  IJanusWriteChannel
  (route [_ _ctx]
    [:error {:stage :janus
             :code  (or error-code :JNS_001)
             :detail "Simulated channel write failure"}]))

(defn make-error-channel
  "Crea un ErrorWriteChannel con el código de error especificado."
  ([] (->ErrorWriteChannel :JNS_001))
  ([code] (->ErrorWriteChannel code)))

;; ═══════════════════════════════════════════════════════════════════════════
;; IAuditInterceptor — Stubs de auditoría
;; ═══════════════════════════════════════════════════════════════════════════

(defrecord SpyAuditInterceptor [calls-atom]
  IAuditInterceptor
  (audit! [_ request result]
    (swap! calls-atom conj {:request request :result result})
    nil))

(defn make-spy-audit
  "Crea un SpyAuditInterceptor.
   Acceder a las llamadas via @(:calls-atom audit)."
  []
  (->SpyAuditInterceptor (atom [])))

;; ── NoOpAuditInterceptor ─────────────────────────────────────────────────

(defrecord NoOpAuditInterceptor []
  IAuditInterceptor
  (audit! [_ _request _result] nil))

(defn make-noop-audit [] (->NoOpAuditInterceptor))

;; ── ThrowingAuditInterceptor ─────────────────────────────────────────────

(defrecord ThrowingAuditInterceptor []
  IAuditInterceptor
  (audit! [_ _request _result]
    (throw (RuntimeException. "Simulated audit failure"))))

(defn make-throwing-audit [] (->ThrowingAuditInterceptor))

;; ═══════════════════════════════════════════════════════════════════════════
;; Moira Emitter — stubs como fns (no es un Protocol, es una fn inyectada)
;; ═══════════════════════════════════════════════════════════════════════════

(defn make-spy-moira
  "Crea un emitter Moira que registra sus invocaciones.
   Retorna {:calls-atom atom :fn fn}."
  []
  (let [calls (atom [])]
    {:calls-atom calls
     :fn         (fn [ctx] (swap! calls conj ctx) :ok)}))

(defn make-throwing-moira
  "Crea un emitter Moira que lanza excepción para verificar transparencia."
  []
  (fn [_ctx] (throw (RuntimeException. "Simulated Moira failure"))))

;; ═══════════════════════════════════════════════════════════════════════════
;; Fixtures canónicas — ctx y requests de prueba
;; ═══════════════════════════════════════════════════════════════════════════

(def canonical-request
  "Request gRPC canónico para tests. Nunca modificado por el IOP."
  {:entity-type "asset"
   :operation   :create
   :payload     {:name "Test Asset" :status "active"}
   :metadata    {:authorization "Bearer test-token-123"}})

(def canonical-ctx
  "Ctx enriquecido por CedarAuthorizer — entrada típica de QuotaGuard / Janus."
  {:tenant-id            "tnt-test-01"
   :user-id              "usr-test-01"
   :roles                #{"field-tech"}
   :is-super-master      false
   :cross-tenant-scope   "NONE"
   :granted-action-keys  #{"asset:CREATE"}
   :request              canonical-request})

(def canonical-bulk-request
  "Request BulkIngest canónico — para tests del OLAPChannel."
  {:entity-type "meter_reading"
   :operation   :bulk-ingest
   :data        [{:asset_id "ast-001" :value 42.5 :unit "kWh"}
                 {:asset_id "ast-002" :value 13.0 :unit "kWh"}
                 {:asset_id "ast-003" :value 77.1 :unit "kWh"}]
   :metadata    {:authorization "Bearer test-token-456"}})

(def canonical-bulk-ctx
  "Ctx para tests del OLAPChannel."
  {:tenant-id   "tnt-test-01"
   :user-id     "usr-test-01"
   :entity-type "meter_reading"
   :schema      {:entity            "meter_reading"
                 :engine            "olap"
                 :partition_strategy "YYYY-MM-DD"}
   :request     canonical-bulk-request})
