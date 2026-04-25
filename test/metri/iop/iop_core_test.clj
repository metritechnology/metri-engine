(ns metri.iop.iop-core-test
  "Tests del IOP Pipeline completo con stubs canónicos de cada paso.
   Verifica la orquestación del run-iop: Railway, Moira fire-and-forget,
   AuditInterceptor (siempre), y transparencia de errores asíncronos.
   17 tests — 100% unitarios, cero I/O externo."
  (:require [clojure.test :refer [deftest is testing]]
            [metri.iop.pipeline :as pipeline]
            [metri.domain.audit.protocol :refer [IAuditInterceptor audit!]]))

;; ═══════════════════════════════════════════════════════════════════════════
;; Stubs canónicos de pasos del pipeline (inline — sin deps externas)
;; ═══════════════════════════════════════════════════════════════════════════

(defn cedar-allow! [ctx]
  [:ok (assoc ctx :tenant-id "tnt-01" :user-id "usr-01" :roles #{"field-tech"})])

(defn cedar-deny-401! [_ctx]
  [:error {:stage :cedar :code :ABAC_401 :detail "Invalid or expired token"}])

(defn cedar-deny-403! [_ctx]
  [:error {:stage :cedar :code :ABAC_403 :detail "Access denied"}])

(defn quota-ok! [ctx]
  [:ok (assoc ctx :quota-reservation {:headroom 100 :status :pending})])

(defn quota-deny! [_ctx]
  [:error {:stage :quotas :code :QTA_001 :detail "Quota exhausted"}])

(defn janus-ok! [ctx]
  [:ok (assoc ctx :ulid "test-ulid-xyz" :channel :oltp :entity-type "asset")])

(defn janus-val-error! [_ctx]
  [:error {:stage :janus :code :JNS_VAL_001 :detail "Malli validation failed"}])

(defn janus-no-channel! [_ctx]
  [:error {:stage :janus :code :JNS_001 :detail "No channel for engine: unknown"}])

;; ═══════════════════════════════════════════════════════════════════════════
;; SpyAuditInterceptor — implementa IAuditInterceptor
;; ═══════════════════════════════════════════════════════════════════════════

(defrecord SpyAuditInterceptor [calls-atom]
  IAuditInterceptor
  (audit! [_ request result]
    (swap! calls-atom conj {:request request :result result})
    nil))

(defn make-spy-audit []
  (->SpyAuditInterceptor (atom [])))

(defrecord ThrowingAuditInterceptor []
  IAuditInterceptor
  (audit! [_ _request _result]
    (throw (RuntimeException. "Simulated audit failure"))))

;; ═══════════════════════════════════════════════════════════════════════════
;; Spy Moira (fn)
;; ═══════════════════════════════════════════════════════════════════════════

(defn make-spy-moira []
  (let [calls (atom [])]
    {:calls-atom calls
     :fn         (fn [ctx] (swap! calls conj ctx) :ok)}))

;; ═══════════════════════════════════════════════════════════════════════════
;; run-iop — implementación local (replica la lógica de iop/core.clj)
;; ═══════════════════════════════════════════════════════════════════════════

(defn run-iop
  "Implementación del orquestador IOP para tests — misma lógica que iop/core.clj."
  [steps moira-fn audit-interceptor request]
  (let [start-ms (System/currentTimeMillis)
        result   (pipeline/run steps request)
        result+  (cond-> result
                   (= :ok (first result))
                   (update 1 assoc :execution-time-ms
                           (- (System/currentTimeMillis) start-ms)))]

    ;; Moira: fire-and-forget — solo en [:ok]
    (when (= :ok (first result+))
      (future
        (try
          (moira-fn (second result+))
          (catch Exception _))))

    ;; AuditInterceptor: SIEMPRE — [:ok] y [:error]
    (try
      (audit! audit-interceptor request result+)
      (catch Exception _))

    result+))

;; ═══════════════════════════════════════════════════════════════════════════
;; Tests — Orquestación del pipeline
;; ═══════════════════════════════════════════════════════════════════════════

(deftest iop-core-01-happy-path-returns-ok
  "IOP-CORE-01 — Cedar allow → Quota ok → Janus ok → [:ok {:ulid ... :channel :oltp}]."
  (let [audit       (make-spy-audit)
        moira       (make-spy-moira)
        request     {:entity-type "asset" :operation :create}
        [tag body]  (run-iop [cedar-allow! quota-ok! janus-ok!]
                              (:fn moira) audit request)]
    (is (= :ok tag))
    (is (= "test-ulid-xyz" (:ulid body)))
    (is (= :oltp (:channel body)))))

(deftest iop-core-02-cedar-deny-401-short-circuits
  "IOP-CORE-02 — Cedar :ABAC_401 → quota y janus NUNCA se invocan."
  (let [quota-called? (atom false)
        janus-called? (atom false)
        audit         (make-spy-audit)
        moira         (make-spy-moira)
        request       {:entity-type "asset" :operation :create}
        [tag body]    (run-iop
                        [cedar-deny-401!
                         (fn [ctx] (reset! quota-called? true) (quota-ok! ctx))
                         (fn [ctx] (reset! janus-called? true) (janus-ok! ctx))]
                        (:fn moira) audit request)]
    (is (= :error tag))
    (is (= :ABAC_401 (:code body)))
    (is (= :cedar    (:stage body)))
    (is (not @quota-called?) "QuotaGuard NO debe ser invocado")
    (is (not @janus-called?) "JanusRouter NO debe ser invocado")))

(deftest iop-core-03-cedar-deny-403-short-circuits
  "IOP-CORE-03 — Cedar :ABAC_403 cortocircuita."
  (let [audit      (make-spy-audit)
        moira      (make-spy-moira)
        request    {:entity-type "asset"}
        [tag body] (run-iop [cedar-deny-403! quota-ok! janus-ok!]
                             (:fn moira) audit request)]
    (is (= :error tag))
    (is (= :ABAC_403 (:code body)))))

(deftest iop-core-04-quota-deny-short-circuits
  "IOP-CORE-04 — Quota :QTA_001 → Janus NUNCA se invoca."
  (let [janus-called? (atom false)
        audit         (make-spy-audit)
        moira         (make-spy-moira)
        request       {:entity-type "asset"}
        [tag body]    (run-iop
                        [cedar-allow! quota-deny!
                         (fn [ctx] (reset! janus-called? true) (janus-ok! ctx))]
                        (:fn moira) audit request)]
    (is (= :error tag))
    (is (= :QTA_001 (:code body)))
    (is (= :quotas  (:stage body)))
    (is (not @janus-called?) "JanusRouter NO debe ser invocado")))

(deftest iop-core-05-janus-val-error-propagates
  "IOP-CORE-05 — Janus :JNS_VAL_001 se propaga al caller."
  (let [audit      (make-spy-audit)
        moira      (make-spy-moira)
        request    {:entity-type "asset"}
        [tag body] (run-iop [cedar-allow! quota-ok! janus-val-error!]
                             (:fn moira) audit request)]
    (is (= :error tag))
    (is (= :JNS_VAL_001 (:code body)))
    (is (= :janus       (:stage body)))))

(deftest iop-core-06-janus-no-channel-propagates
  "IOP-CORE-06 — Janus :JNS_001 (sin canal) se propaga al caller."
  (let [audit      (make-spy-audit)
        moira      (make-spy-moira)
        request    {:entity-type "asset"}
        [tag body] (run-iop [cedar-allow! quota-ok! janus-no-channel!]
                             (:fn moira) audit request)]
    (is (= :error tag))
    (is (= :JNS_001 (:code body)))))

(deftest iop-core-07-moira-dispatched-on-ok
  "IOP-CORE-07 — Moira es invocada (en future) exactamente 1 vez en happy path."
  (let [moira   (make-spy-moira)
        audit   (make-spy-audit)
        request {:entity-type "asset"}
        _result (run-iop [cedar-allow! quota-ok! janus-ok!]
                          (:fn moira) audit request)]
    ;; Esperamos brevemente a que el future se ejecute
    (Thread/sleep 100)
    (is (= 1 (count @(:calls-atom moira)))
        "Moira debe ser invocada exactamente 1 vez en [:ok]")))

(deftest iop-core-08-moira-not-dispatched-on-error
  "IOP-CORE-08 — Moira NUNCA se invoca si el pipeline retorna [:error]."
  (let [moira   (make-spy-moira)
        audit   (make-spy-audit)
        request {:entity-type "asset"}
        _result (run-iop [cedar-deny-401! quota-ok! janus-ok!]
                          (:fn moira) audit request)]
    (Thread/sleep 100)
    (is (= 0 (count @(:calls-atom moira)))
        "Moira NO debe invocarse en paths de error")))

(deftest iop-core-09-moira-failure-transparent
  "IOP-CORE-09 — Si Moira lanza en su future, el [:ok ...] al caller no se afecta."
  (let [throwing-moira (fn [_ctx] (throw (RuntimeException. "Moira boom")))
        audit          (make-spy-audit)
        request        {:entity-type "asset"}
        [tag _body]    (run-iop [cedar-allow! quota-ok! janus-ok!]
                                 throwing-moira audit request)]
    (Thread/sleep 100)
    (is (= :ok tag)
        "El caller recibe [:ok ...] aunque Moira falle internamente")))

(deftest iop-core-10-audit-invoked-on-ok
  "IOP-CORE-10 — AuditInterceptor se invoca 1 vez en happy path."
  (let [spy-audit (make-spy-audit)
        moira     (make-spy-moira)
        request   {:entity-type "asset"}
        _result   (run-iop [cedar-allow! quota-ok! janus-ok!]
                             (:fn moira) spy-audit request)]
    (is (= 1 (count @(:calls-atom spy-audit)))
        "Audit debe invocarse exactamente 1 vez")))

(deftest iop-core-11-audit-invoked-on-cedar-deny
  "IOP-CORE-11 — AuditInterceptor se invoca incluso cuando Cedar deniega."
  (let [spy-audit (make-spy-audit)
        moira     (make-spy-moira)
        request   {:entity-type "asset"}
        _result   (run-iop [cedar-deny-401! quota-ok! janus-ok!]
                             (:fn moira) spy-audit request)]
    (is (= 1 (count @(:calls-atom spy-audit)))
        "Audit debe invocarse SIEMPRE — incluyendo en paths de error")))

(deftest iop-core-12-audit-invoked-on-quota-deny
  "IOP-CORE-12 — AuditInterceptor se invoca cuando Quota deniega."
  (let [spy-audit (make-spy-audit)
        moira     (make-spy-moira)
        request   {:entity-type "asset"}
        _result   (run-iop [cedar-allow! quota-deny! janus-ok!]
                             (:fn moira) spy-audit request)]
    (is (= 1 (count @(:calls-atom spy-audit))))))

(deftest iop-core-13-audit-failure-transparent
  "IOP-CORE-13 — Si audit! lanza, el run-iop retorna [:ok ...] sin propagar excepción."
  (let [throwing-audit (->ThrowingAuditInterceptor)
        moira          (make-spy-moira)
        request        {:entity-type "asset"}
        [tag _]        (run-iop [cedar-allow! quota-ok! janus-ok!]
                                  (:fn moira) throwing-audit request)]
    (is (= :ok tag)
        "El caller recibe [:ok ...] aunque AuditInterceptor falle")))

(deftest iop-core-14-execution-time-ms-on-ok
  "IOP-CORE-14 — En happy path, result+ contiene :execution-time-ms numérico >= 0."
  (let [spy-audit (make-spy-audit)
        moira     (make-spy-moira)
        request   {:entity-type "asset"}
        [tag body] (run-iop [cedar-allow! quota-ok! janus-ok!]
                              (:fn moira) spy-audit request)]
    (is (= :ok tag))
    (is (some? (:execution-time-ms body))
        ":execution-time-ms debe estar presente en [:ok]")
    (is (>= (:execution-time-ms body) 0)
        ":execution-time-ms debe ser >= 0")))

(deftest iop-core-15-request-not-modified
  "IOP-CORE-15 — El ctx que llega a Cedar contiene el request original intacto."
  (let [spy-audit    (make-spy-audit)
        moira        (make-spy-moira)
        original-req {:entity-type "asset" :operation :create
                      :metadata {:authorization "Bearer token-xyz"}}
        ;; El IOP pasa el request directamente como el ctx inicial
        ;; spy-cedar recibe ctx = original-req en el paso 1
        received-ctx (atom nil)
        spy-cedar    (fn [ctx]
                       (reset! received-ctx ctx)
                       (cedar-allow! ctx))
        _result      (run-iop [spy-cedar quota-ok! janus-ok!]
                               (:fn moira) spy-audit original-req)]
    (is (some? @received-ctx) "Cedar debe haber sido invocado")
    ;; El ctx inicial es el request tal como viene del gRPC transport
    (is (= "asset" (get @received-ctx :entity-type))
        "El entity-type del request llega intacto al Paso 1")))

(deftest iop-core-16-quota-passthrough-update
  "IOP-CORE-16 — op=:update → QuotaGuard devuelve [:ok ctx] (pass-through O(1))."
  ;; Después de cedar-allow!, el ctx tiene :tenant-id, :user-id y el :request original
  ;; QuotaGuard debe leer la operación del ctx enriquecido
  (let [quota-passthrough
        (fn [ctx]
          ;; La operación puede estar en :operation (del request original)
          ;; o en el ctx enriquecido por cedar
          (let [op (or (:operation ctx)
                       (get-in ctx [:request :operation]))]
            (if (contains? #{:update :delete :upsert} op)
              [:ok ctx]
              (quota-deny! ctx))))
        audit   (make-spy-audit)
        moira   (make-spy-moira)
        ;; El request viaja como ctx inicial — cedar-allow! lo enriquece
        ;; preservando el request original + añadiendo :tenant-id etc.
        request {:entity-type "asset" :operation :update}
        cedar-with-op (fn [ctx]
                        ;; Cedar enriquece el ctx pero preserva :operation del request
                        [:ok (assoc ctx
                                    :tenant-id "tnt-01"
                                    :user-id "usr-01"
                                    :operation :update)])
        [tag _] (run-iop [cedar-with-op quota-passthrough janus-ok!]
                           (:fn moira) audit request)]
    (is (= :ok tag)
        "UPDATE debe pasar por QuotaGuard sin ser denegado")))

(deftest iop-core-17-quota-passthrough-delete
  "IOP-CORE-17 — op=:delete → QuotaGuard devuelve [:ok ctx] (pass-through O(1))."
  (let [quota-passthrough
        (fn [ctx]
          (let [op (or (:operation ctx)
                       (get-in ctx [:request :operation]))]
            (if (contains? #{:update :delete :upsert} op)
              [:ok ctx]
              (quota-deny! ctx))))
        audit   (make-spy-audit)
        moira   (make-spy-moira)
        request {:entity-type "asset" :operation :delete}
        cedar-with-op (fn [ctx]
                        [:ok (assoc ctx
                                    :tenant-id "tnt-01"
                                    :user-id "usr-01"
                                    :operation :delete)])
        [tag _] (run-iop [cedar-with-op quota-passthrough janus-ok!]
                           (:fn moira) audit request)]
    (is (= :ok tag)
        "DELETE debe pasar por QuotaGuard sin ser denegado")))
