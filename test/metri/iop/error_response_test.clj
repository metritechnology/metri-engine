(ns metri.iop.error-response-test
  "Matriz TDD — ERR-01..09 (IOP Módulo IV)
   Testea build-error-dto y sanitize-context (vía acceso por reflection).
   Estrategia: with-redefs de span/get-span y errors/lookup para cero I/O.

   Fuente: src/metri/iop/error_response.clj"
  (:require [clojure.test :refer [deftest is testing]]
            [metri.iop.error-response :as er]
            [steffan-westcott.clj-otel.api.trace.span :as span]))

;; ─── Helpers ─────────────────────────────────────────────────────────────────

(def ^:private fake-span
  "Span stub que retorna un SpanContext con valores fijos."
  (reify Object))

(defn- with-noop-span
  "Ejecuta f con el span actual simulado (span-context retorna mapa fijo)."
  [f]
  (with-redefs [span/get-span      (fn [] fake-span)
                span/span-context  (fn [_] {:trace-id "aabbcc00112233440000000000000001"
                                            :span-id  "aabbcc0011223344"})]
    (f)))

(def ^:private sample-schema
  {:attributes [{:name "password" :sensitive true}
                {:name "card_number" :sensitive true}
                {:name "user_id"}]})

(def ^:private sample-error-map
  {:code    :ABAC_401
   :detail  "No valid session"
   :stage   :cedar
   :retryable? false})

(def ^:private sample-ctx
  {:tenant-id "t1" :user-id "u1"})

;; ─── ERR-01..09 ──────────────────────────────────────────────────────────────

(deftest err-01-build-error-dto-shape
  "ERR-01 — build-error-dto retorna mapa con :status 'error' y clave :error"
  (with-noop-span
    (fn []
      (let [dto (er/build-error-dto sample-error-map sample-ctx nil)]
        (is (= "error" (:status dto)))
        (is (map? (:error dto)))))))

(deftest err-02-build-error-dto-code
  "ERR-02 — :error/:code es el string del keyword (:code del error-map)"
  (with-noop-span
    (fn []
      (let [dto (er/build-error-dto sample-error-map sample-ctx nil)]
        (is (= "ABAC_401" (get-in dto [:error :code])))))))

(deftest err-03-build-error-dto-trace-id
  "ERR-03 — :error/:trace_id no es nil (viene del span OTel simulado)"
  (with-noop-span
    (fn []
      (let [dto (er/build-error-dto sample-error-map sample-ctx nil)]
        (is (string? (get-in dto [:error :trace_id])))
        (is (not (clojure.string/blank? (get-in dto [:error :trace_id]))))))))

(deftest err-04-build-error-dto-correlation-id
  "ERR-04 — :error/:correlation_id tiene prefijo 'REQ-'"
  (with-noop-span
    (fn []
      (let [dto (er/build-error-dto sample-error-map sample-ctx nil)]
        (is (clojure.string/starts-with? (get-in dto [:error :correlation_id]) "REQ-"))))))

(deftest err-05-build-error-dto-tenant-user
  "ERR-05 — :error/:tenant_id y :user_id vienen del ctx"
  (with-noop-span
    (fn []
      (let [dto (er/build-error-dto sample-error-map sample-ctx nil)]
        (is (= "t1" (get-in dto [:error :tenant_id])))
        (is (= "u1" (get-in dto [:error :user_id])))))))

(deftest err-06-build-error-dto-retryable-false
  "ERR-06 — :error/:retryable refleja :retryable? del catálogo"
  (with-noop-span
    (fn []
      (let [dto (er/build-error-dto sample-error-map sample-ctx nil)]
        (is (false? (get-in dto [:error :retryable])))))))

(deftest err-07-sanitize-sensitive-fields
  "ERR-07 — campos :sensitive en el schema se reemplazan con '[REDACTED]'"
  (with-noop-span
    (fn []
      (let [context {:password "secret123" :card_number "4111111111111111" :user_id "u1"}
            err-map (assoc sample-error-map :context context)
            dto     (er/build-error-dto err-map sample-ctx sample-schema)
            ctx-out (get-in dto [:error :context])]
        (is (= "[REDACTED]" (:password ctx-out))   "password debe estar REDACTED")
        (is (= "[REDACTED]" (:card_number ctx-out)) "card_number debe estar REDACTED")
        (is (= "u1"         (:user_id ctx-out))     "user_id NO es sensitive")))))

(deftest err-08-sanitize-nil-context
  "ERR-08 — context nil con schema no lanza — retorna nil"
  (with-noop-span
    (fn []
      (let [err-map (dissoc sample-error-map :context)
            dto     (er/build-error-dto err-map sample-ctx sample-schema)]
        (is (map? (:error dto)) "DTO debe construirse aunque context sea nil")))))

(deftest err-09-build-error-dto-nil-schema
  "ERR-09 — schema nil no rompe la construcción del DTO"
  (with-noop-span
    (fn []
      (let [err-map (assoc sample-error-map :context {:any-key "any-val"})
            dto     (er/build-error-dto err-map sample-ctx nil)]
        (is (= "error" (:status dto)))
        (is (= "any-val" (get-in dto [:error :context :any-key])))))))
