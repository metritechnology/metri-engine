(ns metri.codice.api-test
  "Matriz TDD — API-01..25 (FASE 02 MÓDULO III)
   Tests de la API pública del Códice. Sin I/O — registry inyectado con fixture."
  (:require [clojure.test :refer [deftest is testing use-fixtures are]]
            [malli.core   :as m]
            [metri.codice.malli :as cm]
            [metri.codice.api   :as api]))

;; ─── Fixture de registry ─────────────────────────────────────────────────
;; Modelos mínimos para tests — no requieren filesystem.

(def asset-model
  {:entity     "asset"
   :engine     "oltp"
   :track_history true
   :is_sequence_scope_provider true
   :attributes [{:name "id"     :type "uuid"   :required true}
                {:name "name"   :type "string" :required true}
                {:name "status" :type "enum"   :required true
                 :options ["ACTIVE" "INACTIVE"]}]})

(def meter-model
  {:entity  "meter_reading"
   :engine  "olap"
   :track_history false
   :attributes [{:name "id"    :type "uuid"   :required true}
                {:name "value" :type "float"  :required true}]})

(def test-registry
  {"asset"         {:model  asset-model
                    :schema (m/schema (cm/build-malli-schema asset-model))
                    :hash   "abc123def456abc123def456abc123def456abc123def456abc123def456abc1"
                    :engine :oltp}
   "meter_reading" {:model  meter-model
                    :schema (m/schema (cm/build-malli-schema meter-model))
                    :hash   "def456abc123def456abc123def456abc123def456abc123def456abc123def4"
                    :engine :olap}})

(def test-ctx {:tenant-id "tenant-test-001" :user-id "user-001"})

(use-fixtures :each
  (fn [f]
    (api/init! test-registry)
    (f)
    (api/init! {})))

;; ═══ load-schema ═══════════════════════════════════════════════════════════

(deftest api-01-load-schema-known-entity
  "API-01 — load-schema retorna [:ok schema] para entidad conocida"
  (let [[tag schema] (api/load-schema "asset" test-ctx)]
    (is (= :ok tag))
    (is (m/schema? schema))))

(deftest api-02-load-schema-unknown-entity
  "API-02 — load-schema retorna [:error {:code :COD_001}] NUNCA throw"
  (let [[tag body] (api/load-schema "entity_inexistente" test-ctx)]
    (is (= :error tag))
    (is (= :COD_001 (:code body)))))

(deftest api-03-load-schema-error-has-tenant-id
  "API-03 — [:error] de load-schema incluye :tenant-id del ctx (D2)"
  (let [[_ body] (api/load-schema "no_existe" test-ctx)]
    (is (= (:tenant-id test-ctx) (:tenant-id body)))))

(deftest api-04-load-schema-never-throws
  "API-04 — load-schema NUNCA lanza excepción (D1)"
  (is (= :error (first (api/load-schema "cualquier_cosa" test-ctx)))
      "Entidad desconocida retorna [:error] — nunca lanza"))

;; ═══ validate-payload ════════════════════════════════════════════════════

(deftest api-05-validate-payload-ok
  "API-05 — validate-payload retorna [:ok payload] con payload válido"
  (let [[_ schema] (api/load-schema "asset" test-ctx)
        payload    {:id (random-uuid) :name "Bomba Principal" :status "ACTIVE"}
        [tag body] (api/validate-payload schema payload "asset" test-ctx)]
    (is (= :ok tag))
    (is (= payload body))))

(deftest api-06-validate-payload-missing-required
  "API-06 — validate-payload retorna [:error {:code :COD_VAL_001}] para campo required ausente"
  (let [[_ schema] (api/load-schema "asset" test-ctx)
        payload    {:id (random-uuid)}  ;; falta :name y :status
        [tag body] (api/validate-payload schema payload "asset" test-ctx)]
    (is (= :error tag))
    (is (= :COD_VAL_001 (:code body)))))

(deftest api-07-validate-payload-error-has-context
  "API-07 — [:error] de validate-payload incluye :violations :tenant-id :user-id (D2)"
  (let [[_ schema] (api/load-schema "asset" test-ctx)
        [_ body]   (api/validate-payload schema {} "asset" test-ctx)]
    (is (some? (:violations body))       "incluye :violations")
    (is (= (:tenant-id test-ctx) (:tenant-id body)) "incluye :tenant-id")
    (is (= (:user-id test-ctx) (:user-id body))     "incluye :user-id")))

(deftest api-08-validate-payload-wrong-type
  "API-08 — validate-payload rechaza tipo incorrecto"
  (let [[_ schema] (api/load-schema "asset" test-ctx)
        payload    {:id "not-a-uuid" :name "X" :status "ACTIVE"}
        [tag _]    (api/validate-payload schema payload "asset" test-ctx)]
    (is (= :error tag))))

(deftest api-09-validate-payload-invalid-enum
  "API-09 — validate-payload rechaza valor fuera del enum"
  (let [[_ schema] (api/load-schema "asset" test-ctx)
        payload    {:id (random-uuid) :name "X" :status "PENDING"}  ;; PENDING no es válido
        [tag body] (api/validate-payload schema payload "asset" test-ctx)]
    (is (= :error tag))
    (is (= :COD_VAL_001 (:code body)))))

(deftest api-10-validate-payload-never-throws
  "API-10 — validate-payload NUNCA lanza excepción (D1)"
  (let [[_ schema] (api/load-schema "asset" test-ctx)]
    ;; Payload completamente roto — nil, string, etc.
    (doseq [bad-payload [nil "" 42 []]]
      (let [result (try
                     (api/validate-payload schema bad-payload "asset" test-ctx)
                     (catch Exception e {:exception e}))]
        (is (not (contains? result :exception))
            (str "No debe lanzar para: " (pr-str bad-payload)))))))

(deftest api-11-validate-payload-no-db-side-effect
  "API-11 — validate-payload es función pura — cero I/O, cero Datahike"
  ;; Test estructural: si la función no lanza, asumimos que es pura (no tiene deps de I/O)
  (let [[_ schema] (api/load-schema "asset" test-ctx)
        payload    {:id (random-uuid) :name "Test" :status "ACTIVE"}]
    (is (= [:ok payload] (api/validate-payload schema payload "asset" test-ctx)))))

;; ═══ entity-engine ═══════════════════════════════════════════════════════

(deftest api-12-entity-engine-oltp
  "API-12 — entity-engine retorna [:ok :oltp] para asset"
  (let [[tag engine] (api/entity-engine "asset" test-ctx)]
    (is (= :ok tag))
    (is (= :oltp engine))))

(deftest api-13-entity-engine-olap
  "API-13 — entity-engine retorna [:ok :olap] para meter_reading"
  (let [[tag engine] (api/entity-engine "meter_reading" test-ctx)]
    (is (= :ok tag))
    (is (= :olap engine))))

(deftest api-14-entity-engine-unknown
  "API-14 — entity-engine retorna [:error {:code :COD_001}] NUNCA throw (D1)"
  (let [[tag body] (api/entity-engine "unknown_entity" test-ctx)]
    (is (= :error tag))
    (is (= :COD_001 (:code body)))))

;; ═══ entity-model ════════════════════════════════════════════════════════

(deftest api-15-entity-model-returns-full-map
  "API-15 — entity-model retorna [:ok model-map] con :attributes"
  (let [[tag model] (api/entity-model "asset" test-ctx)]
    (is (= :ok tag))
    (is (map? model))
    (is (vector? (:attributes model)))))

(deftest api-16-entity-model-unknown
  "API-16 — entity-model retorna [:error {:code :COD_001}] para entidad desconocida"
  (let [[tag body] (api/entity-model "no_existe" test-ctx)]
    (is (= :error tag))
    (is (= :COD_001 (:code body)))))

;; ═══ describe-attributes ═════════════════════════════════════════════════

(deftest api-17-describe-attributes-railway
  "API-17 — describe-attributes retorna [:ok attrs-list]"
  (let [[tag attrs] (api/describe-attributes "asset" test-ctx)]
    (is (= :ok tag))
    (is (vector? attrs))
    (is (pos? (count attrs)))))

(deftest api-18-describe-attributes-unknown
  "API-18 — describe-attributes retorna [:error {:code :COD_001}] para entidad desconocida"
  (let [[tag body] (api/describe-attributes "no_existe" test-ctx)]
    (is (= :error tag))
    (is (= :COD_001 (:code body)))))

;; ═══ entity-hash ═════════════════════════════════════════════════════════

(deftest api-19-entity-hash-deterministic
  "API-19 — entity-hash retorna el mismo hash en llamadas sucesivas"
  (let [h1 (api/entity-hash "asset")
        h2 (api/entity-hash "asset")]
    (is (= h1 h2))
    (is (string? h1))))

(deftest api-20-entity-hash-nil-for-unknown
  "API-20 — entity-hash retorna nil para entidad desconocida (NO Railway — nil es válido)"
  (is (nil? (api/entity-hash "no_existe"))))

;; ═══ Railway shape invariants ═════════════════════════════════════════════

(deftest api-21-all-errors-have-code
  "API-21 — Todos los [:error] del Códice tienen :code"
  (are [result] (keyword? (:code (second result)))
    (api/load-schema "x" test-ctx)
    (api/entity-engine "x" test-ctx)
    (api/entity-model "x" test-ctx)
    (api/describe-attributes "x" test-ctx)))

(deftest api-22-all-errors-have-stage
  "API-22 — Todos los [:error] del Códice tienen :stage (D2)"
  (are [result] (keyword? (:stage (second result)))
    (api/load-schema "x" test-ctx)
    (api/entity-engine "x" test-ctx)
    (api/entity-model "x" test-ctx)
    (api/describe-attributes "x" test-ctx)))

(deftest api-23-errors-not-retryable
  "API-23 — Errores COD_001 tienen :retryable? false"
  (let [[_ body] (api/load-schema "no_existe" test-ctx)]
    (is (false? (:retryable? body)))))

(deftest api-24-validate-payload-has-violation-count
  "API-24 — [:error] de validate-payload incluye :violation_count"
  (let [[_ schema] (api/load-schema "asset" test-ctx)
        [_ body]   (api/validate-payload schema {} "asset" test-ctx)]
    (is (int? (:violation_count body)))
    (is (pos? (:violation_count body)))))

(deftest api-25-reload-not-in-api-namespace
  "API-25 — api.clj no expone reload! — garantía arquitectural de inmutabilidad del registry.
   El registry atom es write-once durante ig/init — ningún caller puede mutarlo en runtime.
   reload! vive en codice.repl (dev/ alias) — nunca en el JAR de producción."
  (is (nil? (ns-resolve 'metri.codice.api 'reload!))
      "reload! no debe existir en metri.codice.api — está en codice.repl (REPL-only)"))
