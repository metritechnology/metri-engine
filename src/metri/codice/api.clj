;; [PORTED_TO_RUST: src/codice/registry.rs]
;; NO MODIFICAR ESTE ARCHIVO — la fuente de verdad ahora reside en Rust.
(ns metri.codice.api
  "FASE 02 — MÓDULO III: API pública del Códice.
   Registro en memoria de schemas Malli compilados — acceso O(1).

   CONTRATO RAILWAY (FASE 10 D1/D2/D3):
   - Todas las funciones retornan [:ok val] | [:error map]
   - NUNCA lanzan excepciones — el caller (Janus/IOP) decide qué hacer
   - Todo error se construye con errors/error del catálogo maestro (D3)
   - Todo error incluye :stage :code :tenant-id :user-id :trace-id (D2)

   CONTRATO OTel (FASE 10 D4/D5):
   - Cada función tiene otel/with-span con el nombre documentado en MÓDULO VII
   - Todo span anota tenant.id y user.id cuando ctx disponible
   - Error → otel/set-status! :error + error.code anotado"
  (:require [malli.core           :as m]
            [malli.error          :as me]
            [taoensso.timbre      :as log]
            [metri.domain.errors  :as errors]
            [metri.otel.spans     :as otel]))

;; ── Atom del registry — inmutable post-arranque ───────────────────────────
(def ^:private registry (atom {}))

(defn init!
  "Inyecta el registry construido por build-registry en el atom interno.
   Llamado UNA SOLA VEZ por ig/init-key :codice/registry."
  [built-registry]
  (reset! registry built-registry)
  (log/info "Códice API: registry activo con" (count built-registry) "entidades"))

;; ── load-schema — O(1) + Railway + OTel ──────────────────────────────────
(defn load-schema
  "Busca el schema Malli compilado para entity-type.
   Retorna [:ok schema] | [:error {:code :COD_001 ...}].
   NUNCA lanza — el caller (Janus) decide la acción. (D1)"
  [entity-type ctx]
  (otel/with-span ["codice.load-schema" {:kind :internal}]
    (let [span (otel/current-span)]
      (otel/set-attributes! span
        {"entity.type" entity-type
         "tenant.id"   (str (:tenant-id ctx))
         "user.id"     (str (:user-id ctx))})
      (if-let [schema (get-in @registry [entity-type :schema])]
        (do (otel/set-status! span :ok)
            [:ok schema])
        (do (otel/set-status! span :error "Unknown entity-type")
            (otel/set-attributes! span {"error.code" "COD_001"})
            (errors/error :COD_001
                          {:entity_type entity-type
                           :tenant-id   (:tenant-id ctx)
                           :user-id     (:user-id ctx)
                           :trace-id    (otel/trace-id span)}))))))

;; ── validate-payload — Railway + OTel ────────────────────────────────────
(defn validate-payload
  "Valida payload contra schema Malli compilado.
   Retorna [:ok payload] | [:error {:code :COD_VAL_001 ...}].
   NUNCA lanza — resultado funcional puro. (D1)"
  [schema payload entity-type ctx]
  (otel/with-span ["codice.validate-payload" {:kind :internal}]
    (let [span (otel/current-span)]
      (otel/set-attributes! span
        {"entity.type" entity-type
         "tenant.id"   (str (:tenant-id ctx))
         "user.id"     (str (:user-id ctx))})
      ;; D1: try/catch garantiza Railway puro — m/explain puede lanzar si payload
      ;; no es un mapa (nil, int, string). Convertimos toda excepción a [:error].
      (try
        (let [result (m/explain schema payload)]
          (if (nil? result)
            (do (otel/set-status! span :ok)
                [:ok payload])
            (let [violations (me/humanize result)
                  vcount     (count violations)]
              (otel/set-status! span :error "Payload validation failed")
              (otel/set-attributes! span {"error.code"      "COD_VAL_001"
                                         "violation.count" vcount})
              (errors/error :COD_VAL_001
                            {:entity_type     entity-type
                             :violations      violations
                             :violation_count vcount
                             :field           (str (first (keys violations)))
                             :expected        "see :violations"
                             :received        "see :violations"
                             :tenant-id       (:tenant-id ctx)
                             :user-id         (:user-id ctx)
                             :trace-id        (otel/trace-id span)}))))
        (catch Exception ex
          ;; Payload no-mapa (nil, int, string) — Railway puro: nunca lanzamos (D1).
          (otel/set-status! span :error "Payload type rejected by Malli")
          (errors/error :COD_VAL_001
                        {:entity_type     entity-type
                         :violations      [(ex-message ex)]
                         :violation_count 1
                         :field           "payload"
                         :expected        "map"
                         :received        (str (type payload))
                         :tenant-id       (:tenant-id ctx)
                         :user-id         (:user-id ctx)
                         :trace-id        (otel/trace-id span)}))))))

;; ── entity-engine — O(1) + Railway + OTel ────────────────────────────────
(defn entity-engine
  "Retorna [:ok engine-keyword] | [:error {:code :COD_001}].
   engine-keyword = :oltp | :olap — decide el canal de Janus. (D1)"
  [entity-type ctx]
  (otel/with-span ["codice.entity-engine" {:kind :internal}]
    (let [span (otel/current-span)]
      (otel/set-attributes! span
        {"entity.type" entity-type
         "tenant.id"   (str (:tenant-id ctx))})
      (if-let [engine (get-in @registry [entity-type :engine])]
        (do (otel/set-status! span :ok)
            (otel/set-attributes! span {"engine" (name engine)})
            [:ok engine])
        (do (otel/set-status! span :error "Unknown entity-type")
            (otel/set-attributes! span {"error.code" "COD_001"})
            (errors/error :COD_001
                          {:entity_type entity-type
                           :tenant-id   (:tenant-id ctx)
                           :user-id     (:user-id ctx)
                           :trace-id    (otel/trace-id span)}))))))

;; ── entity-model — O(1) + Railway + OTel ─────────────────────────────────
(defn entity-model
  "Retorna [:ok model-edn-map] | [:error {:code :COD_001}].
   El mapa es el JSON completo parseado como EDN. (D1)"
  [entity-type ctx]
  (otel/with-span ["codice.entity-model" {:kind :internal}]
    (let [span (otel/current-span)]
      (otel/set-attributes! span
        {"entity.type" entity-type
         "tenant.id"   (str (:tenant-id ctx))})
      (if-let [model (get-in @registry [entity-type :model])]
        (do (otel/set-status! span :ok)
            [:ok model])
        (do (otel/set-status! span :error "Unknown entity-type")
            (otel/set-attributes! span {"error.code" "COD_001"})
            (errors/error :COD_001
                          {:entity_type entity-type
                           :tenant-id   (:tenant-id ctx)
                           :user-id     (:user-id ctx)
                           :trace-id    (otel/trace-id span)}))))))

;; ── describe-attributes — O(1) + Railway + OTel ──────────────────────────
(defn describe-attributes
  "Retorna [:ok attributes-list] | [:error {:code :COD_001}].
   attributes-list = vector de maps de atributos del modelo JSON. (D1)"
  [entity-type ctx]
  (otel/with-span ["codice.describe-attributes" {:kind :internal}]
    (let [span (otel/current-span)]
      (otel/set-attributes! span {"entity.type" entity-type})
      (if-let [attrs (get-in @registry [entity-type :model :attributes])]
        (do (otel/set-status! span :ok)
            [:ok attrs])
        (do (otel/set-status! span :error "Unknown entity-type")
            (otel/set-attributes! span {"error.code" "COD_001"})
            (errors/error :COD_001
                          {:entity_type entity-type
                           :tenant-id   (:tenant-id ctx)
                           :user-id     (:user-id ctx)
                           :trace-id    (otel/trace-id span)}))))))

;; ── entity-hash — O(1) — NO Railway (nil es retorno válido) ──────────────
(defn entity-hash
  "Retorna el SHA-256 fingerprint del schema de entity-type, o nil si no existe.
   NO Railway — nil es un retorno válido informacional."
  [entity-type]
  (get-in @registry [entity-type :hash]))

;; ── discovery-handler — gRPC unary endpoint ────────────────────────────────
(defn discovery-handler
  "Maneja requests gRPC Discovery. Retorna [:discovery-response {:schemas [...]}]"
  [ctx]
  (otel/with-span ["codice.discovery" {:kind :internal}]
    (let [span (otel/current-span)
          filter-type (:type ctx)
          include-attr? (boolean (:include-attributes ctx))
          schemas (reduce-kv
                    (fn [acc entity-name data]
                      (if (or (empty? filter-type) (= filter-type entity-name))
                        (let [model (:model data)
                              attrs (if include-attr? (:attributes model) [])
                              schema {:entity entity-name
                                      :label (get model :label entity-name)
                                      :icon (get model :icon "")
                                      :primary-key (get model :primary_key "id")
                                      :fts-fields (get model :fts_fields [])
                                      :total-count 0
                                      :attributes (mapv (fn [a]
                                                          (-> a
                                                              (assoc :enum-values (or (:options a) (:enum-values a) []))
                                                              (assoc :entity-ref (or (:entityRef a) ""))))
                                                        attrs)}]
                          (conj acc schema))
                        acc))
                    []
                    @registry)]
      (otel/set-status! span :ok)
      [:ok {:schemas schemas
            :has-next false
            :next-cursor ""}])))

;; ── explore-handler — gRPC unary endpoint ────────────────────────────────
(defn explore-handler
  "Maneja requests gRPC Explore. Retorna [:ok {:values [...]}]"
  [ctx]
  (otel/with-span ["codice.explore" {:kind :internal}]
    (otel/set-status! (otel/current-span) :ok)
    [:ok {:values []}]))
