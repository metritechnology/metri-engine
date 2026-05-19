;; [PORTED_TO_RUST: src/codice/sequence.rs]
;; NO MODIFICAR — fuente de verdad en Rust
(ns metri.codice.sequence
  "FASE 02 — MÓDULO IV: Generador secuencial ACID.
   Genera el siguiente código con scope resolution y WRITE ACID en sequence_registry.

   CUMPLIMIENTO FASE 10 + D7 (Pool Model):
   - READ usa tenant-guard/query-with-tenant (NO d/q directo)
   - WRITE usa tenant-guard/transact-with-tenant! (NO d/transact directo)
   - Retorna [:ok string] | [:error {:code :JNS_SEQ_001 ...}]
   - Todo error construido con errors/error del catálogo maestro (D3)"
  (:require [malli.core          :as m]
            [taoensso.timbre     :as log]
            [metri.domain.errors :as errors]
            [metri.otel.spans    :as otel]))

;; ── Constantes ────────────────────────────────────────────────────────────
(def ^:const SEQ-SUFFIX "_seq")

;; ── Helpers de sequence_code ──────────────────────────────────────────────
(defn- build-sequence-code
  "Construye la clave canónica del contador.
   Con scope: 'tenant_id:field_name:scope_tag_seq'
   Sin scope:  'tenant_id:field_name_seq'"
  [tenant-id field-name scope-tag]
  (if scope-tag
    (str tenant-id ":" field-name ":" scope-tag SEQ-SUFFIX)
    (str tenant-id ":" field-name SEQ-SUFFIX)))

(defn- format-code
  "Aplica zero-padding: prefix + zero-padded new-value."
  [prefix padding new-value]
  (str prefix (format (str "%0" padding "d") new-value)))

;; ── READ sequence_registry vía tenant-guard ───────────────────────────────
;; D7: NUNCA usa d/q directo — toda lectura pasa por tenant-guard.
;; El tenant-guard es el objeto Integrant que implementa ITenantGuard.
;; La invocación es vía protocolos Clojure — sin require del namespace infra.
(defn- read-sequence
  [tenant-guard db-conn tenant-id sequence-code]
  (try
    (otel/with-span ["codice.autogen.sequential.read" {:kind :internal}]
      (let [span (otel/current-span)]
        (otel/set-attributes! span
          {"tenant.id"     (str tenant-id)
           "sequence_code" sequence-code})
        ;; Invocación vía protocolo — el objeto tenant-guard implementa query-with-tenant
        (let [result ((:query-with-tenant tenant-guard)
                      @db-conn tenant-id
                      '[:find (pull ?e [:sequence_registry/current_value
                                        :sequence_registry/prefix
                                        :sequence_registry/padding_length])
                         :in $ ?code
                         :where [?e :sequence_registry/sequence_code ?code]]
                      sequence-code)]
          (first (first result)))))
    (catch Exception e
      (log/warn "sequence READ failed" {:code sequence-code :error (ex-message e)})
      nil)))

;; ── WRITE ACID vía tenant-guard ───────────────────────────────────────────
;; D7: NUNCA usa d/transact directo — toda escritura pasa por tenant-guard.
;; unique:identity en sequence_code → UPSERT ACID (idempotente).
(defn- write-sequence!
  [tenant-guard db-conn tenant-id sequence-code prefix padding new-value scope-tag]
  (otel/with-span ["codice.autogen.sequential.write" {:kind :internal}]
    (let [span (otel/current-span)]
      (otel/set-attributes! span
        {"tenant.id"      (str tenant-id)
         "sequence_code"  sequence-code
         "new_value"      new-value})
      ;; Invocación vía protocolo — el objeto tenant-guard implementa transact-with-tenant!
      ((:transact-with-tenant! tenant-guard)
       db-conn tenant-id
       [{:sequence_registry/sequence_code    sequence-code
         :sequence_registry/tenant_id        (str tenant-id)
         :sequence_registry/prefix           prefix
         :sequence_registry/padding_length   padding
         :sequence_registry/current_value    new-value
         :sequence_registry/parent_scope_tag (str scope-tag)}])
      (otel/set-status! span :ok))))

;; ── next! — punto de entrada público ─────────────────────────────────────
;;
;; Flujo:
;;   1. Determinar scope-tag del payload (directo, indirecto o global)
;;   2. Construir sequence_code
;;   3. READ sequence_registry vía tenant-guard (D7)
;;   4. Si no existe + nearest_registered → FALLBACK a código GLOBAL
;;   5. WRITE ACID vía tenant-guard (D7)
;;   6. Format: prefix + zero-pad(new-value, padding)
;;
;; Retorna: [:ok string] ej: [:ok "WO-L-K92MXA-0043"] o [:ok "WO-0042"]
;;          [:error map] con :code :JNS_SEQ_001 si falla el WRITE
(defn next!
  "Genera el siguiente código secuencial ACID con scope resolution.
   Llamado por generator/inject! — nunca directamente desde Janus.
   D7: todo I/O a Datahike pasa por tenant-guard."
  [db-conn tenant-guard attr-config scope-field tenant-id payload]
  (otel/with-span ["codice.autogen.sequential" {:kind :internal}]
    (let [span        (otel/current-span)
          field-name  (name (:name attr-config))
          prefix      (get attr-config :prefix "")
          padding     (get attr-config :padding 4)
          resolution  (get attr-config :scope_resolution "exact")]

      (otel/set-attributes! span
        {"tenant.id"  (str tenant-id)
         "field.name" field-name})

      (try
        ;; 1. Resolver scope tag desde el payload
        (let [scope-tag  (when scope-field
                           (some-> (get payload (keyword (:name scope-field)))
                                   str))

              seq-code   (build-sequence-code tenant-id field-name scope-tag)
              existing   (read-sequence tenant-guard db-conn tenant-id seq-code)

              ;; 2. Determinar current-value según lo encontrado y la policy
              [final-code current-val]
              (cond
                ;; Registro encontrado — usa el valor actual
                existing
                [seq-code (:sequence_registry/current_value existing 0)]

                ;; nearest_registered sin registro scoped → FALLBACK GLOBAL
                (and scope-tag (= resolution "nearest_registered"))
                (let [global-code (build-sequence-code tenant-id field-name nil)
                      global-rec  (read-sequence tenant-guard db-conn tenant-id global-code)]
                  (otel/set-attributes! span {"scope.global_fallback" "true"})
                  [global-code (:sequence_registry/current_value global-rec 0)])

                ;; Sin scope o exact/root — crea/usa el código directamente
                :else
                [seq-code 0])

              new-val (inc current-val)]

          ;; 3. WRITE ACID
          (write-sequence! tenant-guard db-conn tenant-id
                           final-code prefix padding new-val scope-tag)

          ;; 4. Format y retorno Railway
          (let [generated (format-code prefix padding new-val)]
            (otel/set-status! span :ok)
            (otel/set-attributes! span {"generated_value" generated})
            [:ok generated]))

        (catch Exception e
          (otel/set-status! span :error "sequence/next! failed")
          (errors/error :JNS_SEQ_001
                        {:field    field-name
                         :tenant   (str tenant-id)
                         :cause    (ex-message e)}))))))
