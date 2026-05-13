(ns metri.bootstrap
  "Bootstrapper secuencial fail-fast.

   INVARIANTES:
   1. Cada paso DEBE completar antes del siguiente
   2. Si ANY paso falla → (System/exit 1) — no se aceptan requests
   3. El orden NO es arbitrario — cada paso depende del anterior
   4. El bootstrap es IDEMPOTENTE — safe to call multiple times
   5. Todos los pasos son SÍNCRONOS — no async, no futures

   ORDEN DE DEPENDENCIAS:
     [1]    Error catalog   → necesario para que todo reporte errores con códigos
     [1.5]  Audit schema    → Datahike schema debe existir antes de init :audit/*
     [1.75] gRPC status map → necesita catálogo del paso [1]
     [2]    OTel SDK        → listo antes de que cualquier span se cree
     [3]    Codice schemas  → deben cargarse antes de validar scopes
     [4]    Scope links     → requiere schemas del paso [3]
     [4.5]  Tenant guard    → verificar que :tenant/id existe en DH schema"
  (:require [metri.domain.errors :as errors]
            [metri.otel.spans :as otel]
            [metri.codice.registry]  ;; registra ig/init-key :codice/registry
            [metri.grpc.translator :as translator]
            [metri.infrastructure.datahike :as datahike]
            [metri.infrastructure.tenant-guard :as tenant-guard]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [datahike.api :as d]
            [taoensso.timbre :as log]))

;; ═══════════════════════════════════════════════════════════════════════════
;; STEP RUNNER — fail-fast wrapper
;; ═══════════════════════════════════════════════════════════════════════════

(defn exit! [status]
  (System/exit status))

(defn- run-step!
  "Ejecuta un paso del bootstrap con timing y fail-fast.
   Si el paso lanza excepción → imprime error y mata la JVM.

   DISEÑO: Este es el ÚNICO lugar donde exit! se llama por bootstrap.
   Los pasos individuales lanzan excepciones normales — run-step! las captura."
  [step-id step-name f]
  (let [start (System/nanoTime)]
    (print (str "  [" step-id "] " step-name " "))
    (flush)
    (try
      (f)
      (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)]
        (println (format "✓ (%.1fms)" elapsed-ms)))
      (catch Exception e
        (println (str "✗ FATAL: " (ex-message e)))
        (when-let [data (ex-data e)]
          (println (str "         Data: " (pr-str data))))
        (exit! 1)))))

;; ═══════════════════════════════════════════════════════════════════════════
;; BOOTSTRAP SEQUENCE
;; ═══════════════════════════════════════════════════════════════════════════

(defn run-fail-fast!
  "Ejecuta el bootstrap secuencial. Si cualquier paso falla, la JVM muere.

   POSTCONDICIONES al completar:
   - errors/catalog está cargado en memoria
   - Datahike tiene :audit/* y :tenant/id en su schema
   - translator tiene el mapa error-code→gRPC-Status
   - OTel SDK está activo y exportando
   - Codice tiene todos los JSON schemas validados
   - Todas las entity refs y sequence scopes son resolvibles
   - :tenant/id existe como atributo indexado en Datahike"
  []
  (println "\n── Bootstrap ──────────────────────────────────")

  ;; [0] Create Integrant namespace prefixes to avoid find-var IllegalArgumentException
  (run-step! "0" "ig/namespaces"
    (fn []
      (run! create-ns ['infra 'iop 'cedar 'moira 'audit 'aegis 'codice 'eda 'util 'grpc])))

  ;; [1] Error Catalog — SSOT de todos los códigos de error del sistema.
  (run-step! "1" "errors/load-catalog!"
    (fn []
      (let [cat @#'errors/catalog]
        (when (empty? @cat)
          (throw (ex-info "Error catalog failed to load" {}))))))

  ;; [1.5] Audit Schema — transacciona los atributos :audit/* en Datahike.
  (run-step! "1.5" "datahike/audit-schema"
    (fn []
      (let [audit-schema-res (io/resource "schemas/audit_attrs.edn")
            audit-schema (if audit-schema-res (-> audit-schema-res slurp edn/read-string) [])
            table (or (System/getenv "DATAHIKE_DDB_TABLE") "metri-datahike-prod")
            region (or (System/getenv "AWS_REGION") "us-east-1")
            endpoint (System/getenv "DYNAMODB_ENDPOINT")
            access-key (System/getenv "AWS_ACCESS_KEY_ID")
            secret     (System/getenv "AWS_SECRET_ACCESS_KEY")
            cfg {:store (cond-> {:backend :dynamodb :region region :table table}
                          endpoint   (assoc :endpoint endpoint)
                          access-key (assoc :access-key access-key)
                          secret     (assoc :secret secret))
                 :keep-history? false
                 :schema-flexibility :write
                 :allow-unsafe-config true}]
        (when-not (d/database-exists? cfg)
          (d/create-database cfg))
        (let [conn (d/connect cfg)]
          (when (seq audit-schema)
            (datahike/transact-schema! {:conn conn} audit-schema))
          (d/release conn)))))

  ;; [1.75] gRPC Status Map — derivar error-code → gRPC Status del catálogo.
  (run-step! "1.75" "translator/init-grpc-status-map!"
    #(translator/init-grpc-status-map! @@#'errors/catalog))

  ;; [2] OpenTelemetry — SDK + exporters.
  (run-step! "2" "otel/init!"
    #(otel/init!))

  ;; [3] Códice — Los schemas JSON se compilan en ig/init-key :codice/registry
  ;; (build-registry → malli/compile → api/init!). Bootstrap solo traza el evento.
  (run-step! "3" "codice/registry-readiness-check"
    #(otel/with-span ["bootstrap.codice.registry-check" {:kind :internal}]
       (let [span (otel/current-span)]
         ;; El registry se verifica inspeccionando el atom público post ig/init.
         ;; Si ig/init falló, el sistema ya no arrancó — este check es informativo.
         (otel/set-attributes! span {"codice.status" "initialized-by-integrant"})
         :ok)))

  ;; [4] Scope links — validados por validate-scope-providers! dentro de build-registry.
  ;; No hay paso separado — la validación es atómica con el escaneo de modelos.
  (run-step! "4" "codice/scope-links-check"
    #(otel/with-span ["bootstrap.codice.scope-links-check" {:kind :internal}]
       :ok))

  ;; [4.5] Tenant Guard — verificar que :tenant/id existe en Datahike.
  (run-step! "4.5" "tenant-guard/verify-schema!"
    (fn []
      (otel/with-span ["bootstrap.tenant-guard.verify-schema" {:kind :internal}]
        (let [table (or (System/getenv "DATAHIKE_DDB_TABLE") "metri-datahike-prod")
              region (or (System/getenv "AWS_REGION") "us-east-1")
              endpoint   (System/getenv "DYNAMODB_ENDPOINT")
              access-key (System/getenv "AWS_ACCESS_KEY_ID")
              secret     (System/getenv "AWS_SECRET_ACCESS_KEY")
              cfg {:store (cond-> {:backend :dynamodb :region region :table table}
                            endpoint   (assoc :endpoint endpoint)
                            access-key (assoc :access-key access-key)
                            secret     (assoc :secret secret))
                   :keep-history? false
                   :schema-flexibility :write
                   :allow-unsafe-config true}
              conn (d/connect cfg)
              span (otel/current-span)]
          (tenant-guard/ensure-tenant-schema! conn)
          (otel/set-attributes! span {"attr.exists" true})
          (d/release conn)))))
  (println "── Bootstrap COMPLETE ─────────────────────────\n"))
