;; [PORTED_TO_RUST: src/main.rs]
;; NO MODIFICAR ESTE ARCHIVO — la fuente de verdad ahora reside en Rust.
(ns metri.main
  "Entry point de la JVM — orquesta el ciclo de vida completo.

   RESPONSABILIDADES (y SOLO estas):
   1. Detectar entorno (dev/staging/production)
   2. Ejecutar bootstrap fail-fast
   3. Inicializar sistema Integrant
   4. Registrar shutdown hook
   5. Bloquear hilo principal

   NO CONTIENE: lógica de negocio, traducción proto, I/O directo.
   NO IMPORTA: clases Protobuf, SDKs AWS, componentes de dominio."
  (:gen-class)  ;; AOT compile — necesario para -main como entry point JVM
  (:require [integrant.core :as ig]
            [clojure.java.io :as io]
            [metri.bootstrap :as bootstrap]
            [metri.config.readers]
            [metri.stubs]
            ;; FASE 03 - IOP Real & Janus
            [metri.iop.core]
            [metri.janus-router.core]
            [metri.janus-router.channels.oltp]
            [metri.janus-router.channels.olap]
            ;; FASE 06 - Cedar Stub
            [metri.cedar.stub]
            ;; FASE 07 - Quota Stub
            [metri.quota.stub]
            ;; FASE 04 - Moira Stub
            [metri.moira.stub]
            ;; FASE 09 - Audit Stub
            [metri.infrastructure.audit.stub]
            ;; Infra
            [metri.infrastructure.athena]
            [metri.infrastructure.kinesis]
            [metri.infrastructure.eventbridge]
            [metri.infrastructure.session-store]
            ;; FASE 05 - Motor Analítico Core (Janus Cerebro + Aegis Transmuter)
            [metri.janus.ast-compiler]   ;; :janus/ast-compiler
            [metri.aegis.core]           ;; :aegis/transmuter
            [metri.janus.core]           ;; :aegis/pipeline (override del stub)
            ;; Stubs Residuales
            [metri.iop.sherlog]
            [metri.aegis.stub]           ;; registra :aegis/pipeline stub — overrideado por janus.core
            [metri.eda.stub]
            [metri.codice.registry]
            [metri.codice.generator]
            [metri.infrastructure.tenant-guard]
            [metri.grpc.dispatcher]
            [metri.grpc.server]
            [metri.grpc.service]))  ;; side-effect: registra #env, #env-bool, #env-int

;; ═══════════════════════════════════════════════════════════════════════════
;; CONSTANTES
;; ═══════════════════════════════════════════════════════════════════════════

(def ^:private version "0.1.0-SNAPSHOT")

(def ^:private banner
  "
  ╔══════════════════════════════════════════════════╗
  ║                                                  ║
  ║   █▀▄▀█ █▀▀ ▀█▀ █▀█ █                            ║
  ║   █ ▀ █ ██▄  █  █▀▄ █                            ║
  ║                                                  ║
  ║   Engine — Analytical gRPC Runtime               ║
  ║                                                  ║
  ╚══════════════════════════════════════════════════╝")

(defn exit! [status]
  (System/exit status))

;; ═══════════════════════════════════════════════════════════════════════════
;; FUNCIONES PRIVADAS
;; ═══════════════════════════════════════════════════════════════════════════

(defn- detect-environment
  "Detecta el entorno de ejecución desde la variable ENVIRONMENT.
   Retorna keyword: :development, :staging, :production.
   Default: :development (fail-safe para devs)."
  []
  (let [env (or (System/getenv "ENVIRONMENT") "development")]
    (keyword env)))

(defn- select-config-resource
  "Selecciona el recurso EDN según el entorno.
   - :production / :staging → config/system.edn
   - :development           → config/system.dev.edn (con overrides)"
  [environment]
  (case environment
    :development "config/system.dev.edn"
    :local       "config/system.dev.edn"
    ;; staging y production usan el mismo system.edn
    ;; — las diferencias viven en las variables de entorno
    "config/system.edn"))

(defn- load-system-config
  "Carga y parsea el system.edn con resolución de custom reader tags.
   PRECONDICIÓN: metri.config.readers ya fue loaded (side-effect al require)."
  [config-resource]
  (let [resource (io/resource config-resource)]
    (when-not resource
      (throw (ex-info (str "Config resource not found: " config-resource)
                      {:resource config-resource
                       :code     :BOOT_CONFIG_NOT_FOUND})))
    (ig/read-string {:readers metri.config.readers/all} (slurp resource))))

(defn- register-shutdown-hook!
  "Registra el shutdown hook de la JVM.
   Secuencia de shutdown:
   1. Log inicio de shutdown
   2. Health → NOT_SERVING (stop accepting traffic on NLB)
   3. Esperar drain (gRPC server cierra gracefully)
   4. ig/halt! en orden INVERSO de inicialización
   5. OTel flush — exportar spans pendientes
   6. Log fin de shutdown"
  [system environment]
  (.addShutdownHook (Runtime/getRuntime)
    (Thread.
      (fn []
        (let [start (System/nanoTime)]
          (println "\n── Shutdown ──────────────────────────────────")
          (println "  Environment:" (name environment))

          ;; [1] Health → NOT_SERVING
          (when-let [hm (:grpc/health-manager system)]
            (println "  [1] Health → NOT_SERVING")
            (.setStatus hm "metri.MetriService" :not-serving))

          ;; [2] gRPC server graceful shutdown (drain connections)
          (when-let [server (:grpc/server system)]
            (println "  [2] gRPC server shutting down (30s drain)...")
            (.shutdown (:server server))
            (.awaitTermination (:server server) 30 java.util.concurrent.TimeUnit/SECONDS))

          ;; [3] Integrant halt — reverse topological order
          (println "  [3] ig/halt! — releasing resources...")
          (ig/halt! system)

          ;; [4] OTel flush
          (println "  [4] OTel flush — exporting pending spans...")
          (try
            (when-let [otel (:otel/sdk system)]
              (.close otel))
            (catch Exception _))  ;; best-effort — no fail on shutdown

          (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)]
            (println (format "── Shutdown COMPLETE (%.0fms) ─────────────────\n" elapsed-ms))))))))

(defn- print-ready-banner
  "Imprime el banner de READY con información del sistema."
  [system environment]
  (let [port (get-in system [:grpc/server :port] 9090)]
    (println (str "\n✓ Metri Engine READY"))
    (println (str "  Environment:  " (name environment)))
    (println (str "  gRPC port:    " port))
    (println (str "  Health check: SERVING"))
    (println (str "  Version:      " version))
    (println (str "  PID:          " (.pid (java.lang.ProcessHandle/current))))
    (println "")))

;; ═══════════════════════════════════════════════════════════════════════════
;; ENTRY POINT
;; ═══════════════════════════════════════════════════════════════════════════

(defn -main
  "Entry point de la JVM.

   Secuencia:
   1. Banner + environment detection
   2. Bootstrap fail-fast (sequential, any failure → exit 1)
   3. Integrant init (topological dependency resolution)
   4. Register shutdown hook (SIGTERM → graceful drain)
   5. Block until SIGTERM

   CONTRATOS:
   - Si bootstrap falla → exit 1 (JVM muere, ECS/Lambda reintenta)
   - Si ig/init falla    → exit 2 (config error, no reintentable)
   - Si todo OK          → bloquea hasta SIGTERM, exit 0

   INVARIANTE: cero requests aceptados hasta que FASE 5 emite SERVING."
  [& _args]

  ;; ── FASE 1: Banner + Environment ─────────────────────────────────────
  (println banner)
  (let [environment (detect-environment)]
    (println (str "  Environment: " (name environment)))
    (println (str "  Version:     " version))
    (println (str "  JVM:         " (System/getProperty "java.version")))
    (println (str "  Timestamp:   " (java.time.Instant/now)))
    (println "")

    ;; ── FASE 2: Bootstrap Fail-Fast ──────────────────────────────────
    ;; Si cualquier paso falla → System/exit 1
    ;; No se acepta ningún request hasta que bootstrap complete.
    (bootstrap/run-fail-fast!)

    ;; ── FASE 3: Integrant Init ───────────────────────────────────────
    ;; Carga system.edn, resuelve el DAG topológico, instancia todos
    ;; los componentes en orden de dependencias.
    (println "── Integrant ─────────────────────────────────")
    (let [config-resource (select-config-resource environment)
          _               (println (str "  Config: " config-resource))
          config          (try
                            (load-system-config config-resource)
                            (catch Exception e
                              (println (str "  ✗ FATAL: Cannot load config: " (ex-message e)))
                              (exit! 2)))
          system          (try
                            (let [start (System/nanoTime)
                                  sys   (ig/init config)
                                  ms    (/ (- (System/nanoTime) start) 1e6)]
                              (println (format "  ✓ System initialized (%.0fms) — %d components"
                                               ms (count sys)))
                              sys)
                            (catch Exception e
                              (println (str "  ✗ FATAL: ig/init failed: " (ex-message e)))
                              (.printStackTrace e)
                              (when-let [data (ex-data e)]
                                (println (str "  Data: " (pr-str data))))
                              (exit! 2)))]

      ;; ── FASE 4: Shutdown Hook ────────────────────────────────────
      (register-shutdown-hook! system environment)

      ;; ── FASE 5: Ready + Block ────────────────────────────────────
      (print-ready-banner system environment)

      ;; Bloquear el hilo principal hasta SIGTERM.
      ;; @(promise) nunca se resuelve — el shutdown hook se encarga.
      @(promise))))
