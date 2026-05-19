;; [PORTED_TO_RUST: src/codice/registry.rs]
;; NO MODIFICAR ESTE ARCHIVO — la fuente de verdad ahora reside en Rust.
(ns metri.codice.registry
  "FASE 02 — MÓDULO I: Bootstrapper — Registry Guard.
   Escanea el filesystem de modelos JSON, hashea, compila y valida cada schema.
   Llamado UNA SOLA VEZ desde ig/init-key :codice/registry durante el arranque.
   Toda excepción aquí es fatal — la JVM no arranca si algún modelo es inválido."
  (:require [clojure.java.io     :as io]
            [cheshire.core       :as json]
            [integrant.core      :as ig]
            [malli.core          :as m]
            [taoensso.timbre     :as log]
            [metri.codice.api    :as api]
            [metri.codice.malli  :as cm])
  (:import  [java.security MessageDigest]))

;; ── SHA-256 fingerprint de un schema ─────────────────────────────────────
;; Entrada: entity name + lista de nombres de atributos (ordenada)
;; Salida:  string hex de 64 caracteres
;; Usado para detectar colisiones entre schemas distintos (COD_003)
(defn- sha256 [^String s]
  (let [md (MessageDigest/getInstance "SHA-256")
        b  (.digest md (.getBytes s "UTF-8"))]
    (format "%064x" (BigInteger. 1 b))))

(defn- schema-fingerprint
  "Genera un fingerprint SHA-256 determinístico para un modelo.
   Sensible al nombre de la entidad y a los nombres de sus atributos."
  [{:keys [entity attributes]}]
  (sha256 (str entity (mapv :name attributes))))

;; ── Parseo de un archivo JSON de modelo ──────────────────────────────────
;; Usa cheshire con keyword keys para obtener un mapa EDN navegable.
(defn- load-model-file [file]
  (with-open [r (io/reader file)]
    (json/parse-stream r true)))

(defn- list-model-files
  "Retorna una secuencia de File o URL de modelos JSON.
   Soporta filesystem directo (REPL/dev) y classpath embebido (uberjar)."
  [models-dir]
  (let [dir (io/file models-dir)]
    (if (.isDirectory dir)
      ;; Filesystem normal (REPL/dev)
      (->> (file-seq dir)
           (filter #(and (.isFile %) (.endsWith (.getName %) ".json")))
           (sort-by #(.getName %)))
      ;; Classpath / uberjar
      (let [classpath-dir (if (.startsWith models-dir "resources/")
                            (subs models-dir 10)
                            models-dir)
            resource-url  (io/resource classpath-dir)]
        (if (nil? resource-url)
          []
          (let [protocol (.getProtocol resource-url)]
            (cond
              ;; Dentro de un JAR
              (= protocol "jar")
              (let [jar-path (-> resource-url .getPath (.split "!") first (subs 5))
                    jar-file (java.util.jar.JarFile. jar-path)
                    prefix   (str classpath-dir "/")]
                (->> (enumeration-seq (.entries jar-file))
                     (filter #(and (-> % .getName (.startsWith prefix))
                                   (-> % .getName (.endsWith ".json"))))
                     (sort-by #(.getName %))
                     (mapv (fn [entry] (io/resource (.getName entry))))))
              ;; Directorio en classpath
              (= protocol "file")
              (->> (file-seq (io/file (.toURI resource-url)))
                   (filter #(and (.isFile %) (.endsWith (.getName %) ".json")))
                   (sort-by #(.getName %)))
              :else [])))))))

;; ── Compilación del schema Malli ─────────────────────────────────────────
;; Delega en metros.codice.malli/build-malli-schema y lo empaqueta con m/schema.
;; Si el schema es inválido (Malli lo rechaza), lanza → fail-fast.
(defn- compile-schema [model]
  (m/schema (cm/build-malli-schema model)))

;; ── Extracción de reglas EDA del modelo (event_rules) ────────────────────
;; Los modelos pueden declarar un bloque :event-rules con reglas de routing EDA.
;; El Bootstrapper los acumula en memoria para seed posterior en Datahike.
;; SSOT de los datos: event_routing_rule en Datahike (no los archivos JSON).
(defn- extract-event-rules [{:keys [entity event-rules]}]
  (mapv (fn [rule]
          (assoc rule
                 :target-entity-name entity
                 :is-system-seeded   true))
        (or event-rules [])))

;; ── Validación de scope: is_sequence_scope → entityRef debe ser provider ─
;; Llamado DESPUÉS de que el registry completo está construido.
;; Para cada atributo con is_sequence_scope o is_sequence_scope_via,
;; verifica que el entityRef apunte a una entidad con is_sequence_scope_provider:true.
;; Lanza :COD_SCOPE_001 si no (fail-fast de bootstrap).
(defn- check-scope-attr!
  "Verifica que un atributo con is_sequence_scope apunte a un entityRef
   que sea is_sequence_scope_provider:true. Lanza COD_SCOPE_001 si no."
  [entity-name attr registry]
  (let [entity-ref (get attr :entityRef)]
    (when (and entity-ref (contains? registry entity-ref))
      (let [provider? (get-in registry [entity-ref :model :is_sequence_scope_provider])]
        (when-not provider?
          (throw (ex-info
                   (str "COD_SCOPE_001: entity '" entity-name
                        "' attr '" (:name attr)
                        "' → entityRef '" entity-ref
                        "' is not is_sequence_scope_provider:true")
                   {:code      :COD_SCOPE_001
                    :entity    entity-name
                    :field     (:name attr)
                    :entityRef entity-ref})))))))

(defn- validate-scope-providers! [registry]
  (doseq [[entity-name {:keys [model]}] registry
          attr (get model :attributes [])
          :when (or (:is_sequence_scope attr) (:is_sequence_scope_via attr))]
    (check-scope-attr! entity-name attr registry)))


;; ── Bootstrapper principal: construye el registry completo ───────────────
;;
;; Flujo:
;;   1. Escanear models/*.json
;;   2. Parsear JSON → EDN map
;;   3. SHA-256 hash → collision detection
;;   4. Compilar schema Malli
;;   5. Acumular registry + hashes + event-rules-seed
;;   6. Validar scope providers (post-build)
;;
;; Retorna: {:registry {...} :event-rules-seed [...]}
;; El :registry es el mapa completo de todas las entidades compiladas.
;; El :event-rules-seed es la lista de reglas para seed en Datahike.
;;
;; Lanza en cualquier condición de error — el JVM no debe arrancar con un
;; registry incompleto o inconsistente.
(defn build-registry
  "Carga, valida, hashea y compila todos los modelos JSON del directorio dado.
   Llamado UNA SOLA VEZ desde ig/init-key :codice/registry en el arranque.
   Retorna {:registry {...} :event-rules-seed [...]}."
  [models-dir]
  (let [files (list-model-files models-dir)]

    (when (empty? files)
      (throw (ex-info (str "Códice: no JSON model files found in: " models-dir)
                      {:code :COD_002 :entity "NONE"})))

    (log/info "Códice: scanning" (count files) "JSON models from" models-dir)

    (let [{:keys [registry hashes event-rules-seed]}
          (reduce
            (fn [{:keys [registry hashes event-rules-seed]} file]
              (let [model      (load-model-file file)
                    entity     (name (:entity model))   ;; keyword → string
                    hash       (schema-fingerprint model)
                    raw-rules  (extract-event-rules model)]

                ;; Guard: entity duplicada (COD_002)
                (when (contains? registry entity)
                  (throw (ex-info (str "COD_002: Duplicate entity in Códice: '" entity "'")
                                  {:code :COD_002 :entity entity})))

                ;; Guard: hash collision entre distintas entidades (COD_003)
                (when-let [existing (get hashes hash)]
                  (throw (ex-info (str "COD_003: SHA-256 collision: '"
                                       entity "' y '" existing
                                       "' tienen el mismo fingerprint")
                                  {:code      :COD_003
                                   :entity-a  entity
                                   :entity-b  existing
                                   :hash      hash})))

                {:registry         (assoc registry entity
                                           {:model  model
                                            :schema (compile-schema model)
                                            :hash   hash
                                            :engine (keyword (get model :engine "oltp"))})
                 :hashes           (assoc hashes hash entity)
                 :event-rules-seed (into event-rules-seed raw-rules)}))

            {:registry {} :hashes {} :event-rules-seed []}
            files)]

      ;; Post-build: validar scope providers
      (validate-scope-providers! registry)

      (log/info "Códice: registry built —"
                (count registry) "entities,"
                (count event-rules-seed) "event rules")
      {:registry         registry
       :event-rules-seed event-rules-seed})))

;; ── Seed de event_routing_rule en Datahike ───────────────────────────────
;; D7 FIX: usa tenant-guard/transact-with-tenant! en lugar de d/transact directo.
;; El seed es operación del SISTEMA (Tenant-0) — usa SYSTEM_TENANT_ID.
;; Pool Model PROHIBE d/transact directo — toda escritura pasa por tenant-guard.
;;
;; Si el tenant-guard o el conn no están disponibles (entorno local/test),
;; el seed se omite con una advertencia — no es fatal fuera de producción.
(defn seed-event-routing-rules!
  "Seed UPSERT ACID de las reglas EDA extraídas de los modelos JSON hacia Datahike.
   Usa tenant-guard/transact-with-tenant! con SYSTEM_TENANT_ID (D7 — Pool Model).
   Idempotente: uses :rule/rule-code con unique:identity → nunca crea duplicados.

   El tenant-guard es cualquier objeto que implemente el protocolo ITenantGuard
   (definido en metri.infrastructure.tenant-guard). La llamada es dinámica para
   permitir la compilación sin que ese namespace esté cargado en tests."
  [conn tenant-guard event-rules-seed]
  (when (and conn tenant-guard (seq event-rules-seed))
    (try
      (let [tx-data (mapv (fn [rule]
                            {:rule/rule-code          (str (:rule-code rule))
                             :rule/target-entity-name (str (:target-entity-name rule))
                             :rule/event-trigger-type (keyword (get rule :event-trigger-type "CREATED"))
                             :rule/filter-conditions  (pr-str (get rule :filter-conditions {}))
                             :rule/detail-type-output (str (get rule :detail-type-output ""))
                             :rule/is-system-seeded   true})
                          event-rules-seed)
            ;; Invocación dinámica — permite compilar sin que tenant-guard esté en classpath
            transact-fn (requiring-resolve 'metri.infrastructure.tenant-guard/transact-with-tenant!)
            system-tid  (requiring-resolve 'metri.infrastructure.tenant-guard/SYSTEM_TENANT_ID)]
        (transact-fn tenant-guard conn @system-tid tx-data)
        (log/info "Códice: seeded" (count event-rules-seed) "event routing rules"))
      (catch Exception e
        (throw (ex-info "COD_SEED_001: Failed to seed event_routing_rule in Datahike"
                        {:code  :COD_SEED_001
                         :cause (ex-message e)}))))))

;; ── ig/init-key :codice/registry — SRP: solo scan + compile + api/init! ──
;;
;; Responsabilidad ÚNICA: construir el registry en memoria.
;; NO hace I/O a Datahike — eso es responsabilidad de :codice/event-seeder.
;;
;; Retorna mapa con:
;;   :entity-count     — número de entidades compiladas (para telemetría)
;;   :event-rules-seed — reglas EDA extraídas del modelo (para event-seeder)
;;
;; D7: tenant-guard y datahike NO son parámetros de esta key — viven en event-seeder.
;; Función pública extraída para testabilidad directa (sin ig/init en tests).
;; El multimethod delega a ella — misma lógica, accesible por tests unitarios.
(defn ig-init-key-codice-registry
  "Construye el registry en memoria y retorna {:entity-count N :event-rules-seed [...]}.
   Usada directamente por REG-09/REG-10 para testear sin depender de Integrant."
  [{:keys [models-dir]}]
  (let [dir (or models-dir "resources/models")
        {:keys [registry event-rules-seed]} (build-registry dir)]
    (api/init! registry)
    (log/info "Códice: registry inicializado con" (count registry) "entidades")
    {:entity-count     (count registry)
     :event-rules-seed event-rules-seed
     :discovery        api/discovery-handler
     :explore          api/explore-handler}))

(defmethod ig/init-key :codice/registry [_ opts]
  (ig-init-key-codice-registry opts))

(defmethod ig/halt-key! :codice/registry [_ _]
  (api/init! {})  ;; limpia el registry atom al apagar el sistema
  (log/info "Códice: registry cerrado"))

;; ── ig/init-key :codice/event-seeder — SRP: solo seed EDA en Datahike ────
;;
;; Responsabilidad ÚNICA: escribir las reglas EDA a Datahike.
;; Depende de :codice/registry (recibe :event-rules-seed via ig/ref).
;; Idempotente: unique:identity en :rule/rule-code → UPSERT seguro.
;;
;; En entornos sin Datahike (dev local), recibe {} y retorna {:seeded-count 0}.
;; En producción, recibe tenant-guard y datahike reales.
;;
;; Si falla: el sistema NO arranca (fail-fast de bootstrap).
;; El registry en memoria sigue operativo — el error es aislado al I/O.
(defmethod ig/init-key :codice/event-seeder
  [_ {:keys [codice-state tenant-guard datahike]}]
  (let [event-rules-seed (get codice-state :event-rules-seed [])]
    (if (and (seq event-rules-seed) tenant-guard datahike)
      (do
        (seed-event-routing-rules! (:conn datahike) tenant-guard event-rules-seed)
        (log/info "Códice: event-seeder completado —"
                  (count event-rules-seed) "reglas EDA en Datahike")
        {:seeded-count (count event-rules-seed)})
      (do
        (when (seq event-rules-seed)
          (log/info "Códice: event-seeder saltado — sin tenant-guard o datahike (entorno dev)"))
        {:seeded-count 0}))))

(defmethod ig/halt-key! :codice/event-seeder [_ _]
  (log/info "Códice: event-seeder cerrado"))
