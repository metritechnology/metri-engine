(ns metri.codice.registry-test
  "Matriz TDD — REG-01..10 (FASE 02 MÓDULO I)
   Tests del Bootstrapper: carga, hash, compilación, seed de event_rules y SRP."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [malli.core      :as m]
            [metri.codice.registry :as registry]))

(def ^:private fixtures-dir
  "test/metri/codice/fixtures")

(defn- fixtures-path [& parts]
  (apply str fixtures-dir "/" parts))

;; ─── REG-01: carga del directorio de fixtures válidos ────────────────────

(deftest reg-01-loads-valid-models
  "REG-01 — build-registry carga modelos JSON válidos"
  (let [tmp-dir (doto (io/file (System/getProperty "java.io.tmpdir") "codice-test-valid")
                  (.mkdirs))]
    (try
      ;; Solo copiar archivos válidos — no los duplicates
      (io/copy (io/file (fixtures-path "valid_model.json"))
               (io/file tmp-dir "asset.json"))
      (io/copy (io/file (fixtures-path "scope_provider.json"))
               (io/file tmp-dir "location.json"))
      (let [result (registry/build-registry (.getAbsolutePath tmp-dir))]
        (is (map? (:registry result))            "registry es un mapa")
        (is (= 2 (count (:registry result)))     "2 entidades cargadas")
        (is (vector? (:event-rules-seed result)) "event-rules-seed es vector"))
      (finally
        (doseq [f (.listFiles tmp-dir)] (.delete f))
        (.delete tmp-dir)))))

;; ─── REG-02: falla con JSON malformado ───────────────────────────────────

(deftest reg-02-fails-on-invalid-json
  "REG-02 — build-registry lanza al encontrar JSON malformado"
  (let [tmp-dir (doto (io/file (System/getProperty "java.io.tmpdir") "codice-test-invalid")
                  (.mkdirs))]
    (try
      ;; Copiar solo el archivo malformado al directorio temporal
      (io/copy (io/file (fixtures-path "invalid_model.json"))
               (io/file tmp-dir "invalid_model.json"))
      (is (thrown? Exception (registry/build-registry (.getAbsolutePath tmp-dir)))
          "Debe lanzar excepción con JSON malformado")
      (finally
        ;; Limpiar el directorio temporal
        (doseq [f (.listFiles tmp-dir)] (.delete f))
        (.delete tmp-dir)))))

;; ─── REG-03: falla con entidad duplicada → COD_002 ───────────────────────

(deftest reg-03-fails-on-duplicate-entity
  "REG-03 — build-registry lanza :COD_002 cuando dos archivos declaran la misma entity"
  (let [tmp-dir (doto (io/file (System/getProperty "java.io.tmpdir") "codice-test-dup")
                  (.mkdirs))]
    (try
      (io/copy (io/file (fixtures-path "duplicate_entity_a.json"))
               (io/file tmp-dir "a.json"))
      (io/copy (io/file (fixtures-path "duplicate_entity_b.json"))
               (io/file tmp-dir "b.json"))
      (let [ex (try (registry/build-registry (.getAbsolutePath tmp-dir))
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
        (is (some? ex) "Debe lanzar ExceptionInfo")
        (is (= :COD_002 (:code (ex-data ex))) "Código debe ser :COD_002"))
      (finally
        (doseq [f (.listFiles tmp-dir)] (.delete f))
        (.delete tmp-dir)))))

;; ─── REG-04: compila schema Malli por entidad ────────────────────────────

(deftest reg-04-compiles-malli-schema
  "REG-04 — registry entry tiene :schema compilado como Malli schema"
  (let [tmp-dir (doto (io/file (System/getProperty "java.io.tmpdir") "codice-test-schema")
                  (.mkdirs))]
    (try
      (io/copy (io/file (fixtures-path "valid_model.json"))
               (io/file tmp-dir "asset.json"))
      (let [{:keys [registry]} (registry/build-registry (.getAbsolutePath tmp-dir))
            entry (get registry "asset")]
        (is (some? entry)           "entidad asset debe existir en registry")
        (is (some? (:schema entry)) "entry debe tener :schema")
        (is (m/schema? (:schema entry)) "schema debe ser un Malli schema"))
      (finally
        (doseq [f (.listFiles tmp-dir)] (.delete f))
        (.delete tmp-dir)))))

;; ─── REG-05: engine oltp → keyword :oltp ─────────────────────────────────

(deftest reg-05-oltp-engine-keyword
  "REG-05 — modelo con \"engine\":\"oltp\" → :oltp keyword en registry"
  (let [tmp-dir (doto (io/file (System/getProperty "java.io.tmpdir") "codice-test-oltp")
                  (.mkdirs))]
    (try
      (io/copy (io/file (fixtures-path "valid_model.json"))
               (io/file tmp-dir "asset.json"))
      (let [{:keys [registry]} (registry/build-registry (.getAbsolutePath tmp-dir))]
        (is (= :oltp (get-in registry ["asset" :engine]))
            "engine debe ser :oltp keyword"))
      (finally
        (doseq [f (.listFiles tmp-dir)] (.delete f))
        (.delete tmp-dir)))))

;; ─── REG-06: fingerprint SHA-256 determinístico ──────────────────────────

(deftest reg-06-idempotent-hash
  "REG-06 — build-registry produce el mismo hash para el mismo modelo"
  (let [tmp-dir (doto (io/file (System/getProperty "java.io.tmpdir") "codice-test-hash")
                  (.mkdirs))]
    (try
      (io/copy (io/file (fixtures-path "valid_model.json"))
               (io/file tmp-dir "asset.json"))
      (let [{:keys [registry] :as r1} (registry/build-registry (.getAbsolutePath tmp-dir))
            {:keys [registry] :as r2} (registry/build-registry (.getAbsolutePath tmp-dir))]
        (is (= (get-in (:registry r1) ["asset" :hash])
               (get-in (:registry r2) ["asset" :hash]))
            "Hash debe ser idéntico en dos ejecuciones consecutivas"))
      (finally
        (doseq [f (.listFiles tmp-dir)] (.delete f))
        (.delete tmp-dir)))))

;; ─── REG-07: hash es string SHA-256 de 64 chars ──────────────────────────

(deftest reg-07-hash-is-sha256
  "REG-07 — el hash en registry es string hexadecimal de 64 caracteres"
  (let [tmp-dir (doto (io/file (System/getProperty "java.io.tmpdir") "codice-test-sha")
                  (.mkdirs))]
    (try
      (io/copy (io/file (fixtures-path "valid_model.json"))
               (io/file tmp-dir "asset.json"))
      (let [{:keys [registry]} (registry/build-registry (.getAbsolutePath tmp-dir))
            hash (get-in registry ["asset" :hash])]
        (is (string? hash)         "hash debe ser string")
        (is (= 64 (count hash))    "hash SHA-256 tiene 64 caracteres hex")
        (is (re-matches #"[0-9a-f]+" hash) "solo caracteres hexadecimales"))
      (finally
        (doseq [f (.listFiles tmp-dir)] (.delete f))
        (.delete tmp-dir)))))

;; ─── REG-08: model y engine accesibles desde el registry ─────────────────

(deftest reg-08-model-and-engine-accessible
  "REG-08 — registry entry tiene :model completo con :attributes"
  (let [tmp-dir (doto (io/file (System/getProperty "java.io.tmpdir") "codice-test-model")
                  (.mkdirs))]
    (try
      (io/copy (io/file (fixtures-path "valid_model.json"))
               (io/file tmp-dir "asset.json"))
      (let [{:keys [registry]} (registry/build-registry (.getAbsolutePath tmp-dir))
            entry (get registry "asset")]
        (is (map?    (:model entry))              "entry.model es mapa")
        (is (vector? (:attributes (:model entry))) "model.attributes es vector")
        (is (keyword? (:engine entry))             "engine es keyword"))
      (finally
        (doseq [f (.listFiles tmp-dir)] (.delete f))
        (.delete tmp-dir)))))

;; ─── REG-09: ig/init-key retorna mapa con :entity-count y :event-rules-seed ─
;; Verifica el contrato de la key Integrant tras la separación SRP (Hallazgo #3).

(deftest reg-09-ig-init-key-returns-state-map
  "REG-09 — ig-init-key-codice-registry retorna {:entity-count N :event-rules-seed [...]}
   No retorna :codice/ready (eso era el contrato del stub eliminado).
   El mapa es el estado que :codice/event-seeder consume via ig/ref."
  (let [tmp-dir (doto (io/file (System/getProperty "java.io.tmpdir") "codice-test-init-key")
                  (.mkdirs))]
    (try
      (io/copy (io/file (fixtures-path "valid_model.json"))
               (io/file tmp-dir "asset.json"))
      (let [result (registry/ig-init-key-codice-registry {:models-dir (.getAbsolutePath tmp-dir)})]
        (is (map? result)                        "ig/init-key retorna un mapa, no keyword")
        (is (contains? result :entity-count)     ":entity-count presente en el retorno")
        (is (contains? result :event-rules-seed) ":event-rules-seed presente en el retorno")
        (is (integer? (:entity-count result))    ":entity-count es entero")
        (is (vector?  (:event-rules-seed result)) ":event-rules-seed es vector")
        (is (= 1 (:entity-count result))         "1 entidad cargada"))
      (finally
        (doseq [f (.listFiles tmp-dir)] (.delete f))
        (.delete tmp-dir)))))

;; ─── REG-10: ig/init-key no hace I/O a Datahike (SRP) ────────────────────
;; Verifica que la responsabilidad de seed EDA está en :codice/event-seeder, no aquí.

(deftest reg-10-ig-init-key-does-not-seed-datahike
  "REG-10 — ig-init-key-codice-registry NO llama seed-event-routing-rules!
   El seeder es responsabilidad de :codice/event-seeder (SRP).
   Verificación: init-key sin tenant-guard ni datahike no lanza excepción."
  (let [tmp-dir (doto (io/file (System/getProperty "java.io.tmpdir") "codice-test-no-seed")
                  (.mkdirs))]
    (try
      (io/copy (io/file (fixtures-path "valid_model.json"))
               (io/file tmp-dir "asset.json"))
      ;; Si ig/init-key intentara seedear, necesitaría tenant-guard y datahike — lanzaría sin ellos.
      ;; Que no lance prueba que init-key ya no hace I/O a Datahike (SRP cumplido).
      (let [threw? (try
                     (registry/ig-init-key-codice-registry {:models-dir (.getAbsolutePath tmp-dir)})
                     false
                     (catch Exception _ true))]
        (is (false? threw?)
            "ig/init-key no lanza sin tenant-guard/datahike — SRP: no hace seed"))
      (finally
        (doseq [f (.listFiles tmp-dir)] (.delete f))
        (.delete tmp-dir)))))
