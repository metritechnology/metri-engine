(ns metri.janus.validator
  "Registry y Validador de Malli para los esquemas FASE 10 de Janus.
   Carga dinámicamente los contratos desde resources/schema/janus-ast-ir.edn"
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [malli.core :as m]
            [malli.error :as me]
            [malli.registry :as mr]
            [taoensso.timbre :as log]))

(defonce ast-registry (atom nil))

(defn init-registry! []
  (when-not @ast-registry
    (log/info "[Janus Validator] Hidratando Malli Registry desde resources/schema/janus-ast-ir.edn...")
    (if-let [schema-url (io/resource "schema/janus-ast-ir.edn")]
      (let [schema-edn (edn/read-string (slurp schema-url))
            registry-map (:metri.ast/registry schema-edn)
            cedar-schemas (select-keys schema-edn [:metri.cedar/domain-boundary :metri.cedar/context-invariant])
            ;; Combinamos con el default registry de Malli para tener tipos básicos
            combined-registry (merge (m/default-schemas) registry-map cedar-schemas)]
        (reset! ast-registry (mr/registry combined-registry))
        (log/info "[Janus Validator] Registry hidratado exitosamente con" (count registry-map) "esquemas."))
      (throw (ex-info "Falta el contrato janus-ast-ir.edn en resources/schema/" {})))))

(defn validate!
  "Valida un payload contra un schema-key del Registry.
   Lanza ExceptionInfo (FASE 10) si la validación falla."
  [schema-key payload]
  (when-not @ast-registry
    (init-registry!))
  (when-not (m/validate schema-key payload {:registry @ast-registry})
    (let [explain (m/explain schema-key payload {:registry @ast-registry})
          humanized (me/humanize explain)
          err-str (pr-str humanized)]
      (log/error "[Janus Validator] Defensa Inquebrantable bloqueó una mutación/estructura inválida"
                 {:schema schema-key
                  :payload payload
                  :errors humanized})
      (throw (ex-info "Estructura inválida interceptada (Defensa Inquebrantable)"
                      {:code       :JANUS_VAL_001
                       :tenant-id  (or (:tenant-id payload) "unknown-tenant")
                       :schema_key schema-key
                       :message    (str "Contract Schema Validation Failed: " err-str)
                       :errors     humanized})))))
