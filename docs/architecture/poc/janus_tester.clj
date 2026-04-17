(ns janus-tester
  (:require [malli.core :as m]
            [malli.error :as me]
            [malli.transform :as mt]
            [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]))

;; --- 1. CARGA DEL MANIFIESTO (v3.7) ---
(def contract (edn/read-string (slurp "../janus-aegis-contract-v3.edn")))
(def registry (:metri.ast/registry contract))

;; --- 2. CONFIGURACIÓN DEL MOTOR DE COERCIÓN ---
;; El 'coercer' permite que datos de gRPC (strings) se conviertan en Keywords/Fechas automáticamente.
(def query-request-schema (m/schema [:ref :metri.spec/query-request] {:registry registry}))
(def ast-coercer (m/coercer query-request-schema mt/json-transformer))

;; --- 3. GUARDRAILS MÍNIMOS DSL (Janus/Aegis Runtime Validators) ---

(def dsl-max-formula-length 512)

(def canonical-dsl-error-codes
  #{:DSL_SYNTAX_ERROR
    :TYPE_MISMATCH
    :DSL_SECURITY_VIOLATION
    :DSL_DEPTH_LIMIT
    :OUTPUT_CAST_VIOLATION})

(defn- mk-error [code message]
  {:ok? false
   :error-code code
   :message message})

(defn- mk-ok []
  {:ok? true})

(defn- deep-contains?
  "Busca un string en cualquier valor anidado de una estructura."
  [x needle]
  (cond
    (string? x) (str/includes? (str/lower-case x) needle)
    (map? x) (boolean (some #(deep-contains? % needle) (concat (keys x) (vals x))))
    (sequential? x) (boolean (some #(deep-contains? % needle) x))
    :else false))

(defn validate-dsl-security
  "Escudo mínimo anti inyección para payloads con fórmulas."
  [payload]
  (let [forbidden ["; drop table" "--" "/*" "*/" " xp_" " union select "]]
    (if (some #(deep-contains? payload %) forbidden)
      (mk-error :DSL_SECURITY_VIOLATION "Se detectó patrón potencial de inyección en payload DSL.")
      (mk-ok))))

(defn validate-dsl-depth
  "Anti-billion-laughs simple: limita el largo de fórmulas en `:measures`."
  [payload]
  (let [formulas (for [[_qk {:keys [ast]}] (:queries payload)
                       {:keys [formula]} (:measures ast)]
                   formula)
        oversized (first (filter #(> (count %) dsl-max-formula-length) formulas))]
    (if oversized
      (mk-error :DSL_DEPTH_LIMIT (str "Fórmula excede límite de " dsl-max-formula-length " caracteres."))
      (mk-ok))))

(defn validate-output-cast
  "Valida restricciones mínimas del output cast contra el AST."
  [ast meta]
  (let [viz (:viz meta)
        by-dim? (seq (:dimensions ast))
        has-time? (or (some :dimension/interval (:dimensions ast))
                      (:time-frame ast))
        kpi-shape? (and (<= (count (:metrics ast [])) 1)
                        (empty? (:dimensions ast)))
        cast (keyword (or (:output-cast meta) viz ""))]
    (case cast
      :kpi (if kpi-shape?
             (mk-ok)
             (mk-error :OUTPUT_CAST_VIOLATION "KPI exige una salida agregada sin dimensiones y con una métrica principal."))
      :pie (if by-dim?
             (mk-ok)
             (mk-error :OUTPUT_CAST_VIOLATION "PIE/BUBBLE requieren al menos una dimensión (BY)."))
      :bubble (if by-dim?
                (mk-ok)
                (mk-error :OUTPUT_CAST_VIOLATION "PIE/BUBBLE requieren al menos una dimensión (BY)."))
      :timeseries (if has-time?
                    (mk-ok)
                    (mk-error :OUTPUT_CAST_VIOLATION "TIMESERIES requiere dimensión temporal o time-frame explícito."))
      :table (mk-ok)
      :csv_export (mk-ok)
      :unspecified (mk-ok)
      nil (mk-ok)
      (mk-error :DSL_SYNTAX_ERROR (str "Output cast no soportado: " cast)))))

(defn validate-ast-type-guards
  "Guardia semántica mínima: evita filtros booleanos sobre atributos que parezcan numéricos de costo."
  [ast]
  (let [filters (:filters ast)
        bad? (some (fn [node]
                     (and (sequential? node)
                          (= := (first node))
                          (re-find #"cost|amount|price|duration" (name (second node)))
                          (boolean? (nth node 2 nil))))
                   filters)]
    (if bad?
      (mk-error :TYPE_MISMATCH "TypeMismatch: atributo numérico comparado contra boolean.")
      (mk-ok))))

(defn validate-request-guards
  "Ejecuta validadores runtime mínimos de Janus/Aegis."
  [payload]
  (let [per-query-results
        (for [[_qk {:keys [ast meta]}] (:queries payload)]
          (let [type-check (validate-ast-type-guards ast)]
            (if (:ok? type-check)
              (validate-output-cast ast meta)
              type-check)))
        guards [(validate-dsl-security payload)
                (validate-dsl-depth payload)
                (first per-query-results)]
        failed (first (remove :ok? guards))]
    (or failed (mk-ok))))

;; --- 4. DATOS "SUCIOS" (Simulando Entrada gRPC/JSON) ---
;; Noten que :entity es un string y :aggregation es un string, no keywords.
(def dirty-payload
  {:tenant-id "tenant_matrix_01"
   :context {:common-entity "asset"
             :common-filters [["and" ["=" "status" "ACTIVE"]]]}
   :queries
   {:kpi_costos
    {:ast {:entity "work_order"
           :search "reparacion motor"
           :metrics [{:metric/attribute "total_cost"
                      :metric/aggregation "sum"
                      :metric/filter [":=" "priority" "HIGH"]}]
           :select ["description" {:completed_by ["name" "email"]}]
           :time-frame {:time/type "last-n-days"
                        :time/n-value 30
                        :time/timezone "America/Bogota"}}
     :meta {:viz "bar" :output-cast "table"}}}})


;; --- 5. EJECUCIÓN Y REPORTE ---
(defn run-poc-optimization []
  (println "\n==============================================")
  (println "METRI ENGINE - JANUS BRAIN OPTIMIZED (v3.7)")
  (println "==============================================\n")
  
  (println "[STEP 1] Recibiendo payload crudo (Simulación gRPC/JSON)...")
  (pprint dirty-payload)
  
  (println "\n[STEP 2] Aplicando Motor de Coerción y Validación...")
  (let [clean-ast (ast-coercer dirty-payload)]
    (cond
      (not (m/validate query-request-schema clean-ast))
      (do
        (println "[FAIL] El contrato v3.7 ha rechazado la petición.")
        (pprint (me/humanize (m/explain query-request-schema clean-ast))))

      (not (:ok? (validate-request-guards clean-ast)))
      (do
        (let [{:keys [error-code message]} (validate-request-guards clean-ast)]
          (println "[FAIL] Guardrails semánticos activados.")
          (println "[ERROR_CODE]" (name error-code))
          (println "[MESSAGE]" message)))

      :else
      (do
        (println "[SUCCESS] Coerción Exitosa. Janus ha generado el AST Limpio:")
        (pprint clean-ast)
        (println "\n[INFO] Nota técnica: Los strings se han convertido en :keywords automáticamente cuando aplica.")
        (println "[INFO] El AST está listo para ser enviado a Aegis Engine.")))))

(defn -main []
  (run-poc-optimization))
