;; [PORTED_TO_RUST: src/aegis/label_template.rs]
;; NO MODIFICAR ESTE ARCHIVO.
;; La fuente de verdad para esta lógica ahora reside en Rust.
(ns metri.aegis.label-template
  "Interpolación de label templates Mustache-style para viz line, area, pie.
   SRP: resolución pura de templates — sin I/O, sin estado.

   Sintaxis: {{campo}}  →  valor del campo en cada data-row.
   Ejemplo:  '{{asset_name}} - {{area_value}} KW'
             con row {:asset_name 'Pump A' :area_value 45.2}
             → 'Pump A - 45.2 KW'

   Reglas:
     - Los placeholders son case-sensitive y deben coincidir exactamente con las keys del row.
     - Si el campo no existe en el row, se reemplaza con cadena vacía ''.
     - Si el template está vacío o nil, retorna nil (sin label calculado).
     - Los rows pueden ser mapas con keyword keys o string keys.
     - El valor numérico se formatea sin decimales si es entero, con 2 decimales si es float.")

(def ^:private placeholder-pattern
  "Regex que captura {{campo}} en cualquier posición del template."
  #"\{\{([^}]+)\}\}")

(defn- coerce-key
  "Busca un campo en el row tolerando keyword y string keys, con y sin namespace."
  [row field-str]
  (or (get row (keyword field-str))
      (get row field-str)
      ;; Intentar sin namespace (para keys como :entity/asset_name → buscar :asset_name)
      (some (fn [[k v]]
              (when (= (name k) field-str) v))
            row)))

(defn- format-value
  "Formatea un valor para display en un label:
   - nil       → ''
   - entero    → '42'  (sin .0)
   - decimal   → '45.23' (2 decimales)
   - string    → valor directo"
  [v]
  (cond
    (nil? v)                          ""
    (and (number? v) (= v (Math/floor (double v)))) (str (long v))
    (number? v)                       (format "%.2f" (double v))
    :else                             (str v)))

(defn interpolate
  "Interpola un template string contra un data row.
   Template: '{{asset_name}} - {{area_value}} KW'
   Row:      {:asset_name 'Pump A' :area_value 45.2}
   Retorna:  'Pump A - 45.2 KW'

   Retorna nil si el template es nil o blank.
   Retorna el template sin modificar si no tiene placeholders."
  [template row]
  (when (and (string? template) (seq template))
    (clojure.string/replace
      template
      placeholder-pattern
      (fn [[_ field-str]]
        (format-value (coerce-key row (clojure.string/trim field-str)))))))

(defn interpolate-rows
  "Aplica interpolate a todos los rows de un RowSet.
   Añade la key :_label a cada row con el label resuelto.
   Si el template es nil/blank, los rows no se modifican.

   Parámetros:
     rows     — vector de mapas (los data rows del resultado)
     template — string con placeholders {{campo}}

   Uso típico en el normalizer/translator para PIE/LINE/AREA:
     (label-template/interpolate-rows rows (:label_template chart-decoration))"
  [rows template]
  (if (and (string? template) (seq template))
    (mapv (fn [row]
            (assoc row :_label (interpolate template row)))
          rows)
    rows))

(defn extract-fields
  "Extrae los nombres de campo referenciados en un template.
   Útil para validar que los campos existen en el schema antes de ejecutar.
   Ejemplo: '{{asset_name}} - {{area_value}} KW' → ['asset_name' 'area_value']"
  [template]
  (when (and (string? template) (seq template))
    (mapv second (re-seq placeholder-pattern template))))
