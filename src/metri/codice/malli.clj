(ns metri.codice.malli
  "FASE 02 — MÓDULO II: Compilador Malli.
   Transforma un modelo JSON del Códice en un schema Malli validable.
   Función pura — cero I/O, cero Datahike, cero side-effects.
   Llamado por registry/build-registry durante el arranque del Bootstrapper.")

;; ── Mapping de tipos JSON → predicados Malli ──────────────────────────────
;; Referencia: 02_FASE_MOTOR_SCHEMA_DRIVEN_CORE.md — MÓDULO II
(def ^:private type->malli
  {"string"    :string
   "uuid"      :uuid
   "reference" :string    ;; FK shape (ULID string) — validación de existencia = OLTPChannel
   "int"       :int
   "integer"   :int
   "long"      pos-int?
   "float"     number?
   "double"    number?
   "decimal"   decimal?
   "epoch"     pos-int?   ;; epoch millis
   "instant"   inst?
   "boolean"   :boolean
   "json"      :map       ;; opaque blob
   "map"       :map
   "enum"      nil        ;; → manejo especial con options
   "array"     nil})      ;; → manejo especial con cardinality:many

;; ── Compilar un atributo individual → entrada Malli ───────────────────────
;;
;; Reglas:
;;   type:enum  + options:[A,B]            → [:enum ["A" "B"]]
;;   cardinality:many + type:reference     → [:vector :uuid]
;;   cardinality:many + tipo primitivo     → [:vector T]
;;   required:true                         → [key T]
;;   required:false (o ausente)            → [key {:optional true} T]
(defn- compile-attr
  [{:keys [name type required options cardinality]}]
  (let [many?      (= cardinality "many")
        base-type  (cond
                     ;; enum con options
                     (= type "enum")
                     (into [:enum] options)

                     ;; array de referencias (entityRef + cardinality:many)
                     (and many? (= type "reference"))
                     [:vector :string]

                     ;; array de tipos primitivos
                     (and many? (contains? type->malli type))
                     [:vector (get type->malli type :any)]

                     ;; array explícito en type (ej. role.json grants)
                     (= type "array")
                     [:vector :any]

                     ;; tipo simple
                     :else
                     (get type->malli type :any))
        kw         (keyword name)]
    (if required
      [kw base-type]
      [kw {:optional true} base-type])))

;; ── build-malli-schema — punto de entrada público ─────────────────────────
;;
;; Entrada:  model EDN map (parseado de JSON con keyword keys)
;;           {:entity "asset" :attributes [{:name "id" :type "uuid" ...} ...]}
;; Salida:   vector Malli [:map attr1 attr2 ...]
;;           Listo para (m/schema ...) o (m/validate ... payload)
;;
;; Nota: NO lanza — si un type es desconocido usa :any como fallback seguro.
(defn build-malli-schema
  "Compila el modelo JSON en un schema Malli [:map ...] validable.
   Función pura — sin I/O. Llamada por registry/build-registry en bootstrap."
  [{:keys [attributes]}]
  (into [:map] (mapv compile-attr attributes)))
