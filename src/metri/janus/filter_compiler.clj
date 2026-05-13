(ns metri.janus.filter-compiler
  "Compilador de FilterNode → nodos AST IR.
   SRP: transforma árboles de filtros del cliente en nodos del AST IR inmutable.
   Sin I/O. Sin estado. Sin side-effects.

   Soporta:
     • FilterCriteria leaf  → operadores EQ/NEQ/GT/… /BETWEEN/MATCHES
     • FilterGroup recursivo → AND/OR/NOT
     • Dot-path cross-entity → [:ref-filter :entity/ref-field inner-node]
     • Enum sentinel gate   → rechaza _UNSPECIFIED con :JANUS_400"
  (:require [clojure.string :as str]
            [metri.codice.api :as codice]))

;; ═══════════════════════════════════════════════════════════════════════════
;; SCHEMA HELPERS — introspección del Códice
;; ═══════════════════════════════════════════════════════════════════════════

(defn find-attr
  "Retorna el mapa de atributo del schema para field-name, o nil si no existe."
  [schema field-name]
  (some #(when (= (name (:name %)) field-name) %)
        (get schema :attributes [])))

(defn attr-exists?
  "Verifica que el campo existe en el schema del Códice,
   o que sea una columna de sistema universal."
  [schema field-name]
  (or (#{"id" "created_at" "updated_at"} field-name)
      (some? (find-attr schema field-name))))

(defn- coerce-epoch
  "Coerce un valor epoch a long entero para evitar precision loss cuando
   proto transporta :number-val como double.
   created_at y updated_at se almacenan como bigint en Iceberg/Athena;
   si llegan como 1.7466624E12 (double) el BETWEEN no matchea ningún registro."
  [v]
  (if (number? v) (long v) v))

(def ^:private system-ts-fields #{"created_at" "updated_at"})

;; Mapeo de campos de sistema OLTP (Datahike) que se almacenan bajo
;; el namespace :meta/ — no bajo el namespace de la entidad.
;; :asset/created_at NO existe en Datahike → lanzaría 0 resultados.
;; :meta/created_at SÍ existe → valor epoch-ms generado en ingestión.
(def ^:private oltp-system-field-map
  {"created_at" :meta/created_at
   "updated_at" :meta/updated_at})

;; ═══════════════════════════════════════════════════════════════════════════
;; OPERADORES HOJA — FilterCriteria → nodo AST IR
;; ═══════════════════════════════════════════════════════════════════════════

(defn- build-leaf-node
  "Convierte un FilterCriteria (ya validado) en un nodo AST IR hoja.
   entity    — namespace del keyword resultante (e.g. \"asset\")
   f         — mapa {:field :op-ref :value} post proto->clj"
  [entity f]
  (let [fname   (name (or (:field f) ""))
        ;; OLTP system fields live under :meta/ namespace in Datahike,
        ;; not under the entity namespace. Routing them here prevents
        ;; [:asset/created_at ?v] queries that return 0 results.
        field   (or (oltp-system-field-map fname)
                    (keyword entity fname))
        op-raw  (or (:op-ref f) "EQ")
        op-str  (if (keyword? op-raw) (name op-raw) (str op-raw))
        val-map (:value f {})
        val     (or (:string-val val-map)
                    (:number-val val-map)
                    (:bool-val val-map)
                    (:list-val val-map))
        ;; list_val proto->clj = {:values ["a" "b"]} (StringList) — extraemos el vector
        list-vals (let [lv (:list-val val-map)]
                    (cond
                      (nil? lv)  []
                      (map? lv)  (or (:values lv) [])   ;; StringList
                      (coll? lv) (vec lv)
                      :else      []))
        ;; range_values proto->clj = {:values [{:number-val 50.0} ...]} (FilterValueList)
        range-vals (let [rv (:range-values val-map)]
                     (cond
                       (nil? rv)  []
                       (map? rv)  (mapv #(or (:number-val %) (:string-val %)) (or (:values rv) []))
                       (coll? rv) (vec rv)
                       :else      []))]
    (case op-str
      "EQ"          [:= field val]
      "NEQ"         [:not= field val]
      ;; Para GT/GTE/LT/LTE sobre campos timestamp sistema → coerce a long
      "GT"          (let [v (or (:number-val val-map) (:string-val val-map))]
                     [:> field (if (system-ts-fields (name field)) (coerce-epoch v) v)])
      "GTE"         (let [v (or (:number-val val-map) (:string-val val-map))]
                     [:>= field (if (system-ts-fields (name field)) (coerce-epoch v) v)])
      "LT"          (let [v (or (:number-val val-map) (:string-val val-map))]
                     [:< field (if (system-ts-fields (name field)) (coerce-epoch v) v)])
      "LTE"         (let [v (or (:number-val val-map) (:string-val val-map))]
                     [:<= field (if (system-ts-fields (name field)) (coerce-epoch v) v)])
      "IN"          [:in field list-vals]
      "NOT_IN"      [:not-in field list-vals]
      "LIKE"        [:like field val]
      "CONTAINS"    [:contains field val]
      "IS_NULL"     [:is-null field]
      "IS_NOT_NULL" [:is-not-null field]
      "BETWEEN"     (let [lo (coerce-epoch (first range-vals))
                          hi (coerce-epoch (second range-vals))
                          fname (name field)]
                      ;; Para campos timestamp del sistema, aplica coercion adicional
                      (if (system-ts-fields fname)
                        [:between field [(long lo) (long hi)]]
                        [:between field [lo hi]]))
      "MATCHES"     [:matches field (or (:string-val val-map) "")]
      nil)))

;; ═══════════════════════════════════════════════════════════════════════════
;; DOT-PATH — cross-entity ref-filter con anidamiento arbitrario
;; ═══════════════════════════════════════════════════════════════════════════

(declare build-ref-filter-node)

(defn- build-ref-filter-node
  "Construye [:ref-filter :entity/ref-field inner-node] para un dot-path field.

   Validaciones (en orden):
     1. ref-field debe existir en el schema de la entidad actual
     2. ref-field debe ser type=reference
     3. ref-entity debe existir en el Códice
     4. target-field debe existir en el schema de ref-entity

   Anidamiento:  'location_id.parent_location_id.type'
     → [:ref-filter :asset/location_id
          [:ref-filter :location/parent_location_id
            [:= :location/type \"SITE\"]]]"
  [entity dot-field op-str val-map schema]
  (let [dot-idx    (str/index-of dot-field ".")
        ref-field  (subs dot-field 0 dot-idx)
        rest-field (subs dot-field (inc dot-idx))
        ref-attr   (find-attr schema ref-field)]

    (when (nil? ref-attr)
      (throw (ex-info "Unknown ref attribute in dot-path filter"
                      {:code   :JANUS_400
                       :reason "unknown-ref-attribute"
                       :field  ref-field
                       :entity entity})))
    (when (not= "reference" (name (or (:type ref-attr) "")))
      (throw (ex-info "Dot-path attribute is not a reference type"
                      {:code        :JANUS_400
                       :reason      "not-a-reference"
                       :field       ref-field
                       :actual-type (:type ref-attr)})))

    (let [ref-entity (name (or (:entityRef ref-attr) ""))
          schema-res (codice/entity-model ref-entity {})]

      (when (= :error (first schema-res))
        (throw (ex-info "Referenced entity not found in Códice"
                        {:code   :JANUS_400
                         :reason "unknown-ref-entity"
                         :entity ref-entity})))

      (let [ref-schema (second schema-res)
            ref-kw     (keyword entity ref-field)
            inner-node (if (str/includes? rest-field ".")
                         ;; Más puntos → recursión
                         (build-ref-filter-node ref-entity rest-field op-str val-map ref-schema)
                         ;; Hoja: validar target-field y construir nodo
                         (let [target-attr (find-attr ref-schema rest-field)]
                           (when (nil? target-attr)
                             (throw (ex-info "Unknown attribute in referenced entity"
                                             {:code   :JANUS_400
                                              :reason "unknown-ref-target-attribute"
                                              :field  rest-field
                                              :entity ref-entity})))
                           (build-leaf-node ref-entity {:field  rest-field
                                                        :op-ref op-str
                                                        :value  val-map})))]
        [:ref-filter ref-kw inner-node]))))

;; ═══════════════════════════════════════════════════════════════════════════
;; ÁRBOL FilterNode — compilador recursivo
;; ═══════════════════════════════════════════════════════════════════════════

(defn- check-not-unspecified!
  "Gate §8: lanza :JANUS_400 si el valor de un enum tiene sufijo _UNSPECIFIED."
  [value label extra]
  (when (str/ends-with? (or value "") "_UNSPECIFIED")
    (throw (ex-info (str "Unspecified enum in " label)
                    (merge {:code :JANUS_400 :reason "unspecified-enum"} extra)))))

(defn- compile-criteria
  "Compila un FilterCriteria leaf.
   Valida campo contra el Códice. Soporta dot-path."
  [criteria schema entity]
  (let [field-name (name (or (:field criteria) ""))
        op-raw     (or (:op-ref criteria) "EQ")
        op-str     (if (keyword? op-raw) (name op-raw) (str op-raw))
        val-map    (:value criteria {})]
    (check-not-unspecified! op-str "filter operator" {:field field-name :op op-str})
    (if (str/includes? field-name ".")
      (build-ref-filter-node entity field-name op-str val-map schema)
      (if (attr-exists? schema field-name)
        (build-leaf-node entity criteria)
        (throw (ex-info "Unknown attribute in filter"
                        {:code   :JANUS_400
                         :reason "unknown-attribute"
                         :field  field-name}))))))

(declare compile-node)

(defn- compile-group
  "Compila un FilterGroup recursivo → nodo :and / :or / :not."
  [{:keys [conjunction nodes]} schema entity]
  (check-not-unspecified! conjunction "conjunction" {:conjunction conjunction})
  ;; NOTA: proto->clj convierte enums a keywords (:AND, :OR, :NOT).
  ;; Normalizamos con (name) para soportar tanto keywords como strings.
  (let [conj-str (if (keyword? conjunction) (name conjunction) (str conjunction))
        op        (case conj-str
                    "AND" :and
                    "OR"  :or
                    "NOT" :not
                    :and)
        sub-nodes (keep #(compile-node % schema entity) (or nodes []))]
    (when (seq sub-nodes)
      (if (= op :not)
        [:not (first sub-nodes)]
        (into [op] sub-nodes)))))

(defn- compile-node
  "Dispatcher principal: {:criteria ...} | {:group ...}."
  [node schema entity]
  (cond
    (:criteria node) (compile-criteria (:criteria node) schema entity)
    (:group node)    (compile-group    (:group node)    schema entity)
    :else             nil))

;; ═══════════════════════════════════════════════════════════════════════════
;; API PÚBLICA
;; ═══════════════════════════════════════════════════════════════════════════

(defn compile-filters
  "Compila una seq de FilterNode (proto->clj shape) → vector de nodos AST IR.
   Soporta FilterGroup recursivo (AND/OR/NOT) y FilterCriteria leaf con dot-path.
   Lanza ExceptionInfo en campos desconocidos o enums UNSPECIFIED."
  [filters schema entity]
  (into [] (keep #(compile-node % schema entity) (or filters []))))
