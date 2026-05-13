(ns metri.aegis.datalog.hierarchy
  "HierarchyContext: filtro parent-field + inject-has-children via reverse-ref pull.
   SRP: construcción de predicados jerárquicos — sin I/O, sin estado.

   HierarchyContext (§4 contrato):
     :parent-field        → WHERE [?e parent-field current-node-id]
     :current-node-id     → valor del nodo padre a filtrar
     :inject-has-children → pull con reverse-ref _parent-field [:db/id]
                            post-procesado en executor como :has_children boolean

   Ejemplo:
     hierarchy = {:parent-field \"parent_id\"
                  :current-node-id \"abc\"
                  :inject-has-children true}

     → :extra-clauses  [['?e :parent_id \"abc\"]]
       :pull-additions  [{:_parent_id [:db/id]}]
       :has-children?   true
       :rev-key         :_parent_id"
  (:require [clojure.string :as str]))

(defn build-hierarchy-parts
  "HierarchyContext → {:extra-clauses [...] :pull-additions [...] :has-children? bool :rev-key kw}
   Retorna nil si hierarchy es nil.

   pull-additions para el reverse-ref solicita [:db/id :entity/ulid] para que:
     1. inject-has-children pueda evaluar (seq children) correctamente
     2. El executor pueda resolver el ULID de los hijos si se necesita"
  [hierarchy entity-name]
  (when hierarchy
    (let [raw-field (or (:parent-field hierarchy) "parent_id")
          ;; En Datalog/Datahike, los atributos están namespaced por la entidad.
          ;; Si raw-field ya tiene namespace (slash), lo respetamos. Si no, lo prefijamos con entity-name.
          pf        (if (str/includes? raw-field "/")
                      raw-field
                      (str (name entity-name) "/" raw-field))
          pf-kw     (keyword pf)
          ;; El reverse keyword en Datomic/Datahike se forma añadiendo "_" después del slash.
          ;; Ejemplo: :location/parent_location_id -> :location/_parent_location_id
          ns-part   (namespace pf-kw)
          name-part (name pf-kw)
          rev-kw    (keyword ns-part (str "_" name-part))
          cid       (:current-node-id hierarchy)
          hc?       (boolean (:inject-has-children hierarchy))]
      (cond-> {}
        cid (assoc :extra-clauses [['?e pf-kw cid]])
        hc? (assoc :pull-additions   [{rev-kw [:db/id :entity/ulid]}]
                   :has-children?    true
                   :rev-key          rev-kw)))))

(defn inject-has-children
  "Post-procesa rows añadiendo :has_children boolean desde reverse-ref de Datahike.
   rev-key: keyword del reverse-ref (e.g. :_parent_location_id)

   Modo 1 (ref): el pull de Datahike retorna el reverse-ref como vector de entity-maps:
     - Tiene hijos  → [{:db/id 12} {:db/id 34} ...] (seq no-vacío)
     - Sin hijos    → nil

   Elimina el rev-key del row y añade :has_children boolean."
  [rows rev-key]
  (mapv (fn [row]
          (let [children (get row rev-key)
                has-ch   (boolean (seq children))]
            (-> row
                (dissoc rev-key)
                (assoc :has_children has-ch))))
        rows))

(defn inject-has-children-from-rows
  "A1-fix: Has-children en-memoria para campos :db.type/string.

   Cuando parent_location_id es :db.type/string (no :db.type/ref), Datahike
   no genera reverse-ref indexes. Este fn calcula has_children en-memoria:
     1. Extrae todos los valores de parent-field-kw en todos los rows
        (estos son los IDs de los nodos que TIENEN hijos)
     2. Para cada row, :has_children = (contains? parent-ids (:id row))

   parent-field-kw: keyword del campo parent en los rows post-flatten
                    (e.g. :parent_location_id)"
  [rows parent-field-kw]
  (let [;; IDs de entidades que son padres de algún otro row
        parent-ids (into #{} (keep #(get % parent-field-kw) rows))]
    (mapv (fn [row]
            (assoc row :has_children (contains? parent-ids (:id row))))
          rows)))
