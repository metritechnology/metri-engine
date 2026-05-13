(ns metri.aegis.sql.hierarchy
  "HierarchyContext: parent-field filter + inject-has-children EXISTS correlated subquery.
   SRP: construcción de predicados jerárquicos — sin I/O, sin estado.

   HierarchyContext (§4 contrato):
     :parent-field        → WHERE parent_field = :current-node-id
     :current-node-id     → valor del nodo padre a filtrar
     :inject-has-children → añade EXISTS(SELECT 1 FROM t WHERE parent_field = t.id)"
  (:require [clojure.string :as str]))

(defn build-hierarchy-parts
  "HierarchyContext → {:extra-where honey-expr :extra-select [expr alias]} | nil.

   Retorna nil si hierarchy es nil.
   Retorna mapa vacío {} si hierarchy existe pero sin current-node-id ni inject-has-children.

   NOTA: el caller debe usar alias 't' en la tabla principal cuando :extra-select está presente,
   para que la correlated subquery pueda referenciar 't.id'."
  [hierarchy tbl-str tenant-id]
  (when hierarchy
    (let [pf  (str/replace (or (:parent-field hierarchy) "parent_id") "-" "_")
          cid (:current-node-id hierarchy)
          hc? (:inject-has-children hierarchy false)]
      (cond-> {}
        cid  (assoc :extra-where [:= (keyword pf) cid])
        hc?  (assoc :extra-select
                    ;; EXISTS(SELECT 1 FROM same-table
                    ;;         WHERE tenant_id = ? AND parent_field = t.id)
                    ;; Referencia correlated: t.id → alias 't' del outer query
                    [[:exists {:select [[[:raw "1"]]]
                               :from   [[(keyword tbl-str)]]
                               :where  [:and
                                        [:= :_tenant tenant-id]
                                        [:= (keyword pf) [:raw "t.id"]]]}]
                     :has_children])))))
