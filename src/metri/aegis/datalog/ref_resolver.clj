(ns metri.aegis.datalog.ref-resolver
  "Ref-Filter Resolver: resuelve nodos :ref-filter del AST IR cuando los campos
   'reference' son :db.type/string en Datahike (ULIDs almacenados como strings).

   Problema:
     El compilador where.clj genera traversal de grafo para :ref-filter:
       [?e :asset/location_id ?ref_1]
       [?ref_1 :location/type \"BUILDING\"]
     Esto solo funciona con :db.type/ref. Con :db.type/string, ?ref_1 es un
     string ULID y Datahike no puede tratarlo como entidad con atributos.

   Solución (sub-query en 2 pasos):
     1. Para cada nodo :ref-filter, ejecutar un sub-query Datahike sobre la
        entidad referenciada (p.ej. location) con el inner-node como WHERE.
        Obtener los ULIDs coincidentes.
     2. Reemplazar el nodo :ref-filter por [:in :entity/ref-field [ulid1 ulid2 ...]]
        o [:= :entity/ref-field ulid] si es un solo resultado.
     3. Si no hay matches → [:in :entity/ref-field []] → 0 resultados garantizado.

   Invariante:
     - Funciona para anidamiento arbitrario (recursión en inner-node).
     - Compatible con :and / :or / :not — los nodos de conjunción se preservan.
     - Sin I/O fuera de Datahike. Sin estado global.
     - Si Datahike lanza → log warn + retorna [] (fail-safe, no rompe la query).

   Ejemplo de transformación:
     AST IR ANTES:
       [:ref-filter :asset/location_id [:= :location/type \"BUILDING\"]]

     Sub-query ejecutado en Datahike sobre 'location':
       {:find  [(pull ?e [:entity/ulid])]
        :where [[?e :entity/type :location]
                [?e :location/type \"BUILDING\"]]}
       → #{[{:entity/ulid \"01KQW...\"}] [{:entity/ulid \"01KQX...\"}]}

     AST IR DESPUÉS (2 matches):
       [:in :asset/location_id [\"01KQW...\" \"01KQX...\"]]"
  (:require [datahike.api    :as d]
            [taoensso.timbre :as log]
            [metri.aegis.datalog.where :as w]))

;; ─────────────────────────────────────────────────────────────────────────────
;; Helpers
;; ─────────────────────────────────────────────────────────────────────────────

(defn- ref-entity-from-inner
  "Extrae el nombre de la entidad referenciada desde el inner-node del ref-filter.
   inner-node = [:= :location/type \"BUILDING\"] → namespace del 2do arg = \"location\"
   inner-node = [:and [:= :location/type \"X\"] ...] → namespace del 1er hijo del 2do arg"
  [inner-node]
  (let [[_op field & _] inner-node]
    (when (keyword? field)
      (namespace field))))

(defn- inner-node->where-clauses
  "Compila el inner-node del ref-filter como cláusulas Datalog sobre ?e.
   Reutiliza where.clj con un contador fresco."
  [inner-node]
  (let [counter (atom 0)]
    (w/where-node->parts inner-node counter)))

(defn- run-ref-subquery
  "Ejecuta un sub-query Datahike para la entidad ref-entity con inner-node como WHERE.
   Retorna un vector de ULIDs (strings) que coinciden.
   Si error → retorna [] con log de warning (fail-safe)."
  [db ref-entity inner-node]
  (try
    (let [{:keys [clauses in-sym->val]} (inner-node->where-clauses inner-node)
          type-clause ['?e :entity/type (keyword ref-entity)]
          all-clauses (into [type-clause] clauses)
          ;; Misma forma de find que compile-oltp-query: (list 'pull '?e [...])
          ;; Datahike retorna: #{[{:entity/ulid "01KQW..."}] ...}
          pull-spec   [:entity/ulid]
          find-spec   [(list 'pull '?e pull-spec)]
          query (cond-> {:find  find-spec
                         :where all-clauses}
                  (seq in-sym->val)
                  (assoc :in (into ['$] (keys in-sym->val))))
          args  (when (seq in-sym->val) (vec (vals in-sym->val)))
          raw   (if (seq args)
                  (apply d/q query db args)
                  (d/q query db))
          ;; raw = #{[{:entity/ulid "01KQW..."}] ...}  — set de tuplas [pull-map]
          ulids (into [] (keep (fn [tuple]
                                 (when (sequential? tuple)
                                   (:entity/ulid (first tuple))))
                               raw))]
      (log/debug "[RefResolver] Sub-query entity=" ref-entity
                 "| clauses=" (pr-str all-clauses)
                 "| matches=" (count ulids))
      ulids)
    (catch Exception e
      (log/warn "[RefResolver] Sub-query failed for" ref-entity ":" (ex-message e))
      [])))

;; ─────────────────────────────────────────────────────────────────────────────
;; Transformador recursivo del AST IR
;; ─────────────────────────────────────────────────────────────────────────────

(declare resolve-node)

(defn- resolve-node
  "Recorre recursivamente el nodo AST IR resolviendo todos los :ref-filter.
   Nodos :and / :or / :not → se recorre cada hijo.
   Nodo :ref-filter → sub-query + reemplazo por [:in ref-field [ulid...]] o [:= ...].
   Nodos hoja (=, not=, >, etc.) → se devuelven intactos."
  [node db]
  (when (nil? node) (throw (ex-info "Null AST node in ref-resolver" {:code :AEG_COMPILE_001})))
  (let [[op & args] node]
    (case op
      ;; ── Conjunciones — resolver recursivamente cada hijo ──────────────────
      :and (into [:and] (map #(resolve-node % db) args))
      :or  (into [:or]  (map #(resolve-node % db) args))
      :not (into [:not] (map #(resolve-node % db) args))

      ;; ── ref-filter — el caso clave ────────────────────────────────────────
      ;; args = [ref-field inner-node]
      ;; ref-field  = :asset/location_id  (keyword namespaced)
      ;; inner-node = [:= :location/type "BUILDING"]  (puede ser nested)
      :ref-filter
      (let [[ref-field inner-node] args
            ;; Resolver recursivamente el inner-node primero (soporte a anidamiento)
            resolved-inner (resolve-node inner-node db)
            ;; Extraer entidad ref del inner-node (namespace del 2do argumento)
            ref-entity     (ref-entity-from-inner resolved-inner)
            ;; Sub-query para obtener los ULIDs que hacen match
            ulids (if ref-entity
                    (run-ref-subquery db ref-entity resolved-inner)
                    [])]
        (cond
          ;; Ningún match → IN con lista vacía → garantiza 0 resultados sin error
          (empty? ulids)
          (do
            (log/debug "[RefResolver] ref-filter resolved to empty set for" ref-field)
            [:in ref-field []])

          ;; 1 match → EQ directo (más eficiente en Datalog)
          (= 1 (count ulids))
          [:= ref-field (first ulids)]

          ;; N matches → IN con todos los ULIDs
          :else
          [:in ref-field (vec ulids)]))

      ;; ── Nodos hoja — devolver intacto ─────────────────────────────────────
      node)))

;; ─────────────────────────────────────────────────────────────────────────────
;; API pública
;; ─────────────────────────────────────────────────────────────────────────────

(defn contains-ref-filter?
  "Retorna true si el árbol AST IR contiene algún nodo :ref-filter.
   Solo desciende en elementos que son vectores (nodos AST).
   Los valores escalares (keywords, strings, numbers) se ignoran."
  [node]
  (and (vector? node)
       (let [[op & args] node]
         (or (= :ref-filter op)
             ;; Solo recursión en children que sean vectores (nodos AST),
             ;; no en keywords/strings que son valores de nodos hoja.
             (some contains-ref-filter? (filter vector? args))))))

(defn resolve-ref-filters
  "Punto de entrada público.
   Recorre el árbol :where del AST IR y reemplaza cada :ref-filter por un
   :in / := con los ULIDs resueltos vía sub-query Datahike.

   Si el árbol no contiene :ref-filter → retorna el nodo intacto (noop).
   Si db es nil                        → retorna el nodo intacto (fail-safe).

   Argumentos:
     where-node — nodo raíz del árbol :where (puede ser nil para query sin filtros)
     db         — snapshot de Datahike (@conn)

   Retorna:
     El mismo árbol con los :ref-filter reemplazados."
  [where-node db]
  (if (or (nil? where-node)
          (nil? db)
          (not (contains-ref-filter? where-node)))
    where-node  ; noop — no hay ref-filters o no hay db
    (do
      (log/info "[RefResolver] Resolviendo ref-filters en AST IR...")
      (resolve-node where-node db))))
