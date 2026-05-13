(ns metri.janus.abac-clauses
  "Construcción de cláusulas ABAC desde el cedar-ctx.
   SRP: transforma {entity + boundaries + user-id + schema} → nodo AST IR ABAC.
   Sin I/O. Sin estado. Sin side-effects.

   Implementa los pasos 4a–4c del compilador Janus:
     4a. Nodo tenant (Zero-Trust cardinal)
     4b. Fronteras geográficas (permitted-locations)
     4c. Predicado de scope (OWN / ASSIGNED / OWN_OR_ASSIGNED / ALL / NONE)
     4f. Campos de propiedad desde el Códice (owner-field / assignee-field)")

;; ═══════════════════════════════════════════════════════════════════════════
;; 4f — Extracción de campos de propiedad
;; ═══════════════════════════════════════════════════════════════════════════

(defn ownership-fields
  "Extrae los atributos de propiedad (owner) y asignación (assignee)
   desde el model schema del Códice.
   Convención: atributo con :is_owner true → owner-field
               atributo con :is_assignee true → assignee-field
   Retorna {:owner-field kw :assignee-field kw} — puede tener nils."
  [entity schema]
  (let [attrs (get schema :attributes [])]
    {:owner-field    (some (fn [a]
                             (when (:is_owner a)
                               (keyword entity (name (:name a)))))
                           attrs)
     :assignee-field (some (fn [a]
                             (when (:is_assignee a)
                               (keyword entity (name (:name a)))))
                           attrs)}))

;; ═══════════════════════════════════════════════════════════════════════════
;; 4a — Nodo tenant
;; ═══════════════════════════════════════════════════════════════════════════

(defn tenant-node
  "4a: Retorna el nodo Zero-Trust cardinal [:= :tenant/id tenant-id].
   SIEMPRE debe ser el primer nodo del :where del AST IR."
  [tenant-id]
  [:= :tenant/id tenant-id])

;; ═══════════════════════════════════════════════════════════════════════════
;; 4b — Fronteras geográficas
;; ═══════════════════════════════════════════════════════════════════════════

(defn- location-boundary-node
  "Construye [:in :entity/location_id [...locs]] para un boundary.
   Retorna nil si el boundary no tiene permitted-locations."
  [boundary entity]
  (when-let [locs (not-empty (:permitted-locations boundary))]
    [:in (keyword entity "location_id") locs]))

;; ═══════════════════════════════════════════════════════════════════════════
;; 4c — Predicado de scope
;; ═══════════════════════════════════════════════════════════════════════════

(defn- scope-predicate-node
  "4c: Traduce scope Cedar → predicado AST IR de propiedad.
   Tabla: docs/architecture/05.02_FASE_JANUS_AST_IR.md §4.2"
  [scope owner-field assignee-field user-id]
  (case scope
    "ALL"             nil
    "OWN"             (when owner-field    [:= owner-field user-id])
    "ASSIGNED"        (when assignee-field [:= assignee-field user-id])
    "OWN_OR_ASSIGNED" (cond
                        (and owner-field assignee-field)
                        [:or [:= owner-field user-id] [:= assignee-field user-id]]
                        owner-field    [:= owner-field user-id]
                        assignee-field [:= assignee-field user-id]
                        :else          nil)
    "NONE"            (throw (ex-info "Scope NONE — no grant for domain"
                                      {:code   :JANUS_400
                                       :reason "scope-none"
                                       :scope  scope}))
    nil))

;; ═══════════════════════════════════════════════════════════════════════════
;; API PÚBLICA
;; ═══════════════════════════════════════════════════════════════════════════

(defn build-abac-node
  "4b+4c: Construye el nodo ABAC combinado desde los boundaries del cedar-ctx.

   Por cada boundary:
     • 4b: [boundary.permitted-locations] → [:in :entity/location_id [...]]
     • 4c: boundary.query-scope          → [:= owner user-id] etc.
     • Si ambos existen → [:and loc-node scope-node]

   Multi-boundary → [:or clause1 clause2 ...]
   Sin restricciones → nil

   Retorna: nodo AST IR | nil"
  [entity boundaries owner-field assignee-field user-id]
  (let [clauses (when (seq boundaries)
                  (keep (fn [boundary]
                          (let [loc-node   (location-boundary-node boundary entity)
                                scope-node (scope-predicate-node
                                             (:query-scope boundary)
                                             owner-field assignee-field user-id)]
                            (cond
                              (and loc-node scope-node) [:and loc-node scope-node]
                              loc-node                  loc-node
                              scope-node                scope-node
                              :else                     nil)))
                        boundaries))]
    (case (count clauses)
      0 nil
      1 (first clauses)
      (into [:or] clauses))))
