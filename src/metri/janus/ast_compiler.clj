;; [PORTED_TO_RUST: src/janus/ast_compiler.rs]
;; NO MODIFICAR ESTE ARCHIVO.
;; La fuente de verdad para esta lógica ahora reside en Rust.
(ns metri.janus.ast-compiler
  "JanusASTCompiler — ensamblador del AST IR.
   SRP: combina los sub-pasos 4a–4f en un AST IR inmutable completo.
   Sin I/O. Sin estado.

   Delega:
     • Filtros de usuario   → metri.janus.filter-compiler/compile-filters
     • Cláusulas ABAC       → metri.janus.abac-clauses/build-abac-node
     • Tenant node          → metri.janus.abac-clauses/tenant-node
     • Campos de propiedad  → metri.janus.abac-clauses/ownership-fields

   Pasos 4a–4f (docs/architecture/05.01-JANUS.md §2):
     4a. Inyectar tenant-node (Zero-Trust cardinal — SIEMPRE primero)
     4b. Inyectar fronteras geográficas (permitted-locations)
     4c. Inyectar predicado de scope (OWN / ASSIGNED / OWN_OR_ASSIGNED)
     4d. Inyectar filtros orgánicos del usuario (validados contra Códice)
     4e. Construir proyección :select
     4f. Resolver campos owner/assignee desde el Códice"
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]
            [metri.codice.api :as codice]
            [metri.janus.ast-specs :as specs]
            [metri.janus.filter-compiler :as fc]
            [metri.janus.abac-clauses :as abac]))

;; ═══════════════════════════════════════════════════════════════════════════
;; PASO 4e — Proyección :select
;; ═══════════════════════════════════════════════════════════════════════════

(defn- build-select
  "4e: Construye la proyección :select desde el descriptor.
   Sin campos explícitos → pull wildcard ['*].
   
   `select_tree` es un google.protobuf.Struct deserializado (o map).
   Si es una lista plana (fallback), se convierte a nombres de atributos.
   Si es un mapa anidado, lo aplanamos a sus llaves principales porque 
   Datahike almacena las relaciones como strings (ULID), lo que rompe el pull anidado.
   La resolución profunda se hará post-query en el ejecutor.
   
   OLTP identity fix: 'id' -> :entity/ulid
   OLAP identity fix: 'id' -> id_column (iceberg table has 'id' column)
   "
  [entity stree schema]
  (let [is-olap? (= (:engine schema) "olap")]
    (cond
    (or (nil? stree) (empty? stree))
    ['*]

    (or (sequential? stree) (set? stree))
    (mapv (fn [field]
            (let [fname (name field)]
              (cond
                (= fname "id")         (if is-olap? (keyword entity "id") :entity/ulid)
                (= fname "created_at") :meta/created_at
                :else                  (keyword entity fname))))
          stree)

    (map? stree)
    (reduce-kv
     (fn [acc k v]
       (let [fname (if (keyword? k) (name k) (str k))]
         (if (or (not v) (= v false) (= v 0))
           acc
           (conj acc (cond
                       (= fname "id")         (if is-olap? (keyword entity "id") :entity/ulid)
                       (= fname "created_at") :meta/created_at
                       :else                  (keyword entity fname))))))
     []
     stree)
     
    :else ['*])))

(defn- build-nested-pulls
  "Analiza el select_tree para generar un mapa de instrucciones N+1 para el executor OLTP.
   Retorna: {:entity/field {:target-entity \"loc\" :select [:loc/id ...] :nested {}}}."
  [entity stree]
  (if-not (map? stree)
    {}
    (let [schema-res (codice/entity-model entity {})
          schema     (if (= :ok (first schema-res)) (second schema-res) {:attributes []})
          attrs-map  (zipmap (map :name (:attributes schema)) (:attributes schema))]
      (reduce-kv
       (fn [acc k v]
         (let [fname (if (keyword? k) (name k) (str k))]
           (if (and v (map? v))
             (let [attr-def (get attrs-map fname)
                   target-entity (or (:entityRef attr-def) fname)]
               (assoc acc (keyword entity fname)
                      {:target-entity target-entity
                       :select (build-select target-entity v schema)
                       :nested-pulls (build-nested-pulls target-entity v)}))
             acc)))
       {}
       stree))))

;; ═══════════════════════════════════════════════════════════════════════════
;; ENSAMBLADOR PRINCIPAL
;; ═══════════════════════════════════════════════════════════════════════════

(defn- compile-ast-internal
  "Ensambla el AST IR completo desde los pasos 4a–4f.
   Retorna mapa AST IR inmutable o lanza ExceptionInfo."
  [query-descriptor cedar-ctx]
  (let [entity     (or (:entity query-descriptor) "")
        tenant-id  (:tenant-id cedar-ctx)
        user-id    (:user-id cedar-ctx)
        boundaries (get (:domain-boundaries cedar-ctx) entity [])
        schema-res (codice/entity-model entity {})]

    (when (= :error (first schema-res))
      (throw (ex-info "Unknown entity in Códice"
                      (second schema-res))))

    (let [schema  (second schema-res)

          ;; 4f: Campos de propiedad desde el Códice
          {:keys [owner-field assignee-field]}
          (abac/ownership-fields entity schema)

          ;; 4a: Tenant isolation — CARDINAL
          tenant-node (abac/tenant-node tenant-id)

          ;; 4b+4c: Nodo ABAC combinado (locations + scope)
          abac-node   (abac/build-abac-node entity boundaries owner-field assignee-field user-id)

          ;; 4d: Filtros orgánicos del usuario (FilterNode tree recursivo)
          filter-nodes (fc/compile-filters (:filters query-descriptor) schema entity)

          ;; 4d-SEARCH: Omnisearch — cuando :search está presente, construye un nodo
          ;; [:or [:contains :entity/fts-field-1 term] [:contains :entity/fts-field-2 term] ...]
          ;; usando SOLO los atributos marcados con fts=true en el Códice.
          ;; Si no hay fts-fields, el nodo se omite (no genera error).
          search-term  (when (seq (:search query-descriptor)) (:search query-descriptor))
          fts-fields   (when search-term
                         (->> (get schema :attributes [])
                              (filter :fts)
                              (map #(keyword entity (name (:name %))))))
          search-node  (when (and search-term (seq fts-fields))
                         ;; :fuzzy = Levenshtein word-level + case-insensitive substring
                         ;; Threshold adaptativo: ≤2c→0 | 3-8c→1 | ≥9c→2
                         ;; Los FilterCriteria CONTAINS del usuario siguen siendo exactos.
                         (if (= 1 (count fts-fields))
                           [:fuzzy (first fts-fields) search-term]
                           (into [:or] (map #(vector :fuzzy % search-term) fts-fields))))

          ;; Ensamblaje :where — tenant-node SIEMPRE primero
          where-clauses (cond-> [tenant-node]
                          abac-node            (conj abac-node)
                          search-node          (conj search-node)
                          (seq filter-nodes)   (into filter-nodes))

          ;; 4e: Proyección :select
          ;; select_tree llega como un google.protobuf.Struct deserializado.
          ;; El Struct ES el mapa de proyección directamente: {"tag" true, "id" true}.
          ;; Bug anterior: buscaba (:fields stree) — campo inexistente — causando wildcard ['*].
          ;; Fix: extraer las claves del Struct cuyo valor sea truthy.
          ;; Fix: pasamos stree directamente para manejar objetos anidados recursivamente
          stree  (or (:select_tree query-descriptor) (:select-tree query-descriptor))
          select (build-select entity stree schema)]

      ;; AST IR final — campos opcionales via cond->
      (cond-> {:entity   entity
               :schema   schema
               :select   select
               :nested-pulls (build-nested-pulls entity stree)
               :where    (into [:and] where-clauses)
               :limit    (or (:limit query-descriptor) 100)
               :cursor   (:cursor query-descriptor)
               :order-by (or (:sort query-descriptor) [])}

        (or (:time_frame query-descriptor) (:time-frame query-descriptor))
        (assoc :time-frame (or (:time_frame query-descriptor) (:time-frame query-descriptor)))

        ;; OLAP routing signal — presencia de :metrics enruta a Athena
        (or (:metrics query-descriptor) (:metrics query-descriptor))
        (assoc :metrics    (or (:metrics query-descriptor) (:metrics query-descriptor))
               :dimensions (or (:dimensions query-descriptor) (:dimensions query-descriptor))
               :group-by   (or (:dimensions query-descriptor) (:dimensions query-descriptor))
               :time-frame (or (:time_frame query-descriptor) (:time-frame query-descriptor))
               ;; label_template: extrae el primero definido entre las dimensiones
               ;; Sintaxis Mustache: "{{asset_name}} - {{area_value}} KW"
               :label-template (some #(let [t (or (:label_template %) (:label-template %))]
                                        (when (seq t) t))
                                     (or (:dimensions query-descriptor) [])))

        ;; Campos pass-through opacos a Aegis
        (or (:measures query-descriptor) (:measures query-descriptor))
        (assoc :measures (or (:measures query-descriptor) (:measures query-descriptor)))

        (or (:hierarchy query-descriptor) (:hierarchy query-descriptor))
        (assoc :hierarchy (or (:hierarchy query-descriptor) (:hierarchy query-descriptor)))

        (or (:comparisons query-descriptor) (:comparisons query-descriptor))
        (assoc :comparisons (or (:comparisons query-descriptor) (:comparisons query-descriptor)))

        (or (:semantic_measures query-descriptor) (:semantic-measures query-descriptor))
        (assoc :semantic-measures (or (:semantic_measures query-descriptor) (:semantic-measures query-descriptor)))

        ;; OutputCastType → routing signal para Aegis executor
        (or (:output_cast query-descriptor) (:output-cast query-descriptor))
        (assoc :output-cast (let [oc (or (:output_cast query-descriptor) (:output-cast query-descriptor))]
                              (cond
                                (keyword? oc) oc
                                (string? oc) (keyword oc)
                                (number? oc) (case (long oc)
                                               1 :KPI
                                               2 :TIMESERIES
                                               3 :TABLE
                                               4 :PIE
                                               5 :BUBBLE
                                               6 :CSV_EXPORT
                                               :BENCHMARK)
                                :else :KPI)))

        (or (:viz query-descriptor) (:viz query-descriptor))
        (assoc :viz (or (:viz query-descriptor) (:viz query-descriptor)))

        ;; Full-text search pass-through
        (:search query-descriptor)
        (assoc :search (:search query-descriptor))))))

;; ═══════════════════════════════════════════════════════════════════════════
;; RECORD — IASTCompiler
;; ═══════════════════════════════════════════════════════════════════════════

(defrecord JanusASTCompiler [codice-registry]
  proto/IASTCompiler

  (compile-ast [_ query-descriptor cedar-ctx]
    (if-not (specs/context-invariant-validator cedar-ctx)
      (errors/error :JANUS_400
                    {:reason    "invalid-cedar-ctx"
                     :tenant-id (str (:tenant-id cedar-ctx))})
      (try
        [:ok (compile-ast-internal query-descriptor cedar-ctx)]
        (catch clojure.lang.ExceptionInfo e
          (let [data (ex-data e)]
            (log/warn "[Janus AST] Compilación abortada:"
                      (:reason data) "| entity:" (:entity data) "| field:" (:field data))
            (errors/error (or (:code data) :JANUS_400)
                          (merge {:reason    (or (:reason data) "compile-error")
                                  :tenant-id (str (:tenant-id cedar-ctx))}
                                 data))))
        (catch Exception e
          (log/error e "[Janus AST] Error inesperado en compile-ast")
          (errors/error :JANUS_400
                        {:reason    "internal-compile-error"
                         :tenant-id (str (:tenant-id cedar-ctx))
                         :detail    (ex-message e)}))))))

;; ═══════════════════════════════════════════════════════════════════════════
;; INTEGRANT — :janus/ast-compiler
;; ═══════════════════════════════════════════════════════════════════════════

(defmethod ig/init-key :janus/ast-compiler
  [_ {:keys [codice-registry]}]
  (log/info "  -> [Janus] ASTCompiler activo")
  (->JanusASTCompiler codice-registry))

(defmethod ig/halt-key! :janus/ast-compiler [_ _] nil)
