(ns metri.aegis.sql.where
  "Compilador WHERE: nodos AST IR → expresiones HoneySQL.
   SRP: transpilación pura de predicados — sin I/O, sin estado.

   Operadores soportados (14 FilterOperator del contrato §1):
     :=  :not=  :>  :<  :>=  :<=  :in  :not-in  :between
     :matches  :like  :contains  :is-null  :is-not-null
     :and  :or  :not  :fts (ignorado con warning)

   Extensión cross-entity (OLAP):
     :ref-filter [ref-field inner-node]
       → ref-field IN (SELECT id FROM db.ref_entity WHERE <inner-honey>)
       ref-entity se deduce del namespace del primer campo del inner-node.

   Ejemplo:
     [:ref-filter :asset/location_id [:= :location/type \"BUILDING\"]]
     → location_id IN (SELECT id FROM metrics_db.location WHERE type = 'BUILDING')"
  (:require [taoensso.timbre :as log]
            [clojure.string :as str]
            [metri.aegis.sql.helpers :as h]
            [metri.aegis.sql.fuzzy-sql :as fz]))

;; ── WHERE node compiler ───────────────────────────────────────────────────────

(declare where-node->honey)

(defn- ref-entity-from-inner
  "Extrae el nombre de la entidad referenciada desde el primer campo con
   namespace del inner-node del :ref-filter.
   [:= :location/type \"BUILDING\"] → \"location\"
   [:and [:= :location/type \"X\"] ...] → \"location\""
  [inner-node]
  (letfn [(find-ns [node]
            (when (vector? node)
              (let [[op & args] node]
                (case op
                  (:and :or :not) (some find-ns (filter vector? args))
                  ;; Nodo hoja: primer arg es el field keyword
                  (when (keyword? (first args))
                    (namespace (first args)))))))]
    (find-ns inner-node)))

(defn where-node->honey
  "Transpila un nodo :where AST IR → expresión HoneySQL.
   ctx (opcional): {:database \"metrics_db\"} — requerido para :ref-filter en OLAP.
   Si se omite ctx, se usa {} (compatible con todos los call sites existentes).
   HoneySQL maneja nil como IS NULL/IS NOT NULL en comparaciones de igualdad."
  ([node] (where-node->honey node {}))
  ([node ctx]
   (when (nil? node)
     (throw (ex-info "Null WHERE node" {:code :AEG_COMPILE_002 :detail "nil node"})))
   (let [[op & args] node
         database    (:database ctx)]
     (case op
       :=        [:=  (h/col-kw (first args)) (second args)]
       :not=     [:<> (h/col-kw (first args)) (second args)]
       :>        [:>  (h/col-kw (first args)) (second args)]
       :<        [:<  (h/col-kw (first args)) (second args)]
       :>=       [:>= (h/col-kw (first args)) (second args)]
       :<=       [:<= (h/col-kw (first args)) (second args)]
       :in       [:in     (h/col-kw (first args)) (vec (second args))]
       :not-in   [:not-in (h/col-kw (first args)) (vec (second args))]
       ;; BETWEEN — HoneySQL: [:between col min max]
       :between  (let [[field [min-v max-v]] args]
                   [:between (h/col-kw field) min-v max-v])
       ;; MATCHES → Athena REGEXP_LIKE(col, pattern)
       :matches  [:regexp_like (h/col-kw (first args)) (second args)]
       :like     [:like (h/col-kw (first args)) (second args)]
       ;; CONTAINS → LIKE con wildcards; escapa metacaracteres del usuario
       :contains (let [escaped (-> (str (second args))
                                   (str/replace "\\" "\\\\")
                                   (str/replace "%" "\\%")
                                   (str/replace "_" "\\_"))]
                   [:like (h/col-kw (first args)) (str "%" escaped "%")])
       :is-null     [:=  (h/col-kw (first args)) nil]
       :is-not-null [:<> (h/col-kw (first args)) nil]
       :and  (into [:and] (map #(where-node->honey % ctx) args))
       :or   (into [:or]  (map #(where-node->honey % ctx) args))
       :not  [:not (where-node->honey (first args) ctx)]
       ;; ── :fuzzy — Fuzzy search cost-safe para Athena/OLAP ────────────────
     ;; Athena recibe LIKE + regexp_like — NUNCA levenshtein_distance().
     ;; 3-8 chars → [:or LIKE ci, regexp_like sustituciones]
     ;; ≤2 o ≥9 → solo LIKE case-insensitive (sin fuzzy expansion)
     ;; Iceberg bloom filters + min/max reducen el scan real.
     :fuzzy
     (let [[field term] args
           col-honey    (h/col-kw field)
           honey        (fz/fuzzy-honey col-honey (str term))]
       (if honey
         (do (log/debug "[Aegis SQL] :fuzzy | campo:" field "| término:" term) honey)
         (do (log/warn "[Aegis SQL] :fuzzy sin expansión válida — fail-open 1=1") [:raw "1=1"])))

     :fts  (do (log/warn "[Aegis WHERE] :fts no soportado en Athena — ignorado")
               [:raw "1=1"])

       ;; ── :ref-filter (cross-entity OLAP) ──────────────────────────────────
       ;; args = [ref-field inner-node]
       ;; ref-field  = :asset/location_id  → columna SQL = location_id
       ;; inner-node = [:= :location/type "BUILDING"]  → WHERE de la subquery
       ;; Genera: location_id IN (SELECT id FROM db.location WHERE type = 'BUILDING')
       :ref-filter
       (let [[ref-field inner-node] args
             ref-col     (h/col-kw ref-field)           ; :location_id
             ref-entity  (ref-entity-from-inner inner-node) ; "location"
             inner-honey (where-node->honey inner-node ctx)
             ref-tbl     (if (and database ref-entity)
                           (keyword (h/table-str ref-entity database))
                           (keyword (or ref-entity "unknown")))]
         (if ref-entity
           (do
             (log/debug "[Aegis WHERE] ref-filter: " ref-col
                        " IN (SELECT id FROM " ref-tbl " WHERE " (pr-str inner-honey) ")")
             [:in ref-col {:select [:id]
                           :from   [ref-tbl]
                           :where  inner-honey}])
           (do
             (log/warn "[Aegis WHERE] :ref-filter sin ref-entity detectado — fail-safe 1=0")
             [:raw "1=0"])))

       (throw (ex-info (str "Unsupported SQL operator: " op)
                       {:code :AEG_COMPILE_002 :operator op}))))))

;; ── TimeFrame helpers ─────────────────────────────────────────────────────────

(defn time-frame->honey-clause
  "TimeFrame {:start-ts :end-ts} → cláusula HoneySQL para el WHERE.
   Retorna nil si ambos timestamps son nil (ALL_TIME)."
  [time-frame ts-col]
  (let [col  (or ts-col :created_at)
        {:keys [start-ts end-ts]} time-frame]
    (cond
      (and start-ts end-ts) [:and [:>= col start-ts] [:<= col end-ts]]
      start-ts              [:>= col start-ts]
      end-ts                [:<= col end-ts]
      :else                 nil)))

(defn add-where
  "Combina base + extra WHERE con [:and ...].
   Retorna base sin modificar si extra es nil."
  [base extra]
  (if extra [:and base extra] base))
