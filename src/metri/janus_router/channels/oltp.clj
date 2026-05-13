(ns metri.janus-router.channels.oltp
  "OLTPChannel — Canal de escritura ACID via Datahike.
   Responsabilidades:
     1. FK integrity (verify-entity-refs)
     2. Enriquecer payload con auto_generate (via Códice)
     3. TX ACID en Datahike (entidad principal + proyecciones)
   SIN imports directos de negocio — todo llega en ctx o inyectado."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [datahike.api :as d]
            [metri.janus-router.channels.protocol :refer [IJanusWriteChannel]]
            [metri.janus-router.projections.protocol :as proj]
            [metri.domain.errors :as errors]
            [metri.janus-router.ulid :as ulid]))

;; ── ULID generation ────────────────────────────────────────────────────────
;; ULID: 48-bit timestamp + 80-bit random, Crockford Base32 (26 chars).
;; Lexicográficamente ordenable, monotónico dentro del mismo ms.
(defn- generate-ulid [] (ulid/generate))



;; ── Coerce payload ────────────────────────────────────────────────────────
(defn- coerce-payload
  "Coerces numeric fields based on model types.
   If type is epoch or number, coerces to Long to avoid schema validation errors."
  [payload model]
  (let [attrs (get model :attributes [])
        type-map (into {} (map (juxt #(keyword (:name %)) :type) attrs))]
    (reduce-kv
      (fn [acc k v]
        (let [t (get type-map k)]
          (assoc acc k
                 (cond
                   (and (= "epoch" t) (number? v)) (long v)
                   (and (= "number" t) (number? v)) (long v)
                   :else v))))
      {}
      payload)))

;; ── Build tx-data: entidad principal ───────────────────────────────────
(defn- build-entity-fact
  "Traduce payload validado + ULID → mapa de hechos Datahike.
   :tenant/id se inyecta desde el payload (ya enriquecido por Janus).

   Los campos 'reference' del modelo Códice se registran en Datahike como
   :db.type/string — el ULID string se persiste directamente.
   El pull [*] retorna el string, que el executor expone en TABLE/TREE."
  [entity-kw payload ulid model]
  (merge
    {:db/id              ulid
     :entity/ulid        ulid
     :entity/type        entity-kw
     :tenant/id          (:tenant_id payload)
     :meta/created_at    (System/currentTimeMillis)}
    (into {}
          (keep (fn [[k v]]
                  (let [k-kw (keyword k)]
                    (when (and (some? v) (not (#{:tenant_id :id :entity_type :tenant-id :entity-type} k-kw)))
                      [(keyword (name entity-kw) (name k)) v])))
                payload))))

;; ── Ejecutar TX ACID ────────────────────────────────────────────────────
(defn- execute-transaction!
  "Ejecuta la TX ACID en Datahike.
   Retorna [:ok tx-report] | [:error {:code :JNS_TX_001 ...}].
   NUNCA lanza — errors/error único constructor de [:error]."
  [conn tx-data ctx]
  (try
    (let [tx-report (d/transact conn {:tx-data tx-data})]
      [:ok tx-report])
    (catch Exception e
      (log/error "[OLTP] TX ACID falló:" (.getMessage e))
      (errors/error :JNS_TX_001
                    {:stage     :janus
                     :detail    (ex-message e)
                     :tenant-id (:tenant-id ctx)
                     :user-id   (:user-id ctx)}))))

;; ── Construir Proyecciones (Vistas Materializadas / CQRS) ──────────────
(defn- build-projections
  "Itera sobre los IProjectionBuilder inyectados.
   Retorna un vector de hechos adicionales o un vector de error [:error ...]."
  [projections schema payload ulid]
  (reduce (fn [acc p]
            (if (proj/applicable? p schema)
              (let [res (proj/build p schema payload ulid)]
                (if (= :error (first res))
                  (reduced res) ;; Cortocircuito en el primer error
                  (let [facts (second res)]
                    (if (sequential? facts)
                      (into acc facts)
                      (conj acc facts)))))
              acc))
          []
          projections))

;; ── OLTPChannel defrecord ──────────────────────────────────────────────
(defrecord OLTPChannel [datahike-conn projections codice-generator-fn]
  IJanusWriteChannel
  (route [_ ctx]
    (try
      (let [{:keys [schema model tenant-id operation]} ctx
            op          (or operation (get-in ctx [:request :operation]))
            entity-type (get-in ctx [:request :entity-type])
            is-bulk?    (or (= :bulk op) (some? (get-in ctx [:request :data])))
            records     (if is-bulk?
                          (get-in ctx [:request :data] [])
                          [(get-in ctx [:request :payload] {})])
            
            ;; Procesar todos los registros (1 o N)
            processed (reduce
                        (fn [acc raw-payload]
                          (let [payload  (coerce-payload raw-payload model)
                                ulid     (or (:id payload) (generate-ulid))
                                gen-res  (codice-generator-fn datahike-conn model tenant-id payload)]
                            (if (= :error (first gen-res))
                              (reduced gen-res) ;; Short-circuit if generation fails
                              (let [enriched (second gen-res)
                                    tx-fact  (build-entity-fact (keyword entity-type) enriched ulid model)
                                    proj-res (build-projections projections model enriched ulid)]
                                (if (and (vector? proj-res) (= :error (first proj-res)))
                                  (reduced proj-res) ;; Cortocircuito si falla
                                  (-> acc
                                      (update :ulids conj ulid)
                                      (update :tx-data conj tx-fact)
                                      (update :tx-data into proj-res)))))))
                        {:ulids [] :tx-data []}
                        records)]
        
        (if (and (vector? processed) (= :error (first processed)))
          processed ;; Retornar error de proyección (ej. falló un IProjectionBuilder)
          (let [tx-data (:tx-data processed)
                ulids   (:ulids processed)
                tx-res  (if (empty? tx-data)
                          [:ok nil] ;; Evitar llamar a Datahike con vector vacío si el batch vino vacío
                          (execute-transaction! datahike-conn tx-data ctx))]
            (if (= :error (first tx-res))
              tx-res
              (do
                (log/info "[Janus OLTP] ✅ TX ACID exitosa | entity:" entity-type
                          "| tenant:" tenant-id
                          "| count:" (count records))
                (if is-bulk?
                  [:ok {:ingested-count (count records)
                        :outbox-count   0}]
                  [:ok {:entity-id   (first ulids)
                        :ulid        (first ulids)
                        :channel     :oltp
                        :entity-type entity-type
                        :tenant-id   tenant-id}]))))))
      (catch Exception e
        (log/error e "[Janus OLTP] Excepción no controlada en TX")
        [:error {:code :JNS_TX_001 :detail (ex-message e)}]))))

(defmethod ig/init-key :janus-router/oltp-channel
  [_ {:keys [datahike-conn projections codice-generator-fn]}]
  (log/info "  -> [Janus] OLTPChannel activo | Datahike backend | Proyecciones:" (count projections))
  (->OLTPChannel (:conn datahike-conn) (or projections []) codice-generator-fn))

(defmethod ig/halt-key! :janus-router/oltp-channel [_ _] nil)
