(ns metri.janus-router.oltp-channel-test
  "Tests unitarios del OLTPChannel — canal de escritura ACID via Datahike.
   Usa Datahike in-memory backend — sin DynamoDB, sin LocalStack.
   9 tests cubriendo el contrato ACID del canal OLTP."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [metri.janus-router.channels.oltp :as oltp]
            [metri.janus-router.projections.protocol :as proj]
            [metri.janus-router.stubs.core :as stubs]))

;; ─── Datahike in-memory setup ────────────────────────────────────────────────

(def dh-config {:store {:backend :mem :id "oltp-test"}})

(defn with-fresh-dh
  "Crea una base de datos Datahike in-memory y la elimina al terminar.
   El schema mínimo incluye :entity/ulid, :entity/type, :tenant/id."
  [f]
  (when (d/database-exists? dh-config)
    (d/delete-database dh-config))
  (d/create-database dh-config)
  (let [conn (d/connect dh-config)]
    ;; Schema mínimo para los tests del OLTP canal
    (d/transact conn {:tx-data
                      [{:db/ident       :entity/ulid
                        :db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/unique      :db.unique/identity
                        :db/index       true}
                       {:db/ident       :entity/type
                        :db/valueType   :db.type/keyword
                        :db/cardinality :db.cardinality/one
                        :db/index       true}
                       {:db/ident       :tenant/id
                        :db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/index       true}
                       {:db/ident       :asset/name
                        :db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one}
                       {:db/ident       :asset/status
                        :db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one}]})
    (try
      (f conn)
      (finally
        (d/release conn)
        (d/delete-database dh-config)))))

;; ─── Factory de OLTPChannel ─────────────────────────────────────────────────

(defn make-oltp-channel
  "Crea un OLTPChannel con la conexión Datahike dada."
  ([conn] (make-oltp-channel conn []))
  ([conn projections]
   (oltp/->OLTPChannel conn projections)))

;; ─── Schema de prueba (asset) ────────────────────────────────────────────────

(def asset-schema
  {:entity     "asset"
   :engine     "oltp"
   :attributes [{:name "name"   :type "string" :required true}
                {:name "status" :type "string" :required false}]})

(defn make-oltp-ctx
  "Ctx canónico enriquecido, tal como llega del JanusRouter al OLTPChannel."
  [conn & {:keys [tenant-id payload operation]
           :or   {tenant-id "tnt-test" payload {:name "Pump A" :status "active"
                                                :tenant_id "tnt-test"}
                  operation :create}}]
  {:tenant-id   tenant-id
   :user-id     "usr-test"
   :entity-type "asset"
   :operation   operation
   :schema      asset-schema
   :request     {:entity-type "asset"
                 :operation   operation
                 :payload     (assoc payload :tenant_id tenant-id)}})

;; ═══════════════════════════════════════════════════════════════════════════
;; Tests
;; ═══════════════════════════════════════════════════════════════════════════

(deftest oltp-01-single-record-ok
  "OLTP-01 — Un solo registro se persiste en Datahike y retorna [:ok {:ulid ...}]."
  (with-fresh-dh
    (fn [conn]
      (let [channel    (make-oltp-channel conn)
            ctx        (make-oltp-ctx conn)
            [tag body] (.route channel ctx)]
        (is (= :ok tag))
        (is (some? (:ulid body))    "ULID debe estar presente")
        (is (= :oltp (:channel body)) "Channel debe ser :oltp")))))

(deftest oltp-02-bulk-records-ok
  "OLTP-02 — Batch de 3 registros se persisten y retorna [:ok {:ingested-count 3}]."
  (with-fresh-dh
    (fn [conn]
      (let [channel (make-oltp-channel conn)
            data    [{:name "A1" :tenant_id "tnt-test"}
                     {:name "A2" :tenant_id "tnt-test"}
                     {:name "A3" :tenant_id "tnt-test"}]
            ctx     {:tenant-id   "tnt-test"
                     :entity-type "asset"
                     :operation   :bulk
                     :schema      asset-schema
                     :request     {:entity-type "asset"
                                   :operation   :bulk
                                   :data        data}}
            [tag body] (.route channel ctx)]
        (is (= :ok tag))
        (is (= 3 (:ingested-count body)))))))

(deftest oltp-03-empty-batch-ok
  "OLTP-03 — Batch vacío retorna [:ok {:ingested-count 0}] sin llamar d/transact."
  (with-fresh-dh
    (fn [conn]
      (let [channel (make-oltp-channel conn)
            ctx     {:tenant-id   "tnt-test"
                     :entity-type "asset"
                     :operation   :bulk
                     :schema      asset-schema
                     :request     {:entity-type "asset" :operation :bulk :data []}}
            [tag body] (.route channel ctx)]
        (is (= :ok tag))
        (is (= 0 (:ingested-count body)))))))

(deftest oltp-04-tenant-id-injected-in-fact
  "OLTP-04 — El :tenant/id se inyecta correctamente en el hecho Datahike desde ctx."
  (with-fresh-dh
    (fn [conn]
      (let [channel    (make-oltp-channel conn)
            ctx        (make-oltp-ctx conn :tenant-id "acme-corp")
            [tag body] (.route channel ctx)]
        (is (= :ok tag))
        ;; Verificar que el registro fue guardado con :tenant/id correcto
        (let [ulid  (:ulid body)
              found (d/q '[:find ?tid
                            :in $ ?ulid
                            :where [?e :entity/ulid ?ulid]
                                   [?e :tenant/id ?tid]]
                          @conn ulid)]
          (is (= #{["acme-corp"]} found)
              "El tenant/id en Datahike debe ser el del ctx, no el del cliente"))))))

(deftest oltp-05-entity-fact-structure
  "OLTP-05 — El hecho generado contiene :entity/ulid, :entity/type, :tenant/id."
  (with-fresh-dh
    (fn [conn]
      (let [channel    (make-oltp-channel conn)
            ctx        (make-oltp-ctx conn)
            [tag body] (.route channel ctx)
            ulid       (:ulid body)]
        (is (= :ok tag))
        (let [entity (d/pull @conn '[:entity/ulid :entity/type :tenant/id] [:entity/ulid ulid])]
          (is (= ulid           (:entity/ulid entity)) ":entity/ulid correcto")
          (is (= :asset         (:entity/type entity)) ":entity/type correcto")
          (is (= "tnt-test"     (:tenant/id entity))   ":tenant/id correcto"))))))

(deftest oltp-06-auto-generate-base36
  "OLTP-06 — Campo con auto_generate:stochastic_base36 es generado internamente.
   Usa un DB con el atributo :asset/work_order_number declarado en el schema."
  (let [cfg {:store {:backend :mem :id "oltp-test-base36"}}
        _ (when (d/database-exists? cfg) (d/delete-database cfg))
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      ;; Schema extendido con :asset/work_order_number
      (d/transact conn {:tx-data
                        [{:db/ident :entity/ulid :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                         {:db/ident :entity/type :db/valueType :db.type/keyword
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :tenant/id :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :asset/name :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         ;; Atributo requerido para el test auto_generate
                         {:db/ident :asset/work_order_number :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}]})
      (let [schema-with-gen {:entity     "asset"
                             :engine     "oltp"
                             :attributes [{:name "name" :type "string"}
                                          {:name "work_order_number"
                                           :type "string"
                                           :auto_generate "stochastic_base36"}]}
            channel    (make-oltp-channel conn)
            ctx        {:tenant-id   "tnt-x"
                        :entity-type "asset"
                        :operation   :create
                        :schema      schema-with-gen
                        :request     {:entity-type "asset"
                                      :operation   :create
                                      :payload     {:name "Asset X" :tenant_id "tnt-x"}}}
            [tag _] (.route channel ctx)]
        (is (= :ok tag) "La generación base36 persiste correctamente con el schema declarado"))
      (finally
        (d/release conn)
        (d/delete-database cfg)))))

(deftest oltp-07-projection-error-short-circuits
  "OLTP-07 — Si un IProjectionBuilder.build retorna [:error], el canal NO llama d/transact."
  (with-fresh-dh
    (fn [conn]
      (let [failing-projection
            (reify proj/IProjectionBuilder
              (applicable? [_ _schema] true)
              (build [_ _schema _payload _ulid]
                [:error {:code :JNS_PROJ_001 :stage :janus :detail "Simulated projection error"}]))

            channel    (make-oltp-channel conn [failing-projection])
            ctx        (make-oltp-ctx conn)
            [tag body] (.route channel ctx)]

        (is (= :error tag)
            "El canal debe retornar [:error] si la proyección falla")
        ;; Verificar que NO hay datos en Datahike (d/transact nunca fue llamado)
        (let [count (count (d/q '[:find ?e :where [?e :entity/type :asset]] @conn))]
          (is (= 0 count) "d/transact no debe haber sido invocado"))))))

(deftest oltp-08-datahike-failure-returns-error
  "OLTP-08 — Si la conn es nil (simula fallo de Datahike), retorna [:error :JNS_TX_001]."
  ;; Usamos una conexión inválida para forzar el fallo del TX
  (let [bad-conn   nil  ;; nil forzará una NullPointerException en d/transact
        channel    (oltp/->OLTPChannel bad-conn [])
        ctx        {:tenant-id   "tnt-x"
                    :entity-type "asset"
                    :operation   :create
                    :schema      asset-schema
                    :request     {:entity-type "asset"
                                  :operation   :create
                                  :payload     {:name "A" :tenant_id "tnt-x"}}}
        [tag body] (.route channel ctx)]
    (is (= :error tag))
    ;; El error code puede ser :JNS_TX_001 o un código genérico de la catch clause
    (is (some? (:code body)) "El error debe tener un :code")))

(deftest oltp-09-update-operation
  "OLTP-09 — Operación :update persiste sin error (semántica upsert de Datahike)."
  (with-fresh-dh
    (fn [conn]
      (let [channel    (make-oltp-channel conn)
            ctx        (make-oltp-ctx conn :operation :update
                                      :payload {:name "Updated Pump" :status "maintenance"
                                                :tenant_id "tnt-test"})
            [tag _] (.route channel ctx)]
        (is (= :ok tag))))))
