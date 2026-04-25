(ns metri.janus-router.janus-router-test
  "Tests unitarios del JanusRouter core.
   Usa codice.api/init! para inyectar schemas de prueba en el Códice.
   Canales inyectados como stubs — cero I/O de infraestructura.
   11 tests cubriendo el pipeline interno completo de routing."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [metri.janus-router.core :as janus]
            [metri.codice.api :as codice]
            [metri.janus-router.channels.protocol :refer [IJanusWriteChannel]]
            [malli.core :as m]))

;; ═══════════════════════════════════════════════════════════════════════════
;; Stubs canónicos de canales
;; ═══════════════════════════════════════════════════════════════════════════

(defrecord SpyWriteChannel [calls-atom]
  IJanusWriteChannel
  (route [_ ctx]
    (swap! calls-atom conj ctx)
    [:ok {:ulid "spy-ulid" :channel :spy}]))

(defn make-spy-channel []
  (->SpyWriteChannel (atom [])))

;; ═══════════════════════════════════════════════════════════════════════════
;; Registry de prueba para el Códice
;; El formato real del registry es:
;;   {"entity-type" {:schema malli-schema :engine kw :model map}}
;; load-schema busca [:schema] -> retorna [:ok schema-value]
;; el Janus router usa el schema para pre-checks (write_path_locked, etc.)
;; que están en el :model, no en el :schema Malli
;; ═══════════════════════════════════════════════════════════════════════════

;; Schema Malli mínimo para asset (acepta cualquier mapa)
(def any-map-schema (m/schema [:map {:closed false}]))

(def test-registry
  {"asset"
   {:schema any-map-schema
    :engine :oltp
    :model  {:entity     "asset"
             :engine     "oltp"
             :attributes [{:name "name"   :type "string" :required true}
                          {:name "status" :type "string" :required false}]}}

   "meter_reading"
   {:schema any-map-schema
    :engine :olap
    :model  {:entity             "meter_reading"
             :engine             "olap"
             :partition_strategy "YYYY-MM-DD"
             :attributes         [{:name "asset_id" :type "string"}
                                  {:name "value"    :type "number"}]}}

   "read_only_report"
   ;; NOTA: el flag write_path_locked está en el schema map que devuelve load-schema
   ;; el Janus router hace (get schema :write_path_locked) sobre el segundo de [:ok schema]
   {:schema {:write_path_locked true :entity "read_only_report" :engine "oltp" :attributes []}
    :engine :oltp
    :model  {:entity "read_only_report" :engine "oltp" :write_path_locked true :attributes []}}

   "system_config"
   {:schema {:is_system_seeded true :entity "system_config" :engine "oltp" :attributes []}
    :engine :oltp
    :model  {:entity "system_config" :engine "oltp" :is_system_seeded true :attributes []}}})


;; Fixture: inyectar/limpiar registry alrededor de CADA test
;; Usamos :each para evitar race conditions con el runner global
(defn with-test-codice [f]
  (codice/init! test-registry)
  (f)
  ;; Mantenemos el registry — no lo limpiamos para no afectar otros tests del suite
  )

(use-fixtures :each with-test-codice)

;; ═══════════════════════════════════════════════════════════════════════════
;; Helper: ctx de entrada al JanusRouter
;; ═══════════════════════════════════════════════════════════════════════════

(defn make-ctx
  [& {:keys [entity-type operation payload data tenant-id user-id]
      :or   {entity-type "asset"
             operation   :create
             payload     {:name "Pump A" :status "active"}
             tenant-id   "tnt-test"
             user-id     "usr-test"}}]
  {:tenant-id            tenant-id
   :user-id              user-id
   :roles                #{"field-tech"}
   :granted-action-keys  #{"asset:CREATE"}
   :quota-reservation    {:headroom 100}
   :request              (cond-> {:entity-type entity-type
                                  :operation   operation
                                  :payload     payload}
                           data (assoc :data data))})

(defn do-route [ctx channel-registry]
  (janus/route ctx {:channel-registry channel-registry}))

;; ═══════════════════════════════════════════════════════════════════════════
;; Tests
;; ═══════════════════════════════════════════════════════════════════════════

(deftest janus-01-route-to-oltp-channel
  "JANUS-01 — entity=asset (engine:oltp) → OLTPChannel invocado, retorna [:ok]."
  (let [spy-oltp  (make-spy-channel)
        registry  {:oltp spy-oltp :olap (make-spy-channel)}
        ctx       (make-ctx)
        [tag body] (do-route ctx registry)]
    (is (= :ok tag))
    (is (= :spy (:channel body)))
    (is (= 1 (count @(:calls-atom spy-oltp)))
        "El OLTPChannel fue invocado exactamente 1 vez")))

(deftest janus-02-route-to-olap-channel
  "JANUS-02 — entity=meter_reading (engine:olap) → OLAPChannel invocado."
  (let [spy-olap  (make-spy-channel)
        registry  {:oltp (make-spy-channel) :olap spy-olap}
        ;; meter_reading es engine:olap — BulkIngest via :data
        ctx       (make-ctx :entity-type "meter_reading"
                            :data        [{:asset_id "a1" :value 42.0}])
        [tag body] (do-route ctx registry)]
    (is (= :ok tag))
    (is (= 1 (count @(:calls-atom spy-olap)))
        "El OLAPChannel fue invocado exactamente 1 vez")))

(deftest janus-03-unknown-entity-type-error
  "JANUS-03 — entity-type desconocido en el Códice → [:error]."
  (let [ctx       (make-ctx :entity-type "totally_unknown_entity_xyz")
        [tag _body] (do-route ctx {:oltp (make-spy-channel)})]
    (is (= :error tag)
        "Debe retornar [:error ...] para entidades no registradas en el Códice")))

(deftest janus-04-write-path-locked
  "JANUS-04 — schema con write_path_locked=true → [:error :JNS_LOCK_001].
   NOTA: El pre-check se activa solo en BulkIngest (bulk skip validate-payload).
   El schema del registry debe ser un mapa EDN con el flag para que funcione."
  ;; Usamos BulkIngest para evitar que validate-payload falle con el schema EDN plano
  (let [ctx       (make-ctx :entity-type "read_only_report"
                            :data        [])  ;; BulkIngest → is-bulk? = true → salta validate
        [tag body] (do-route ctx {:oltp (make-spy-channel) :olap (make-spy-channel)})]
    (is (= :error tag)
        "read_only_report debe retornar [:error] por write_path_locked=true")
    (is (= :JNS_LOCK_001 (:code body))
        "El código debe ser :JNS_LOCK_001 para write_path_locked")))

(deftest janus-05-is-system-seeded
  "JANUS-05 — schema con is_system_seeded=true → [:error :JNS_SEED_001].
   NOTA: El pre-check se activa solo en BulkIngest (bulk skip validate-payload)."
  (let [ctx       (make-ctx :entity-type "system_config"
                            :data        [])  ;; BulkIngest → is-bulk? = true → salta validate
        [tag body] (do-route ctx {:oltp (make-spy-channel) :olap (make-spy-channel)})]
    (is (= :error tag))
    (is (= :JNS_SEED_001 (:code body))
        "El código debe ser :JNS_SEED_001 para is_system_seeded")))

(deftest janus-06-no-channel-for-engine
  "JANUS-06 — Registry sin canal para engine → [:error :JNS_001]."
  (let [ctx       (make-ctx :entity-type "asset")  ;; engine = :oltp
        ;; Registry sin ningún canal — el router no encontrará :oltp
        [tag body] (do-route ctx {})]
    (is (= :error tag))
    (is (= :JNS_001 (:code body))
        "El código debe ser :JNS_001 cuando no hay canal para el engine")))

(deftest janus-07-tenant-id-injected-from-cedar-ctx
  "JANUS-07 — tenant_id en el payload viene del ctx (Cedar), NUNCA del cliente."
  (let [spy-channel (make-spy-channel)
        registry    {:oltp spy-channel :olap (make-spy-channel)}
        ctx         (make-ctx :tenant-id  "real-tenant-from-cedar"
                              :payload    {:name "A" :status "ok"
                                           :tenant_id "FAKE-CLIENT-TENANT"})
        _result     (do-route ctx registry)
        received-ctx (first @(:calls-atom spy-channel))]
    (is (some? received-ctx) "El canal debe haber sido invocado")
    (when received-ctx
      (is (= "real-tenant-from-cedar"
             (get-in received-ctx [:request :payload :tenant_id]))
          "El tenant_id del cliente debe ser sobrescrito por el del ctx"))))

(deftest janus-08-bulk-data-goes-to-olap-channel
  "JANUS-08 — BulkIngest (meter_reading, engine:olap) con :data → canal OLAP."
  (let [spy-olap  (make-spy-channel)
        registry  {:oltp (make-spy-channel) :olap spy-olap}
        ctx       (make-ctx :entity-type "meter_reading"
                            :data        [{:asset_id "a1" :value 99.0}])
        [tag _] (do-route ctx registry)]
    (is (= :ok tag))
    (is (= 1 (count @(:calls-atom spy-olap)))
        "El OLAPChannel fue invocado para la entidad OLAP")))

(deftest janus-09-schema-propagated-in-ctx-to-channel
  "JANUS-09 — El canal recibe :schema en el ctx (cargado UNA VEZ por el router)."
  (let [spy-channel (make-spy-channel)
        registry    {:oltp spy-channel :olap (make-spy-channel)}
        _result     (do-route (make-ctx) registry)
        received-ctx (first @(:calls-atom spy-channel))]
    (is (some? received-ctx) "El canal debe haber sido invocado")
    (when received-ctx
      (is (some? (:schema received-ctx))
          "El canal debe recibir :schema en el ctx — cargado UNA SOLA VEZ"))))

(deftest janus-10-entity-type-in-ctx-to-channel
  "JANUS-10 — El canal recibe :entity-type en el ctx."
  (let [spy-channel (make-spy-channel)
        registry    {:oltp spy-channel :olap (make-spy-channel)}
        _result     (do-route (make-ctx :entity-type "asset") registry)
        received-ctx (first @(:calls-atom spy-channel))]
    (is (some? received-ctx) "El canal debe haber sido invocado")
    (when received-ctx
      (is (= "asset" (:entity-type received-ctx))
          "El canal debe recibir :entity-type correcto"))))

(deftest janus-11-operation-propagated-in-ctx
  "JANUS-11 — El canal recibe :operation en el ctx desde el request original."
  (let [spy-channel (make-spy-channel)
        registry    {:oltp spy-channel :olap (make-spy-channel)}
        _result     (do-route (make-ctx :operation :update) registry)
        received-ctx (first @(:calls-atom spy-channel))]
    (is (some? received-ctx) "El canal debe haber sido invocado")
    (when received-ctx
      (is (= :update (:operation received-ctx))
          "El canal debe recibir :operation correcto"))))
