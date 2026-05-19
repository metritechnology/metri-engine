;; [PORTED_TO_RUST: src/janus/router.rs]
;; NO MODIFICAR — fuente de verdad en Rust
(ns metri.janus-router.core
  "JanusRouter — Router de escritura del Metri Engine.
   Invocado exclusivamente por el IOP, únicamente después de que
   CedarAuthorizer y QuotaGuard hayan emitido [:ok ...].

   Pipeline interno:
     1. load-schema  (Códice) — UNA SOLA VEZ, propaga en ctx
     2. validate-payload (Códice/Malli) — Railway puro
     3. Pre-checks: write_path_locked, is_system_seeded
     4. Resolve engine (oltp|olap) desde el Códice
     5. Enriquecer ctx con :schema + tenant_id inyectado (NUNCA del cliente)
     6. Despachar al canal del registry inyectado"
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.codice.api :as codice]
            [metri.domain.errors :as errors]))

(defn route
  "Punto de entrada del router.
   ctx: salida de QuotaGuard (identidad resuelta + quota-reservation)
   deps: {:channel-registry {engine → IJanusWriteChannel}}"
  [ctx {:keys [channel-registry]}]
  (let [entity-type (get-in ctx [:request :entity-type])
        operation   (get-in ctx [:request :operation])]

    (log/debug "[Janus] Enrutando | entity:" entity-type "| op:" operation
               "| tenant:" (:tenant-id ctx))

    ;; 1. Cargar schema desde el Códice (O(1) desde atom en memoria)
    (let [schema-res (codice/load-schema entity-type {})]
      (if (= :error (first schema-res))
        (do
          (log/warn "[Janus] Entidad desconocida en Códice:" entity-type)
          schema-res)

          (let [schema   (second schema-res)
                payload  (get-in ctx [:request :payload] {})
                is-bulk? (some? (get-in ctx [:request :data]))]

          ;; 2. Validar payload contra schema Malli compilado (solo si no es Bulk)
          (let [val-res (if is-bulk?
                          [:ok payload]
                          (codice/validate-payload schema payload entity-type {}))]
            (if (= :error (first val-res))
              (do
                (log/warn "[Janus] Payload rechazado por Malli | entity:" entity-type
                          "| violations:" (get-in val-res [1 :violations]))
                val-res)

              ;; 3. Pre-checks dirigidos por el Códice
              (cond
                (true? (get schema :write_path_locked))
                (do
                  (log/warn "[Janus] write_path_locked=true | entity:" entity-type)
                  (errors/error :JNS_LOCK_001
                                {:stage     :janus
                                 :entity_type entity-type
                                 :tenant-id (:tenant-id ctx)
                                 :user-id   (:user-id ctx)}))

                (true? (get schema :is_system_seeded))
                (do
                  (log/warn "[Janus] is_system_seeded=true — mutación bloqueada | entity:" entity-type)
                  (errors/error :JNS_SEED_001
                                {:stage     :janus
                                 :entity_type entity-type
                                 :tenant-id (:tenant-id ctx)
                                 :user-id   (:user-id ctx)}))

                :else
                ;; 4. Resolver engine desde el Códice
                (let [engine-res (codice/entity-engine entity-type {})]
                  (if (= :error (first engine-res))
                    engine-res
                    (let [engine  (second engine-res)
                          channel (get channel-registry engine)]

                      (if-not channel
                        (do
                          (log/error "[Janus] Sin canal para engine:" engine)
                          (errors/error :JNS_001
                                        {:stage     :janus
                                         :detail    (str "No channel for engine: " (name engine))
                                         :tenant-id (:tenant-id ctx)
                                         :user-id   (:user-id ctx)}))

                        ;; 5. Enriquecer ctx — tenant_id inyectado (NUNCA del cliente)
                        (let [model-res (codice/entity-model entity-type {})
                              model     (if (= :ok (first model-res)) (second model-res) {})
                              safe-ctx (-> ctx
                                           (assoc :schema schema)
                                           (assoc :model model)
                                           (assoc :entity-type entity-type)
                                           (assoc :operation operation)
                                           (update-in [:request :payload]
                                                      #(when % (assoc % :tenant_id (:tenant-id ctx))))
                                           (cond-> (get-in ctx [:request :data])
                                             (update-in [:request :data]
                                                        #(mapv (fn [row] (assoc row :tenant_id (:tenant-id ctx))) %))))]

                          (log/info "[Janus] Despachando | engine:" engine
                                    "| entity:" entity-type
                                    "| tenant:" (:tenant-id ctx))

                          ;; 6. Despachar al canal del registry
                          (.route channel safe-ctx))))))))))))))

(defmethod ig/init-key :iop/janus-router
  [_ {:keys [channel-registry]}]
  (log/info "  -> [Janus] Router real activo | canales:" (mapv name (keys channel-registry)))
  (fn [ctx]
    (route ctx {:channel-registry channel-registry})))

(defmethod ig/halt-key! :iop/janus-router [_ _] nil)
