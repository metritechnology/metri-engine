(ns metri.janus.core
  "JanusCerebro -- Orquestador del Read Path (Query RPC).
   SRP: gestiona el pipeline Zero-Trust -> Cedar -> compile -> transmutar -> normalizar.
   Wiring Integrant para :janus/query y los endpoints Read Path unarios.

   Delega:
     * BatchContext + CrossFilter enrichment -> metres.janus.batch-enricher
     * MultiSeriesGroup outer-join           -> metres.janus.multi-series
     * FilterNode compilation                -> metres.janus.filter-compiler (via ast-compiler)
     * ABAC clause construction              -> metres.janus.abac-clauses (via ast-compiler)
     * Output contract normalization         -> metres.janus.normalizer

   Pipeline de 7 pasos (docs/architecture/05.01-JANUS.md):
     Paso 1: Gate Zero-Trust (tenant-id present)
     Paso 2: Cedar authorize  -> [:ok cedar-ctx] | [:error]
     Paso 3: Validar cedar-ctx con context-invariant
     Paso 4: Enriquecer queries (BatchContext + CrossFilter)
     Paso 5: compile-ast por cada sub-query del request
     Paso 6: transmute! -> AegisTransmuter -> lazy-seq de chunks
     Paso 7: normalize-chunk -> 100% cobertura del contrato QueryResponse proto"
  (:require [integrant.core :as ig]
            [clojure.string :as str]
            [clojure.core.async :as a]
            [taoensso.timbre :as log]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]
            [metri.janus.ast-specs :as specs]
            [metri.janus.batch-enricher :as enricher]
            [metri.janus.multi-series :as ms]
            [metri.janus.normalizer :as normalizer]
            [metri.janus.validator :as validator]
            [metri.otel.spans :as otel]))

;; 
;; CEDAR / ZERO-TRUST HELPERS
;; 

(defn- build-stub-cedar-ctx
  "Construye un cedar-ctx minimo desde el ctx del IOP para entornos sin Cedar real.
   Usado cuando el cedar-authorizer no esta inyectado o retorna pass-through."
  [ctx]
  {:tenant-id          (or (:tenant-id ctx) "unknown-tenant")
   :user-id            (or (:user-id ctx) "unknown-user")
   :roles              (or (:roles ctx) #{})
   :domain-boundaries  (or (:domain-boundaries ctx) {})
   :is-super-master    (boolean (:is-super-master ctx))
   :cross-tenant-scope (or (:cross-tenant-scope ctx) "NONE")
   :request            (:request ctx)})

(defn- validate-tenant!
  "Gate Zero-Trust: lanza ExceptionInfo si tenant-id esta ausente o es vacio."
  [tenant-id]
  (when (or (nil? tenant-id) (str/blank? (str tenant-id)) (= "unknown-tenant" tenant-id))
    (throw (ex-info "tenant_id empty - Zero-Trust gate rejected"
                    {:code      :JANUS_400
                     :reason    "tenant-id-empty"
                     :tenant-id (str tenant-id)}))))

(defn- cedar-authorize
  "Paso 2: Invoca el cedar-authorizer.
   Soporta fn directa (stub AlwaysAllow) e impl ICedarContext (prod)."
  [cedar-authorizer ctx]
  (if cedar-authorizer
    (if (satisfies? proto/ICedarContext cedar-authorizer)
      (proto/intercept cedar-authorizer ctx)
      (cedar-authorizer ctx))
    [:ok (build-stub-cedar-ctx ctx)]))

;; 
;; EJECUCIN DE UN SUB-QUERY
;; 

(defn- process-single-query
  "Compila el AST IR para un sub-query y lo transmuta via Aegis.
   Retorna seq de chunks decorados con :query-key."
  [query-key query-map cedar-ctx ast-compiler aegis-engine explain-plan?]
  (otel/with-span [(str "janus.query." (name query-key)) {:kind :internal}]
    (try
      (let [span (otel/current-span)]
        (otel/set-attributes! span
          {"query.key"   (name query-key)
           "entity.type" (str (:entity query-map))
           "tenant.id"   (str (:tenant-id cedar-ctx))})
        (let [ast-result (proto/compile-ast ast-compiler query-map cedar-ctx)]
          (if (= :error (first ast-result))
            (do (otel/set-status! span :error "AST compile failed")
                [ast-result])
            (let [ast-ir (second ast-result)]
              (if explain-plan?
                ;; Cortocircuito de Explain Plan: Devolver el AST como resultado
                [[:ok {:data [[(pr-str ast-ir)]]
                       :columns [{:key "ast_ir" :type "string"}]
                       :query-key query-key
                       :channel :explain}]]
                ;; Flujo Normal de Transmutacion
                (let [_ nil
                      chunks (proto/transmute! aegis-engine ast-ir)]
                  (let [label-tmpl (:label-template ast-ir)]
                    (map (fn [chunk]
                           (-> chunk
                               (update 1 (fn [body]
                                           (cond-> (assoc body :query-key query-key)
                                             label-tmpl (assoc :label-template label-tmpl))))
                               normalizer/normalize-chunk))
                         chunks))))))))
      (catch Exception e
        (log/error e "Error processing query:" query-key)
        [[:error (errors/error :JANUS_400 {:reason (.getMessage e) :query-key query-key})]]))))

;; 
;; PIPELINE PRINCIPAL
;; 

(defn run-query-pipeline
  "Ejecuta el pipeline completo Read Path para un ctx de Query.
   Retorna lazy-seq de chunks ([:ok data] | [:error map]).
   Nunca lanza - errores se encapsulan como chunks de error."
  [ctx cedar-authorizer ast-compiler aegis-engine]
  (otel/with-span ["janus.read_path" {:kind :server}]
    (let [span      (otel/current-span)
          tenant-id (:tenant-id ctx)]
      (otel/set-attributes! span {"tenant.id" (str tenant-id)})
      (try
        ;; Paso 1: Gate Zero-Trust
        (validate-tenant! tenant-id)

        ;; Defensa Inquebrantable: Validar request integro contra el contrato Malli de gRPC
        (validator/validate! :metri.spec/query-request ctx)

        ;; Paso 2: Cedar authorize
        (let [cedar-result (cedar-authorize cedar-authorizer ctx)]
          (if (= :error (first cedar-result))
            (do (otel/set-status! span :error "Cedar DENY")
                [cedar-result])

            ;; Paso 3: Validar cedar-ctx contra context-invariant
            (let [cedar-ctx (second cedar-result)]
              (if-not (specs/context-invariant-validator cedar-ctx)
                (do (otel/set-status! span :error "Invalid cedar-ctx")
                    [(errors/error :JANUS_400
                                   {:reason    "invalid-cedar-ctx"
                                    :tenant-id (str tenant-id)})])

                ;; Paso 1b: Enriquecer queries con BatchContext + CrossFilter
                (let [raw-queries (or (:queries ctx) {})]
                  (if (empty? raw-queries)
                    [[:ok {:data [] :channel :empty :tenant-id tenant-id}]]
                    (let [queries (-> raw-queries
                                      (enricher/apply-batch-context (:context ctx))
                                      (enricher/apply-cross-filter (:cross-filter ctx)))
                          merge-groups (or (:merge-groups ctx) [])
                          standalone-q (ms/partition-standalone queries merge-groups)
                          explain?     (:explain-plan? ctx)
                          process-fn   (fn [qk qm]
                                         (process-single-query
                                           qk qm cedar-ctx ast-compiler aegis-engine explain?))]
                      (let [out-ch (a/chan 100)]
                        (a/go
                          (try
                            (let [chs (for [[qk qm] standalone-q]
                                        (a/thread
                                          (try
                                            (let [chunks (process-fn qk qm)]
                                              (doseq [c chunks] (a/>!! out-ch c)))
                                            (catch Exception e
                                              (log/error e "[Janus] Error in standalone query:" qk)
                                              (a/>!! out-ch (errors/error :JANUS_500 {:reason "query-error" :query-key qk}))))))
                                  merge-ch (when (and (seq merge-groups) (not explain?))
                                             (a/thread
                                               (try
                                                 (let [chunks (ms/process-merge-groups merge-groups queries process-fn tenant-id)]
                                                   (doseq [c chunks] (a/>!! out-ch c)))
                                                 (catch Exception e
                                                   (log/error e "[Janus] Error in merge-groups")
                                                   (a/>!! out-ch (errors/error :JANUS_500 {:reason "merge-error"}))))))]
                              (doseq [c chs] (a/<! c))
                              (when merge-ch (a/<! merge-ch)))
                            (finally
                              (a/close! out-ch))))
                        (take-while some? (repeatedly #(a/<!! out-ch)))))))))))

        (catch clojure.lang.ExceptionInfo e
          (let [data (ex-data e)]
            (otel/set-status! span :error (ex-message e))
            (log/error e "[Janus] Pipeline abortado (ExceptionInfo):" (:reason data) "| tenant:" tenant-id "| data:" data)
            [(errors/error (or (:code data) :JANUS_400)
                           (merge {:reason    (or (:reason data) "pipeline-error")
                                   :tenant-id (str tenant-id)}
                                  (dissoc data :code)))]))

        (catch Exception e
          (otel/set-status! span :error (ex-message e))
          (log/error e "[Janus] Error inesperado en Read Path (Exception) | tenant:" tenant-id)
          [(errors/error :JANUS_400
                         {:reason    "internal-error"
                          :tenant-id (str tenant-id)
                          :detail    (ex-message e)})])))))

;; 
;; INTEGRANT - :janus/query
;; 

(defmethod ig/init-key :janus/query
  [_ {:keys [cedar-authorizer ast-compiler aegis-transmuter]}]
  (log/info "  -> [Janus] Cerebro Read Path activo | ast-compiler:"
            (boolean ast-compiler)
            "| aegis:" (boolean aegis-transmuter))
  (fn [ctx]
    (run-query-pipeline ctx cedar-authorizer ast-compiler aegis-transmuter)))

(defmethod ig/halt-key! :janus/query [_ _]
  (log/info "  <- [Janus] Cerebro liberado"))

;; 
;; INTEGRANT - Unary Read Path wrappers (Discovery, Explore, Match)
;; 

(defn wrap-unary-read-path
  "Envuelve un handler unario del Read Path con gate Zero-Trust (Cedar)."
  [action-name handler cedar-authorizer]
  (fn [ctx]
    (otel/with-span [(str "janus.zt/" action-name) {:kind :server}]
      (let [span (otel/current-span)
            tenant-id (or (:tenant-id ctx)
                          (-> ctx :requests first :tenant-id))]
        (try
          (validate-tenant! tenant-id)
          (let [cedar-result (cedar-authorize cedar-authorizer (assoc ctx :tenant-id tenant-id))]
            (if (= :error (first cedar-result))
              (do (log/warn "[Janus] Zero-Trust denegado:" action-name "| tenant:" tenant-id)
                  (errors/error :JANUS_403
                                 {:reason    "access-denied"
                                  :action    action-name
                                  :tenant-id (str tenant-id)}))
              ;; Paso 7 (unary): normaliza el contrato de salida antes de devolver
              (let [result (handler ctx)]
                (if (and (sequential? result) (keyword? (first result)))
                  (normalizer/normalize-unary result)
                  (first (mapv normalizer/normalize-unary result))))))
          (catch clojure.lang.ExceptionInfo e
            (let [data (ex-data e)]
              (otel/set-status! span :error (ex-message e))
              (log/warn "[Janus] Pipeline abortado:" (:reason data) "| tenant:" tenant-id)
              (errors/error (or (:code data) :JANUS_400)
                             (merge {:reason    (or (:reason data) "pipeline-error")
                                     :tenant-id (str tenant-id)}
                                    (dissoc data :code)))))
          (catch Exception e
            (otel/set-status! span :error (ex-message e))
            (log/error e "[Janus] Error inesperado en" action-name "| tenant:" tenant-id)
            (errors/error :JANUS_500
                           {:reason    "internal-error"
                            :tenant-id (str tenant-id)})))))))

(defmethod ig/init-key :janus/discovery
  [_ {:keys [cedar-authorizer codice-registry]}]
  (wrap-unary-read-path "Discovery" (:discovery codice-registry) cedar-authorizer))

(defmethod ig/init-key :janus/explore
  [_ {:keys [cedar-authorizer codice-registry]}]
  (wrap-unary-read-path "Explore" (:explore codice-registry) cedar-authorizer))

(defmethod ig/init-key :janus/rule-matcher
  [_ {:keys [cedar-authorizer rule-matcher]}]
  (wrap-unary-read-path "MatchRoutingRulesBatch" rule-matcher cedar-authorizer))

(defmethod ig/halt-key! :janus/discovery [_ _])
(defmethod ig/halt-key! :janus/explore [_ _])
(defmethod ig/halt-key! :janus/rule-matcher [_ _])
