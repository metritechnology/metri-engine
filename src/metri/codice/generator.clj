(ns metri.codice.generator
  (:require [taoensso.timbre :as log]
            [integrant.core  :as ig]
            [metri.codice.base36 :as base36]
            [metri.codice.sequence :as sequence]
            [metri.otel.spans :as otel]))

(defn- auto-generate-attrs
  [schema]
  (->> (get schema :attributes [])
       (filter :auto_generate)
       (map (fn [attr]
              [(keyword (:name attr))
               (assoc (:auto_generate attr)
                      :name (:name attr))]))))

(defn- find-scope-field
  [schema]
  (let [attrs (get schema :attributes [])]
    (->> attrs
         (filter #(or (:is_sequence_scope %) (:is_sequence_scope_via %)))
         first)))

(defn inject!
  [db-conn tenant-guard schema tenant-id payload]
  (otel/with-span ["codice.autogen.inject" {:kind :internal}]
    (let [span        (otel/current-span)
          entity-type (name (get schema :entity "unknown"))]

      (otel/set-attributes! span
        {"entity.type" entity-type
         "tenant.id"   (str tenant-id)})

      (let [attrs      (auto-generate-attrs schema)
            scope-field (find-scope-field schema)]
        (if (empty? attrs)
          (do (otel/set-status! span :ok) [:ok payload])
          (let [res (reduce
                      (fn [enriched [attr-kw attr-config]]
                        (if (and (vector? enriched) (= :error (first enriched)))
                          enriched
                          (let [result
                                (case (keyword (:strategy attr-config))
                                  :stochastic_base36
                                  [:ok (base36/generate (:prefix attr-config "")
                                                        (long (:length attr-config 7)))]

                                  :sequential
                                  (sequence/next! db-conn tenant-guard attr-config
                                                  scope-field tenant-id enriched)

                                  (do (log/warn "Unknown auto_generate strategy"
                                                {:attr attr-kw :strategy (:strategy attr-config)})
                                      [:ok nil]))]
                            (case (first result)
                              :ok    (if (nil? (second result))
                                       enriched
                                       (assoc enriched attr-kw (second result)))
                              :error (do (otel/set-status! span :error "auto_generate failed")
                                         result)))))
                      payload
                      attrs)]
            (if (and (vector? res) (= :error (first res)))
              res
              (do (otel/set-status! span :ok)
                  [:ok res]))))))))

(defmethod ig/init-key :codice/generator-inject
  [_ {:keys [tenant-guard]}]
  (log/info "Códice: generator-inject inicializado")
  (fn [db-conn schema tenant-id payload]
    (inject! db-conn tenant-guard schema tenant-id payload)))

(defmethod ig/halt-key! :codice/generator-inject [_ _]
  (log/info "Códice: generator-inject cerrado"))
