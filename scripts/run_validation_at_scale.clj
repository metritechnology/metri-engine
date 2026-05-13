(ns run-validation-at-scale
  (:require [datahike.api :as d]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [metri.janus-router.channels.oltp :as oltp]
            [metri.janus-router.channels.protocol :as ch]
            [metri.codice.api :as codice]
            [metri.janus.core :as jcore]
            [metri.aegis.core :as aegis]
            [metri.janus.ast-compiler :as ast]
            [integrant.core :as ig]
            [metri.codice.registry :as r]
            [metri.infrastructure.tenant-guard :as tg]
            [clojure.string :as str]))

(ig/init {:codice/registry {:dir "resources/models"}})

(def cfg {:store {:backend :mem :id "golden-scale-db"}
          :initial-tx []
          :schema-flexibility :read})

(when (d/database-exists? cfg)
  (d/delete-database cfg))
(d/create-database cfg)
(def conn (d/connect cfg))
(tg/ensure-tenant-schema! conn)

(def aegis-transmuter (aegis/->AegisTransmuter conn nil nil))
(def ast-compiler (ast/->JanusASTCompiler nil))
(def cedar-auth (fn [ctx] [:ok (assoc ctx
                                      :user-id "local"
                                      :roles #{}
                                      :domain-boundaries {}
                                      :is-super-master true
                                      :cross-tenant-scope "NONE")]))

(def oltp-channel (oltp/->OLTPChannel conn []))

(defn ingest-entities [entity-type rows]
  (let [schema (second (codice/load-schema entity-type {}))
        rows-with-tenant (mapv #(assoc % :tenant_id "golden-tenant") rows)
        ctx {:schema schema
             :tenant-id "golden-tenant"
             :operation :bulk_ingest
             :request {:entity-type entity-type
                       :data rows-with-tenant}}]
    (println "Ingestando" (count rows) entity-type "...")
    (let [res (ch/route oltp-channel ctx)]
      (when (= :error (first res))
        (println "Error ingestando" entity-type ":" (second res))
        (System/exit 1)))))

(defn close-enough? [a b]
  (if (and (number? a) (number? b))
    (< (Math/abs (- (double a) (double b))) 0.001)
    (= a b)))

(defn run []
  (let [dataset-file "simulation-data/datalog_calibration/dataset.json"
        expectations-file "simulation-data/datalog_calibration/expectations.json"]
    (when-not (.exists (io/file dataset-file))
      (println "dataset.json no encontrado.")
      (System/exit 1))
      
    (let [data (json/read-str (slurp dataset-file) :key-fn keyword)
          expectations (json/read-str (slurp expectations-file))]
      (ingest-entities "location" (:location data))
      (ingest-entities "asset" (:asset data))
      (ingest-entities "work_order" (:work_order data))
      
      (println "\n---- DEBUG RAW DATAHIKE QUERY ----")
      (println "RAW total work_orders:"
               (count (d/q '[:find ?e :where [?e :entity/type :work_order]] @conn)))
      (println "----------------------------------\n")
      
      (println "\n---- INICIANDO VALIDACIÓN A ESCALA (1000 CASOS) ----")
      
      (let [auto-cases (filter (fn [[k _]] (str/starts-with? k "Q_AUTO_")) expectations)
            total (count auto-cases)
            results (reduce
                      (fn [acc [q-key spec]]
                        (let [ast-query {:entity (get spec "entity")
                                         :tenant-id "golden-tenant"
                                         :metrics [{:aggregation (keyword (get spec "agg"))
                                                    :attribute (get spec "field")}]
                                         :filters [{:criteria {:field "status"
                                                               :op-ref :EQ
                                                               :value {:string_val (get spec "filter_status")}}}]}
                              res (jcore/run-query-pipeline
                                    {:tenant-id "golden-tenant"
                                     :queries {q-key ast-query}}
                                    cedar-auth ast-compiler aegis-transmuter)
                              chunk (first res)]
                          (if (= :ok (first chunk))
                            (let [data-list (:data (second chunk))
                                  actual-val (if (empty? data-list)
                                               nil
                                               (let [val (first (vals (first data-list)))]
                                                 (if (nil? val) nil (double val))))
                                  expected-val (get spec "expected_value")]
                              (if (close-enough? actual-val expected-val)
                                (update acc :success inc)
                                (do
                                  (println (str "FAIL " q-key " -> Expected: " expected-val ", Actual: " actual-val))
                                  (update acc :fail inc))))
                            (do
                              (println (str "ERROR " q-key " -> " (second chunk)))
                              (update acc :error inc)))))
                      {:success 0 :fail 0 :error 0}
                      auto-cases)]
        
        (println "\n---- RESULTADOS DE VALIDACIÓN ----")
        (println (format "TOTAL CASOS EVALUADOS : %d" total))
        (println (format "SUCCESS               : %d" (:success results)))
        (println (format "FAIL                  : %d" (:fail results)))
        (println (format "ERRORS                : %d" (:error results)))
        (println "----------------------------------")
        
        (if (and (> total 0) (= (:success results) total))
          (println "¡VALIDACIÓN A ESCALA 100% EXITOSA! Cumplimiento FASE 10.")
          (println "Validación fallida."))))))

(run)
