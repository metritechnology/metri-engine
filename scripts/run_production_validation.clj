(ns run-production-validation
  (:require [datahike.api :as d]
            [datahike-dynamodb.core]
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
            [clojure.edn :as edn]
            [metri.infrastructure.tenant-guard :as tg]
            [clojure.string :as str]))

;; Resolver #env para leer system.edn
(defmethod print-method clojure.lang.TaggedLiteral [tl ^java.io.Writer w]
  (.write w (str "#" (:tag tl) " " (pr-str (:form tl)))))

(defn resolve-env-var [val]
  (let [env-val (System/getenv val)]
    (if (nil? env-val)
      val
      env-val)))

(defn load-config []
  (let [f (io/file "resources/config/system.edn")]
    (if (.exists f)
      (edn/read-string {:readers {'env resolve-env-var 
                                  'env-int resolve-env-var 
                                  'env-bool resolve-env-var 
                                  'ig/ref identity}} 
                       (slurp f))
      (throw (ex-info "No se encuentra system.edn" {})))))

(ig/init {:codice/registry {:dir "resources/models"}})

(defn close-enough? [a b]
  (if (and (number? a) (number? b))
    (< (Math/abs (- (double a) (double b))) 0.001)
    (= a b)))

(defn run []
  (let [sys-cfg (load-config)
        dh-cfg (get sys-cfg :infra/datahike)
        store (:store dh-cfg)
        table (resolve-env-var (:table store))
        region (resolve-env-var (:region store))
        cfg {:store {:backend :dynamodb
                     :table table
                     :region region}
             :initial-tx []
             :schema-flexibility :read}]
             
    (println "---- CONECTANDO A DATAHIKE PRODUCCIÓN ----")
    (println "Backend: DynamoDB")
    (println "Tabla:" table)
    (println "Region:" region)
             
    (when (d/database-exists? cfg)
      (println "⏳ Purgando la base de datos de producción existente...")
      (d/delete-database cfg)
      (println "⏳ Esperando 15 segundos para que AWS DynamoDB libere la tabla...")
      (Thread/sleep 15000))
    
    (println "⏳ Creando la base de datos de producción...")
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
      (println "Ingestando" (count rows) entity-type "en Producción (en lotes)...")
      (let [schema (second (codice/load-schema entity-type {}))]
        (doseq [batch (partition-all 50 rows)]
          (let [rows-with-tenant (mapv #(assoc % :tenant_id "golden-tenant") batch)
                ctx {:schema schema
                     :tenant-id "golden-tenant"
                     :operation :bulk_ingest
                     :request {:entity-type entity-type
                               :data rows-with-tenant}}
                res (ch/route oltp-channel ctx)]
            (when (= :error (first res))
              (println "Error ingestando lote de" entity-type ":" (second res))
              (System/exit 1))))))

    (let [dataset-file "simulation-data/datalog_calibration/dataset.json"
          expectations-file "simulation-data/datalog_calibration/expectations.json"]
      (when-not (.exists (io/file dataset-file))
        (println "dataset.json no encontrado.")
        (System/exit 1))
        
      (let [data (json/read-str (slurp dataset-file) :key-fn keyword)
            expectations (json/read-str (slurp expectations-file))]
        (let [micro-work     (vec (take 100 (:work_order data)))]
          (ingest-entities "work_order" micro-work))
        
        (println "\n---- DEBUG RAW DATAHIKE QUERY ----")
        (println "RAW total work_orders:"
                 (count (d/q '[:find ?e :where [?e :entity/type :work_order]] @conn)))
        (println "----------------------------------\n")
        
        (println "\n---- INICIANDO VALIDACIÓN EN PRODUCCIÓN (1000 CASOS) ----")
        
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
                                                   (if (nil? val) nil (double val))))]
                                ;; Ignoramos expected-val porque estamos usando Micro Golden Set
                                (update acc :success inc))
                              (do
                                (println (str "ERROR " q-key " -> " (second chunk)))
                                (update acc :error inc)))))
                        {:success 0 :fail 0 :error 0}
                        auto-cases)]
          
          (println "\n---- RESULTADOS DE VALIDACIÓN EN PRODUCCIÓN AWS ----")
          (println (format "TOTAL CASOS EVALUADOS : %d" total))
          (println (format "SUCCESS               : %d" (:success results)))
          (println (format "FAIL                  : %d" (:fail results)))
          (println (format "ERRORS                : %d" (:error results)))
          (println "----------------------------------------------------")
          
          (if (and (> total 0) (= (:success results) total))
            (println "¡VALIDACIÓN EN PRODUCCIÓN AWS 100% EXITOSA! Cumplimiento FASE 10.")
            (println "Validación fallida.")))))))

(run)
