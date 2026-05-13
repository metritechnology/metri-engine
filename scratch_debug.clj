(require '[integrant.core :as ig])
(require '[metri.bootstrap :as bootstrap])
(require '[metri.janus.core :as janus])

(bootstrap/run-fail-fast!)
(def sys (ig/init (clojure.edn/read-string {:readers metri.config.readers/all} (slurp "resources/config/system.dev.edn"))))

(let [cedar-ctx {:tenant-id "golden-tenant"}
      queries {"asset_table" {:tenant-id "golden-tenant"
                              :entity "asset"
                              :output-cast :TABLE
                              :viz "table"}}
      ctx {:tenant-id "golden-tenant"
           :queries queries
           :context {}
           :explain-plan? true}
      pipeline (:janus/query sys)
      res (pipeline ctx)]
  (println "RESULT:")
  (clojure.pprint/pprint res))
