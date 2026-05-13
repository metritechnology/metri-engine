(require '[datahike.api :as d])
(require '[taoensso.timbre :as log])

(def ddb-table (System/getenv "DATAHIKE_DDB_TABLE"))
(def region (System/getenv "AWS_REGION"))

(def cfg {:store {:backend :dynamodb
                  :table-name (or ddb-table "metri-datahike-prod")
                  :region (or region "us-east-1")}
          :keep-history? true})

(try
  (when-not (d/database-exists? cfg)
    (println "Database does not exist!"))
  (let [conn (d/connect cfg)
        db (d/db conn)]
    (println "DB schema keys:" (take 10 (keys (:schema db))))
    (let [assets (d/q '[:find (pull ?e [*])
                        :where [?e :entity/type :asset]]
                      db)]
      (println "Assets found:" (count assets))
      (when (seq assets)
        (println "First asset:" (first (first assets))))))
  (catch Exception e
    (println "Error:" (.getMessage e))))
