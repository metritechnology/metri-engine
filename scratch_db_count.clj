(ns scratch-db-count
  (:require [datahike.api :as d]
            [metri.infrastructure.datahike :as dh]))

(def cfg {:store {:backend :dynamodb
                  :table "metri-datahike-prod"
                  :region "us-east-1"}
          :schema-flexibility :read
          :keep-history? false})

(try
  (let [conn (d/connect cfg)
        db   (d/db conn)]
    (println "Total count:" (d/q '[:find (count ?e) . :where [?e :tenant/id]] db))
    (println "Tenants:" (d/q '[:find ?t :where [_ :tenant/id ?t]] db)))
  (catch Exception e
    (println "Error connecting:" (ex-message e))))
