(require '[datahike.api :as d])
(require '[datahike-dynamodb.core])

(def cfg {:store {:backend :dynamodb
                  :region "us-east-1"
                  :table "metri-datahike-prod"}
          :keep-history? false
          :schema-flexibility :write
          :allow-unsafe-config true})

(println "Checking if Datahike DB exists...")
(when-not (d/database-exists? cfg)
  (println "Database does not exist! Creating it now...")
  (d/create-database cfg)
  (println "Database created successfully."))

(println "Connecting to Datahike...")
(def conn (d/connect cfg))
(println "Connected! Database state:")
(def db @conn)

(println "Querying for all assets...")
(def assets (d/q '[:find ?e ?t ?tenant
                   :where [?e :entity/type ?t] [?e :tenant/id ?tenant]]
                 db))
(println "Assets found:" (count assets))
(println "First 5:" (take 5 assets))

(println "Querying specifically for golden-tenant assets...")
(def golden-assets (d/q '[:find ?e
                          :where [?e :entity/type :asset] [?e :tenant/id "golden-tenant"]]
                        db))
(println "Golden-tenant assets found:" (count golden-assets))

(println "Validating timestamps presence...")
(def assets-with-ts (d/q '[:find ?e ?ts
                           :where [?e :entity/type :asset] [?e :asset/timestamp ?ts]]
                         db))
(println "Assets with timestamps found:" (count assets-with-ts))
(println "Sample timestamps (epoch ms):")
(doseq [[e ts] (take 5 assets-with-ts)]
  (println (str "Asset " e " -> Timestamp: " ts " (Date: " (java.util.Date. ts) ")")))

(println "Done.")
