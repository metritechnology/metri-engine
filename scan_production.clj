(require '[datahike.api :as d])
(def cfg {:store {:backend :dynamodb
                  :region "us-east-1"
                  :table "metri-datahike-prod"
                  :endpoint (or (System/getenv "DYNAMODB_ENDPOINT") "http://localhost:8000")
                  :access-key "DUMMYIDEXAMPLE"
                  :secret "DUMMYEXAMPLEKEY"}
          :schema-flexibility :write
          :allow-unsafe-config true})

(when-not (d/database-exists? cfg)
  (println "DB does not exist.")
  (System/exit 1))

(println "Connecting to DB...")
(def conn (d/connect cfg))
(def db (d/db conn))

(println "Executing query...")
(def result (d/q '[:find ?e ?type ?tenant ?name
                   :where 
                   [?e :entity/type ?type]
                   [?e :tenant/id ?tenant]
                   [?e :asset/name ?name]]
                 db))
(println "Found" (count result) "assets.")
(doseq [row (take 5 result)]
  (println row))
(System/exit 0)
