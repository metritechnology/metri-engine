(require '[metri.bootstrap :as bootstrap]
         '[datahike.api :as d])

(println "Iniciando Datahike...")
(bootstrap/start-datahike!)

(def conn @bootstrap/conn)
(def db (d/db conn))

(println "DB Config:" (:config conn))

(def assets (d/q '[:find ?e ?tenant ?tag
                   :where
                   [?e :entity/type :asset]
                   [?e :tenant/id ?tenant]
                   [(get-else $ ?e :asset/tag "no-tag") ?tag]]
                 db))

(println "Total assets found:" (count assets))
(println "Sample assets:" (take 5 assets))
