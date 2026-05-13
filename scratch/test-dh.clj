(ns test-dh
  (:require [datahike.api :as d]
            [datahike-dynamodb.core]))

(def cfg {:store {:backend :dynamodb
                  :table "test-table"
                  :region "us-east-1"}
          :keep-alive? true
          :allow-unsafe-config true})

(println "Config is valid?")
