(ns scratch-check-where
  (:require [metri.aegis.datalog.where :as w]))

(let [counter (atom 0)]
  (println (w/where-node->parts [:= :tenant/id "golden-tenant"] counter)))
