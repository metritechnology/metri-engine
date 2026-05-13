(ns scratch-test
  (:require [metri.aegis.datalog.compiler :as c]))

(let [ast-ir {:entity "asset"
              :metrics [{:attribute "asset/budget"}]
              :dimensions [{:attribute :asset/status}]
              :order-by [{:attribute "asset/name"}]}]
  (println (#'c/infer-required-fields ast-ir :meta/created_at)))
