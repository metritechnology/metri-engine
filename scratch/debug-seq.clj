(ns debug-seq)
(println (seq? (mapcat identity {:a [1 2 3]})))
(println (type (mapcat identity {:a [1 2 3]})))
