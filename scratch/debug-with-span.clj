(ns debug-with-span
  (:require [steffan-westcott.clj-otel.api.trace.span :as span]))

(defn test-span []
  (span/with-span! ["test" {:kind :internal}]
    (map inc [1 2 3])))

(def res (test-span))
(println "type:" (type res))
(println "res:" res)
(println "seq?:" (seq? res))
