(ns validate-contracts
  (:require [clojure.java.io :as io]))

(defn project-root []
  (.getCanonicalFile (io/file ".")))

(defn slurp-file [relative-path]
  (slurp (io/file (project-root) relative-path)))

(defn require-pattern! [errors content pattern label]
  (if (re-find pattern content)
    errors
    (conj errors (str "[missing] " label ": " pattern))))

(defn -main []
  (let [proto (slurp-file "metri.proto")
        edn (slurp-file "docs/architecture/janus-aegis-contract-v3.edn")
        errors (-> []
                   (require-pattern! proto #"rpc\s+Query\s*\(\s*QueryRequest\s*\)\s*returns\s*\(\s*stream\s+QueryResponse\s*\)" "MetriService.Query streaming")
                   (require-pattern! proto #"google\.protobuf\.Struct\s+select_tree\s*=\s*16\s*;" "AnalyticsRequest.select_tree")
                   (require-pattern! proto #"int32\s+limit\s*=\s*7\s*;" "AnalyticsRequest.limit")
                   (require-pattern! proto #"repeated\s+SortDefinition\s+sort\s*=\s*9\s*;" "AnalyticsRequest.sort")
                   (require-pattern! proto #"enum\s+OutputCastType\s*\{" "OutputCastType enum")
                   (require-pattern! proto #"OutputCastType\s+output_cast\s*=\s*17\s*;" "AnalyticsRequest.output_cast")
                   (require-pattern! proto #"message\s+Status\s*\{[\s\S]*string\s+error_code\s*=\s*2\s*;" "Status.error_code")
                   (require-pattern! edn #":metri\.spec/ast-definition" "EDN ast-definition")
                   (require-pattern! edn #":metri\.spec/orchestration-meta" "EDN orchestration-meta")
                   (require-pattern! edn #":metri\.spec/query-request" "EDN query-request")
                   (require-pattern! edn #":select\s+\{:optional\s+true\}\s+:metri\.spec/eql-query" "EDN ast-definition.select"))]
    (if (seq errors)
      (do
        (println "Contract conformance FAILED")
        (doseq [e errors] (println " -" e))
        (System/exit 1))
      (do
        (println "Contract conformance OK")
        (System/exit 0)))))
