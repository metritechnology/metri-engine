(ns validate-traceability
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(def allowed-status #{"exacto" "parcial" "faltante"})

(defn project-root []
  (.getCanonicalFile (io/file ".")))

(defn read-matrix []
  (slurp (io/file (project-root) "docs/architecture/contract-traceability-matrix.md")))

(defn parse-rows [text]
  (->> (str/split-lines text)
       (filter #(and (str/starts-with? % "| ")
                     (not (str/starts-with? % "| ---"))))))

(defn split-row [row]
  (->> (str/split (str/replace row #"^\||\|$" "") #"\|")
       (map str/trim)))

(defn docs-exist? [docs]
  (let [clean (-> docs (str/replace "`" "") str/trim)]
    (or (empty? clean)
        (= clean "-")
        (every? #(-> (io/file (project-root) (str/trim %)) .exists)
                (str/split clean #",")))))

(defn validate-row [row]
  (let [parts (split-row row)]
    (cond
      (< (count parts) 6) [(str "Malformed row: " row)]
      :else
      (let [[capability proto-path edn-path docs estado _notes] (take 6 parts)
            failures (cond-> []
                       (str/blank? capability) (conj (str "Empty capability in row: " row))
                       (not (contains? allowed-status estado)) (conj (str "Invalid estado '" estado "' in row: " row))
                       (not (docs-exist? docs)) (conj (str "Docs path not found for row: " row))
                       (str/blank? proto-path) (conj (str "Empty proto_path in row: " row))
                       (str/blank? edn-path) (conj (str "Empty edn_path in row: " row)))]
        failures))))

(defn -main []
  (let [text (read-matrix)
        rows (parse-rows text)]
    (cond
      (< (count rows) 2)
      (do (println "Traceability matrix has no data rows") (System/exit 1))

      (not-every? #(str/includes? (str/lower-case (first rows)) %) ["capability" "proto_path" "edn_path" "docs" "estado" "notes"])
      (do (println "Missing required columns in matrix header") (System/exit 1))

      :else
      (let [failures (mapcat validate-row (rest rows))
            soft-checks [#"MetriService\.Query" #"AnalyticsRequest\.select_tree" #":metri\.spec/ast-definition"]
            soft-failure (some #(when-not (re-find % text) (str "missing key mapping: " %)) soft-checks)]
        (cond
          (seq failures)
          (do
            (println "Traceability validation FAILED")
            (doseq [f failures] (println " -" f))
            (System/exit 1))

          soft-failure
          (do
            (println "Traceability validation FAILED -" soft-failure)
            (System/exit 1))

          :else
          (do
            (println "Traceability validation OK")
            (System/exit 0)))))))
