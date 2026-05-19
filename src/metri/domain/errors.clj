;; [PORTED_TO_RUST: src/domain/errors.rs]
;; NO MODIFICAR ESTE ARCHIVO — la fuente de verdad ahora reside en Rust.
(ns metri.domain.errors
  "Constructor centralizado de mónadas de error Railway-Oriented.
   Valida y empaqueta DTOs usando el error_catalog.edn como Única Fuente de Verdad."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]))

(def ^:private catalog
  (delay
   (if-let [r (io/resource "errors/error_catalog.edn")]
     (let [cat (-> r slurp edn/read-string :catalog)]
       ;; Indexamos las entradas por su :code para lookup O(1)
       (->> (:entries cat)
            (map (juxt :code identity))
            (into {})))
     (do
       (log/error "CRITICAL: No se pudo cargar errors/error_catalog.edn desde recursos.")
       {}))))

(defn lookup
  "Devuelve la definición de catálogo para el código provisto, o nil si no existe."
  [code]
  (get @catalog code))

(defn error
  "Constructor de mónadas de error. Valida que ctx-map contenga
   todas las variables obligatorias (:context-required) del catálogo.
   Retorna la tupla [:error {...}] estandarizada."
  ([code ctx-map]
   (error code ctx-map lookup))
  ([code ctx-map lookup-fn]
   (if-let [entry (lookup-fn code)]
     (let [missing (remove #(contains? ctx-map %) (:context-required entry))]
       (when (seq missing)
         (log/warn "Incomplete error context" {:code code :missing missing}))
       [:error (merge {:code       code
                       :stage      (:stage entry)
                       :detail     (:description entry)
                       :retryable? (:retryable? entry)}
                      ctx-map)])
     (do
       (log/warn "UNKNOWN ERROR CODE used in domain logic!" {:code code})
       [:error (merge {:code       code
                       :stage      :janus
                       :detail     (or (:reason ctx-map) "Error código no registrado en el catálogo maestro.")
                       :retryable? false}
                      ctx-map)]))))
