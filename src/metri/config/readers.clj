(ns metri.config.readers
  "Custom reader tags para system.edn / system.dev.edn.
   Uso: (ig/read-string (slurp r) {:readers (readers/all)})
   Tags disponibles:
     #env      \"VAR\" → string
     #env-bool \"VAR\" → boolean
     #env-int  \"VAR\" → integer (nil si ausente)"
  (:require [integrant.core :as ig]))

(def all
  "Mapa de custom reader tags para pasar a ig/read-string."
  {'env      (fn [var-name]
               (let [v (System/getenv (str var-name))]
                 (when (and (nil? v) (= "production" (System/getenv "ENVIRONMENT")))
                   (throw (ex-info (str "Required env var missing: " var-name)
                                   {:var var-name})))
                 (or v (str "<unset:" var-name ">"))))
   'env-bool (fn [var-name]
               (= "true" (System/getenv (str var-name))))
   'env-int  (fn [var-name]
               (some-> (System/getenv (str var-name)) Integer/parseInt))})
