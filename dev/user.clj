(ns user
  "Namespace REPL — solo se carga en dev.
   Provee funciones para iniciar/detener/resetear el sistema."
  (:require [integrant.core :as ig]
            [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [integrant.repl.state :refer [system config]]
            [clojure.java.io :as io]
            [metri.bootstrap :as bootstrap]
            [metri.config.readers]))

;; Preparar el config
(integrant.repl/set-prep!
  (fn []
    (bootstrap/run-fail-fast!)
    (-> (io/resource "config/system.dev.edn")
        slurp
        ig/read-string)))

;; Usando el REPL:
(comment
  ;; Iniciar el sistema completo
  (go)

  ;; Detener el sistema
  (halt)

  ;; Resetear (halt + reload namespaces + go)
  (reset)

  ;; Acceder a componentes individuales
  (:infra/datahike system)
  (:iop/pipeline system)

  ;; Ejecutar un pipeline manualmente
  (let [iop (:iop/pipeline system)]
    (iop {:request {:entity-type "work_order"
                    :operation   :create
                    :payload     {:title "Test WO"}}}))
  )
