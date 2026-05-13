(ns metri.stubs
  "Stubs para módulos no implementados (fases futuras) de Integrant."
  (:require [integrant.core :as ig]))

;; :iop/pipeline usa derive → :iop/orchestrator en iop/core.clj — NO stub aquí
(defmethod ig/init-key :iop/bulk-pipeline [_ _] :ok)
;; :aegis/pipeline — implementado por metres.janus.core (JanusCerebro) — FASE 05
;; :codice/registry — implementado en codice/registry.clj — NO stub aquí
;; :eda/rule-matcher — implementado en eda/stub.clj — NO stub aquí


(defmethod ig/init-key :infra/dynamodb          [_ _] :ok)
(defmethod ig/init-key :infra/stubs/eventbridge [_ _] :ok)
(defmethod ig/init-key :infra/stubs/athena      [_ _] :ok)
