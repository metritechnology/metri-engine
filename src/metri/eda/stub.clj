;; [PORTED_TO_RUST: src/eda/outbox.rs]
;; NO MODIFICAR ESTE ARCHIVO.
;; La fuente de verdad para esta lógica ahora reside en Rust.
(ns metri.eda.stub
  "Stub provisional para EDA Rule Matcher (FASE 03)."
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :eda/rule-matcher [_ _]
  (println "  -> [EDA STUB] Rule Matcher stub activo")
  (fn [_ctx]
    [:ok {:responses []}]))

(defmethod ig/halt-key! :eda/rule-matcher [_ _]
  nil)
