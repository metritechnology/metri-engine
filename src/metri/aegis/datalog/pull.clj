;; [PORTED_TO_RUST: src/eav/reader/pull.rs]
;; NO MODIFICAR ESTE ARCHIVO — la fuente de verdad ahora reside en Rust.
(ns metri.aegis.datalog.pull
  "Pull pattern builder: convierte el :select del AST IR → Datahike pull spec.
   SRP: mapeo de proyección — sin I/O, sin estado.

   Reglas de conversión:
     nil | ['*]   → '[*]          (wildcard — todos los atributos)
     keyword      → keyword       (atributo escalar directo)
     map          → map           (relación anidada: {:entity/attr [:sub/attr ...]})
     string       → keyword       (normaliza strings a keywords)")

(defn select->pull-pattern
  "Convierte el :select del AST IR → pull pattern de Datahike.

   Ejemplos:
     nil                                    → '[*]
     [:user/id :user/email]                 → [:user/id :user/email]
     [{:work-order/assigned-to [:user/id]}] → [{:work-order/assigned-to [:user/id]}]"
  [select]
  (if (or (nil? select) (= select '[*]) (= select ['*]))
    '[*]
    (mapv (fn [item]
            (cond
              (map? item)     item
              (keyword? item) item
              (string? item)  (keyword item)
              :else           item))
          select)))

(defn merge-pull-additions
  "Añade pull-additions (e.g. reverse-refs para has_children) al pull pattern base.
   Preserva '[*] añadiendo reverse-refs al final."
  [base-pull additions]
  (if (empty? additions)
    base-pull
    (vec (concat base-pull additions))))
