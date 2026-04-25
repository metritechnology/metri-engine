(ns metri.codice.malli-test
  "Matriz TDD — ML-01..08 (FASE 02 MÓDULO II)
   Tests unitarios del compilador Malli. Cero I/O — función pura."
  (:require [clojure.test :refer [deftest is testing are use-fixtures]]
            [malli.core   :as m]
            [metri.codice.malli :as cm]))

;; ─── ML-01: atributo string required ─────────────────────────────────────

(deftest ml-01-compile-string-attr
  "ML-01 — type:string required:true → campo obligatorio en schema"
  (let [model  {:attributes [{:name "name" :type "string" :required true}]}
        schema (m/schema (cm/build-malli-schema model))]
    (is (m/validate schema {:name "hello"})   "payload válido pasa")
    (is (not (m/validate schema {}))           "payload sin campo requerido falla")))

;; ─── ML-02: atributo optional ────────────────────────────────────────────

(deftest ml-02-compile-optional-attr
  "ML-02 — required:false → {:optional true} en schema"
  (let [model  {:attributes [{:name "id"   :type "uuid"   :required true}
                              {:name "note" :type "string" :required false}]}
        schema (m/schema (cm/build-malli-schema model))]
    (is (m/validate schema {:id (random-uuid)})
        "payload sin campo opcional es válido")))

;; ─── ML-03: atributo uuid ────────────────────────────────────────────────

(deftest ml-03-compile-uuid-attr
  "ML-03 — type:uuid → :uuid predicate en schema"
  (let [model  {:attributes [{:name "id" :type "uuid" :required true}]}
        schema (m/schema (cm/build-malli-schema model))
        valid-uuid (random-uuid)]
    (is (m/validate schema {:id valid-uuid})     "UUID válido pasa")
    (is (not (m/validate schema {:id "not-a-uuid"})) "string no es UUID")))

;; ─── ML-04: atributo enum con options ────────────────────────────────────

(deftest ml-04-compile-enum-attr
  "ML-04 — type:enum + options:[A,B,C] → [:enum A B C]"
  (let [model  {:attributes [{:name "status" :type "enum" :required true
                               :options ["ACTIVE" "INACTIVE" "DECOMMISSIONED"]}]}
        schema (m/schema (cm/build-malli-schema model))]
    (is (m/validate schema {:status "ACTIVE"})        "valor de enum válido")
    (is (not (m/validate schema {:status "PENDING"})) "valor fuera del enum")))

;; ─── ML-05: atributo cardinality:many ────────────────────────────────────

(deftest ml-05-compile-array-attr
  "ML-05 — cardinality:many + type:reference → [:vector :uuid]"
  (let [model  {:attributes [{:name "assignees" :type "reference"
                               :required false :cardinality "many"}]}
        schema (m/schema (cm/build-malli-schema model))]
    (is (m/validate schema {:assignees [(random-uuid) (random-uuid)]})
        "vector de UUIDs válido")
    (is (m/validate schema {})
        "campo opcional ausente es válido")))

;; ─── ML-06: atributo decimal ─────────────────────────────────────────────

(deftest ml-06-compile-decimal-attr
  "ML-06 — type:decimal → decimal? predicate"
  (let [model  {:attributes [{:name "total_cost" :type "decimal" :required true}]}
        schema (m/schema (cm/build-malli-schema model))]
    (is (m/validate schema {:total_cost 42.5M})         "BigDecimal válido")
    (is (not (m/validate schema {:total_cost "forty"})) "string no es decimal")))

;; ─── ML-07: atributo reference → uuid (FK shape) ─────────────────────────

(deftest ml-07-compile-reference-attr
  "ML-07 — type:reference → :uuid (validación de forma, no de existencia)"
  (let [model  {:attributes [{:name "location_id" :type "reference" :required false}]}
        schema (m/schema (cm/build-malli-schema model))]
    (is (m/validate schema {:location_id (random-uuid)}) "UUID referencia válida")
    (is (m/validate schema {})                            "FK opcional ausente OK")))

;; ─── ML-08: atributo json/map → :map opaque ──────────────────────────────

(deftest ml-08-compile-json-attr
  "ML-08 — type:json → :map opaque (cualquier mapa pasa)"
  (let [model  {:attributes [{:name "metadata" :type "json" :required false}]}
        schema (m/schema (cm/build-malli-schema model))]
    (is (m/validate schema {:metadata {:key "val" :nested {:x 1}}})
        "mapa arbitrario es válido como tipo json")))
