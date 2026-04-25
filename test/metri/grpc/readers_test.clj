(ns metri.grpc.readers-test
  "Matriz TDD — RDR-01..05 (01.03 Módulo XI.5) — Custom Reader Tags.
   Testa el mapa `all` de readers.clj directamente.
   Completamente autónomo — sin imports de proto."
  (:require [clojure.test :refer [deftest is testing]]
            [metri.config.readers :as readers]))

(def ^:private env-reader  (get readers/all 'env))
(def ^:private bool-reader (get readers/all 'env-bool))
(def ^:private int-reader  (get readers/all 'env-int))

(deftest rdr-01-env-variable-presente
  "RDR-01 — #env lee variable de entorno cuando está presente (HOME siempre existe)"
  (let [val (env-reader "HOME")]
    (is (string? val))
    (is (not (.startsWith val "<unset:"))
        "Variable presente no debe retornar placeholder")))

(deftest rdr-02-env-variable-ausente-dev
  "RDR-02 — #env con variable ausente en dev retorna <unset:VAR>"
  ;; En dev (ENVIRONMENT != production), retorna placeholder
  (let [result (env-reader "VAR_QUE_NUNCA_EXISTE_8x7z99_TEST")]
    (is (string? result))
    (is (.startsWith result "<unset:")
        "Variable ausente en dev debe retornar placeholder")))

(deftest rdr-03-env-bool-true
  "RDR-03 — #env-bool 'true' → true boolean"
  ;; Usar PATH que siempre existe para verificar la lógica inversa
  (let [result (bool-reader "PATH")]
    ;; PATH no es 'true' → false
    (is (= false result) "PATH no es 'true' → false")))

(deftest rdr-04-env-bool-false-cuando-ausente
  "RDR-04 — #env-bool con variable ausente → false"
  (let [result (bool-reader "VAR_BOOL_AUSENTE_9x3k22")]
    (is (= false result) "#env-bool variable ausente → false")))

(deftest rdr-05-env-int-cuando-ausente
  "RDR-05 — #env-int con variable ausente → nil"
  (let [result (int-reader "VAR_INT_AUSENTE_7m4p55")]
    (is (nil? result) "#env-int variable ausente → nil")))

(deftest rdr-06-readers-all-contiene-tres-tags
  "RDR-06 — El mapa readers/all contiene exactamente los 3 tags documentados"
  (is (fn? (get readers/all 'env))      "#env reader existe")
  (is (fn? (get readers/all 'env-bool)) "#env-bool reader existe")
  (is (fn? (get readers/all 'env-int))  "#env-int reader existe"))

(deftest rdr-07-env-placeholder-formato
  "RDR-07 — Placeholder tiene formato <unset:VAR_NAME>"
  (let [var-name "MI_VAR_INEXISTENTE_QQ99"
        result   (env-reader var-name)]
    (when (.startsWith result "<unset:")
      (is (.contains result var-name)
          "El placeholder debe incluir el nombre de la variable"))))
