(ns metri.codice.base36-test
  "Matriz TDD — B36-01..03 (FASE 02 MÓDULO IV)
   Tests del generador estocástico Base36. Función pura — cero I/O."
  (:require [clojure.test :refer [deftest is testing]]
            [metri.codice.base36 :as b36]))

;; ─── B36-01: longitud y prefijo correctos ────────────────────────────────

(deftest b36-01-generates-correct-length
  "B36-01 — generate produce string de longitud prefix + length"
  (testing "asset tag: prefijo 'A-' + 7 chars = 9 chars total"
    (let [result (b36/generate "A-" 7)]
      (is (string? result)        "resultado es string")
      (is (= 9 (count result))    "A- (2) + 7 = 9 caracteres")
      (is (.startsWith result "A-") "comienza con el prefijo")))

  (testing "location tag: prefijo 'L-' + 6 chars = 8 chars total"
    (let [result (b36/generate "L-" 6)]
      (is (= 8 (count result))    "L- (2) + 6 = 8 caracteres")
      (is (.startsWith result "L-") "comienza con L-")))

  (testing "solo caracteres Base36 en la parte aleatoria"
    (let [result (b36/generate "" 10)
          valid? (re-matches #"[0-9A-Z]+" result)]
      (is (some? valid?) "todos los caracteres son Base36"))))

;; ─── B36-02: unicidad en 1000 llamadas ───────────────────────────────────

(deftest b36-02-unique-across-calls
  "B36-02 — 1000 llamadas producen 1000 valores únicos (P(colisión) < 1/36^7)"
  (let [values (repeatedly 1000 #(b36/generate "A-" 7))
        unique  (set values)]
    (is (= 1000 (count unique))
        "1000 valores generados son únicos — sin colisiones")))

;; ─── B36-03: prefijo vacío ────────────────────────────────────────────────

(deftest b36-03-empty-prefix
  "B36-03 — prefijo vacío produce solo N caracteres Base36"
  (let [result (b36/generate "" 5)]
    (is (= 5 (count result))         "exactamente 5 caracteres")
    (is (re-matches #"[0-9A-Z]{5}" result) "solo Base36")))
