(ns metri.janus-router.partition-test
  "Tests unitarios de parti.clj — motor de particionamiento OLAP S3/Hive.
   Funciones puras — cero I/O, cero stubs de infraestructura.
   9 tests cubriendo todas las estrategias y casos borde de seguridad."
  (:require [clojure.test :refer [deftest is testing]]
            [metri.janus-router.partition :as partition])
  (:import [java.time ZonedDateTime ZoneId Instant]))

;; ─── Helper: timestamp para una fecha fija ──────────────────────────────

(defn ts-for
  "Retorna un epoch-millis para la fecha UTC dada (año, mes, día, hora)."
  ([yyyy mm dd] (ts-for yyyy mm dd 0))
  ([yyyy mm dd hh]
   (.toEpochMilli
    (.toInstant
     (ZonedDateTime/of yyyy mm dd hh 0 0 0
                       (ZoneId/of "UTC"))))))

(def ts-2026-04-25 (ts-for 2026 4 25))
(def ts-2026-04-25T14 (ts-for 2026 4 25 14))

;; ═══════════════════════════════════════════════════════════════════════════
;; pre-evaluate-date-strategy
;; ═══════════════════════════════════════════════════════════════════════════

(deftest pre-evaluate-yyyy-mm-dd
  "PART-01 — Estrategia YYYY-MM-DD genera prefix Hive correcto."
  (let [result (partition/pre-evaluate-date-strategy "YYYY-MM-DD" ts-2026-04-25)]
    (is (= "year=2026/month=04/day=25" result))))

(deftest pre-evaluate-yyyy-mm-dd-with-hour
  "PART-02 — Estrategia YYYY-MM-DD/HH incluye la hora UTC correcta."
  (let [result (partition/pre-evaluate-date-strategy "YYYY-MM-DD/HH" ts-2026-04-25T14)]
    (is (= "year=2026/month=04/day=25/hour=14" result))))

(deftest pre-evaluate-yyyy-mm
  "PART-03 — Estrategia YYYY-MM genera solo año y mes."
  (let [result (partition/pre-evaluate-date-strategy "YYYY-MM" ts-2026-04-25)]
    (is (= "year=2026/month=04" result))))

(deftest pre-evaluate-yyyy
  "PART-04 — Estrategia YYYY genera solo el año."
  (let [result (partition/pre-evaluate-date-strategy "YYYY" ts-2026-04-25)]
    (is (= "year=2026" result))))

(deftest pre-evaluate-nil-strategy-defaults-to-yyyy-mm-dd
  "PART-05 — Estrategia nil hace fallback a YYYY-MM-DD."
  (let [result (partition/pre-evaluate-date-strategy nil ts-2026-04-25)]
    (is (= "year=2026/month=04/day=25" result))))

;; ═══════════════════════════════════════════════════════════════════════════
;; build-dynamic-path
;; ═══════════════════════════════════════════════════════════════════════════

(deftest build-dynamic-path-replaces-attribute
  "PART-06 — Atributo dinámico {region} se reemplaza con el valor del record."
  (let [pre-evaluated "{region}"
        record        {:region "us-east"}
        result        (partition/build-dynamic-path pre-evaluated record)]
    (is (= "region=us-east" result))))

(deftest build-dynamic-path-sanitizes-path-traversal
  "PART-07 — Path traversal (../) y caracteres peligrosos son sanitizados."
  (let [pre-evaluated "{region}"
        record        {:region "us/../east"}
        result        (partition/build-dynamic-path pre-evaluated record)]
    ;; No debe contener / ni . en el valor
    (is (not (.contains result "/")) "No debe haber / sin sanitizar")
    (is (not (.contains result "..")) "No debe haber path traversal")))

(deftest build-dynamic-path-missing-key-yields-unknown
  "PART-08 — Clave ausente en el record produce 'UNKNOWN' en el path."
  (let [pre-evaluated "{region}"
        record        {}
        result        (partition/build-dynamic-path pre-evaluated record)]
    (is (= "region=UNKNOWN" result))))

(deftest build-dynamic-path-combined-date-and-attr
  "PART-09 — Estrategia combinada de fecha + atributo dinámico."
  (let [;; Pre-evaluate la fecha primero
        date-part  (partition/pre-evaluate-date-strategy "YYYY-MM-DD" ts-2026-04-25)
        ;; Luego combinar con un atributo dinámico en la estrategia
        strategy   (str date-part "/{region}")
        record     {:region "us-east"}
        result     (partition/build-dynamic-path strategy record)]
    (is (.startsWith result "year=2026/month=04/day=25"))
    (is (.contains result "region=us-east"))))
