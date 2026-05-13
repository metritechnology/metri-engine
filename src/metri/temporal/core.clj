(ns metri.temporal.core
  "Primitivas temporales canónicas de Metri Engine.
   SSOT para toda aritmética de fechas — sin I/O, sin estado, sin dependencias cruzadas.

   Responsabilidades:
     - Constantes de escala (ms ↔ s) documentadas
     - shift-by-calendar  → shifts tz-aware con java.time real (bisiesto-safe)
     - truncate-to-unit   → date_trunc en Clojure (compartido OLTP + referencia OLAP)
     - parse-athena-ts    → parser robusto de timestamps Athena → epoch-segundos
     - ms->s / s->ms      → conversiones explícitas con nombre semántico

   Invariantes:
     - TODOS los valores de tiempo en esta capa son epoch-SEGUNDOS (Long) salvo que
       se indique explícitamente con el sufijo -ms.
     - CUSTOM_RANGE del proto llega en epoch-MILISEGUNDOS (int64). Usar ms->s antes de procesar."
  (:require [clojure.string :as str])
  (:import [java.time Instant LocalDate LocalDateTime ZonedDateTime ZoneId ZoneOffset DayOfWeek]
           [java.time.format DateTimeFormatter DateTimeParseException]
           [java.time.temporal ChronoUnit TemporalAdjusters]))

;; ── Constantes de Escala ──────────────────────────────────────────────────────

(def ^:const MS-PER-SECOND
  "Factor de conversión: milisegundos → segundos.
   Datahike almacena timestamps como epoch-ms (Long).
   El proto TimeFrameContext.start_ts / end_ts son epoch-ms.
   Toda la lógica interna de Metri Engine opera en epoch-s."
  1000)

(def ^:const SMART-HISTORY-DAYS
  "Ventana histórica por defecto para la estrategia SMART (anomaly detection).
   El CTE SMART calcula AVG + STDDEV_SAMP sobre los últimos N días antes del período actual."
  90)

;; ── Conversiones ms ↔ s ───────────────────────────────────────────────────────

(defn ms->s
  "Convierte epoch-milisegundos → epoch-segundos (Long).
   Uso: CUSTOM_RANGE del proto, timestamps de Datahike."
  [ms]
  (when ms (quot (long ms) MS-PER-SECOND)))

(defn s->ms
  "Convierte epoch-segundos → epoch-milisegundos (Long).
   Uso: cláusulas Datalog para Datahike."
  [s]
  (when s (* (long s) MS-PER-SECOND)))

;; ── Helpers java.time públicos ───────────────────────────────────────────────
;; Expuestos para uso en temporal.time-frame y temporal.adapters sin duplicación.

(defn ep ^long [^Instant i] (.getEpochSecond i))
(defn zone [tz] (ZoneId/of (or tz "UTC")))
(defn as-zdt [^Instant i tz] (ZonedDateTime/ofInstant i (zone tz)))
(defn day0 [^ZonedDateTime z] (.truncatedTo z ChronoUnit/DAYS))

;; ── shift-by-calendar ─────────────────────────────────────────────────────────

(defn shift-by-calendar
  "Desplaza epoch-segundos por `amount` unidades de `unit` en la timezone `tz`.
   Usa java.time real → correcto para años bisiestos, meses de diferente longitud y DST.

   `unit` acepta: :minute :hour :day :week :month :quarter :year
   `amount` puede ser negativo (pasado) o positivo (futuro).

   Retorna epoch-segundos (Long)."
  [^long epoch-secs amount unit tz]
  (let [n (long amount)]
    (-> (Instant/ofEpochSecond epoch-secs)
        (as-zdt tz)
        (cond->
          (= unit :minute)  (.plusMinutes n)
          (= unit :hour)    (.plusHours n)
          (= unit :day)     (.plusDays n)
          (= unit :week)    (.plusWeeks n)
          (= unit :month)   (.plusMonths n)
          (= unit :quarter) (.plusMonths (* n 3))
          (= unit :year)    (.plusYears n))
        .toInstant
        ep)))

;; ── truncate-to-unit ──────────────────────────────────────────────────────────

(defn truncate-to-unit
  "Trunca epoch-segundos al inicio del período dado, respetando la timezone.
   Equivalente a Athena date_trunc() pero ejecutado en la JVM.

   Usado en:
     - OLTP executor (bucketing in-memory de TIMESERIES)
     - Referencia canónica para validar el SQL generado en OLAP

   `unit` acepta: :minute :hour :day :week :month :quarter :year
   Retorna epoch-segundos (Long). Retorna `epoch-secs` sin modificar si `unit` no se reconoce."
  [^long epoch-secs unit tz]
  (case unit
    ;; Truncados aritméticos sin java.time (UTC-safe para minutos/horas)
    :minute (* (quot epoch-secs 60) 60)
    :hour   (* (quot epoch-secs 3600) 3600)
    ;; Truncados de calendario — requieren ZonedDateTime para DST-correctness
    (let [zdt (-> (Instant/ofEpochSecond epoch-secs) (as-zdt tz))]
      (case unit
        :day
        (-> zdt day0 .toInstant ep)

        :week
        (-> zdt
            (.with (TemporalAdjusters/previousOrSame DayOfWeek/MONDAY))
            day0 .toInstant ep)

        :month
        (-> zdt (.withDayOfMonth 1) day0 .toInstant ep)

        :quarter
        (let [qm (inc (* 3 (quot (dec (.getMonthValue zdt)) 3)))]
          (-> zdt (.withMonth qm) (.withDayOfMonth 1) day0 .toInstant ep))

        :year
        (-> zdt (.withDayOfYear 1) day0 .toInstant ep)

        ;; Unit desconocido → sin truncado (safe fallback)
        epoch-secs))))

;; ── parse-athena-ts ───────────────────────────────────────────────────────────

(def ^:private athena-formatters
  "Patrones de timestamp que Athena puede retornar, en orden de probabilidad.
   Athena 3 (Trino) usa ISO con espacio, sin 'T'. Puede incluir ' UTC' al final."
  [(DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSS")
   (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SS")
   (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.S")
   (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss")
   (DateTimeFormatter/ofPattern "yyyy-MM-dd")])

(defn parse-athena-ts
  "Parsea un valor de timestamp proveniente de Athena → epoch-segundos (Long).

   Soporta:
     - Número (Double/Long) → conversión directa (to_unixtime retorna double)
     - '2026-03-04 00:00:00.000'         → LocalDateTime UTC → epoch-s
     - '2026-03-04 00:00:00.000 UTC'     → strip timezone suffix → mismo parser
     - '2026-03-04'                       → LocalDate UTC start-of-day → epoch-s
     - epoch como string '1772582400'    → Long/parseLong

   Retorna nil si no puede parsear (NO retorna el string original — falla explícita)."
  [v]
  (cond
    ;; Ya es número — from_unixtime retorna double; date_trunc retorna timestamp (string)
    (instance? Long v)   v
    (number? v)          (long v)
    ;; String → intentar parsear en orden de especificidad
    (string? v)
    (let [s (str/trim (str/replace v #"\s+UTC$" ""))]
      (or
       ;; Intento 1: epoch numérico como string
       (try (Long/parseLong s)   (catch NumberFormatException _ nil))
       (try (long (Double/parseDouble s)) (catch NumberFormatException _ nil))
       ;; Intento 2: formatos LocalDateTime (sin timezone — asume UTC)
       (reduce (fn [_ ^DateTimeFormatter fmt]
                 (try
                   (reduced (.toEpochSecond (LocalDateTime/parse s fmt) ZoneOffset/UTC))
                   (catch DateTimeParseException _ nil)))
               nil
               (drop-last athena-formatters)) ; drop-last excluye "yyyy-MM-dd"
       ;; Intento 3: LocalDate → inicio del día UTC
       (try
         (.toEpochSecond (.atStartOfDay (LocalDate/parse s
                           (last athena-formatters)))
                         ZoneOffset/UTC)
         (catch Exception _ nil))))
    :else nil))

;; ── Helpers de período de comparación ────────────────────────────────────────

(defn shift-period
  "Desplaza el par {:start-ts :end-ts} (epoch-s) hacia el pasado/futuro
   por `amount` unidades de `unit`, respetando la timezone `tz`.

   Usa shift-by-calendar → bisiesto-safe, DST-aware.

   Retorna {:prev-start Long :prev-end Long}."
  [{:keys [start-ts end-ts]} amount unit tz]
  {:prev-start (shift-by-calendar (or start-ts 0) (- (long amount)) unit tz)
   :prev-end   (shift-by-calendar (or end-ts (ep (Instant/now))) (- (long amount)) unit tz)})

(defn duration-seconds
  "Calcula la duración en segundos entre dos epoch-segundos."
  [start-ts end-ts]
  (- (or end-ts (ep (Instant/now)))
     (or start-ts 0)))
