(ns metri.temporal.comparison
  "Resolución de AnalyticalComparison → períodos de comparación temporal.
   Reemplaza la lógica de shortcut->period y comparison->period en aegis.sql.comparison.

   CORRECCIONES vs la implementación anterior:
     1. Todos los shortcuts usan shift-by-calendar (java.time) — bisiesto-safe
     2. SAME_PERIOD_LAST_YEAR ya no usa 365*86400 fijo
     3. SAME_PERIOD_LAST_QUARTER ya no usa 91*86400 fijo
     4. Timezone propagada en todos los shortcuts
     5. TIME_SHIFT_RELATIVE granularidades usan shift-by-calendar — 'month' es mes real"
  (:require [metri.temporal.core :as t])
  (:import [java.time Instant]))

;; ── Helpers privados ──────────────────────────────────────────────────────────

(defn- ep ^long [^Instant i] (.getEpochSecond i))
(defn- now-s [] (ep (Instant/now)))

;; ── Shortcut resolver ─────────────────────────────────────────────────────────

(defn resolve-shortcut
  "ShiftShortcut keyword + {:start-ts :end-ts} + tz → {:prev-start :prev-end} epoch-s.
   Usa shift-by-calendar (java.time) → correcto para bisiestos y meses reales.
   Retorna nil para shortcuts no reconocidos."
  [shortcut {:keys [start-ts end-ts]} tz]
  (let [cs  (or start-ts 0)
        ce  (or end-ts (now-s))
        dur (- ce cs)
        ;; shift-simétrico: desplaza ambos extremos por la misma cantidad de calendario
        shift-cal (fn [amount unit]
                    {:prev-start (t/shift-by-calendar cs (- amount) unit tz)
                     :prev-end   (t/shift-by-calendar ce (- amount) unit tz)})]
    (case shortcut
      ;; El período previo de exactamente la misma duración (aritmético — correcto)
      :PREVIOUS_PERIOD
      {:prev-start (- cs dur) :prev-end cs}

      ;; Shifts de 1 año — bisiesto-safe vía java.time
      :SAME_PERIOD_LAST_YEAR   (shift-cal 1 :year)
      :SAME_DAY_LAST_YEAR      (shift-cal 1 :year)
      :TODAY_LAST_YEAR         (shift-cal 1 :year)
      :YESTERDAY_LAST_YEAR     {:prev-start (t/shift-by-calendar (- cs 86400) -1 :year tz)
                                :prev-end   (t/shift-by-calendar cs           -1 :year tz)}

      ;; Shifts de 1 trimestre — java.time (91d sería incorrecto)
      :SAME_PERIOD_LAST_QUARTER (shift-cal 1 :quarter)

      ;; Shifts de 1 mes — java.time (30d sería incorrecto para meses cortos)
      :SAME_PERIOD_LAST_MONTH  (shift-cal 1 :month)
      :SAME_DAY_LAST_MONTH     (shift-cal 1 :month)
      :TODAY_LAST_MONTH        (shift-cal 1 :month)
      :YESTERDAY_LAST_MONTH    {:prev-start (t/shift-by-calendar (- cs 86400) -1 :month tz)
                                :prev-end   (t/shift-by-calendar cs           -1 :month tz)}

      ;; Shifts de 1 semana (7 días — aritmético, correcto)
      :SAME_DAY_LAST_WEEK      (shift-cal 1 :week)
      :YESTERDAY_LAST_WEEK     {:prev-start (t/shift-by-calendar (- cs 86400) -1 :week tz)
                                :prev-end   (t/shift-by-calendar cs           -1 :week tz)}

      :SHIFT_SHORTCUT_UNSPECIFIED nil
      nil)))

;; ── Granularidad relativa → unit keyword ──────────────────────────────────────

(defn- gran->unit [gran]
  (case (or gran "day")
    "minute"  :minute
    "hour"    :hour
    "day"     :day
    "week"    :week
    "month"   :month
    "quarter" :quarter
    "year"    :year
    :day))

;; ── API pública ───────────────────────────────────────────────────────────────

(defn resolve-comparison-period
  "AnalyticalComparison × {:start-ts :end-ts} × tz → {:prev-start :prev-end} | nil.
   Retorna nil para BENCHMARK y SMART (no producen ventana temporal — se manejan en el caller).

   Tipos:
     :TIME_SHIFT_RELATIVE  → desplaza por N × granularidad usando shift-by-calendar
     :TIME_SHIFT_SHORTCUT  → shortcuts nombrados con java.time (bisiesto-safe)
     :TIME_SHIFT_ABSOLUTE  → ventana explícita (absolute-start-ts / absolute-end-ts, epoch-s)
     :BENCHMARK            → nil (columna inline, sin CTE)
     :SMART                → nil (CTE histórico, manejado por caller)"
  [comp time-range tz]
  (case (:type comp)
    :TIME_SHIFT_RELATIVE
    (let [unit   (gran->unit (:relative-granularity comp))
          amount (long (or (:relative-amount comp) 1))]
      {:prev-start (t/shift-by-calendar (or (:start-ts time-range) 0) (- amount) unit tz)
       :prev-end   (t/shift-by-calendar (or (:end-ts time-range) (now-s)) (- amount) unit tz)})

    :TIME_SHIFT_SHORTCUT
    (resolve-shortcut (:shortcut comp) time-range tz)

    :TIME_SHIFT_ABSOLUTE
    {:prev-start (:absolute-start-ts comp)
     :prev-end   (:absolute-end-ts comp)}

    :BENCHMARK nil
    :SMART     nil
    :COMPARISON_TYPE_UNSPECIFIED nil
    nil))

;; ── Smart history window ───────────────────────────────────────────────────────

(defn smart-history-window
  "Genera la ventana histórica para la estrategia SMART (anomaly detection).
   Retorna {:start-ts :end-ts} para el período de N días antes del inicio del período actual.
   Por defecto usa SMART-HISTORY-DAYS (90 días)."
  [{:keys [start-ts]} & [history-days]]
  (let [cs  (or start-ts (now-s))
        days (or history-days t/SMART-HISTORY-DAYS)]
    {:start-ts (- cs (* days 86400))
     :end-ts   cs}))
