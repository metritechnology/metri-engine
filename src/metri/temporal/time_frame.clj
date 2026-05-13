(ns metri.temporal.time-frame
  "Resuelve TimeFrameContext (29 tipos del contrato §4) → {:start-ts :end-ts} epoch segundos.
   Namespace SSOT — reemplaza metri.aegis.time-frame.

   CORRECCIONES vs la implementación anterior:
     1. CUSTOM_RANGE: el proto envía epoch-MILISEGUNDOS → se convierte a segundos con ms->s
     2. Todos los shifts usan shift-by-calendar (java.time) — bisiesto-safe, DST-aware
     3. ALL_TIME retorna {:start-ts nil :end-ts nil} de forma explícita
     4. Tipo no reconocido retorna nil (loggeable upstream)"
  (:require [metri.temporal.core :as t])
  (:import [java.time Instant ZonedDateTime DayOfWeek]
           [java.time.temporal TemporalAdjusters ChronoUnit]))

;; ── Helpers de inicio de período ─────────────────────────────────────────────

(defn- today-start [now tz]
  (-> now (t/as-zdt tz) (t/day0) .toInstant))

(defn- week-start [now tz]
  (-> now (t/as-zdt tz)
      (.with (TemporalAdjusters/previousOrSame DayOfWeek/MONDAY))
      (t/day0) .toInstant))

(defn- month-start [now tz]
  (-> now (t/as-zdt tz) (.withDayOfMonth 1) (t/day0) .toInstant))

(defn- quarter-start [now tz]
  (let [z  (t/as-zdt now tz)
        qm (inc (* 3 (quot (dec (.getMonthValue z)) 3)))]
    (-> z (.withMonth qm) (.withDayOfMonth 1) (t/day0) .toInstant)))

(defn- year-start [now tz]
  (-> now (t/as-zdt tz) (.withDayOfYear 1) (t/day0) .toInstant))

;; ── API pública ───────────────────────────────────────────────────────────────

(defn resolve-time-frame
  "TimeFrameContext → {:start-ts Long :end-ts Long} (epoch segundos UTC).
   Retorna nil para TimeFrameContext nil.
   Retorna {:start-ts nil :end-ts nil} para ALL_TIME (sin restricción temporal).

   CUSTOM_RANGE: el proto define start_ts / end_ts como epoch-MILISEGUNDOS (int64).
   Esta función convierte automáticamente con ms->s.

   29 tipos soportados (28 relativos + CUSTOM_RANGE)."
  [tf]
  (when tf
    (let [tz  (or (:timezone tf) "UTC")
          now (Instant/now)
          n   (long (or (:n-value tf) 1))

          ;; Snapshots — todos usan el mismo 'now' para consistencia
          ep  t/ep
          td  #(today-start now tz)
          wk  #(week-start now tz)
          mo  #(month-start now tz)
          qt  #(quarter-start now tz)
          yr  #(year-start now tz)

          ;; Shifts usando temporal.core/shift-by-calendar (java.time real)
          +d  (fn [^Instant i d] (t/shift-by-calendar (ep i) d :day tz))
          +m  (fn [^Instant i m] (t/shift-by-calendar (ep i) m :month tz))
          +y  (fn [^Instant i y] (t/shift-by-calendar (ep i) y :year tz))
          +w  (fn [^Instant i w] (t/shift-by-calendar (ep i) w :week tz))]

      (case (:type tf)
        ;; ── CUSTOM_RANGE: proto envía epoch-ms → convertir a epoch-s ──────
        :CUSTOM_RANGE
        {:start-ts (t/ms->s (:start-ts tf))
         :end-ts   (t/ms->s (:end-ts tf))}

        ;; ── Diario / Horario ───────────────────────────────────────────────
        :TODAY         {:start-ts (ep (td))       :end-ts (+d (td) 1)}
        :YESTERDAY     {:start-ts (+d (td) -1)    :end-ts (ep (td))}
        :TOMORROW      {:start-ts (+d (td) 1)     :end-ts (+d (td) 2)}
        :LAST_N_MINUTES {:start-ts (ep (.minusSeconds now (* n 60))) :end-ts (ep now)}
        :LAST_N_HOURS  {:start-ts (ep (.minus now n ChronoUnit/HOURS)) :end-ts (ep now)}
        :LAST_N_DAYS   {:start-ts (+d (td) (- n)) :end-ts (ep now)}
        :NEXT_N_DAYS   {:start-ts (ep now)         :end-ts (+d now n)}

        ;; ── Semanal ────────────────────────────────────────────────────────
        :THIS_WEEK     {:start-ts (ep (wk))        :end-ts (ep now)}
        :LAST_WEEK     {:start-ts (+w (wk) -1)     :end-ts (ep (wk))}
        :NEXT_WEEK     {:start-ts (+w (wk) 1)      :end-ts (+w (wk) 2)}
        :LAST_N_WEEKS  {:start-ts (+w (wk) (- n))  :end-ts (ep now)}
        :NEXT_N_WEEKS  {:start-ts (ep now)          :end-ts (+w now n)}
        :WEEK_TO_DATE  {:start-ts (ep (wk))         :end-ts (ep now)}

        ;; ── Mensual ────────────────────────────────────────────────────────
        :THIS_MONTH    {:start-ts (ep (mo))         :end-ts (ep now)}
        :LAST_MONTH    {:start-ts (+m (mo) -1)      :end-ts (ep (mo))}
        :NEXT_MONTH    {:start-ts (+m (mo) 1)       :end-ts (+m (mo) 2)}
        :LAST_N_MONTHS {:start-ts (+m (mo) (- n))   :end-ts (ep now)}
        :NEXT_N_MONTHS {:start-ts (ep now)           :end-ts (+m now n)}
        :MONTH_TO_DATE {:start-ts (ep (mo))          :end-ts (ep now)}

        ;; ── Trimestral ─────────────────────────────────────────────────────
        :THIS_QUARTER    {:start-ts (ep (qt))          :end-ts (ep now)}
        :LAST_QUARTER    {:start-ts (+m (qt) -3)       :end-ts (ep (qt))}
        :LAST_N_QUARTERS {:start-ts (+m (qt) (* -3 n)) :end-ts (ep now)}
        :QUARTER_TO_DATE {:start-ts (ep (qt))           :end-ts (ep now)}

        ;; ── Anual ──────────────────────────────────────────────────────────
        :THIS_YEAR     {:start-ts (ep (yr))         :end-ts (ep now)}
        :LAST_YEAR     {:start-ts (+y (yr) -1)      :end-ts (ep (yr))}
        :LAST_N_YEARS  {:start-ts (+y (yr) (- n))   :end-ts (ep now)}
        :YEAR_TO_DATE  {:start-ts (ep (yr))          :end-ts (ep now)}

        ;; ── Sin filtro temporal ────────────────────────────────────────────
        :ALL_TIME {:start-ts nil :end-ts nil}

        ;; Tipo no reconocido → nil
        nil))))
