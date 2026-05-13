(ns metri.temporal.adapters
  "Adaptadores temporales: TimeRange → cláusulas engine-específicas.
   SRP: traducir {:start-ts :end-ts} epoch-s a la sintaxis de cada motor.

   Adaptadores disponibles:
     to-datalog-clauses  → cláusulas Datahike (convierte a ms con s->ms)
     to-honey-clause     → HoneySQL BETWEEN para Athena/SQL
     to-bucket-fn        → fn de bucketing Clojure para OLTP TIMESERIES"
  (:require [metri.temporal.core :as t]))

;; ── OLTP: Datahike Datalog ────────────────────────────────────────────────────

(defn to-datalog-clauses
  "TimeRange × ts-field × counter → cláusulas Datalog para Datahike.

   IMPORTANTE: Datahike almacena timestamps como epoch-MILISEGUNDOS.
   La constante MS-PER-SECOND (1000) convierte epoch-s → epoch-ms.

   Retorna [] si time-range es nil o ambos límites son nil.

   Parámetros:
     time-range  {:start-ts Long :end-ts Long} (epoch-s)
     ts-field    keyword — ej. :_created_at, :inventory-movement/reading_ts
     counter     atom compartido con where-node->parts para vars únicos (?ts1, ?ts2, ...)"
  [time-range ts-field counter]
  (let [field (or ts-field :created_at)]
    (when time-range
      (let [{:keys [start-ts end-ts]} time-range]
        (when (or start-ts end-ts)
          (let [ts-sym (symbol (str "?ts" (swap! counter inc)))]
            (cond-> [['?e field ts-sym]]
              start-ts (conj [(list '>= ts-sym (t/s->ms start-ts))])
              end-ts   (conj [(list '<= ts-sym (t/s->ms end-ts))]))))))))

;; ── OLAP: HoneySQL Athena ─────────────────────────────────────────────────────

(defn to-honey-clause
  "TimeRange × col-kw → HoneySQL WHERE clause (BETWEEN implícito con >= y <=).

   Si solo hay start-ts → [:>= col start-ts]
   Si solo hay end-ts   → [:<= col end-ts]
   Si ambos             → [:and [:>= ...] [:<= ...]]
   Si nil / ambos nil   → nil (sin cláusula temporal)

   Los valores se pasan directamente como epoch-segundos (Long).
   Athena compara timestamps numéricos directamente — el campo `timestamp`
   en Iceberg es epoch-ms en el schema físico, pero los literales SQL son epoch-s
   tras el from_unixtime() en el WHERE."
  [time-range col-kw]
  (when time-range
    (let [{:keys [start-ts end-ts]} time-range
          col (or col-kw :created_at)]
      (cond
        (and start-ts end-ts)
        [:and [:>= col start-ts] [:<= col end-ts]]

        start-ts
        [:>= col start-ts]

        end-ts
        [:<= col end-ts]

        :else nil))))

;; ── OLTP: Bucket fn para TIMESERIES in-memory ────────────────────────────────

(defn to-bucket-fn
  "DimensionDefinition × tz → fn: epoch-segundos → epoch-segundos-truncado.

   Genera una función de bucketing para TIMESERIES in-memory en OLTP,
   usando temporal.core/truncate-to-unit como implementación canónica.

   La función retornada acepta un valor epoch (puede ser ms si viene de Datahike)
   y normaliza a segundos antes de truncar.

   `interval` acepta: 'minute' 'hour' 'day' 'week' 'month' 'quarter' 'year'"
  [{:keys [interval]} tz]
  (let [unit (keyword (or interval "day"))
        tz   (or tz "UTC")]
    (fn [epoch-val]
      (when epoch-val
        (let [n (long epoch-val)
              ;; Normalizar: si > 1e11 → está en ms → convertir a s
              epoch-s (if (> n 100000000000) (quot n 1000) n)]
          (t/truncate-to-unit epoch-s unit tz))))))
