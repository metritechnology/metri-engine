(ns metri.janus-router.partition
  "Dominio puro para evaluación de estrategias de particionamiento S3/Hive.
   Totalmente agnóstico de infraestructura (Kinesis/Kafka) y del canal."
  (:require [clojure.string :as str])
  (:import [java.time Instant ZoneId]))

(defn pre-evaluate-date-strategy
  "Evalúa la parte estática de la estrategia (fechas UTC) una sola vez por request batch."
  [strategy timestamp]
  (let [base-strategy (or strategy "YYYY-MM-DD")
        instant (Instant/ofEpochMilli timestamp)
        zdt     (.atZone instant (ZoneId/of "UTC"))
        yyyy    (format "%04d" (.getYear zdt))
        mm      (format "%02d" (.getMonthValue zdt))
        dd      (format "%02d" (.getDayOfMonth zdt))
        hh      (format "%02d" (.getHour zdt))]
    (-> base-strategy
        (str/replace "YYYY-MM-DD/HH" (str "year=" yyyy "/month=" mm "/day=" dd "/hour=" hh))
        (str/replace "YYYY-MM-DD" (str "year=" yyyy "/month=" mm "/day=" dd))
        (str/replace "YYYY-MM" (str "year=" yyyy "/month=" mm))
        (str/replace "YYYY" (str "year=" yyyy)))))

(defn build-dynamic-path
  "Evalúa solo los atributos dinámicos del registro sobre la estrategia ya pre-calculada en tiempo."
  [date-evaluated-strategy record]
  (str/replace date-evaluated-strategy #"\{([^}]+)\}"
               (fn [[_ k]]
                 (let [raw-val (str (get record (keyword k) "UNKNOWN"))
                       ;; SANITIZACIÓN ESTRICTA: Previene Path Traversal (../) e inyección S3
                       safe-val (str/replace raw-val #"[^a-zA-Z0-9\-_]" "_")]
                   (str k "=" safe-val)))))
