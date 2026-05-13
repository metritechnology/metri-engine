(ns metri.aegis.datalog.time-frame
  "TimeFrameContext → cláusulas Datalog de rango :meta/created_at.
   SRP: adaptador de tiempo para Datahike — sin I/O, sin estado.

   Delega a metri.temporal.adapters/to-datalog-clauses para la conversión
   epoch-s → epoch-ms (Datahike) y generación de cláusulas.

   Cláusulas generadas (cuando aplica):
     [?e :meta/created_at ?tsN]
     [(>= ?tsN start-ms)]   ← solo si start-ts no nil
     [(<= ?tsN end-ms)]     ← solo si end-ts no nil

   ALL_TIME / nil time-frame → retorna [] (sin filtro temporal)"
  (:require [metri.temporal.time-frame :as tf]
            [metri.temporal.adapters :as adapters]))

(defn time-frame->clauses
  "Resuelve TimeFrameContext → vector de cláusulas Datalog para rango :meta/created_at.
   counter: atom compartido con where-node->parts para nombres de var únicos.
   Retorna [] si time-frame es nil o resolve retorna {:start-ts nil :end-ts nil}."
  [time-frame counter & [ts-field]]
  (let [field (or ts-field :meta/created_at)]
    (when time-frame
      (let [time-range (tf/resolve-time-frame time-frame)]
        (or (adapters/to-datalog-clauses time-range field counter)
            [])))))

