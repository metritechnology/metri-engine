(ns metri.aegis.sql.time-frame
  "Re-export de metri.aegis.time-frame para backward compatibility.
   La lógica de resolución temporal vive en el namespace compartido aegis.time-frame."
  (:require [metri.aegis.time-frame :as tf]))

(def resolve-time-frame
  "TimeFrameContext (28 tipos) → {:start-ts :end-ts} epoch segundos.
   Ver metri.aegis.time-frame/resolve-time-frame"
  tf/resolve-time-frame)
