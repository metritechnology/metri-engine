(ns metri.aegis.time-frame
  "Re-export de metri.temporal.time-frame para backward compatibility.
   La lógica de resolución temporal vive en el namespace SSOT temporal.time-frame.
   Este namespace se mantiene para no romper imports existentes en la codebase."
  (:require [metri.temporal.time-frame :as tf]))

(def resolve-time-frame
  "TimeFrameContext (29 tipos) → {:start-ts :end-ts} epoch segundos.
   Ver metri.temporal.time-frame/resolve"
  tf/resolve-time-frame)
