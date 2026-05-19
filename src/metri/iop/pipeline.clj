;; [PORTED_TO_RUST: src/iop/pipeline.rs]
;; NO MODIFICAR — fuente de verdad en Rust
(ns metri.iop.pipeline
  "Motor Railway del IOP — composición pura de pasos.
   chain/run: vector de fns [ctx → [:ok ctx'] | [:error body]]
   Cortocircuita en el primer [:error]. Sin I/O, sin estado.")

(defn run
  "Ejecuta pasos secuencialmente. Cortocircuita en el primer [:error].
   Retorna [:ok ctx-final] | [:error body]."
  [steps ctx]
  (reduce
    (fn [[tag current-ctx] step-fn]
      (if (= :ok tag)
        (step-fn current-ctx)
        [:error current-ctx]))       ; ya en error, propagar sin ejecutar más pasos
    [:ok ctx]
    steps))
