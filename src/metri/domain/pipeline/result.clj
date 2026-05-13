(ns metri.domain.pipeline.result
  "Railway Pattern — helpers puros para operar sobre resultados [:ok …] | [:error …].
   Sin I/O, sin estado. Usable por cualquier capa sin introducir dependencias.")

(defn ok?
  "Retorna true si el resultado es [:ok …]."
  [result]
  (= :ok (first result)))

(defn error?
  "Retorna true si el resultado es [:error …]."
  [result]
  (= :error (first result)))

(defn unwrap
  "Extrae el body de [:ok body]. Lanza ExceptionInfo si es [:error …]."
  [[tag body :as result]]
  (if (= :ok tag)
    body
    (throw (ex-info "Cannot unwrap an error result" {:result result}))))
