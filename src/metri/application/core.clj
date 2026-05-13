(ns metri.application.core
  "Bridge de compatibilidad Lambda para el Metri Engine.
   En modo ECS/local: la entrada real es metri.main/-main.
   En modo Lambda: la entrada real es metri.lambda.handler/-handleRequest.
   Este namespace se mantiene por compatibilidad con el SAM template existente."
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler]))

(defn -handleRequest
  "Stub de compatibilidad — delega al handler real de Lambda dinámicamente
   para evitar problemas de AOT compilation con core.async."
  [_this input-stream output-stream _context]
  (require 'metri.lambda.handler)
  (let [handler-fn (resolve 'metri.lambda.handler/-handleRequest)]
    (handler-fn _this input-stream output-stream _context)))

(defn -main [& _args]
  ;; En producción, el entrypoint real es metri.main/-main
  ;; Aquí solo redirigimos
  (require 'metri.main)
  (apply (resolve 'metri.main/-main) _args))
