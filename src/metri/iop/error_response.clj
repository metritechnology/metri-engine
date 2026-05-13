(ns metri.iop.error-response
  "Construye el DTO forense (Módulo IV) para escalar errores del dominio hacia la capa gRPC.
   Asegura la correlación OTel y sanitiza la PII basada en el Códice schema."
  (:require [steffan-westcott.clj-otel.api.trace.span :as span]
            [metri.domain.errors :as errors]))

(defn- sanitize-context
  "Filtra llaves marcadas como :sensitive true en el Códice schema,
   reemplazando su valor con '[REDACTED]'."
  [context schema]
  (if (or (nil? context) (nil? schema))
    context
    (let [sensitive-keys (->> (:attributes schema)
                              (filter :sensitive)
                              (map (comp keyword :name))
                              set)]
      (reduce (fn [ctx k]
                (if (contains? sensitive-keys k)
                  (assoc ctx k "[REDACTED]")
                  ctx))
              context (keys context)))))

(defn build-error-dto
  "Toma la mónada [:error error-map], extrae el TraceID actual de OTel
   y retorna el DTO estructurado para la respuesta JSON/gRPC."
  [error-map ctx schema]
  (let [current-span (span/get-span)
        trace-id     (or (:trace-id (span/span-context current-span)) "00000000000000000000000000000000")
        span-id      (or (:span-id (span/span-context current-span)) "0000000000000000")
        ;; :code debe existir si pasó por metri.domain.errors, pero prevemos un default
        raw-code     (:code error-map :UNKNOWN_ERROR)
        cat-entry    (errors/lookup raw-code)]
    {:status "error"
     :error  {:code           (name raw-code)
              :description    (:detail error-map)
              :trace_id       trace-id
              :span_id        span-id
              :correlation_id (str "REQ-" (subs trace-id 0 (min (count trace-id) 8)))
              :tenant_id      (:tenant-id ctx)
              :user_id        (:user-id ctx)
              :timestamp      (System/currentTimeMillis)
              :stage          (name (:stage error-map :unknown))
              :retryable      (boolean (:retryable? cat-entry))
              :context        (sanitize-context (:context error-map) schema)}}))
