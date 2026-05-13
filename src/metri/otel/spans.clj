(ns metri.otel.spans
  "Wrapper de clj-otel para el Metri Engine.
   Provee macros y funciones para crear spans y agregar atributos."
  (:require [steffan-westcott.clj-otel.api.trace.span :as span]
            [steffan-westcott.clj-otel.sdk.otel-sdk :as sdk]))

(defmacro with-span
  "Ejecuta el body dentro de un span OTel.
   opts debe ser un vector como [\"span-name\" {:kind :internal}]."
  [opts & body]
  `(span/with-span! ~opts ~@body))

(defn current-span
  "Retorna el span actual en el thread local."
  []
  (span/get-span))

(defn set-attributes!
  "Añade atributos al span actual."
  [_span attrs]
  (span/add-span-data! {:attributes attrs}))

(defn set-status!
  "Marca el estado del span OTel.
   status = :ok | :error
   Cuando :error, acepta un mensaje opcional de descripción."
  ([_span status]
   (span/add-span-data! {:status status}))
  ([_span status description]
   (span/add-span-data! {:status      status
                         :description description})))

(defn trace-id
  "Extrae el trace-id del span actual como string hexadecimal.
   Retorna nil si no hay span activo (modo NoOp / tests sin OTel)."
  [_span]
  (try
    (some-> (span/get-span)
            span/get-span-context
            .getTraceId)
    (catch Exception _ nil)))

(defn init!
  "Alias para compatibilidad con bootstrap."
  []
  ;; Inicializamos NoOp SDK tempranamente para FASE 01 (bootstrap).
  ;; Integrant luego lo puede reconfigurar en infra/tracer.
  (try (sdk/init-otel-sdk!) (catch Exception _))
  :ok)
