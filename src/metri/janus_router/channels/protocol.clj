(ns metri.janus-router.channels.protocol
  "Protocolo único que todo canal de escritura de Janus debe satisfacer.
   CONTRATO RAILWAY OBLIGATORIO:
     Toda implementación DEBE retornar:
       [:ok  {:ulid str :channel kw}]              — escritura exitosa
       [:error {:stage :janus :code kw :detail str}] — fallo controlado
   PROHIBIDO lanzar excepciones.
   Stubs canónicos: InMemoryOLTPStub, InMemoryOLAPStub, NoOpWriteChannel.")

(defprotocol IJanusWriteChannel
  (route [this ctx]
    "ctx :: {:tenant-id str :user-id str :entity-type str
              :schema map :operation kw :request map}
     ret :: [:ok {:ulid str :channel kw}] | [:error {:stage :janus ...}]"))
