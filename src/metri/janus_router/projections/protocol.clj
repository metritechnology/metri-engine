(ns metri.janus-router.projections.protocol
  "Protocolo de proyecciones ACID para OLTPChannel.
   Cada proyección es una implementación registrable en system.edn
   sin modificar OLTPChannel ni build-tx-data (OCP).")

(defprotocol IProjectionBuilder
  (applicable? [this schema]
    "Retorna true si esta proyección aplica al schema dado.
     Puro — sin I/O. Solo lee el schema.")
  (build [this schema payload parent-ulid]
    "Construye uno o más mapas de hechos Datahike para esta proyección.
     Retorna un mapa o un vector de mapas.
     Nunca lanza — si hay error, retorna [:error ...]."))
