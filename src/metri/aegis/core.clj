(ns metri.aegis.core
  "AegisTransmuter — Motor Analítico Core.
   SRP: enrutar AST IR al canal correcto (OLTP o OLAP) y ejecutar.
   DIP: consume IQueryEngine (Athena/stub) via inyección — sin import directo de infra.
   SOLID-D: solo conoce protocolos, no implementaciones.

   Señal de enrutamiento: presencia de :metrics en AST IR → OLAP (Athena)
                          ausencia de :metrics              → OLTP (Datahike)"
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]
            [metri.otel.spans :as otel]
            [metri.aegis.datalog :as datalog]
            [metri.aegis.sql :as sql]))

;; ─── Constantes ──────────────────────────────────────────────────────────────

(def ^:private default-database "metri_analytics")

;; ═══════════════════════════════════════════════════════════════════════════
;; RECORD — IAegisEngine implementation
;; ═══════════════════════════════════════════════════════════════════════════

(defrecord AegisTransmuter [datahike-conn query-engine database]
  proto/IAegisEngine

  (transmute! [_ ast-ir]
    (otel/with-span ["aegis.transmute" {:kind :internal}]
      (let [span      (otel/current-span)
            tenant-id (or (get-in ast-ir [:where 1 2])  ;; heurística: 2do arg del nodo :=
                          "unknown")
            entity    (or (:entity ast-ir) "unknown")
            schema    (:schema ast-ir)
            engine    (keyword (or (:engine schema) "oltp"))
            is-olap?  (= engine :olap)]

        (otel/set-attributes! span
          {"tenant.id"   tenant-id
           "entity.type" entity
           "engine"      (name engine)})

        (if is-olap?
          ;; ── BULK PATH: Athena ──────────────────────────────────────────
          (do
            (log/info "[Aegis] OLAP path | entity:" entity "| tenant:" tenant-id)
            (otel/set-attributes! span {"aegis.path" "olap"})
            (let [chunks (sql/run-olap-chunks
                           query-engine ast-ir tenant-id
                           (or database default-database)
                           datahike-conn)]
              (doseq [c chunks]
                (when (= :ok (first c))
                  (otel/set-attributes! span {"rows" (count (:data (second c)))})))
              chunks))

          ;; ── FAST PATH: Datahike ────────────────────────────────────────
          (do
            (log/info "[Aegis] OLTP path | entity:" entity "| tenant:" tenant-id)
            (otel/set-attributes! span {"aegis.path" "oltp"})
            (if datahike-conn
              (let [chunks (datalog/run-oltp-chunks
                             datahike-conn ast-ir tenant-id)]
                (doseq [c chunks]
                  (when (= :ok (first c))
                    (otel/set-attributes! span {"rows" (count (:data (second c)))})))
                chunks)
              (do
                (log/warn "[Aegis] OLTP requested pero datahike-conn es nil (modo stub)")
                [[:ok {:data [] :channel :oltp :tenant-id tenant-id :stub? true}]]))))))))

;; ═══════════════════════════════════════════════════════════════════════════
;; INTEGRANT — :aegis/transmuter
;; ═══════════════════════════════════════════════════════════════════════════

(defmethod ig/init-key :aegis/transmuter
  [_ {:keys [datahike query-engine database]}]
  (log/info "  -> [Aegis] Transmuter real activo | OLTP:"
            (boolean (some? datahike))
            "| OLAP:" (boolean (some? query-engine)))
  (->AegisTransmuter
    (when datahike (:conn datahike))
    query-engine
    (or database default-database)))

(defmethod ig/halt-key! :aegis/transmuter [_ _]
  (log/info "  <- [Aegis] Transmuter liberado"))
