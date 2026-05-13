(ns metri.codice.repl
  "Utilidades REPL-only para el Códice.
   NUNCA incluido en el JAR de producción — vive en dev/ (alias :dev de deps.edn).

   Uso desde el REPL:
     (require '[metri.codice.repl :as cr])
     (cr/reload! \"resources/models\")
     (cr/inspect)
     (cr/entity \"asset\")"
  (:require [metri.codice.registry :as registry]
            [metri.codice.api      :as api]
            [taoensso.timbre       :as log]))

;; ── reload! ─────────────────────────────────────────────────────────────────
;; Reconstruye el registry en memoria sin reiniciar la JVM.
;; Útil cuando se edita un JSON de modelo en desarrollo y se quiere probar
;; sin hacer un ig/halt + ig/init completo.
;;
;; Precaución: no usar con cargas en vuelo — el atom se resetea atómicamente
;; pero hay una ventana entre reset y el próximo request entrante.
(defn reload!
  "Reconstruye el registry Códice desde el filesystem sin reiniciar la JVM.
   Solo disponible en REPL dev. Nunca llamar desde código de producción.

   Ejemplo: (reload! \"resources/models\")"
  [models-dir]
  (log/info "REPL[Códice]: reconstruyendo registry desde" models-dir)
  (let [{:keys [registry event-rules-seed]} (registry/build-registry models-dir)]
    (api/init! registry)
    (log/info "REPL[Códice]: registry recargado —"
              (count registry) "entidades,"
              (count event-rules-seed) "event rules (no seeded en REPL)")
    {:entity-count     (count registry)
     :event-rules-seed (count event-rules-seed)}))

;; ── inspect ─────────────────────────────────────────────────────────────────
;; Vista tabular del estado actual del registry en memoria.
(defn inspect
  "Imprime el estado del registry: entidades, engine, y hash SHA-256 corto.
   Ejemplo: (inspect)"
  []
  (let [reg @#'api/registry]
    (println (format "\nCódice Registry — %d entidades\n" (count reg)))
    (println (format "  %-30s %-6s %-12s" "ENTITY" "ENGINE" "HASH (8)"))
    (println (apply str (repeat 55 "-")))
    (doseq [[entity {:keys [engine hash]}] (sort reg)]
      (println (format "  %-30s %-6s %s"
                       entity
                       (name (or engine :?))
                       (subs (or hash "?") 0 8))))
    (println)))

;; ── entity ──────────────────────────────────────────────────────────────────
;; Inspección detallada de una entidad específica.
(defn entity
  "Imprime el modelo completo de una entidad del registry.
   Ejemplo: (entity \"asset\")"
  [entity-type]
  (let [ctx {:tenant-id "repl" :user-id "dev"}]
    (let [[tag model] (api/entity-model entity-type ctx)]
      (if (= :ok tag)
        (clojure.pprint/pprint model)
        (println "Entidad no encontrada:" entity-type)))))

;; ── validate ─────────────────────────────────────────────────────────────────
;; Valida un payload contra el schema de una entidad — útil para debugging.
(defn validate
  "Valida un payload EDN contra el schema de la entidad dada.
   Ejemplo: (validate \"asset\" {:id (random-uuid) :name \"Bomba A\" :status \"ACTIVE\"})"
  [entity-type payload]
  (let [ctx {:tenant-id "repl" :user-id "dev"}]
    (let [[stag schema] (api/load-schema entity-type ctx)]
      (if (= :ok stag)
        (let [[tag result] (api/validate-payload schema payload entity-type ctx)]
          (if (= :ok tag)
            (println "✅ Payload válido")
            (do (println "❌ Payload inválido")
                (clojure.pprint/pprint result))))
        (println "Entidad no encontrada:" entity-type)))))
