(ns metri.janus-router.stubs.generator-stub
  "Stubs de codice-generator-fn para tests de OLTPChannel (Janus).
   Permiten testear el pipeline sin un Datahike real ni sequence_registry.")

(defn identity-generator-fn
  "Stub: retorna el payload sin modificar.
   Permite testear OLTPChannel en aislamiento del generador del Códice."
  [_db-conn _schema _tenant-id payload]
  [:ok payload])

(defn predictable-generator-fn
  "Stub con payload pre-enriquecido determinista.
   Permite testear el pipeline completo con valores conocidos.
   Ejemplo: (predictable-generator-fn {:work_order_number \"WO-TEST-0001\"})"
  [enrichments]
  (fn [_db-conn _schema _tenant-id payload]
    [:ok (merge payload enrichments)]))

(defn failing-generator-fn
  "Stub que siempre retorna [:error {:code :JNS_SEQ_001}].
   Permite testear el path de error del OLTPChannel ante fallo del generador."
  [error-map]
  (fn [_db-conn _schema _tenant-id _payload]
    [:error (merge {:code :JNS_SEQ_001 :stage :codice} error-map)]))
