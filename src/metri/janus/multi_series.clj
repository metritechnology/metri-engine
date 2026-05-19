;; [PORTED_TO_RUST: src/janus/multi_series.rs]
;; NO MODIFICAR ESTE ARCHIVO.
;; La fuente de verdad para esta lógica ahora reside en Rust.
(ns metri.janus.multi-series
  "Motor de fusión MultiSeriesGroup — Full Outer Join asintótico.
   SRP: realiza y fusiona resultados de sub-queries agrupados bajo un group-id.

   Principios:
     • DIP: recibe process-query-fn como parámetro — no depende de core.clj.
     • Sin conocimiento de Cedar, AST compiler o Aegis.
     • La unión de filas es un Full Outer Join por hash de la row completa."
  (:require [taoensso.timbre :as log]
            [metri.otel.spans :as otel]))

;; ═══════════════════════════════════════════════════════════════════════════
;; FULL OUTER JOIN — fusión de filas multi-query
;; ═══════════════════════════════════════════════════════════════════════════

(defn- outer-join-data
  "Full Outer Join de filas de múltiples sub-queries.

   Para cada combinación única de valores de dimensión (representada como hash
   de la row completa), combina las métricas de todos los sub-queries en una
   única fila mediante merge de mapas.

   Filas sin contraparte en otros sub-queries reciben nil implícito para las
   métricas ausentes (Clojure map merge semántica)."
  [rows-per-qk]
  (if (= 1 (count rows-per-qk))
    (val (first rows-per-qk))
    (vals
      (reduce-kv
        (fn [idx _qk rows]
          (reduce (fn [i row]
                    (update i (hash row) #(merge (or % {}) row)))
                  idx rows))
        {}
        rows-per-qk))))

(defn- realize-query-data
  "Realiza los chunks de un sub-query y extrae las filas de datos.
   Descarta chunks de error — el Outer Join omite silenciosamente sub-queries fallidos."
  [chunks]
  (into []
        (comp (filter #(= :ok (first %)))
              (mapcat #(or (:data (second %)) [])))
        chunks))

;; ═══════════════════════════════════════════════════════════════════════════
;; PARTICIÓN — standalone vs agrupadas
;; ═══════════════════════════════════════════════════════════════════════════

(defn partition-standalone
  "Retorna el subconjunto de queries que NO pertenecen a ningún merge-group.
   Si merge-groups está vacío, retorna queries completo (O(1))."
  [queries merge-groups]
  (if (empty? merge-groups)
    queries
    (let [merged-qks (into #{} (mapcat :query-keys merge-groups))]
      (reduce-kv
        (fn [m qk qm]
          (if (contains? merged-qks (name qk)) m (assoc m qk qm)))
        {}
        queries))))

;; ═══════════════════════════════════════════════════════════════════════════
;; API PÚBLICA
;; ═══════════════════════════════════════════════════════════════════════════

(defn process-merge-groups
  "Ejecuta y fusiona los MultiSeriesGroup mediante Full Outer Join.

   Para cada grupo:
     1. Ejecuta cada sub-query vía process-query-fn (DIP — fn inyectada).
     2. Realiza los chunks → extrae filas de datos.
     3. Full Outer Join de filas de todos los sub-queries del grupo.
     4. Emite un chunk [:ok {:data joined :query-key (keyword group-id) ...}].

   process-query-fn: (fn [query-key query-map]) → seq de chunks
   tenant-id:        decorado en el chunk resultante para el cliente."
  [merge-groups queries process-query-fn tenant-id]
  (mapcat
    (fn [{:keys [group-id query-keys override-viz]}]
      (otel/with-span [(str "janus.merge-group." group-id) {:kind :internal}]
        (let [rows-per-qk
              (reduce
                (fn [acc qk-str]
                  (let [qk (keyword qk-str)
                        qm (get queries qk)]
                    (if (nil? qm)
                      (do (log/warn "[Janus] MultiSeriesGroup: query-key no encontrado"
                                    qk-str "| group:" group-id)
                          acc)
                      (assoc acc qk
                             (realize-query-data (process-query-fn qk qm))))))
                {}
                (or query-keys []))
              joined-data (outer-join-data rows-per-qk)]
          [[:ok {:data         joined-data
                 :query-key    (keyword group-id)
                 :group-id     group-id
                 :override-viz override-viz
                 :merged-keys  (mapv keyword (or query-keys []))
                 :tenant-id    tenant-id}]])))
    merge-groups))
