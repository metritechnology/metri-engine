(ns metri.aegis.sql.executor
  "Ejecutor Athena: compila SQL + polling IQueryEngine con backoff exponencial.
   SRP: orquestar compile-athena-sql + proto/start-query! + poll -- sin logica SQL.

   Contrato de salida (compatible con metres.janus.normalizer Paso 7):
   Cada chunk [:ok body] incluye:
     :response-type  -> :query-response  (dispatch exacto en normalizer multimethod)
     :channel        -> :olap            (para engine-str en QueryMetadata)
     :data           -> rows (vector de vectores OLAP, ya paginados)
     :columns        -> vector de {:key str} (para VizMeta y RowSet)
     :total          -> count total ANTES de paginar (para Pagination y metadata.total-count)
     :limit          -> limit del AST (para page-size)
     :pagination     -> mapa completo con cursores reales (next-cursor, previous-cursor)
     :start-ts       -> epoch-ms de inicio (para execution-time-ms en normalizer)
     :output-cast    -> keyword de viz cast (:KPI :PIE :TIMESERIES :BUBBLE :TABLE)
     :viz            -> string de viz hint (para infer-viz-type en normalizer)
     :tenant-id      -> string del tenant
     :execution-id   -> ID de la ejecucion Athena (trazabilidad)
     :comparison?    -> boolean
     :benchmarks     -> vector de benchmarks

   Polling strategy:
     Max intentos: 10
     Backoff: 200ms inicial, doble por intento, tope en 5000ms (~51s total max)"
  (:require [taoensso.timbre :as log]
            [clojure.string :as str]
            [metri.aegis.sql.compiler :as compiler]
            [metri.aegis.datalog.ref-resolver :as ref-res]
            [metri.aegis.pagination :as pagination]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]))

;; -- Polling sincrono con backoff exponencial ----------------------------------

(defn- poll-athena-results
  "Polling bloqueante de resultados Athena.
   Reintenta si el estado es RUNNING o QUEUED, falla en error definitivo.
   Retorna [:ok {:columns [...] :rows [...]}] | [:error ...]."
  [query-engine execution-id tenant-id]
  (loop [attempt 1
         wait-ms 200]
    (if (> attempt 10)
      (errors/error :AEG_005
                    {:execution-id execution-id
                     :tenant-id    tenant-id
                     :detail       "Athena polling timeout after 10 attempts"})
      (let [result (proto/get-query-results query-engine execution-id)]
        (if (= :ok (first result))
          result
          (let [detail (str (get-in result [1 :detail]) " " (get-in result [1 :cause]))]
            (if (or (str/includes? detail "RUNNING")
                    (str/includes? (str detail) "QUEUED"))
              (do
                (log/debug "[Aegis OLAP] Athena RUNNING | attempt:" attempt
                           "| wait:" wait-ms "ms | exec:" execution-id)
                (Thread/sleep wait-ms)
                (recur (inc attempt) (min (* wait-ms 2) 5000)))
              result)))))))

;; -- API publica ---------------------------------------------------------------

(defn run-olap-chunks
  "Compila AST IR -> SQL, ejecuta en Athena, retorna vector de chunks.
   datahike-conn (opcional): si se provee y el AST contiene :ref-filter apuntando
   a entidades OLTP, se pre-resuelven los IDs desde Datahike antes de compilar SQL.
   Esto maneja el caso meter_reading.asset_id → asset (OLTP) donde la tabla
   'asset' no existe en el catálogo Iceberg/Athena.

   Chunk de salida:
     [:ok {:response-type :query-response
           :channel       :olap
           :data          rows
           :columns       [{:key str} ...]
           :total         int
           :limit         int
           :start-ts      epoch-ms
           :output-cast   keyword
           :viz           string
           :tenant-id     str
           :execution-id  str
           :comparison?   bool
           :benchmarks    [...]}]
     | [:error ...]"
  ([query-engine ast-ir tenant-id database]
   (run-olap-chunks query-engine ast-ir tenant-id database nil))
  ([query-engine ast-ir tenant-id database datahike-conn]
  (let [start-ts   (System/currentTimeMillis)
        ;; ── Ref-Filter Resolution (híbrido OLAP) ──────────────────────
        ;; Si el :where del AST contiene :ref-filter Y tenemos conn a Datahike,
        ;; pre-resolvemos los IDs desde Datahike y reemplazamos el :ref-filter
        ;; por [:in ref-field [ulid1 ulid2 ...]] antes de compilar el SQL.
        ;; Esto soporta cross-entity OLAP → OLTP (e.g. meter_reading → asset).
        ;; Si el conn no está disponible, deja el :ref-filter intacto para que
        ;; where.clj lo convierta en subquery SQL (OLAP → OLAP nativo).
        resolved-ast-ir
        (if (and datahike-conn
                 (:where ast-ir)
                 (ref-res/contains-ref-filter? (:where ast-ir)))
          (let [db @datahike-conn]
            (log/info "[Aegis OLAP] Pre-resolviendo ref-filters via Datahike...")
            (update ast-ir :where #(ref-res/resolve-ref-filters % db)))
          ast-ir)
        sql-result (compiler/compile-athena-sql resolved-ast-ir database tenant-id)]
    (if (= :error (first sql-result))
      [sql-result]
      (let [{:keys [sql output-cast comparison? benchmarks]} (second sql-result)
            start-result (proto/start-query! query-engine sql database)]
        (if (= :error (first start-result))
          (do
            (log/error "[Aegis OLAP] Athena start-query! fallo. Cause:"
                       (get-in start-result [1 :cause] "unknown"))
            [(errors/error :AEG_004
                           {:detail    (get-in start-result [1 :cause] "unknown")
                            :tenant-id tenant-id})])
          (let [exec-id  (:execution-id (second start-result))
                data-res (poll-athena-results query-engine exec-id tenant-id)]
            (if (= :error (first data-res))
              [data-res]
              (let [{:keys [columns rows]} (second data-res)
                    ;; Normalizar columnas a {:key str :type str} para normalizer y translator
                    ;; ANO-001/006 fix: tipar created_at y campos temporales como "timestamp"
                    ts-cols #{"created_at" "timestamp" "updated_at" "deleted_at" "ingested_at"}
                    cols-raw (mapv (fn [c]
                                     (let [base (if (map? c) c {:key (str c)})
                                           k    (:key base "")]
                                       (if (ts-cols k)
                                         (assoc base :type "timestamp")
                                         base)))
                                   columns)
                    ;; Columnas del sistema Iceberg — NUNCA se exponen al usuario final
                    ;; en ninguna estrategia (KPI, TIMESERIES, TABLE, PIE, BUBBLE, CSV).
                    ;; created_at SÍ se expone — es un campo de auditoría visible para el usuario.
                    system-cols #{"_tenant" "_entity" "_partition_path"}
                    cols (filterv #(not (system-cols (:key %))) cols-raw)
                    ;; Índices de columnas visibles — para proyectar las rows alineadas
                    visible-idxs (keep-indexed (fn [i c]
                                                 (when-not (system-cols (:key c)) i))
                                               cols-raw)
                    rows (if (seq rows)
                           (mapv (fn [row]
                                   (if (vector? row)
                                     (mapv #(nth row %) visible-idxs)
                                     row))
                                 rows)
                           rows)
                    limit   (:limit ast-ir)
                    ;; OLAP total-count semantico:
                    ;; Para KPI/COUNT, Athena retorna 1 fila agregada => count(rows)=1.
                    ;; Fix: si la primera columna es count_*, usamos su valor como total-count
                    ;; (cuantos registros que participaron en la agregacion).
                    ;; Para SUM/AVG/etc., count(rows) es el total correcto.
                    first-col-key (some-> cols first :key)
                    first-val     (some-> rows first first)
                    total-raw     (if (and first-col-key
                                           (clojure.string/starts-with? first-col-key "count_")
                                           (number? first-val))
                                    (long first-val)
                                    (count rows))

                    ;; -- Cursor-based pagination --------------------------------
                    ;; Athena ya aplica LIMIT al SQL (en compiler.clj).
                    ;; Para paginacion multi-pagina en OLAP pedimos limit+1 al SQL
                    ;; y hacemos el offset en memoria (resultado ya es pequeño con LIMIT).
                    ;; Athena no soporta cursor nativo — offset en memoria es la estrategia
                    ;; correcta para datasets paginados (limit tipico: 10-100 filas).
                    cursor-map    (pagination/decode-cursor (:cursor ast-ir) (or limit 50))
                    offset        (:offset cursor-map)
                    total         total-raw   ; total real para build-pagination

                    ;; Paginar: OFFSET offset sobre los resultados de Athena
                    rows  (pagination/paginate-rows rows offset (or limit 50))

                    ;; Paginacion real: cursores desde offset
                    pag-map (when limit
                              (pagination/build-pagination
                                {:offset offset
                                 :limit  limit
                                 :total  total}))]
                [[:ok (cond-> {:response-type  :query-response
                               :channel        :olap
                               :data           rows
                               :columns        cols
                               :total          total
                               :limit          limit
                               :start-ts       start-ts
                               :tenant-id      tenant-id
                               :execution-id   exec-id
                               :output-cast    output-cast
                               :viz            (:viz ast-ir)
                               :comparison?    comparison?
                               :benchmarks     benchmarks
                               ;; SCATTER-fix: propagar dims/metrics del AST para
                               ;; que ensure-viz-meta construya ejes correctos
                               :ast-dimensions (seq (:dimensions ast-ir))
                               :ast-metrics    (seq (:metrics ast-ir))}
                        pag-map (assoc :pagination pag-map))]])))))))))
