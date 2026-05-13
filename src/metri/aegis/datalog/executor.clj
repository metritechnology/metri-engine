(ns metri.aegis.datalog.executor
  "Ejecutor Datahike: compila AST IR + ejecuta query Datalog + post-procesa.
   SRP: orquestar compile-oltp-query + d/q + sort + chunking -- sin logica de compilacion.

   Contrato de salida (compatible con metres.janus.normalizer Paso 7):
   Cada chunk [:ok body] incluye:
     :response-type  -> :query-response  (dispatch exacto en normalizer multimethod)
     :channel        -> :oltp            (para engine-str en QueryMetadata)
     :data           -> rows (vector de mapas OLTP)
     :columns        -> vector de {:key str :label str :sortable bool} (para VizMeta + RowSet)
     :total          -> count total pre-limit (para Pagination has-next)
     :limit          -> limit del AST (para page-size)
     :pagination     -> mapa completo con cursores reales (next-cursor, previous-cursor)
     :start-ts       -> epoch-ms de inicio del query (para execution-time-ms)
     :output-cast    -> keyword de viz cast (:KPI :PIE :TIMESERIES :BUBBLE :TABLE)
     :viz            -> string de viz hint (para infer-viz-type en normalizer)
     :tenant-id      -> string del tenant

   Post-procesamiento (pipeline ordenado):
     1. inject-has-children -> :has_children boolean si HierarchyContext lo requiere
     2. sort-oltp-result    -> orden en memoria por SortDefinition
     3. take limit          -> Datahike no tiene LIMIT nativo (excepto CSV_EXPORT)
     4. OutputCastType routing + agregacion in-memory:
          :TABLE / :CSV_EXPORT -> rows tal cual (pull projection)
          :KPI                 -> apply-metrics sobre todos los rows -> 1 fila
          :PIE / :BUBBLE       -> group-by dimensiones -> apply-metrics por grupo
          :TIMESERIES          -> group-by (interval-bucket + dims) -> apply-metrics + :bucket
     5. AnalyticalComparison  -> queries shifted + merge resultado plano (cuando hay :metrics)
     6. partition-all 100     -> chunking para streaming gRPC"
  (:require [datahike.api :as d]
            [taoensso.timbre :as log]
            [clojure.string :as str]
            [metri.aegis.datalog.compiler    :as compiler]
            [metri.aegis.datalog.sort        :as s]
            [metri.aegis.datalog.hierarchy   :as h]
            [metri.aegis.datalog.aggregation :as agg]
            [metri.aegis.datalog.comparison  :as cmp]
            [metri.aegis.datalog.ref-resolver :as ref-res]
            [metri.aegis.pagination          :as pagination]
            [metri.domain.errors :as errors])
  (:import [java.time Instant ZonedDateTime ZoneOffset DayOfWeek]
           [java.time.temporal TemporalAdjusters ChronoUnit]))

;; -- Interval bucketing -------------------------------------------------------

(defn- truncate-to-interval
  "Trunca epoch-segundos al inicio del intervalo dado (UTC).
   Soporta: minute hour day week month quarter year.
   Retorna el epoch tal cual si el interval no se reconoce o epoch-secs es nil."
  [epoch-secs interval]
  (when epoch-secs
    (let [es (long epoch-secs)]
      (case interval
        "minute"  (* (quot es 60) 60)
        "hour"    (* (quot es 3600) 3600)
        "day"     (* (quot es 86400) 86400)
        ;; Para week/month/quarter/year usamos java.time para correccion de calendario
        (let [inst (Instant/ofEpochSecond es)
              zdt  (ZonedDateTime/ofInstant inst ZoneOffset/UTC)]
          (case interval
            "week"
            (-> zdt
                (.with (TemporalAdjusters/previousOrSame DayOfWeek/MONDAY))
                (.truncatedTo ChronoUnit/DAYS)
                .toInstant
                .getEpochSecond)
            "month"
            (-> zdt (.withDayOfMonth 1) (.truncatedTo ChronoUnit/DAYS)
                .toInstant .getEpochSecond)
            "quarter"
            (let [qm (inc (* 3 (quot (dec (.getMonthValue zdt)) 3)))]
              (-> zdt (.withMonth qm) (.withDayOfMonth 1) (.truncatedTo ChronoUnit/DAYS)
                  .toInstant .getEpochSecond))
            "year"
            (-> zdt (.withDayOfYear 1) (.truncatedTo ChronoUnit/DAYS)
                .toInstant .getEpochSecond)
            ;; Intervalo desconocido -> sin truncado
            es))))))

;; -- OutputCastType routing ---------------------------------------------------

(defn- apply-output-cast
  "Aplica el routing de OutputCastType sobre rows + metrics + dimensions.
   Retorna rows post-procesados segun el tipo de visualizacion.

   :TABLE / :CSV_EXPORT  -> rows sin agregacion (pull projection directa)
   :KPI                  -> [{:alias value ...}] (un unico mapa de metricas)
   :PIE / :BUBBLE        -> group-by atributos de dimension -> [{dim-kv + metrics}]
   :TIMESERIES           -> group-by (bucket + dims) -> [{:bucket epoch + dim-kv + metrics}]
   nil (sin cast)        -> si hay metricas -> KPI, si no -> TABLE

   ts-field: keyword del campo timestamp resuelto desde el schema (ej. :inventory-movement/timestamp).
             Si es nil, cae en :_timestamp como default."
  [rows metrics dimensions output-cast ts-field]
  (cond
    ;; Sin metricas -> proyeccion directa (TABLE / CSV_EXPORT / nil)
    (nil? metrics)
    rows

    ;; TABLE y CSV_EXPORT no agregan aunque haya metricas definidas
    (#{:TABLE :CSV_EXPORT} output-cast)
    rows

    ;; TIMESERIES -> group-by (interval-bucket, dims) + :bucket en output
    (= :TIMESERIES output-cast)
    (let [time-dim   (first (filter :interval dimensions))
          other-dims (seq (remove :interval dimensions))
          ;; ts-field puede ser :entity-name/field-name (namespace) — los rows
          ;; post-pull son mapas aplanados (keyword sin namespace).
          ;; Probamos primero el nombre sin namespace, luego :meta/created_at como fallback.
          effective-ts-kw (if ts-field
                            (keyword (name ts-field))  ; strip namespace
                            :meta/created_at)
          ;; Normalizar: si el ts viene en ms (>1e11) convertir a segundos
          normalize-ts (fn [ts]
                         (when ts
                           (let [n (long ts)]
                             (if (> n 100000000000) (quot n 1000) n))))
          bucket-fn  (fn [row]
                       (let [ts (or (get row effective-ts-kw)
                                    (get row :meta/created_at))  ; doble fallback
                             ts-secs (normalize-ts ts)]
                         (truncate-to-interval ts-secs (or (:interval time-dim) "day"))))
          other-keys (when other-dims (mapv #(keyword (or (:attribute %) (:field %))) other-dims))
          group-fn   (fn [row]
                       (into [(bucket-fn row)]
                             (when other-keys (mapv #(get row %) other-keys))))]
      (mapv (fn [[group-key group-rows]]
              (let [[bucket & other-vals] group-key
                    dim-map (when other-keys (zipmap other-keys other-vals))
                    bucket-kw (keyword (or (:attribute time-dim) (:field time-dim) "bucket"))]
                (cond-> (agg/apply-metrics group-rows metrics)
                  bucket  (assoc bucket-kw bucket)
                  dim-map (merge dim-map))))
            (sort-by (comp first key)            ; ordenar por bucket asc
                     (group-by group-fn rows))))

    ;; KPI / PIE / BUBBLE / nil-con-metricas -> group-by dims o aggregar todo
    :else
    (if (seq dimensions)
      (let [dim-keys (mapv #(keyword (or (:attribute %) (:field %))) dimensions)]
        (mapv (fn [[group-vals group-rows]]
                (merge (zipmap dim-keys group-vals)
                       (agg/apply-metrics group-rows metrics)))
              (group-by (apply juxt dim-keys) rows)))
      [(agg/apply-metrics rows metrics)])))

;; -- Derivar columnas desde rows OLTP -----------------------------------------

(defn- derive-columns
  "Infiere columnas en formato {:key str :label str :type str :sortable bool :is-dimension bool :is-measure bool}
   a partir de la UNION de claves de TODOS los rows. Usado cuando el AST no especifica columns.

   A2-fix: Usamos la unión (reduce into) en lugar de las keys del primer row.
   B3-fix: Priorizamos dimensiones sobre métricas en el orden final.
   SCATTER-fix: Anota cada columna con :is-dimension y :is-measure para que el
   normalizer pueda construir ChartDecoration sin inferir por posición."
  [rows ast-ir]
  (when (and (seq rows) (map? (first rows)))
    (let [all-keys (reduce (fn [acc row] (into acc (keys row))) #{} rows)
          dim-kws  (set (map #(keyword (or (:attribute %) (:field %) %)) (:group-by ast-ir)))
          ;; Claves de métricas: priorizar :name explícito, fallback a alias generado
          metric-kws (set (keep (fn [m]
                                  (when m
                                    (keyword (or (:name m)
                                                 (when (and (:aggregation m) (or (:attribute m) (:field m)))
                                                   (str (clojure.string/lower-case (name (:aggregation m)))
                                                        "_" (or (:attribute m) (:field m))))))))
                                (:metrics ast-ir)))
          sort-fn  (fn [k]
                     [(cond (dim-kws k) 0
                            (metric-kws k) 2
                            :else 1)
                      (if k (name k) "")])
          sorted-keys (sort-by sort-fn all-keys)]
      (mapv (fn [k]
              (let [k-name    (if k (name k) "")
                    ;; ANO-001/006: forzar "timestamp" para created_at y campos temporales conocidos
                    ts-field? (contains? #{"created_at" "timestamp" "updated_at"
                                           "deleted_at" "ingested_at"} k-name)
                    v (get (first rows) k)
                    t (cond
                        ts-field?    "timestamp"
                        (number? v)  (if (> (double v) 1e11) "timestamp" "number")
                        (boolean? v) "boolean"
                        :else        "string")]
                (cond-> {:key      k-name
                         :label    k-name
                         :type     t
                         :sortable true}
                  (dim-kws k)    (assoc :is-dimension true)
                  (metric-kws k) (assoc :is-measure   true))))
            sorted-keys))))

(defn- normalize-timestamps-in-rows
  "A3-fix: Convierte valores epoch-en-ms a epoch-en-segundos para campos :timestamp.
   Se aplica en TABLE/TREE/CSV_EXPORT donde los valores se exponen crudos.
   TIMESERIES ya normaliza internamente en apply-output-cast → no se aplica allí.
   Regla: si el número > 1e11 → está en ms → dividir entre 1000."
  [rows ts-field]
  (let [;; El campo timestamp puede venir como :timestamp (post-flatten) o con namespace.
        ;; Después del flatten, siempre es :meta/created_at (namespaced attr global).
        ts-kw (if ts-field (keyword (name ts-field)) :meta/created_at)]
    (mapv (fn [row]
            (if-let [ts (get row ts-kw)]
              (let [ts-num (when (number? ts) (double ts))]
                (if (and ts-num (> ts-num 1e11))
                  (assoc row ts-kw (long (/ ts-num 1000)))
                  row))
              row))
          rows)))
(defn- format-pull-value
  "Limpia namespaces y formatea el valor devuelto por Datahike d/pull.
   Si es un string ULID pero tiene instrucciones en nested-pulls, realiza un N+1.
   Mantiene retrocompatibilidad: si es una referencia plana devuelve string ULID.
   Si es una relación anidada, extrae recursivamente limpiando namespaces."
  [v db nested-pulls]
  (cond
    ;; Ref solo con :db/id o {:db/id ID :entity/ulid ULID} -> ulid escalar
    (and (map? v)
         (empty? (remove (fn [[k _]] (#{:db/id :tenant/id :entity/type :entity/ulid} k)) v)))
    (if (:entity/ulid v)
      (:entity/ulid v)
      (or (:entity/ulid (d/pull db [:entity/ulid] (:db/id v))) (str (:db/id v))))

    ;; Mapa anidado con campos de negocio -> objeto aplanado sin namespace
    (map? v)
    (into {}
          (keep (fn [[sub-k sub-v]]
                  (when-not (#{:db/id :tenant/id :entity/type} sub-k)
                    (let [col-key (keyword (if (= sub-k :entity/ulid) "id" (name sub-k)))
                          n-pull  (get nested-pulls sub-k)
                          ;; Resolución manual N+1 para referencias que Datahike guarda como string
                          resolved-v (if (and (string? sub-v) n-pull)
                                       (try
                                         (d/pull db (:select n-pull) [:entity/ulid sub-v])
                                         (catch Exception _
                                           ;; Fallback si el lookup ref está roto
                                           {:entity/ulid sub-v}))
                                       sub-v)]
                      [col-key (format-pull-value resolved-v db (:nested-pulls n-pull))]))))
          v)

    ;; Colección de referencias
    (sequential? v)
    (mapv #(format-pull-value % db nested-pulls) v)

    :else v))

;; -- API publica --------------------------------------------------------------

(defn run-oltp-query
  "Compila y ejecuta la query Datalog contra el snapshot de Datahike.
   Pipeline completo: d/q -> inject-has-children -> sort -> limit ->
                      OutputCastType -> AnalyticalComparison.
   Retorna [:ok {:rows [...] :channel :oltp :tenant-id str :total int}] | [:error ...]."
  [conn ast-ir tenant-id]
  (try
    (let [db  @conn

          ;; ── Ref-Filter Resolution ────────────────────────────────────────
          ;; Los campos reference en Datahike son :db.type/string (ULID).
          ;; El traversal de grafo [?e :asset/location_id ?ref] [?ref :location/type "X"]
          ;; solo funciona con :db.type/ref. Fix: pre-resolver cada :ref-filter
          ;; con un sub-query sobre la entidad referenciada → [:in ref-field [ulids...]].
          resolved-ast-ir
          (if (:where ast-ir)
            (update ast-ir :where #(ref-res/resolve-ref-filters % db))
            ast-ir)

          {:keys [query args limit sort hier
                  metrics dimensions comparisons output-cast
                  base-clauses in-sym->val pull-pattern resolved-tf ts-field]}
          (compiler/compile-oltp-query resolved-ast-ir)

          _ (log/info "[Aegis OLTP] Executing Datalog Query:" (pr-str query))
          _ (try
              (let [total-assets (d/q '[:find (count ?e) :where [?e :entity/type :asset]] db)]
                (log/info "DEBUG: Total :asset entities in DB right now:" total-assets))
              (catch Exception e
                (log/error "DEBUG: Error counting total assets:" (ex-message e))))
          _ (log/info "[Aegis OLTP] With args:" (pr-str args))
          raw (if (seq args)
                (apply d/q query db args)
                (d/q query db))

          ;; -- Flatten pull results -> row maps (recursive for nested pulls via N+1) ---
          rows (mapv (fn [r]
                       (format-pull-value (first r) db (:nested-pulls resolved-ast-ir)))
                     raw)

          ;; -- Post-procesamiento -------------------------------------------
          ;; A1-fix: has_children dual-mode
          ;; Modo 1 (ref): reverse-ref Datahike (:db.type/ref) → pull returns [{:db/id ...}]
          ;; Modo 2 (string): parent field es :db.type/string → fallback in-memory
          rows (if (:has-children? hier)
                 (let [rows-r (h/inject-has-children rows (:rev-key hier))]
                   (if (some :has_children rows-r)
                     rows-r
                     ;; Modo 2: rev-key = :loc/_parent_loc_id → strip "_" → :parent_loc_id
                     (let [rn  (name (:rev-key hier))
                           pfk (keyword (if (str/starts-with? rn "_") (subs rn 1) rn))]
                       (h/inject-has-children-from-rows rows pfk))))
                 rows)

          rows (s/sort-oltp-result rows sort)

          ;; A3-fix: normalizar timestamps ms→s en TABLE/TREE/CSV_EXPORT
          ;; TIMESERIES normaliza internamente — aquí solo afecta proyecciones directas
          rows (if (or (nil? output-cast)
                       (#{:TABLE :CSV_EXPORT} output-cast))
                 (normalize-timestamps-in-rows rows ts-field)
                 rows)

          ;; -- OutputCastType -> agregacion / bucketing ----------------------
          ;; ts-field proviene del compiler (campo epoch resuelto desde el schema)
          rows (apply-output-cast rows metrics dimensions output-cast ts-field)

          ;; -- AnalyticalComparison -> queries shifted + merge ---------------
          ;; Solo se activa cuando hay metricas Y comparaciones definidas.
          ;; Se ejecuta ANTES de paginar para que la comparación sea sobre el total real.
          rows (if (and comparisons metrics (seq rows))
                 (let [current-metrics (first rows)   ; KPI -> siempre 1 fila
                       merged (cmp/run-comparisons
                                db base-clauses in-sym->val pull-pattern
                                metrics comparisons resolved-tf current-metrics ts-field)]
                   [merged])
                 rows)

          ;; -- Cursor-based pagination ----------------------------------------
          ;; decode-cursor lee el cursor Base64(offset:limit) enviado por el frontend.
          ;; Si el cursor es nil/vacío → offset=0 (primera página).
          ;; La paginación se aplica DESPUÉS de ordenar, OutputCast y comparaciones,
          ;; garantizando resultados estables entre páginas.
          cursor-map (pagination/decode-cursor (:cursor ast-ir) (or limit 50))
          offset     (:offset cursor-map)
          ;; total = número de filas ANTES de paginar (para has-next/has-previous)
          total-before-page (count rows)

          ;; Paginar: OFFSET offset LIMIT limit (equivalente SQL)
          rows (if (= :CSV_EXPORT output-cast)
                 rows
                 (pagination/paginate-rows rows offset (or limit 50)))

          ;; Paginación real: cursores calculados desde offset conocido
          pag-map (when limit
                    (pagination/build-pagination
                      {:offset offset
                       :limit  limit
                       :total  total-before-page}))]

      (log/debug "[Aegis OLTP] Query OK | tenant:" tenant-id
                 "| page:" (count rows) "/" total-before-page
                 "| offset:" offset "| limit:" limit
                 "| cast:" output-cast
                 (when metrics (str " | metrics:" (count metrics)))
                 (when comparisons (str " | comparisons:" (count comparisons))))
      [:ok {:rows      rows
            :channel   :oltp
            :tenant-id tenant-id
            :total     total-before-page
            :pagination pag-map}])

    (catch clojure.lang.ExceptionInfo e
      (log/warn "[Aegis OLTP] Compile error:" (ex-message e))
      (errors/error :AEG_COMPILE_001
                    {:detail    (ex-message e)
                     :tenant-id tenant-id}))
    (catch Exception e
      (log/error e "[Aegis OLTP] Datahike query failed")
      (errors/error :AEG_002
                    {:detail    (ex-message e)
                     :tenant-id tenant-id}))))

(defn run-oltp-chunks
  "Compila + ejecuta + chunking en batches de 100 filas.
   Cada chunk incluye todos los campos que el normalizer (Paso 7) necesita:
     :response-type :query-response  -- dispatch exacto en multimethod
     :channel       :oltp
     :columns       derivadas o del AST
     :total         count pre-paginado (para metadata.total-count)
     :limit         del AST
     :pagination    mapa con cursores reales (generado por run-oltp-query)
     :start-ts      epoch-ms inicio
     :output-cast   / :viz para infer-viz-type
   | [:error ...]."
  [conn ast-ir tenant-id]
  (let [start-ts (System/currentTimeMillis)
        result   (run-oltp-query conn ast-ir tenant-id)]
    (if (= :error (first result))
      [result]
      ;; run-oltp-query ya aplica offset + limit y genera pag-map con cursores reales
      (let [{:keys [rows total pagination]} (second result)
            cols    (or (:columns ast-ir) (derive-columns rows ast-ir))
            limit   (:limit ast-ir)
            pag-map pagination]
        (if (empty? rows)
          [[:ok (cond-> {:response-type  :query-response
                         :channel        :oltp
                         :data           []
                         :columns        (or cols [])
                         :total          total
                         :limit          limit
                         :start-ts       start-ts
                         :output-cast    (:output-cast ast-ir)
                         :viz            (:viz ast-ir)
                         :tenant-id      tenant-id
                         ;; SCATTER-fix: propagar AST dims/metrics incluso cuando vacío
                         :ast-dimensions (seq (:dimensions ast-ir))
                         :ast-metrics    (seq (:metrics ast-ir))}
                  pag-map (assoc :pagination pag-map))]]
          (mapv (fn [batch]
                  [:ok (cond-> {:response-type  :query-response
                                :channel        :oltp
                                :data           (vec batch)
                                :columns        (or cols [])
                                :total          total
                                :limit          limit
                                :start-ts       start-ts
                                :output-cast    (:output-cast ast-ir)
                                :viz            (:viz ast-ir)
                                :tenant-id      tenant-id
                                ;; SCATTER-fix: propagar dimensiones y métricas del AST
                                ;; para que ensure-viz-meta construya ejes correctos
                                ;; sin depender de la inferencia por posición de columnas.
                                :ast-dimensions (seq (:dimensions ast-ir))
                                :ast-metrics    (seq (:metrics ast-ir))}
                         pag-map (assoc :pagination pag-map))])
                (partition-all 100 rows)))))))
