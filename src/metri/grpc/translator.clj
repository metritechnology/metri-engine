;; [PORTED_TO_RUST: src/grpc/translator.rs]
;; NO MODIFICAR ESTE ARCHIVO.
;; La fuente de verdad para esta lógica ahora reside en Rust.
(ns metri.grpc.translator
  "Traduccion pura Protobuf <-> Clojure. Sin side-effects.
   Unica capa del sistema que importa clases de metri.data.grpc.*

   CONTRATO DE ENTRADA (post-normalizador):
   Todos los body Railway que llegan a este ns YA fueron procesados por
   metres.janus.normalizer, por lo tanto:
     - :status   -> siempre presente { :success, :error-code?, :error-message? }
     - :metadata -> siempre presente { :engine, :query-id, :total-count, ... }
     - :pagination -> presente cuando hay limit/cursor
     - :links    -> presente (al menos :self y :refresh)
     - :viz-ext  -> presente para QueryResponse (normalizer lo infiere)
   El translator solo serializa. No infiere ni rellena campos."
  (:import
   ;; Service
   [metri.data.grpc MetriServiceGrpc$MetriServiceImplBase]
   ;; Enums
   [metri.data.grpc OperationAction]
   ;; Operations
   [metri.data.grpc TransactionRequest TransactionResponse
                    BulkRequest BulkResponse]
   ;; Metadata
   [metri.data.grpc DiscoveryRequest DiscoveryResponse
                    ExploreRequest   ExploreResponse
                    EntitySchema     AttributeSchema]
   ;; Analytics
   [metri.data.grpc QueryRequest QueryResponse QueryMetadata
                    AnalyticsRequest RowSet ColumnSchema DataRow DataRowList]
   ;; EDA
   [metri.data.grpc MatchRoutingRulesBatchRequest MatchRoutingRulesBatchResponse
                    MatchRoutingRulesRequest       MatchRoutingRulesResponse
                    MatchedRule WebhookTarget FilterNode]
   ;; Visualizacion - 4 VizMeta + tipos de respuesta analitica
   [metri.data.grpc VizMeta AnalyticalSignal IntelligenceSignal IndicatorThreshold
                    ChartDecoration TableMeta TableColumn Action TreeMeta BreakdownSignal]
   ;; Chronos - 4 ChronosAlertTask (Fase futura)
   [metri.data.grpc ChronosAlertTask]
   ;; Base
   [metri.data.grpc Status Pagination Link]
   ;; Protobuf
   [com.google.protobuf Struct Value ListValue NullValue]
   [com.google.protobuf Value$KindCase]
   [com.google.protobuf.util JsonFormat]
   ;; gRPC
   [io.grpc Status$Code]))

;; -----------------------------------------------------------------------------
;; ERROR CODE -> gRPC Status map (inicializado desde error_catalog.edn)
;; -----------------------------------------------------------------------------

(def ^:private grpc-status-map (atom {}))

(defn init-grpc-status-map!
  "Deriva el mapa {:ERR_CODE Status$Code} del catalogo de errores.
   Llamar desde bootstrap DESPUS de cargar el catalogo."
  [catalog]
  (reset! grpc-status-map
    (reduce-kv
      (fn [acc code {:keys [http-status]}]
        (assoc acc code
          (cond
            (= 400 http-status) Status$Code/INVALID_ARGUMENT
            (= 401 http-status) Status$Code/UNAUTHENTICATED
            (= 403 http-status) Status$Code/PERMISSION_DENIED
            (= 404 http-status) Status$Code/NOT_FOUND
            (= 409 http-status) Status$Code/ALREADY_EXISTS
            (= 429 http-status) Status$Code/RESOURCE_EXHAUSTED
            :else               Status$Code/INTERNAL)))
      {}
      catalog)))

(defn error->grpc-status
  "Mapea un error Railway {:code :message} -> io.grpc.Status."
  [{:keys [code message]}]
  (let [status-code (get @grpc-status-map code Status$Code/INTERNAL)]
    (-> (io.grpc.Status/fromCode status-code)
        (.withDescription (or message (name code))))))

;; -----------------------------------------------------------------------------
;; Protobuf Struct  Clojure map (recursivo)
;; -----------------------------------------------------------------------------

(declare struct->map map->struct)

(defn- value->clj [^Value v]
  (condp = (.getKindCase v)
    Value$KindCase/STRING_VALUE  (.getStringValue v)
    Value$KindCase/NUMBER_VALUE  (.getNumberValue v)
    Value$KindCase/BOOL_VALUE    (.getBoolValue v)
    Value$KindCase/NULL_VALUE    nil
    Value$KindCase/STRUCT_VALUE  (struct->map (.getStructValue v))
    Value$KindCase/LIST_VALUE    (mapv value->clj (.. v getListValue getValuesList))
    nil))

(defn struct->map
  "Convierte google.protobuf.Struct -> Clojure map con keyword keys."
  [^Struct s]
  (when s
    (reduce-kv
      (fn [acc k v]
        (assoc acc (keyword k) (value->clj v)))
      {}
      (.getFieldsMap s))))

(defn- clj->value [v]
  (let [builder (Value/newBuilder)]
    (cond
      (nil? v)     (.setNullValue builder com.google.protobuf.NullValue/NULL_VALUE)
      (string? v)  (.setStringValue builder v)
      (number? v)  (.setNumberValue builder (double v))
      (boolean? v) (.setBoolValue builder v)
      (map? v)     (.setStructValue builder (map->struct v))
      (coll? v)    (.setListValue builder
                     (-> (ListValue/newBuilder)
                         (.addAllValues (mapv clj->value v))
                         (.build)))
      :else        (.setStringValue builder (str v)))
    (.build builder)))

(defn map->struct
  "Convierte Clojure map -> google.protobuf.Struct."
  [m]
  (when m
    (let [builder (Struct/newBuilder)]
      (doseq [[k v] m]
        (.putFields builder (name k) (clj->value v)))
      (.build builder))))

;; -----------------------------------------------------------------------------
;; Reflexion Universal AST (Descriptors.Descriptor) - FASE 05.01 JANUS
;; -----------------------------------------------------------------------------

(declare proto->clj)

(defn- proto-value->clj
  [^com.google.protobuf.Descriptors$FieldDescriptor fd val]
  (cond
    ;; Si es un Struct de protobuf, usamos la logica estricta existente
    (instance? com.google.protobuf.Struct val)
    (struct->map val)

    ;; Si es un Mensaje anidado
    (= (.getJavaType fd) com.google.protobuf.Descriptors$FieldDescriptor$JavaType/MESSAGE)
    (proto->clj val)

    ;; Si es un Enum, extraemos el nombre como string y lo volvemos keyword
    (= (.getJavaType fd) com.google.protobuf.Descriptors$FieldDescriptor$JavaType/ENUM)
    (if (instance? com.google.protobuf.Descriptors$EnumValueDescriptor val)
      (keyword (.getName ^com.google.protobuf.Descriptors$EnumValueDescriptor val))
      (keyword (str val)))

    ;; Primitivos (string, int, bool, etc)
    :else
    val))

(defn proto->clj
  "Transforma CUALQUIER mensaje Protobuf (y sus anidaciones) en un mapa Clojure (AST)
   utilizando la reflexion nativa (Descriptors.Descriptor), sin dependencias estaticas.
   Ideal para QueryRequest / AnalyticsRequest hacia el JanusRouter (FASE 05.01)."
  [^com.google.protobuf.MessageOrBuilder msg]
  (if (nil? msg)
    nil
    (reduce-kv
      (fn [acc ^com.google.protobuf.Descriptors$FieldDescriptor fd val]
        (let [k (keyword (clojure.string/replace (.getName fd) "_" "-"))
              v (cond
                  (.isMapField fd)
                  (reduce (fn [m ^com.google.protobuf.Message entry]
                            (let [entry-map (proto->clj entry)]
                              (assoc m (keyword (:key entry-map)) (:value entry-map))))
                          {}
                          val)
                  (.isRepeated fd)
                  (mapv #(proto-value->clj fd %) val)
                  :else
                  (proto-value->clj fd val))]
          (assoc acc k v)))
      {}
      (.getAllFields msg))))

;; Status helpers
;; -----------------------------------------------------------------------------

(defn- ok-status []
  (-> (Status/newBuilder) (.setSuccess true) (.build)))

(defn- error-status [body]
  (let [code    (:code body)
        msg-base (or (:message body) (:reason body) (:detail body))
        message  (if (and (:reason body) (:detail body))
                   (str (:reason body) " | " (:detail body))
                   msg-base)]
    (-> (Status/newBuilder)
        (.setSuccess false)
        (.setErrorCode (if (some? code) (name code) "INTERNAL_ERROR"))
        (.setErrorMessage (or message "Error interno del servidor"))
        (.build))))

(defn- build-status
  "Lee el :status normalizado del body y lo serializa a proto Status.
   Si el normalizer garantizo el campo, lo usamos directamente.
   Fallback seguro: ok-status / error-status segun tag."
  [body tag]
  (if-let [s (:status body)]
    (let [b (Status/newBuilder)]
      (.setSuccess b (boolean (:success s)))
      (when-let [ec (:error-code s)]    (.setErrorCode b (str ec)))
      (when-let [em (:error-message s)] (.setErrorMessage b (str em)))
      (.build b))
    (if (= tag :ok) (ok-status) (error-status body))))

(defn- build-link
  "Serializa un mapa {:rel :href :method} a proto Link."
  [lnk]
  (-> (Link/newBuilder)
      (.setRel    (or (:rel lnk) ""))
      (.setHref   (or (:href lnk) ""))
      (.setMethod (or (:method lnk) "POST"))
      (.build)))

;; -----------------------------------------------------------------------------
;; OperationAction sentinel guard
;; -----------------------------------------------------------------------------

(def ^:private action->keyword
  {OperationAction/CREATE :create
   OperationAction/UPDATE :update
   OperationAction/DELETE :delete
   OperationAction/UPSERT :upsert
   OperationAction/GET    :get})
;; OPERATION_ACTION_UNSPECIFIED = 0 -> nil -> IOP rechaza con :JNS_VAL_001

;; -----------------------------------------------------------------------------
;; MDULO: Operations - Transact
;; -----------------------------------------------------------------------------

(defn transaction-request->ctx
  "TransactionRequest -> mapa canonico IOP.
   tenant_id se transporta solo para logging - Cedar lo sobreescribe."
  [^TransactionRequest req]
  {:request {:entity-type      (.getEntityType req)
             :entity-id        (let [id (.getEntityId req)]
                                 (when (seq id) id))
             :operation        (action->keyword (.getAction req)) ;; nil si UNSPECIFIED
             :payload          (struct->map (.getPayload req))
             :suppress-events  (.getSuppressEvents req)
             :proto-tenant-id  (.getTenantId req)}})

(defn iop-result->transaction-response
  "Railway result -> TransactionResponse protobuf.
   Usa :status del body normalizado si esta presente."
  [[tag body]]
  (-> (TransactionResponse/newBuilder)
      (.setStatus   (build-status body tag))
      (.setEntityId (or (:entity-id body) ""))
      (cond-> (and (= tag :ok) (:result body))
        (.setResult (map->struct (:result body))))
      (.build)))

;; -----------------------------------------------------------------------------
;; MDULO: Operations - BulkIngest
;; -----------------------------------------------------------------------------

(defn- rowset->maps [^RowSet rowset]
  (let [columns (mapv #(.getKey %) (.getColumnsList rowset))
        rows    (.getIterList (.getRowsJson rowset))]
    (mapv (fn [^DataRow row]
            (let [vals (mapv (fn [v]
                               (cond
                                 (.hasStringValue v) (.getStringValue v)
                                 (.hasNumberValue v) (.getNumberValue v)
                                 (.hasBoolValue v)   (.getBoolValue v)
                                 :else nil))
                             (.getValuesList row))]
              (zipmap (map keyword columns) vals)))
          rows)))

(defn bulk-request->ctx
  [^BulkRequest req]
  {:request {:entity-type (.getEntityType req)
             :operation   (action->keyword (.getAction req))
             :data        (let [rs (.getData req)]
                            (if rs (rowset->maps rs) []))
             :proto-tenant-id (.getTenantId req)}})

(defn iop-result->bulk-response
  "Railway result -> BulkResponse protobuf.
   Usa :status del body normalizado si esta presente."
  [[tag body]]
  (-> (BulkResponse/newBuilder)
      (.setStatus       (build-status body tag))
      (.setIngestedCount (int (or (:ingested-count body) 0)))
      (.setOutboxCount   (int (or (:outbox-count body) 0)))
      (.build)))

;; -----------------------------------------------------------------------------
;; MDULO: Metadata - Discovery
;; -----------------------------------------------------------------------------

(declare map->entity-schema map->attribute-schema)

(defn discovery-request->ctx
  [^DiscoveryRequest req]
  {:tenant-id          (.getTenantId req)
   :type               (let [t (.getType req)] (when (seq t) t))
   :include-attributes (.getIncludeAttributes req)
   :cursor             (let [c (.getCursor req)] (when (seq c) c))
   :page-size          (let [ps (.getPageSize req)] (if (pos? ps) ps 50))})

(defn discovery-result->response
  "Railway result -> DiscoveryResponse protobuf.
   Usa :status del body normalizado. :schemas y :has-next garantizados por normalizer."
  [[tag body]]
  (let [b (DiscoveryResponse/newBuilder)]
    (.setStatus b (build-status body tag))
    (when (= tag :ok)
      (.setNextCursor b (or (:next-cursor body) ""))
      (.setHasNext b (boolean (:has-next body)))
      (doseq [s (or (:schemas body) [])]
        (.addSchemas b (map->entity-schema s))))
    (.build b)))

(defn- map->entity-schema [m]
  (let [b (EntitySchema/newBuilder)]
    (.setEntity b (or (:entity m) ""))
    (.setLabel  b (or (:label m) ""))
    (.setIcon   b (or (:icon m) ""))
    (.setPrimaryKey b (or (:primary-key m) "id"))
    (.setTotalCount b (long (or (:total-count m) 0)))
    (doseq [f (:fts-fields m)] (.addFtsFields b f))
    (when (:attributes m)
      (doseq [a (:attributes m)] (.addAttributes b (map->attribute-schema a))))
    (.build b)))

(defn- map->attribute-schema [m]
  (let [b (AttributeSchema/newBuilder)]
    (.setName        b (or (:name m) ""))
    (.setType        b (or (:type m) "string"))
    (.setSubtype     b (or (:subtype m) ""))
    (.setLabel       b (or (:label m) ""))
    (.setIcon        b (or (:icon m) ""))
    (.setUnit        b (or (:unit m) ""))
    (.setFts         b (boolean (:fts m)))
    (.setFilterable  b (boolean (:filterable m)))
    (.setSortable    b (boolean (:sortable m)))
    (.setGroupable   b (boolean (:groupable m)))
    (.setAggregatable b (boolean (:aggregatable m)))
    (.setEntityRef   b (or (:entity-ref m) ""))
    (.setDefaultValue b (or (:default-value m) ""))
    (doseq [v (:enum-values m)] (.addEnumValues b v))
    (.build b)))

;; -----------------------------------------------------------------------------
;; MDULO: Metadata - Explore
;; -----------------------------------------------------------------------------

(defn explore-request->ctx
  [^ExploreRequest req]
  {:tenant-id (.getTenantId req)
   :entity    (.getEntity req)
   :attribute (.getAttribute req)
   :limit     (let [l (.getLimit req)] (if (pos? l) l 20))})

(defn explore-result->response
  "Railway result -> ExploreResponse protobuf.
   Usa :status del body normalizado. :values garantizado por normalizer (nunca nil)."
  [[tag body]]
  (let [b (ExploreResponse/newBuilder)]
    (.setStatus b (build-status body tag))
    (when (= tag :ok)
      (doseq [v (or (:values body) [])] (.addValues b (str v))))
    (.build b)))

;; -----------------------------------------------------------------------------
;; MDULO: Analytics - Query (server-streaming)
;; -----------------------------------------------------------------------------

(defn query-request->ctx
  [^QueryRequest req]
  (let [ctx {:tenant-id    (.getTenantId req)
             ;; Convertimos dinamicamente cada AnalyticsRequest en un AST IR (Malli-ready)
             :queries      (reduce
                             (fn [m ^java.util.Map$Entry entry]
                               (assoc m (keyword (.getKey entry)) (proto->clj (.getValue entry))))
                             {}
                             (.entrySet (.getQueriesMap req)))
             ;; BatchContext convertido - common-filters ya como Clojure maps para Janus
             :context      (when (.hasContext req)
                             (proto->clj (.getContext req)))
             ;; MultiSeriesGroup convertidos a Clojure maps
             :merge-groups (mapv proto->clj (.getMergeGroupsList req))
             :cross-filter (when (.hasCrossFilterContext req)
                             (proto->clj (.getCrossFilterContext req)))
             :request      {}
             :explain-plan? (.getExplainPlan req)}]
    (into {} (remove (comp nil? val) ctx))))

(defn- rows->rowset
  "Convierte rows del chunk Aegis -> RowSet proto.
   OLTP rows: vector de mapas {:entity/attr val ...}
   OLAP rows: vector de vectores [[v1 v2 ...] ...] con :columns [col1 col2 ...]

   A2-fix: oltp-keys se deriva de :columns (orden declarado por derive-columns),
   NO de las keys del primer row. Así los valores de cada row se emiten en el
   mismo orden que las columnas -> el cliente ZIP es correcto."
  [body]
  (let [rows     (or (:data body) [])
        is-oltp? (or (and (seq rows) (map? (first rows)))
                     (= (:output-cast body) :raw))
        ;; Determinar columns primero (fuente de verdad del orden)
        columns  (or (:columns body)
                     (when (and is-oltp? (seq rows))
                       (let [all-keys (reduce (fn [acc row] (into acc (keys row))) #{} rows)]
                         (mapv (fn [k] {:key (name k) :type "string" :label (name k)}) (sort all-keys)))))
        ;; oltp-keys: keywords alineados al orden de columns (no al primer row)
        oltp-keys (when is-oltp?
                    (if (seq columns)
                      (mapv #(keyword (:key %)) columns)
                      (if (seq rows) (vec (keys (first rows))) [:id])))
        rs-b     (RowSet/newBuilder)
        dl-b     (DataRowList/newBuilder)]

    ;; Construir DataRows
    (if is-oltp?
      ;; OLTP: vector de mapas -> each map is one row
      ;; Los valores se emiten en el orden de oltp-keys (= orden de columns)
      (doseq [row-map rows]
        (let [dr-b (DataRow/newBuilder)]
          (doseq [k oltp-keys]
            (.addValues dr-b (clj->value (get row-map k))))
          (.addIter dl-b (.build dr-b))))
      ;; OLAP: vector de vectores
      (doseq [row-vec rows]
        (when (sequential? row-vec)
          (let [dr-b (DataRow/newBuilder)]
            (doseq [v row-vec]
              (.addValues dr-b (clj->value v)))
            (.addIter dl-b (.build dr-b))))))

    (when (seq columns)
      (doseq [col columns]
        (let [cs-b (ColumnSchema/newBuilder)]
          (when (:key col) (.setKey cs-b (:key col)))
          (when (:label col) (.setLabel cs-b (:label col)))
          (when (:type col) (.setType cs-b (:type col)))
          (when (:format col) (.setFormat cs-b (:format col)))
          (when (contains? col :is-dimension) (.setIsDimension cs-b (boolean (:is-dimension col))))
          (when (contains? col :is-measure) (.setIsMeasure cs-b (boolean (:is-measure col))))
          (.addColumns rs-b (.build cs-b)))))

    (.setRowsJson rs-b (.build dl-b))
    (.build rs-b)))

(defn- build-query-metadata
  "Construye QueryMetadata desde el mapa :metadata garantizado por el normalizer.
   Lee :engine, :query-id, :total-count, :execution-time-ms, etc."
  [body]
  (let [m (or (:metadata body) {})]
    (-> (QueryMetadata/newBuilder)
        (.setExecutionTimeMs (long (or (:execution-time-ms m) 0)))
        (.setEngine          (or (:engine m) (name (or (:channel body) :unknown))))
        (.setTotalCount      (long (or (:total-count m) 0)))
        (.setTotalQueries    (int  (or (:total-queries m) 1)))
        (.setCacheHits       (int  (or (:cache-hits m) 0)))
        (.setCacheTtlSeconds (int  (or (:cache-ttl-seconds m) 0)))
        (.setParallelismFactor (double (or (:parallelism-factor m) 1.0)))
        (.setIsSemantic      (boolean (:is-semantic m)))
        (cond-> (:query-id m)
          (.setQueryId (:query-id m)))
        (.build))))

;; -----------------------------------------------------------------------------
;; MDULO: VizMeta - 4 Tipos de respuesta visual (janus-ast-ir.edn 4 Visualizacion)
;; Builders puros: Clojure map -> proto builder. Sin side-effects.
;; -----------------------------------------------------------------------------

(defn- safe-double [x]
  (if (string? x)
    (try (Double/parseDouble x) (catch Exception _ 0.0))
    (double (or x 0.0))))

(defn- safe-double-or-nil
  "Como safe-double pero retorna nil cuando x es nil/ausente.
   Usado para detectar 'sin datos' vs 'valor real 0.0' en BENCHMARK/TIME_SHIFT.
   Evita comparaciones enganosas del tipo: 0.0 (sin datos) vs benchmark 100 → down -100%."
  [x]
  (when (some? x)
    (safe-double x)))

(defn- build-intelligence-signal
  "4 IntelligenceSignal - tendencias y anomalias para un KPI/Sparkline."
  [m]
  (-> (IntelligenceSignal/newBuilder)
      (.setDirection       (or (:direction m) ""))
      (.setPercentage      (safe-double (:percentage m)))
      (.setDeltaAbs        (safe-double (:delta-abs m)))
      (.setPreviousValue   (safe-double (:previous-value m)))
      (.setLabel           (or (:label m) ""))
      (.setIsAnomaly       (boolean (:is-anomaly m)))
      (.setZScore          (safe-double (:z-score m)))
      (.setRepresentsInitial (boolean (:represents-initial m)))
      (.build)))

(defn- build-indicator-threshold
  "4 IndicatorThreshold - umbral de referencia con color y etiqueta."
  [m]
  (-> (IndicatorThreshold/newBuilder)
      (.setValue (safe-double (:value m)))
      (.setColor (or (:color m) ""))
      (.setLabel (or (:label m) ""))
      (.build)))

(defn- build-analytical-signal
  "4 AnalyticalSignal - senal analitica con historial y umbrales."
  [m]
  (let [b (doto (AnalyticalSignal/newBuilder)
            (.setValue         (safe-double (:value m)))
            (.setPreviousValue (safe-double (:previous-value m)))
            (.setUnit          (or (:unit m) ""))
            (.setStatusLabel   (or (:status-label m) ""))
            (.setEntityRef     (or (:entity-ref m) "")))]
    (doseq [h (or (:history m) [])]
      (.addHistory b (safe-double h)))
    (when-let [intel (:intelligence m)]
      (.setIntelligence b (build-intelligence-signal intel)))
    (doseq [t (or (:thresholds m) [])]
      (.addThresholds b (build-indicator-threshold t)))
    (.build b)))

(defn- build-chart-decoration
  "4 ChartDecoration - config grafica: ejes, colores, estilo.
   ANO-005: serializa :fill-gaps (bool) → fill_gaps proto field 10.
   Guard defensivo: setFillGaps se aplica solo cuando el JAR compilado
   expone el método (protege ante desfase de versión proto/JAR)."
  [m]
  (let [b (doto (ChartDecoration/newBuilder)
             (.setXDimension  (or (:x-dimension m) ""))
             (.setColorScheme (or (:color-scheme m) ""))
             (.setShowLegend  (boolean (:show-legend m)))
             (.setShowTooltip (boolean (:show-tooltip m)))
             (.setTitle       (or (:title m) ""))
             (.setStacked     (boolean (:stacked m)))
             (.setSmooth      (boolean (:smooth m))))]
    (when (:fill-gaps m)
      (try (.setFillGaps b (boolean (:fill-gaps m)))
           (catch IllegalArgumentException _
             nil))) ; proto JAR desactualizado — campo ignorado hasta recompilacion
    (doseq [y (or (:y-dimensions m) [])]
      (.addYDimensions b (str y)))
    (when-let [lt (:label-template m)] (.setLabelTemplate b lt))
    (.build b)))

(defn- build-action
  "4 Action - accion de fila: label, icon, tipo."
  [m]
  (let [b (Action/newBuilder)]
    (.setId    b (or (:id m) ""))
    (.setLabel b (or (:label m) ""))
    (.setIcon  b (or (:icon m) ""))
    (.setType  b (or (:type m) ""))
    (when-let [lnk (:link m)]
      (.setLink b (-> (Link/newBuilder)
                      (.setRel (or (:rel lnk) ""))
                      (.setHref (or (:href lnk) ""))
                      (.setMethod (or (:method lnk) "GET"))
                      (.build))))
    (.build b)))

(defn- build-table-column
  "4 TableColumn - columna de tabla: key, label, tipo, formato."
  [m]
  (let [b (TableColumn/newBuilder)]
    (.setKey       b (or (:key m) ""))
    (.setLabel     b (or (:label m) ""))
    (.setType      b (or (:type m) "string"))
    (.setSortable  b (boolean (:sortable m)))
    (.setFormat    b (or (:format m) ""))
    (.setEntityRef b (or (:entity-ref m) ""))
    (when (:align m)
      (case (:align m)
        :LEFT   (.setAlignValue b 1)
        :CENTER (.setAlignValue b 2)
        :RIGHT  (.setAlignValue b 3)
        nil))
    (when (:metadata m)
      (.setMetadata b (map->struct (:metadata m))))
    (.build b)))

(defn- build-table-meta
  "4 TableMeta - metadatos de tabla: columnas, acciones de fila, links globales."
  [m]
  (let [b (TableMeta/newBuilder)]
    (doseq [col (or (:columns m) [])]
      (.addColumns b (build-table-column col)))
    (doseq [act (or (:row-actions m) [])]
      (.addRowActions b (build-action act)))
    (doseq [lnk (or (:global-links m) [])]
      (.addGlobalLinks b (-> (Link/newBuilder)
                             (.setRel    (or (:rel lnk) ""))
                             (.setHref   (or (:href lnk) ""))
                             (.setMethod (or (:method lnk) "GET"))
                             (.build))))
    (.build b)))

(defn- build-tree-meta
  "4 TreeMeta - metadatos de arbol jerarquico."
  [m]
  (-> (TreeMeta/newBuilder)
      (.setIdKey          (or (:id-key m) ""))
      (.setParentIdKey    (or (:parent-id-key m) ""))
      (.setLabelKey       (or (:label-key m) ""))
      (.setHasChildrenKey (or (:has-children-key m) ""))
      (.setIconKey        (or (:icon-key m) ""))
      (.build)))

(defn- build-breakdown-signal
  "4 BreakdownSignal - desglose por dimension: map-of string -> AnalyticalSignal."
  [m]
  (let [b (BreakdownSignal/newBuilder)]
    (doseq [[k signal] (or (:signals m) {})]
      (.putSignals b (str k) (build-analytical-signal signal)))
    (.build b)))

(defn- build-viz-meta
  "4 VizMeta - contenedor de visualizacion. Dispatch por payload tipo:
   :signal | :chart | :table | :breakdown | :tree"
  [m]
  (let [b    (doto (VizMeta/newBuilder) (.setType (or (:type m) "")))
        payload (:payload m {})]
    (cond
      (:signal    payload) (.setSignal    b (build-analytical-signal (:signal    payload)))
      (:chart     payload) (.setChart     b (build-chart-decoration  (:chart     payload)))
      (:table     payload) (.setTable     b (build-table-meta        (:table     payload)))
      (:breakdown payload) (.setBreakdown b (build-breakdown-signal  (:breakdown payload)))
      (:tree      payload) (.setTree      b (build-tree-meta         (:tree      payload))))
    (.build b)))

;; -----------------------------------------------------------------------------
;; 4 ChronosAlertTask - stub para RPC de alertas programadas (Fase futura)
;; -----------------------------------------------------------------------------

(defn chronos-alert-task->ctx
  "4 ChronosAlertTask -> Clojure map. Stub para integracion futura con el motor
   de alertas programadas. El campo :condition-query es un QueryRequest completo."
  [^ChronosAlertTask task]
  {:task-id          (.getTaskId task)
   :cron-expression  (.getCronExpression task)
   :alert-threshold  (.getAlertThreshold task)
   :webhook-url      (.getWebhookUrl task)
   :email-recipients (vec (.getEmailRecipientsList task))
   ;; :condition-query es un QueryRequest - se traduce con query-request->ctx cuando se implemente el RPC
   :condition-query  (when (.hasConditionQuery task)
                       (query-request->ctx (.getConditionQuery task)))})

(defn- extract-analytical-signal
  "Extrae metadata de AnalyticalSignal analizando dinamicamente columnas de Aegis.

   Estrategias soportadas:
     1. TIME_SHIFT (prev_0_*): current vs previous period → direction + percentage + delta-abs
     2. BENCHMARK: current vs benchmark_value → direction + percentage + delta-abs
     3. SMART (z_score_*): z-score + is-anomaly
     4. Sin comparación: solo :value

   El argumento `benchmarks` es el vector [{:type :BENCHMARK :benchmark-value N :label L}]
   propagado desde compiler/executor a través del chunk :benchmarks."
  ([kvs columns] (extract-analytical-signal kvs columns nil))
  ([kvs columns benchmarks]
   (let [curr-k     (some #(when (clojure.string/starts-with? (name (:key %)) "current_") (:key %)) columns)
         main-k     (keyword (or curr-k (:key (first columns))))
         ;; safe-double-or-nil: nil cuando no hay dato → evita comparar 0.0 vs benchmark
         raw-val    (get kvs main-k)
         curr-val   (safe-double-or-nil raw-val)   ;; nil = sin datos
         no-data?   (nil? curr-val)                ;; flag: skip intelligence si sin datos
         curr-val   (or curr-val 0.0)              ;; para la serialización :value
         prev-k     (some #(when (clojure.string/starts-with? (name (:key %)) "prev_0_") (:key %)) columns)
         prev-raw   (when prev-k (get kvs (keyword prev-k)))
         prev-val   (if prev-k (safe-double prev-raw) 0.0)
         z-k        (some #(when (clojure.string/starts-with? (name (:key %)) "z_score_") (:key %)) columns)
         z-score    (if z-k (safe-double (get kvs (keyword z-k))) 0.0)
         has-cmp?   (and (some? prev-k) (not no-data?))

         ;; ── BENCHMARK: solo si hay datos reales (curr-val no es nil) ─────────
         bench      (when (and (not has-cmp?) (not no-data?) (seq benchmarks))
                      (first (filter #(= :BENCHMARK (:type %)) benchmarks)))
         bench-val  (when bench (safe-double (:benchmark-value bench 0.0)))
         has-bench? (and bench (some? bench-val))]

     (cond-> {:value curr-val}
       ;; ── TIME_SHIFT: delta vs período anterior ───────────────────────────
       has-cmp? (assoc :previous-value prev-val
                       :intelligence {:previous-value prev-val
                                      :delta-abs   (- curr-val prev-val)
                                      :percentage  (if (zero? prev-val)
                                                     (if (zero? curr-val) 0.0 100.0)
                                                     (* 100.0 (/ (- curr-val prev-val) prev-val)))
                                      :direction   (cond (> curr-val prev-val) "UP"
                                                         (< curr-val prev-val) "DOWN"
                                                         :else "FLAT")})
       ;; ── BENCHMARK: delta vs valor de referencia fijo ────────────────────
       has-bench? (assoc :previous-value bench-val
                         :intelligence {:previous-value bench-val
                                        :delta-abs   (- curr-val bench-val)
                                        :percentage  (if (zero? bench-val)
                                                       (if (zero? curr-val) 0.0 100.0)
                                                       (* 100.0 (/ (- curr-val bench-val) bench-val)))
                                        :direction   (cond (> curr-val bench-val) "UP"
                                                           (< curr-val bench-val) "DOWN"
                                                           :else "FLAT")
                                        :label       (or (:label bench) "vs Benchmark")})
       ;; ── SMART z-score: añadir a intelligence existente o crear nuevo ────
       z-k (-> (assoc-in [:intelligence :z-score]   z-score)
               (assoc-in [:intelligence :is-anomaly] (> (Math/abs ^double z-score) 2.0)))
       (and z-k (not has-cmp?) (not has-bench?))
       (assoc :intelligence {:z-score   z-score
                             :is-anomaly (> (Math/abs ^double z-score) 2.0)})))))

(defn- build-viz-meta-from-chunk
  "Wrapper generico adaptado al 100% a VizMeta.
   Infiere la estrategia visual (bar, indicator, timeseries, pie, etc) dinamicamente a partir de viz_hint,
   eliminando el hardcoding cerrado de :KPI o :PIE. Despacha al oneof schema correspondiente de forma organica."
  [output-cast rows is-oltp? body]
  (let [is-oltp? (or is-oltp? (= (:output-cast body) :raw))
        columns (or (:columns body)
                    (when is-oltp? 
                      (if (seq rows)
                        (mapv (fn [k] {:key (name k) :type "string" :label (name k)}) (keys (first rows)))
                        [{:key "id" :type "string" :label "id"}])))
        row->kvs (fn [r] (if is-oltp? 
                           (into {} (map (fn [[k v]] [(keyword k) v]) r)) 
                           (into {} (map (fn [c v] [(keyword (:key c)) v]) columns r))))
        
        ;; Determinar el tipo de ECharts real (bar, line, scatter, pie, indicator)
        ;; Preferimos viz que viene propagado desde Janus AST
        viz-type (or (:viz body)
                     (case output-cast
                       :KPI "indicator"
                       :PIE "pie"
                       :TIMESERIES "timeseries"
                       :BUBBLE "scatter"
                       "table"))
        
        ;; Mapeo de la estrategia visual (type de ECharts) al payload de metri.proto (oneof)
        payload-strategy (cond
                           (#{"indicator" "kpi"} viz-type) :signal
                           (#{"pie" "donut" "bar"} viz-type) :breakdown
                           (#{"line" "scatter" "timeseries"} viz-type) :chart
                           :else :table)
        
        payload (case payload-strategy
                  :signal
                  {:signal (when (seq rows)
                             (extract-analytical-signal
                               (row->kvs (first rows))
                               columns
                               (:benchmarks body)))}
                  
                  :breakdown
                  (let [dim-k (keyword (:key (first columns)))]
                    {:breakdown {:signals (into {} (map (fn [r]
                                                          (let [kvs (row->kvs r)]
                                                            [(str (get kvs dim-k)) (extract-analytical-signal kvs (next columns))]))
                                                        rows))}})
                  
                  :chart
                  (let [x-dim  (some-> columns first :key name)
                        y-dims (some->> columns next (mapv #(name (:key %))))]
                    {:chart {:x-dimension x-dim
                             :y-dimensions y-dims
                             :show-tooltip true
                             :show-legend true}})
                  
                  :table
                  {:table {:columns (mapv (fn [c] {:key (:key c) :label (or (:label c) (:key c)) :type (or (:type c) "string") :sortable true}) columns)}})]
    (when payload
      {:type viz-type
       :payload payload})))

(defn- build-pagination
  "Construye el mensaje Pagination desde el mapa :pagination normalizado.
   Incluye los Links HATEOAS (first/last/next/prev) garantizados por el normalizer."
  [pag-map]
  (let [b (Pagination/newBuilder)]
    (when-let [nc (:next-cursor pag-map)]     (.setNextCursor b nc))
    (when-let [pc (:previous-cursor pag-map)] (.setPreviousCursor b pc))
    (when-let [ps (:page-size pag-map)]       (.setPageSize b (int ps)))
    (when (contains? pag-map :has-next)       (.setHasNext b (boolean (:has-next pag-map))))
    (when (contains? pag-map :has-previous)   (.setHasPrevious b (boolean (:has-previous pag-map))))
    ;; HATEOAS links dentro de Pagination (first/last/next/prev)
    (doseq [lnk (or (:links pag-map) [])]
      (.addLinks b (build-link lnk)))
    (.build b)))

(defn aegis-chunk->query-response
  "Convierte un chunk del stream Aegis -> QueryResponse proto.

   CONTRATO: el body YA fue normalizado por metres.janus.normalizer (Paso 7).
   Por lo tanto todos los campos estructurales (:status, :metadata, :pagination,
   :viz-ext, :links) estan garantizados - el translator solo serializa.

   Modo batch:  si :query-key presente -> anida en batch_results.
   Modo plano:  si no hay :query-key   -> respuesta directa (backward compat)."
  [[tag body]]
  (let [b (QueryResponse/newBuilder)]
    (case tag
      :ok
      (let [qr          (QueryResponse/newBuilder)
            rows        (or (:data body) [])
            rows        (if (and (seq rows) (vector? (first rows)))
                          (filterv #(some some? %) rows)
                          rows)
            body        (assoc body :data rows)
            ;; Normalizer garantiza :viz-ext; build-viz-meta-from-chunk es fallback legacy
            viz         (or (:viz-ext body)
                           (build-viz-meta-from-chunk (:output-cast body) rows
                                                      (and (seq rows) (map? (first rows)))
                                                      body))]

        ;; Status desde body normalizado
        (.setStatus qr (build-status body tag))
        ;; RowSet
        (.setData qr (rows->rowset body))
        ;; QueryMetadata enriquecida (lee :metadata del body)
        (.setMetadata qr (build-query-metadata body))
        ;; Pagination + HATEOAS links dentro de Pagination
        (when-let [pag (:pagination body)]
          (.setPagination qr (build-pagination pag)))
        ;; VizMeta
        (when viz
          (.setVizExt qr (build-viz-meta viz)))
        ;; Links HATEOAS nivel raiz (self, refresh) - garantizados por normalizer
        (doseq [lnk (or (:links body) [])]
          (.addLinks qr (build-link lnk)))

        (if-let [qk (:query-key body)]
          ;; Modo batch: anidar en batch_results
          (do (.setStatus b (build-status body tag))
              (.putBatchResults b (name qk) (.build qr)))
          ;; Modo plano: propagar todos los campos al builder raiz
          (do (.setStatus b (build-status body tag))
              (.setData b (.getData qr))
              (.setMetadata b (.getMetadata qr))
              (when (.hasPagination qr) (.setPagination b (.getPagination qr)))
              (when (.hasVizExt qr) (.setVizExt b (.getVizExt qr)))
              (doseq [lnk (.getLinksList qr)] (.addLinks b lnk)))))

      :error
      (.setStatus b (build-status body tag)))
    (.build b)))

;; -----------------------------------------------------------------------------
;; MDULO: EDA - MatchRoutingRulesBatch
;; -----------------------------------------------------------------------------

(declare map->match-response map->matched-rule)

(defn match-batch-request->ctx
  [^MatchRoutingRulesBatchRequest req]
  {:requests (mapv (fn [^MatchRoutingRulesRequest r]
                     {:tenant-id    (.getTenantId r)
                      :entity-name  (.getEntityName r)
                      :trigger-type (.getTriggerType r)
                      :cdc-payload  (.getCdcPayloadJson r)})
                   (.getRequestsList req))})

(defn match-batch-result->response
  "Railway result -> MatchRoutingRulesBatchResponse protobuf.
   Usa :status del body normalizado. :responses garantizado por normalizer."
  [[tag body]]
  (let [b (MatchRoutingRulesBatchResponse/newBuilder)]
    (.setStatus b (build-status body tag))
    (when (= tag :ok)
      (doseq [r (or (:responses body) [])]
        (.addResponses b (map->match-response r))))
    (.build b)))

(defn- map->match-response
  "Serializa un item de :responses de MatchRoutingRulesBatchResponse.
   Usa :status normalizado si esta presente; fallback por :error."
  [m]
  (let [b     (MatchRoutingRulesResponse/newBuilder)
        tag   (if (:error m) :error :ok)
        body  (if (:error m) (:error m) m)]
    (.setStatus b (build-status body tag))
    (doseq [rule (or (:matched-rules m) [])]
      (.addMatchedRules b (map->matched-rule rule)))
    (.build b)))

(defn- map->webhook-target [m]
  (let [b (WebhookTarget/newBuilder)]
    (.setTargetUrl         b (or (:target-url m) ""))
    (.setHttpMethod        b (or (:http-method m) "POST"))
    (.setAuthType          b (or (:auth-type m) "NONE"))
    (.setResolvedAuthSecret b (or (:resolved-auth-secret m) ""))
    (.setMaxRetries        b (int (or (:max-retries m) 0)))
    (.setTimeoutSeconds    b (int (or (:timeout-seconds m) 10)))
    (doseq [[k v] (:headers m)]
      (.putHeaders b (name k) (str v)))
    (.build b)))

(defn- map->matched-rule [m]
  (let [b (MatchedRule/newBuilder)]
    (.setRuleCode         b (or (:rule-code m) ""))
    (.setDetailTypeOutput b (or (:detail-type-output m) ""))
    (.setPriority         b (int (or (:priority m) 0)))
    (.setName             b (or (:name m) ""))
    (doseq [w (:webhooks m)]
      (.addWebhooks b (map->webhook-target w)))
    ;; FilterNode -> condition (requiere un conversor desde ast, temporalmente omitido si no hay ast->proto para FilterNode o implementar basico)
    (when (:condition m)
      nil)
    (.build b)))
