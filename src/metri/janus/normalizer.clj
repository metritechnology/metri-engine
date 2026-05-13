(ns metri.janus.normalizer
  "Janus Output Contract Guarantor - Cobertura 100% de TODOS los Response del contrato.

   Cubre los 7 tipos de respuesta definidos en janus-ast-ir.edn / metres.proto:
     :query-response                   -> QueryResponse (streaming)
     :discovery-response               -> DiscoveryResponse (unary)
     :explore-response                 -> ExploreResponse (unary)
     :transaction-response             -> TransactionResponse (unary, IOP)
     :bulk-response                    -> BulkResponse (unary, IOP)
     :match-routing-rules-response     -> MatchRoutingRulesResponse (unary)
     :match-routing-rules-batch-response -> MatchRoutingRulesBatchResponse (unary)

   ARQUITECTURA DE CAPAS:
     janus/core.clj       -> normalize-chunk       (QueryResponse streaming, Paso 7)
     janus/core.clj       -> normalize-unary       (Discovery, Explore, Match - Janus-owned)
     iop/core.clj (future)-> normalize-unary       (Transact, Bulk - IOP-owned)

   PRINCIPIOS SOLID:
     SRP: un unico modulo responsable del contrato de salida
     OCP: agregar nuevo Response = nuevo defmethod, sin tocar lo existente
     DIP: las capas dependen de normalize-* (abstraccion), no de Protobuf"
  (:require [taoensso.timbre :as log]
            [metri.aegis.label-template :as lt])
  (:import [java.util UUID]))

;; =============================================================================
;; Helpers compartidos
;; =============================================================================

(defn- safe-double [v]
  (if (string? v)
    (try (Double/parseDouble v) (catch Exception _ 0.0))
    (double (or v 0.0))))

(defn- new-query-id []
  (str (UUID/randomUUID)))

(defn- current-epoch-ms []
  (System/currentTimeMillis))

;; =============================================================================
;; BLOQUE 1 - Status (comun a TODOS los responses)
;; =============================================================================

(defn ensure-status
  "Garantiza que :status este presente en cualquier body de respuesta.
   - tag :ok    -> { :success true }
   - tag :error -> { :success false, :error-code, :error-message }"
  [body tag]
  (if (= tag :ok)
    (update body :status #(merge {:success true} %))
    (let [err (or (:error body) body)
          msg-base (or (:message err) (:reason err) (:detail err) "Error interno")
          message  (if (and (:reason err) (:detail err))
                     (str (:reason err) " | " (:detail err))
                     msg-base)]
      (assoc body :status
             {:success       false
              :error-code    (or (some-> err :code name) "INTERNAL_ERROR")
              :error-message message}))))

;; =============================================================================
;; BLOQUE 2 - QueryResponse: metadata, pagination, HATEOAS, VizMeta, Intelligence
;; (Exclusivo para el tipo :query-response)
;; =============================================================================

(defn- ensure-metadata [chunk]
  (let [channel     (or (:channel chunk) :unknown)
        engine-str  (case channel :olap "olap" :oltp "oltp" "unknown")
        existing    (or (:metadata chunk) {})
        start-ts    (:start-ts chunk)
        exec-ms     (when start-ts (- (current-epoch-ms) start-ts))
        total       (or (:total chunk) 0)]
    (assoc chunk :metadata
           (merge {:engine             engine-str
                   :query-id           (new-query-id)
                   :total-count        total
                   :total-queries      1
                   :parallelism-factor 1.0
                   :cache-hits         0}
                  existing
                  (when exec-ms {:execution-time-ms exec-ms})))))

(defn- build-hateoas-pagination-links
  [{:keys [has-next has-previous next-cursor previous-cursor tenant-id query-key]}]
  (let [base-href (fn [cursor]
                    (str "/query?tenant_id=" (or tenant-id "")
                         "&cursor=" (or cursor "")
                         (when query-key (str "&query_key=" (name query-key)))))]
    (cond-> []
      true         (conj {:rel "first"  :href (base-href "")           :method "POST"})
      true         (conj {:rel "last"   :href (base-href "LAST")        :method "POST"})
      has-next     (conj {:rel "next"   :href (base-href next-cursor)   :method "POST"})
      has-previous (conj {:rel "prev"   :href (base-href previous-cursor) :method "POST"}))))

(defn- ensure-pagination [chunk]
  (let [limit     (:limit chunk)
        total     (or (get-in chunk [:metadata :total-count]) (:total chunk) 0)
        existing  (:pagination chunk)
        page-size (or (:page-size existing) limit)
        has-next  (if (contains? existing :has-next)
                    (:has-next existing)
                    (and (some? page-size) (> total page-size)))
        has-prev  (if (contains? existing :has-previous)
                    (:has-previous existing)
                    (boolean (:previous-cursor existing)))
        pag-map   (when (or limit existing)
                    (let [base (merge {:page-size    (or page-size 0)
                                       :has-next     (boolean has-next)
                                       :has-previous (boolean has-prev)}
                                      existing)]
                      (assoc base :links
                             (or (seq (:links existing))
                                 (build-hateoas-pagination-links
                                   (merge base
                                          {:tenant-id (:tenant-id chunk)
                                           :query-key (:query-key chunk)}))))))]
    (if pag-map (assoc chunk :pagination pag-map) chunk)))

(defn- ensure-hateoas-links [chunk]
  (let [existing  (or (:links chunk) [])
        rels      (set (map :rel existing))
        base-href (str "/query?tenant_id=" (or (:tenant-id chunk) ""))
        defaults  (cond-> []
                    (not (contains? rels "self"))
                    (conj {:rel "self"    :href base-href :method "POST"})
                    (not (contains? rels "refresh"))
                    (conj {:rel "refresh" :href base-href :method "POST"}))]
    (assoc chunk :links (into existing defaults))))

(defn- infer-viz-type [output-cast viz-hint]
  (or viz-hint
      (case output-cast
        :KPI        "indicator"
        :PIE        "pie"
        :TIMESERIES "line"
        :BUBBLE     "scatter"
        :TABLE      "table"
        :CSV_EXPORT "table"
        nil         "table"
                    "table")))

(defn- extract-columns [body is-oltp?]
  (or (:columns body)
      (when (and is-oltp? (seq (:data body)) (map? (first (:data body))))
        (mapv (fn [k] {:key (name k) :label (name k) :sortable true})
              (keys (first (:data body)))))))

(defn- build-table-meta [columns]
  {:columns      (mapv (fn [c]
                         (let [k (or (:key c) (name c))]
                           (merge {:key k :label k :sortable true :type "string"}
                                  (when (map? c) c))))
                       (or columns []))
   :row-actions  []
   :global-links []})

(defn- build-chart-meta
  "Construye ChartDecoration meta para LINE/AREA/SCATTER/TIMESERIES.
   ANO-005 fix: incluye :fill-gaps true para TIMESERIES, garantizando
   continuidad visual cuando hay buckets sin datos."
  [columns & [output-cast]]
  (let [col-names  (mapv #(or (:key %) (name %)) columns)
        timeseries? (= output-cast :TIMESERIES)]
    (cond-> {:x-dimension  (first col-names)
             :y-dimensions (vec (rest col-names))
             :show-tooltip true
             :show-legend  true}
      timeseries? (assoc :fill-gaps true))))

(defn- build-scatter-chart-meta
  "SCATTER-fix: construye ChartDecoration para BUBBLE/scatter usando las
   definiciones semánticas del AST IR en lugar de la posición de columnas.

   Estrategia de resolución (por orden de prioridad):
   1. :ast-dimensions y :ast-metrics del chunk  → fuente de verdad del AST
   2. Columnas con flag :is-dimension / :is-measure  → anotadas por derive-columns
   3. Fallback posicional: primera columna = x, resto = y (comportamiento legacy)

   Garantiza que x-dimension e y-dimensions siempre se populen aunque los
   datos estén vacíos (el AST siempre tiene la definición completa)."
  [columns ast-dims ast-metrics]
  (cond
    ;; Prioridad 1: definiciones semánticas del AST IR
    (and (seq ast-dims) (seq ast-metrics))
    (let [x-dim  (or (:attribute (first ast-dims)) (:name (first ast-dims)))
          y-dims (mapv (fn [m]
                         (or (:name m)
                             (when (and (:aggregation m) (:attribute m))
                               (str (clojure.string/lower-case (name (:aggregation m)))
                                    "_" (:attribute m)))
                             (:attribute m)))
                       ast-metrics)]
      {:x-dimension  x-dim
       :y-dimensions (filterv some? y-dims)
       :show-tooltip true
       :show-legend  true})

    ;; Prioridad 2: columnas anotadas con :is-dimension/:is-measure
    (and (seq columns) (some :is-dimension columns))
    (let [dim-cols  (filterv :is-dimension columns)
          meas-cols (filterv :is-measure   columns)]
      {:x-dimension  (some-> dim-cols first :key)
       :y-dimensions (mapv :key meas-cols)
       :show-tooltip true
       :show-legend  true})

    ;; Fallback posicional (legacy)
    :else
    (let [col-names (mapv #(or (:key %) (name %)) columns)]
      {:x-dimension  (first col-names)
       :y-dimensions (vec (rest col-names))
       :show-tooltip true
       :show-legend  true})))

(defn- build-breakdown-meta
  "Construye BreakdownSignal para PIE/DONUT.
   Si template es non-nil, el nombre de cada slice se interpola contra la row completa.
   Ejemplo: '{{asset_name}} - {{area_value}} KW' → 'Pump A - 45.20 KW'."
  [rows columns & [template]]
  (let [dim-col (first columns)
        dim-k   (when dim-col (keyword (or (:key dim-col) (name dim-col))))
        val-col (second columns)
        val-k   (when val-col (keyword (or (:key val-col) (name val-col))))]
    {:signals
     (into {}
           (map (fn [r]
                  (let [is-map? (map? r)
                        kvs     (if is-map?
                                  (into {} (map (fn [[k v]] [(keyword k) v]) r))
                                  (into {} (map (fn [c v] [(keyword (or (:key c) (name c))) v]) columns r)))
                        ;; Nombre del slice: template > valor raw de la dimensión
                        dim-val (if (and template (seq template))
                                  (or (lt/interpolate template kvs)
                                      (str (get kvs dim-k "unknown")))
                                  (str (get kvs dim-k "unknown")))
                        val     (safe-double (get kvs val-k 0.0))]
                    [dim-val {:value val}]))
                (or rows [])))}))

(defn- ensure-viz-meta [chunk]
  (if (:viz-ext chunk)
    chunk
    (let [output-cast (:output-cast chunk)
          viz-hint    (:viz chunk)
          viz-type    (infer-viz-type output-cast viz-hint)
          rows        (or (:data chunk) [])
          is-oltp?    (and (seq rows) (map? (first rows)))
          columns     (extract-columns chunk is-oltp?)
          ;; label-template: propagado desde el AST IR a través del chunk
          label-tmpl  (:label-template chunk)
          ;; SCATTER-fix: definiciones semánticas del AST propagadas por los ejecutores
          ast-dims    (:ast-dimensions chunk)
          ast-metrics (:ast-metrics chunk)
          payload-kw  (cond
                        (#{"indicator" "kpi" "gauge"} viz-type) :signal
                        (#{"pie" "donut"}              viz-type) :breakdown
                        (#{"line" "bar" "area" "scatter" "timeseries"} viz-type) :chart
                        :else :table)
          payload     (case payload-kw
                        :signal    {:signal (if (seq rows)
                                              (let [row (first rows)
                                                    vals-seq (if is-oltp? (vals row) row)
                                                    v (first vals-seq)
                                                    has-prev? (> (count columns) 1)
                                                    prev (when has-prev? (second vals-seq))]
                                                (cond-> {:value (safe-double v)}
                                                  has-prev? (assoc :previous-value (safe-double prev))))
                                              {:value 0.0})}
                        ;; PIE: pasa el template para interpolación de nombres de slices
                        :breakdown {:breakdown (build-breakdown-meta rows columns label-tmpl)}
                        ;; SCATTER: usa build-scatter-chart-meta para ejes semánticos correctos
                        ;; LINE/AREA/TIMESERIES: usa build-chart-meta posicional con fill-gaps
                        :chart     {:chart (let [scatter? (= output-cast :BUBBLE)
                                                chart-m  (if scatter?
                                                           (build-scatter-chart-meta
                                                             (or columns []) ast-dims ast-metrics)
                                                           (build-chart-meta
                                                             (or columns []) output-cast))]
                                             (cond-> chart-m
                                               (seq label-tmpl) (assoc :label-template label-tmpl)))}
                        :table     {:table (build-table-meta columns)})]
      (assoc chunk :viz-ext {:type viz-type :payload payload}))))
(defn- enrich-viz-intelligence [chunk]
  (let [signal    (get-in chunk [:viz-ext :payload :signal])
        benchmarks (:benchmarks chunk)
        bench     (when (seq benchmarks)
                    (first (filter #(= :BENCHMARK (:type %)) benchmarks)))
        bench-val (when bench (safe-double (:benchmark-value bench 0.0)))]
    (cond
      ;; ── Caso 1: TIME_SHIFT — hay previous-value en signal, calcular delta ──
      (and signal (:previous-value signal) (nil? (:intelligence signal)))
      (let [curr  (safe-double (:value signal))
            prev  (safe-double (:previous-value signal))
            delta (- curr prev)
            pct   (if (zero? prev)
                    (cond (pos? curr) 100.0 (neg? curr) -100.0 :else 0.0)
                    (* 100.0 (/ delta (Math/abs ^double prev))))
            dir   (cond (> delta 0.001) "up" (< delta -0.001) "down" :else "neutral")]
        (assoc-in chunk [:viz-ext :payload :signal :intelligence]
                  {:direction      dir
                   :percentage     (double pct)
                   :delta-abs      (double delta)
                   :previous-value prev
                   :label          (str (if (pos? pct) "+" "") (format "%.1f%%" pct))}))

      ;; ── Caso 2: BENCHMARK — no hay previous-value pero hay benchmark metadata ──
      ;; GUARD: solo si :value es número real (not nil) — nil = sin datos → omitir
      (and signal (nil? (:previous-value signal)) bench (some? bench-val)
           (some? (:value signal))       ;; ← sin datos → no comparar
           (nil? (:intelligence signal)))
      (let [curr  (safe-double (:value signal))
            delta (- curr bench-val)
            pct   (if (zero? bench-val)
                    (cond (pos? curr) 100.0 (neg? curr) -100.0 :else 0.0)
                    (* 100.0 (/ delta (Math/abs ^double bench-val))))
            dir   (cond (> delta 0.001) "up" (< delta -0.001) "down" :else "neutral")]
        (-> chunk
            (assoc-in [:viz-ext :payload :signal :previous-value] bench-val)
            (assoc-in [:viz-ext :payload :signal :intelligence]
                      {:direction      dir
                       :percentage     (double pct)
                       :delta-abs      (double delta)
                       :previous-value bench-val
                       :label          (str (if (pos? pct) "+" "") (format "%.1f%%" pct))})))

      ;; ── Caso 3: nada que enriquecer ─────────────────────────────────────────
      :else chunk)))

(defn- ensure-table-actions [chunk]
  (if (get-in chunk [:viz-ext :payload :table])
    (-> chunk
        (update-in [:viz-ext :payload :table :row-actions]  #(or % []))
        (update-in [:viz-ext :payload :table :global-links] #(or % [])))
    chunk))

;; =============================================================================
;; BLOQUE 3 - Discovery-specific
;; =============================================================================

(defn- ensure-discovery-defaults [body]
  "Garantiza campos requeridos de DiscoveryResponse:
   :schemas [] (array nunca nil), :has-next false, :next-cursor vacio"
  (-> body
      (update :schemas #(or % []))
      (update :has-next #(boolean %))
      (update :next-cursor #(or % ""))))

;; =============================================================================
;; BLOQUE 4 - Explore-specific
;; =============================================================================

(defn- ensure-explore-defaults [body]
  "Garantiza :values [] (array nunca nil) en ExploreResponse"
  (update body :values #(or % [])))

;; =============================================================================
;; BLOQUE 5 - Transaction-specific
;; =============================================================================

(defn- ensure-transaction-defaults [body]
  "Garantiza :entity-id siempre string en TransactionResponse"
  (update body :entity-id #(or % "")))

;; =============================================================================
;; BLOQUE 6 - Bulk-specific
;; =============================================================================

(defn- ensure-bulk-defaults [body]
  "Garantiza :ingested-count y :outbox-count siempre int en BulkResponse"
  (-> body
      (update :ingested-count #(or % 0))
      (update :outbox-count   #(or % 0))))

;; =============================================================================
;; BLOQUE 7 - MatchRoutingRules-specific
;; =============================================================================

(defn- ensure-match-defaults [body]
  "Garantiza :matched-rules [] (array nunca nil) en MatchRoutingRulesResponse"
  (update body :matched-rules #(or % [])))

(defn- ensure-match-batch-defaults [body]
  "Garantiza :responses [] (array nunca nil) en MatchRoutingRulesBatchResponse"
  (update body :responses #(or % [])))

;; =============================================================================
;; API PUBLICA - Multimethod normalize-response
;; =============================================================================

(defmulti normalize-response
  "Normaliza cualquier respuesta del contrato janus-ast-ir.edn al 100% de cobertura.

   Dispatch por :response-type en el body. Si no existe, inferido por estructura.

   Metodos disponibles:
     :query-response                     -> QueryResponse (Status + Meta + Pagination + VizMeta)
     :discovery-response                 -> DiscoveryResponse (Status + schemas + cursor)
     :explore-response                   -> ExploreResponse (Status + values)
     :transaction-response               -> TransactionResponse (Status + entity-id)
     :bulk-response                      -> BulkResponse (Status + counts)
     :match-routing-rules-response       -> MatchRoutingRulesResponse (Status + matched-rules)
     :match-routing-rules-batch-response -> MatchRoutingRulesBatchResponse (Status + responses)
     :default                            -> solo garantiza Status"
  (fn [[_tag body]]
    (or (:response-type body)
        (cond
          ;; Inferencia por claves diagnosticas unicas
          (or (contains? body :data) (contains? body :viz-ext)
              (contains? body :query-key) (contains? body :output-cast))
          :query-response

          (contains? body :schemas)
          :discovery-response

          (contains? body :values)
          :explore-response

          (contains? body :entity-id)
          :transaction-response

          (or (contains? body :ingested-count) (contains? body :outbox-count))
          :bulk-response

          (contains? body :responses)
          :match-routing-rules-batch-response

          (contains? body :matched-rules)
          :match-routing-rules-response

          :else :default))))

;; ---- QueryResponse (streaming, Janus Paso 7) ---------------------------------

(defmethod normalize-response :query-response
  [[tag body]]
  (try
    [tag (-> body
             (ensure-status tag)
             ensure-metadata
             ensure-pagination
             ensure-hateoas-links
             ensure-viz-meta
             enrich-viz-intelligence
             ensure-table-actions)]
    (catch Exception e
      (log/error e "[Normalizer:QueryResponse] Error en pipeline" {:tag tag})
      [tag body])))

;; ---- DiscoveryResponse (unary, Janus-owned) ----------------------------------

(defmethod normalize-response :discovery-response
  [[tag body]]
  (try
    [tag (-> body
             (ensure-status tag)
             ensure-discovery-defaults)]
    (catch Exception e
      (log/error e "[Normalizer:DiscoveryResponse] Error" {:tag tag})
      [tag body])))

;; ---- ExploreResponse (unary, Janus-owned) ------------------------------------

(defmethod normalize-response :explore-response
  [[tag body]]
  (try
    [tag (-> body
             (ensure-status tag)
             ensure-explore-defaults)]
    (catch Exception e
      (log/error e "[Normalizer:ExploreResponse] Error" {:tag tag})
      [tag body])))

;; ---- TransactionResponse (unary, IOP-owned) ----------------------------------

(defmethod normalize-response :transaction-response
  [[tag body]]
  (try
    [tag (-> body
             (ensure-status tag)
             ensure-transaction-defaults)]
    (catch Exception e
      (log/error e "[Normalizer:TransactionResponse] Error" {:tag tag})
      [tag body])))

;; ---- BulkResponse (unary, IOP-owned) -----------------------------------------

(defmethod normalize-response :bulk-response
  [[tag body]]
  (try
    [tag (-> body
             (ensure-status tag)
             ensure-bulk-defaults)]
    (catch Exception e
      (log/error e "[Normalizer:BulkResponse] Error" {:tag tag})
      [tag body])))

;; ---- MatchRoutingRulesResponse (unary, Janus-owned) --------------------------

(defmethod normalize-response :match-routing-rules-response
  [[tag body]]
  (try
    [tag (-> body
             (ensure-status tag)
             ensure-match-defaults)]
    (catch Exception e
      (log/error e "[Normalizer:MatchResponse] Error" {:tag tag})
      [tag body])))

;; ---- MatchRoutingRulesBatchResponse (unary, Janus-owned) ---------------------

(defmethod normalize-response :match-routing-rules-batch-response
  [[tag body]]
  (try
    [tag (-> body
             (ensure-status tag)
             ensure-match-batch-defaults)]
    (catch Exception e
      (log/error e "[Normalizer:MatchBatchResponse] Error" {:tag tag})
      [tag body])))

;; ---- Default: solo garantiza Status (fallback seguro) -----------------------

(defmethod normalize-response :default
  [[tag body]]
  [tag (ensure-status body tag)])

;; =============================================================================
;; Aliases de conveniencia - compatibilidad con Janus Pipeline (Paso 7)
;; =============================================================================

(defn normalize-chunk
  "Alias para normalize-response con semantica de chunk streaming (QueryResponse).
   Usado en janus/core.clj Paso 7.
   Garantiza que el body tenga :response-type :query-response para dispatch exacto."
  [[tag body :as chunk]]
  (normalize-response [tag (assoc body :response-type :query-response)]))

(defn normalize-unary
  "Normaliza una respuesta unaria ([:ok body] | [:error body]).
   Infierre el tipo de response automaticamente desde las claves del body.
   Usado en wrap-unary-read-path para Discovery, Explore, Match."
  [chunk]
  (normalize-response chunk))

(defn normalize-chunks
  "Aplica normalize-chunk a un vector de chunks (modo batch)."
  [chunks]
  (mapv normalize-chunk chunks))
