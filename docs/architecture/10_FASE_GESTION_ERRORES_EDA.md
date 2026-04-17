# FASE 10: GESTIÓN DE ERRORES Y OBSERVABILIDAD

Cuatro pilares garantizan que cada anomalía sea un **activo analizable**:

1. **Railway Pattern** — Zero-Exception en el núcleo funcional.
2. **Rich Context Error DTO** — Empaquetado forense con user / tenant / causa.
3. **OpenTelemetry (Metri Trace)** — Span por fase del pipeline, sin vendor lock-in.
4. **Sherlog Pipeline** — Errores `WARNING+` como eventos EDA persistidos en OLAP.

---

## MÓDULO I: Railway Pattern — Zero-Exception

Ninguna capa de lógica de negocio lanza excepciones. Las librerías externas se capturan
en la **frontera del namespace** y se convierten en la estructura controlada.

### I.1 — Contrato Railway (Clojure)

```clojure
;; Flujo exitoso
[:ok {:ulid "01HZ4K..." :channel :oltp :tx-id <db-after>}]

;; Flujo fallido — NUNCA nil, NUNCA throw
[:error {:stage    :janus
         :code     :JNS_REF_002       ;; del catálogo maestro
         :trace-id "4bf92f35..."      ;; W3C — correlación OTel
         :tenant-id "uuid-acme"
         :user-id   "uuid-tech-01"
         :detail  "location_id expected :location but got :invoice"}]
```

> [!IMPORTANT]
> `trace-id`, `tenant-id` y `user-id` son **obligatorios** en todo `[:error ...]`.
> Sin ellos es imposible responder: _¿a quién le pasó?_, _¿de qué tenant?_, _¿en qué traza?_

### I.2 — Contrato Railway (TypeScript — MCP)

```typescript
type Result<T> = { ok: true; data: T } | { ok: false; error: MetriError };

interface MetriError {
  code: string; // del catálogo maestro
  traceId: string; // W3C traceparent
  tenantId: string;
  userId?: string;
  stage: string;
  detail: string;
  timestamp: number;
}
// NUNCA: throw new Error("...") — SIEMPRE: return { ok: false, error: {...} }
```

### I.3 — Frontera de captura (único `try/catch` en OLTPChannel)

```clojure
;; ns: metri.janus.channels.oltp
(defn- execute-transaction! [conn tx-data]
  (try
    [:ok (d/transact conn {:tx-data tx-data})]
    (catch Exception e
      [:error {:stage  :janus
               :code   :JNS_TX_001
               :detail (ex-message e)
               :cause  (ex-data e)}])))
```

---

## MÓDULO II: OpenTelemetry — Un Span por Fase

Todo request produce un árbol de spans correlacionados por `trace_id` (W3C).

> [!IMPORTANT]
> El `trace_id` conecta: cliente → Cedar ABAC → JanusRouter → OLTPChannel →
> Datahike → OTel spans (ADOT) → `domain_fault` en Athena.

### II.1 — Árbol de Spans (TX OLTP — `rpc Transact`)

```
Span ROOT: "iop.transact"              [trace_id: 4bf92f35...]
  │  attrs: tenant_id, user_id, entity_type, operation
  ├── "janus.load-schema"              attrs: entity_type, schema_hash
  ├── "janus.validate-payload"         attrs: field_count, violation_count
  ├── "janus.verify-entity-refs"       attrs: ref_count, missing_refs
  ├── "janus.enrich-payload"           attrs: strategy, scope_tag
  └── "janus.execute-transaction"      attrs: tx_fact_count, db_after
```

### II.2 — Implementación OTel en el pipeline

```clojure
;; ns: metri.janus.core — span ROOT con contexto de seguridad
(defn route [ctx deps]
  (otel/with-span ["iop.transact" {:kind :server}]
    (let [span (otel/current-span)]
      (otel/set-attributes! span
        {"tenant.id"  (:tenant-id ctx)
         "user.id"    (:user-id ctx)
         "entity.type" (name (:entity-type ctx))
         "operation"  (name (:operation ctx))})
      ;; ... pipeline completo con spans anidados
      )))

;; ns: metri.janus.channels.oltp — span hijo por función privada
(defn- verify-entity-refs [conn schema payload]
  (otel/with-span ["janus.verify-entity-refs" {:kind :internal}]
    (let [span   (otel/current-span)
          result (run-batch-query conn schema payload)]
      (otel/set-attributes! span {"ref.count" (count (:ref-attrs schema))
                                  "ref.query.batch" true})
      (if (seq (:violations result))
        (do (otel/set-status! span :error "FK validation failed")
            (otel/set-attributes! span {"error.code" (name (-> result :violations first :code))})
            [:error {:stage :janus :code :JNS_REF_001 :violations (:violations result)}])
        (do (otel/set-status! span :ok)
            [:ok])))))
```

### II.3 — OTel Spans: ADOT → Kinesis → Athena

Los spans son infraestructura OTel — **no son entidades Metri**.

```
metri-engine Lambda
    └─ ADOT Lambda Layer (SHUTDOWN)
           └─ OTLP → Kinesis Data Firehose "metri-telemetry-spans"
                  └─ S3 Parquet → Athena (tabla externa OTLP nativa)
```

> [!NOTE]
> `telemetry_span.json` no existe en Metri. La correlación con `domain_fault`
> ocurre en Athena via `trace_id` (W3C) — ambas tablas comparten ese campo.

---

## MÓDULO III: Catálogo de Errores (SSOT)

**Un único archivo EDN** es la fuente autoritativa de todos los errores del sistema.
Si el código no está en el catálogo, no existe en el sistema.

### III.1 — Archivo de referencia

> [!IMPORTANT]
> **Fuente de verdad:** [`resources/errors/error_catalog.edn`](../../resources/errors/error_catalog.edn)
>
> No se documenta inline — editarlo directamente es suficiente. Duplicarlo aquí viola DRY.

Cada entrada tiene este contrato:

```edn
{:code        :JNS_TX_001
 :family      :jns             ;; jns | cod | sec | sys | mcp
 :stage       :janus
 :severity    :error           ;; info | warning | error | fatal
 :http-status 500
 :grpc-status :INTERNAL
 :description "Datahike d/transact failed — transaction rolled back"
 :context-required [:tx_data_size :datahike_error]
 :retryable?  true}            ;; Echo puede recuperar si true — catálogo decide
```

| Familia | Stage                | Retryable | Ejemplos                                                             |
| :------ | :------------------- | :-------- | :------------------------------------------------------------------- |
| `JNS`   | `:janus`             | Mix       | `JNS_REF_001/002`, `JNS_TX_001`, `JNS_CONFLICT_001`, `JNS_SCOPE_001` |
| `COD`   | `:codice`            | Solo KMS  | `COD_001`, `COD_VAL_001`, `COD_KMS_001`, `COD_SCOPE_001`             |
| `SEC`   | `:auth/:abac/:quota` | Nunca     | `SEC_401`, `SEC_403`, `SEC_QTA_001`                                  |
| `SYS`   | `:infra/:kinesis`    | Siempre   | `SYS_000`, `SYS_KNS_001`                                             |
| `MCP`   | `:mcp`               | Siempre   | `MCP_503`                                                            |

### III.2 — Carga y validación en Bootstrap

```clojure
;; ns: metri.common.errors
(defonce ^:private error-catalog (atom nil))

;; En producción: (load-catalog!)                         → lee de io/resource
;; En tests:      (load-catalog! #(pr-str test-catalog))  → stub sin classpath
(defn load-catalog!
  "Falla si faltan campos obligatorios — JVM no arranca. Llamado UNA VEZ por Bootstrapper."
  ([]  (load-catalog! #(slurp (io/resource "errors/error_catalog.edn"))))
  ([reader-fn]
   (let [raw     (edn/read-string (reader-fn))
         entries (get-in raw [:catalog :entries])]
     (doseq [entry entries]
       (when-not (every? #(contains? entry %)
                         [:code :family :stage :severity :http-status
                          :grpc-status :description :context-required])
         (throw (ex-info "Invalid error catalog entry" {:entry entry}))))
     (reset! error-catalog (into {} (map (fn [e] [(:code e) e]) entries)))
     (log/info "Error catalog loaded" {:entries (count entries)}))))

(defn lookup [code]
  (or (get @error-catalog code)
      (throw (ex-info "Error code not in catalog — add to error_catalog.edn first"
                      {:code code}))))

;; En producción: (errors/error :JNS_REF_001 ctx)
;; En tests:      (errors/error :JNS_REF_001 ctx stub-lookup)
(defn error
  ([code ctx-map]          (error code ctx-map lookup))
  ([code ctx-map lookup-fn]
   (let [entry   (lookup-fn code)
         missing (remove #(contains? ctx-map %) (:context-required entry))]
     (when (seq missing)
       (log/warn "Incomplete error context" {:code code :missing missing}))
     [:error (merge {:code   code
                     :stage  (:stage entry)
                     :detail (:description entry)}
                    ctx-map)])))
```

### III.3 — Uso en componentes

```clojure
;; ANTES — string ad-hoc (PROHIBIDO):
[:error {:code "JNS_REF_001" :detail "not found"}]

;; DESPUÉS — desde el catálogo, validado en runtime:
(errors/error :JNS_REF_002
              {:field         "location_id"
               :expected_type :location
               :actual_type   :invoice
               :tenant-id     (:tenant-id ctx)
               :user-id       (:user-id ctx)
               :trace-id      (otel/trace-id (otel/current-span))})
```

> [!IMPORTANT]
> `errors/error` es el **único constructor** de mónadas `[:error]`.
> Cualquier `[:error {:code "..."}]` inline es un bug.

---

## MÓDULO IV: Rich Context Error DTO

Cuando `[:error]` escala a la capa gRPC, se construye un DTO forense
que permite reconstruir el fallo sin logs adicionales.

### IV.1 — Estructura del DTO

```json
{
  "status": "error",
  "error": {
    "code": "JNS_REF_002",
    "description": "location_id expected :location but got :invoice",
    "correlation_id": "REQ-4bf92f35",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "span_id": "00f067aa0ba902b7",
    "tenant_id": "uuid-acme",
    "user_id": "uuid-tech-01",
    "timestamp": 1718221000,
    "stage": "janus.verify-entity-refs",
    "retryable": false,
    "context": {
      "entity_type": "work_order",
      "field": "location_id",
      "expected_type": "location",
      "actual_type": "invoice"
    }
  }
}
```

| Pregunta        | Campo                        | Fuente                                    |
| :-------------- | :--------------------------- | :---------------------------------------- |
| ¿A qué usuario? | `user_id`                    | Cedar ctx — inyectado por el IOP          |
| ¿De qué tenant? | `tenant_id`                  | Cedar ctx — nunca del payload del cliente |
| ¿Por qué?       | `code` + `stage` + `context` | Railway `[:error]` propagado desde la fn  |
| ¿En qué traza?  | `trace_id` + `span_id`       | OTel span activo en el momento del error  |

> [!WARNING]
> Campos `sensitive: true` del schema → `"[REDACTED]"` en `context`. Nunca el valor real.

### IV.2 — Constructor del DTO

```clojure
;; ns: metri.iop.error-response — invocado SOLO en la capa gRPC
(defn build-error-dto [error-map ctx schema]
  (let [span     (otel/current-span)
        trace-id (otel/trace-id span)
        span-id  (otel/span-id span)]
    {:status "error"
     :error  {:code           (name (:code error-map))
              :description    (:detail error-map)
              :trace_id       trace-id
              :span_id        span-id
              :correlation_id (str "REQ-" (subs trace-id 0 8))
              :tenant_id      (:tenant-id ctx)
              :user_id        (:user-id ctx)
              :timestamp      (System/currentTimeMillis)
              :stage          (name (:stage error-map))
              :retryable      (-> (errors/lookup (:code error-map)) :retryable?)
              :context        (sanitize-context (:context error-map) schema)}}))

(defn- sanitize-context [context schema]
  (if (or (nil? context) (nil? schema))
    context
    (let [sensitive-keys (->> (:attributes schema)
                              (filter :sensitive)
                              (map (comp keyword :name))
                              set)]
      (reduce (fn [ctx k]
                (if (contains? sensitive-keys k)
                  (assoc ctx k "[REDACTED]")
                  ctx))
              context (keys context)))))
```

---

## MÓDULO V: Sherlog — Pipeline EDA de Errores

Errores `WARNING+` son transmutados en **eventos `DOMAIN_FAULT_DETECTED`** en EventBridge
y persistidos en `domain_fault` (OLAP) para análisis en Athena.

### V.1 — Umbral de severidad y transporte

| Severidad      | Producción                              | Dev/Staging             |
| :------------- | :-------------------------------------- | :---------------------- |
| `DEBUG`/`INFO` | Solo OTel span attribute                | —                       |
| `WARNING`      | EventBridge `DOMAIN_FAULT_DETECTED`     | SQS LocalStack          |
| `ERROR`        | EventBridge + SNS → PagerDuty           | SQS LocalStack          |
| `FATAL`        | EventBridge + SNS + Lambda auto-restart | SQS LocalStack + stderr |

| Familia                  | Severidad                          |
| :----------------------- | :--------------------------------- |
| `JNS_REF_*`, `COD_VAL_*` | `WARNING` — error del cliente      |
| `JNS_TX_001`, `SYS_000`  | `ERROR` — fallo de infraestructura |
| `SEC_401`, `SEC_403`     | `WARNING` — acceso denegado        |
| `SYS_KNS_001`            | `FATAL` — pipeline de ingesta roto |

> [!NOTE]
> **¿Por qué EventBridge y no SQS directo?** SQS es punto-a-punto. EventBridge permite
> que Echo (retry), PagerDuty y futuros consumidores reciban el mismo evento en paralelo
> mediante Rules — cero cambios en `sherlog.clj`.

### V.2 — `IFaultNotifier` + Implementaciones

```clojure
;; ns: metri.iop.sherlog

(defprotocol IFaultNotifier
  (notify! [this error-dto severity]
    "Envía el DTO al destino externo. Nunca lanza — siempre retorna."))

;; PRODUCCIÓN: EventBridge
(defrecord EventBridgeNotifier [eb-client event-bus-name]
  IFaultNotifier
  (notify! [_ error-dto severity]
    (eb/put-events eb-client
      {:entries [{:event-bus-name event-bus-name
                  :source         "metri-engine"
                  :detail-type    "DOMAIN_FAULT_DETECTED"
                  :detail         (json/write-str
                                    {:severity        (name severity)
                                     :tenant_id       (-> error-dto :error :tenant_id)
                                     :user_id         (-> error-dto :error :user_id)
                                     :error_code      (-> error-dto :error :code)
                                     :trace_id        (-> error-dto :error :trace_id)
                                     :retryable       (-> error-dto :error :retryable)
                                     :circuit_breaker (= (keyword severity) :fatal)
                                     :timestamp       (System/currentTimeMillis)
                                     :dto             error-dto})}]})))

;; DEV/STAGING: SQS via LocalStack
(defrecord SQSNotifier [sqs-client topic]
  IFaultNotifier
  (notify! [_ error-dto severity]
    (sqs/send-message! sqs-client
      {:topic   topic
       :payload {:event_type      "DOMAIN_FAULT_DETECTED"
                 :severity        (name severity)
                 :tenant_id       (-> error-dto :error :tenant_id)
                 :error_code      (-> error-dto :error :code)
                 :trace_id        (-> error-dto :error :trace_id)
                 :retryable       (-> error-dto :error :retryable)
                 :circuit_breaker (= (keyword severity) :fatal)
                 :dto             error-dto}})))

;; Aísla fallos individuales — un notifier fallido no cancela los demás
(defn- do-emit! [notifiers error-dto severity span]
  (doseq [n notifiers]
    (try
      (notify! n error-dto severity)
      (catch Exception e
        (otel/set-status! span :error "Notifier failed")
        (log/error "Sherlog: notifier failed" {:notifier (type n) :cause (ex-message e)})))))

;; IMPORTANTE (SAM/Lambda): SINCRÓNICO dentro de la invocación.
;; EventBridge PutEvents completa ANTES de que el handler retorne (~8-15ms p99).
(defn emit-fault-event! [error-dto severity notifiers]
  (otel/with-span ["sherlog.emit-fault-event" {:kind :producer}]
    (let [span (otel/current-span)]
      (otel/set-attributes! span
        {"fault.code"      (-> error-dto :error :code)
         "fault.severity"  (name severity)
         "fault.tenant"    (-> error-dto :error :tenant_id)
         "fault.trace_id"  (-> error-dto :error :trace_id)
         "notifiers.count" (count notifiers)})
      (if (>= (severity-rank severity) (severity-rank :warning))
        (do (do-emit! notifiers error-dto severity span)
            (otel/set-status! span :ok))
        (do (otel/set-status! span :ok)
            (log/debug "below threshold" {:severity severity}))))))
```

### V.3 — Cold Start Lambda (`defonce`)

```clojure
;; ns: metri.iop.sherlog — inicialización lazy en cold start
(defonce ^:private prod-notifiers
  (delay [(->EventBridgeNotifier
            (eb/make-client {:region (System/getenv "AWS_REGION")})
            (System/getenv "FAULT_BUS_NAME"))]))

(defonce ^:private dev-notifiers
  (delay [(->SQSNotifier
            (sqs/make-client {:endpoint-override (System/getenv "SQS_ENDPOINT_OVERRIDE")})
            "DOMAIN_FAULT_DETECTED")]))

(defn get-notifiers []
  (if (= "production" (System/getenv "ENVIRONMENT"))
    @prod-notifiers
    @dev-notifiers))
;; Uso: (emit-fault-event! dto :warning (get-notifiers))
```

> [!NOTE]
> `defonce` garantiza inicialización única por container (warm start reutiliza).
> `delay` hace la inicialización lazy — ocurre solo en la primera invocación.
> El span `:producer` de Sherlog lo exporta el ADOT Layer en fase SHUTDOWN.

### V.4 — Persistencia OLAP: `domain_fault`

Schema: [`models/domain_fault.json`](models/domain_fault.json)
(`is_system: true`, `engine: olap`, `disable_eda: true`)

Sherlog hace **dos acciones secuenciales** en la misma invocación Lambda:

```
Error detectado
    ├─ 1. emit-fault-event!  → EventBridge (Echo, PagerDuty — async)
    └─ 2. record-fault!      → Janus OLAPChannel (in-process, ~2ms)
                                    └─ Kinesis Firehose → S3 Parquet → Athena
```

```clojure
;; ns: metri.iop.sherlog — segunda acción, nunca lanza
(defn record-fault! [error-dto severity olap-channel]
  (when (>= (severity-rank severity) (severity-rank :warning))
    (try
      (janus.olap/bulk-ingest! olap-channel
        {:entity_type "domain_fault"
         :payload     {:trace_id    (-> error-dto :error :trace_id)
                       :tenant_id   (-> error-dto :error :tenant_id)
                       :user_id     (-> error-dto :error :user_id)
                       :error_code  (-> error-dto :error :code)
                       :severity    (name severity)
                       :stage       (-> error-dto :error :stage)
                       :entity_type (-> error-dto :error :entity_type)
                       :retryable   (-> error-dto :error :retryable)
                       :occurred_at (System/currentTimeMillis)
                       :context     (-> error-dto :error :context)}})
      (catch Exception e
        (log/error "record-fault!: Kinesis write failed" {:cause (ex-message e)})))))

;; ── Orquestador único — PUNTO DE ENTRADA de Sherlog ──────────────────────────
;; Llamado exclusivamente por iop/error_response.clj después de build-error-dto.
;; Garantiza orden: primero EDA (notificación) → luego OLAP (persistencia).
;; Nunca lanza — ni emit ni record propagan excepciones al caller.
(defn handle-fault!
  "Punto de entrada único de Sherlog. Orquesta emit + record en orden garantizado.
   El caller (iop/error_response) solo invoca esta función — nunca emit ni record
   directamente. Esto evita que un desarrollador omita la persistencia OLAP."
  [error-dto severity notifiers olap-channel]
  (emit-fault-event! error-dto severity notifiers)   ;; 1º: bus EDA
  (record-fault!      error-dto severity olap-channel)) ;; 2º: OLAP in-process
```

> [!IMPORTANT]
> `handle-fault!` es el **único punto de entrada** de Sherlog desde `iop/error_response`.
> Llamar `emit-fault-event!` o `record-fault!` directamente viola el contrato —
> el orden EDA → OLAP es una invariante del sistema.

> [!NOTE]
> `disable_eda: true` en `domain_fault.json` previene que Janus emita `DOMAIN_FAULT_RECORDED`
> al bus — sin loop. La correlación causal ocurre en Athena por `trace_id`.

### V.5 — Consumidores de EventBridge

`metri-engine` publica **`DOMAIN_FAULT_DETECTED`**.
Echo puede escalar publicando **`DOMAIN_FAULT_ESCALATED`** cuando agota sus reintentos.

| Evento                   | Consumidor    | Condición                   | Responsabilidad                            |
| :----------------------- | :------------ | :-------------------------- | :----------------------------------------- |
| `DOMAIN_FAULT_DETECTED`  | **Echo**      | `retryable: true`           | Reintenta con backoff exponencial          |
| `DOMAIN_FAULT_DETECTED`  | **PagerDuty** | `severity: ERROR \| FATAL`  | Alerta inmediata al equipo                 |
| `DOMAIN_FAULT_ESCALATED` | **PagerDuty** | siempre (`severity: FATAL`) | Alerta de retry agotado (Rule en Echo SAM) |

---

## MÓDULO VI: Integración con Echo

El campo `retryable` tiene semántica precisa: **no es que el cliente reintente —
es que la infraestructura puede recuperarse automáticamente**.

| Actor            | Responsabilidad                                             | Interfaz                                       |
| :--------------- | :---------------------------------------------------------- | :--------------------------------------------- |
| **metri-engine** | Publica `DOMAIN_FAULT_DETECTED` con `retryable: true/false` | EventBridge PutEvents                          |
| **Echo**         | Consume y reintenta la operación fallida                    | EventBridge Rule → SQS → Lambda                |
| **SQS DLQ**      | Captura intentos agotados                                   | Echo lo escribe cuando `MAX_ATTEMPTS` se agota |

`metri-engine` no sabe ni decide cómo reintentará Echo. Solo empaca el campo
`retryable` desde el catálogo y publica. Echo opera completamente autónomo.

> [!IMPORTANT]
> `build-error-dto` lee `(-> (errors/lookup code) :retryable?)` — nunca el desarrollador decide.
> `retryable?: false` = condición del cliente (`JNS_REF_002`). `retryable?: true` = infraestructura recuperable.

```clojure
;; ns: metri.common.errors — retryability derivada del catálogo (DRY)
(defn retryable? [error-code]
  (-> (lookup error-code) :retryable? boolean))

(defn retryability-report []
  (let [all (vals @error-catalog)]
    {:retryable     (filterv :retryable? all)
     :non-retryable (filterv (complement :retryable?) all)}))
```

> [!NOTE]
> Implementación completa de Echo — backoff, dispatcher, SAM template, TDD:
> [`COMPONENTE_EXTERNO_02_ECHO.md`](COMPONENTE_EXTERNO_02_ECHO.md)

---

## MÓDULO VII: Privacidad en la Telemetría (Zero-Trust)

- **Aislamiento Tenant:** `tenant_id` en `domain_fault` y como atributo OTel garantizan
  que un tenant nunca consulte errores ni trazas de otro — ni en BI ni en dashboards.

- **Censura Tenant Master:** El Administrador Global puede consultar cross-tenant,
  pero no puede acceder al `context` de errores cuyo `resource_domain` pertenezca
  a dominios operacionales del cliente (`cmms`, `iot`, `inventory`).

- **`user_id`:** Permite ver qué usuarios generan errores dentro del tenant,
  sujeto a la misma política de censura ABAC (Fase 06).

---

## MÓDULO VIII: Checklist de Implementación

Todo componente debe cumplir estas reglas antes de producción:

**Railway:**

- [ ] Ninguna función de negocio usa `throw` directamente
- [ ] Todo `try/catch` vive en la frontera del namespace (interop)
- [ ] Todo `[:error]` incluye `stage`, `code`, `tenant-id`, `user-id`, `trace-id`
- [ ] El `code` viene exclusivamente del catálogo maestro

**OpenTelemetry:**

- [ ] Cada fase lógica tiene `otel/with-span` nombrado
- [ ] Todo span anota `tenant.id` y `user.id`
- [ ] En error: `otel/set-status! :error` + `error.code` anotado
- [ ] `trace_id` del span activo propagado al `[:error]`

**Sherlog / EDA:**

- [ ] `iop/error_response` llama **únicamente** a `sherlog/handle-fault!` — nunca a `emit-fault-event!` ni `record-fault!` directamente
- [ ] Errores `WARNING+` emiten `DOMAIN_FAULT_DETECTED` al bus EDA
- [ ] Campos `sensitive: true` → `REDACTED` en el evento antes de publicar
- [ ] `emit-fault-event!` síncrono — completa antes de que el handler retorne
- [ ] Clientes EventBridge/SQS inicializados vía `defonce` en cold start
- [ ] `record-fault!` persiste en `domain_fault` vía OLAPChannel in-process (siempre después de emit)

---

## MÓDULO IX: Estructura de Archivos

### IX.1 — Árbol del subsistema

```
metri-engine/
├── resources/errors/
│   └── error_catalog.edn              ← SSOT de todos los códigos de error
│
├── docs/architecture/models/
│   └── domain_fault.json              ← Schema OLAP de errores severos
│
└── src/metri/
    ├── common/errors.clj              ← load-catalog!, lookup, error constructor
    ├── otel/
    │   ├── spans.clj                  ← with-span, set-attributes!, set-status!
    │   └── propagation.clj            ← W3C traceparent extract/inject
    ├── iop/
    │   ├── error_response.clj         ← build-error-dto, sanitize-context
    │   └── sherlog.clj                ← emit-fault-event!, record-fault!, IFaultNotifier
    ├── janus/core.clj                 ← JanusRouter (span ROOT)
    ├── janus/channels/oltp.clj        ← OLTPChannel (spans hijos, try/catch frontera)
    ├── janus/channels/olap.clj        ← OLAPChannel (bulk-ingest! para domain_fault)
    └── codice/bootstrapper.clj        ← load-catalog! + load-schemas! al arrancar
```

### IX.2 — Bootstrap: orden fail-fast

```
Bootstrapper (JVM no acepta requests hasta completar)
    ├─ [1] errors/load-catalog!      → Valida error_catalog.edn — FALLA → JVM no arranca
    ├─ [2] otel/init!                → SDK OTel + ADOT — FALLA → producción ciega
    ├─ [3] codice/load-schemas!      → Todos los JSON schemas — FALLA → JVM no arranca
    ├─ [4] codice/validate-scope-links! → entityRef is_sequence_scope (:COD_SCOPE_001)
    └─ [5] READY → acepta rpc Transact y rpc BulkIngest
```

### IX.3 — Tabla resumen de archivos

| Archivo              | Responsabilidad                          | Consumido por                 |
| :------------------- | :--------------------------------------- | :---------------------------- |
| `error_catalog.edn`  | SSOT de todos los códigos                | `errors.clj` en bootstrap     |
| `errors.clj`         | `lookup`, `error`, catálogo atom         | Todos los namespaces          |
| `spans.clj`          | Helpers OTel: `with-span`, `set-status!` | `janus/core.clj`, `oltp.clj`  |
| `propagation.clj`    | W3C `traceparent` extract/inject         | `iop/core.clj` (entrada gRPC) |
| `error_response.clj` | `build-error-dto` — Railway→DTO gRPC     | `iop/core.clj` (salida gRPC)  |
| `sherlog.clj`        | `emit-fault-event!` + `record-fault!`    | `iop/error_response.clj`      |
| `domain_fault.json`  | Schema OLAP de errores severos           | `sherlog.clj` → OLAPChannel   |

---

## MÓDULO X: Matriz TDD

Un caso de test por comportamiento. Un error solo está implementado cuando los 4 tests pasan.

### X.1 — Stubs Canónicos (fixtures compartidos)

```clojure
;; ns: metri.test.fixtures.errors

(defrecord SpySpan [attrs-atom status-atom]
  OTelSpan
  (set-attributes! [_ m] (swap! attrs-atom merge m))
  (set-status!     [_ s _] (reset! status-atom s))
  (trace-id        [_] "4bf92f3577b34da6a3ce929d0e0e4736")
  (span-id         [_] "00f067aa0ba902b7"))
(defn spy-span [] (->SpySpan (atom {}) (atom :unset)))

(defrecord SpyNotifier [calls-atom]
  IFaultNotifier
  (notify! [_ error-dto severity]
    (swap! calls-atom conj {:dto error-dto :severity severity})))
(defn spy-notifier [] (->SpyNotifier (atom [])))

(defn mem-conn [entities]
  (let [conn (d/create-conn {})]
    (d/transact conn {:tx-data (mapv #(hash-map :entity/ulid (:ulid %)
                                                :entity/type (:entity-type %))
                                     entities)})
    conn))

(def ^:const sample-ctx
  {:tenant-id "uuid-acme" :user-id "uuid-tech-01"
   :entity-type :work_order :operation :create})

(def ^:const sample-schema
  {:entity     "work_order"
   :attributes [{:name "location_id" :type "reference" :entityRef "location"}
                {:name "asset_id"    :type "reference" :entityRef "asset"}
                {:name "assignees"   :type "reference" :entityRef "user" :cardinality "many"}
                {:name "title"       :type "string"}]})
```

---

### X.2 — `errors.clj` — Catálogo SSOT

| ID       | Caso                                              | Input                               | Output esperado                                            |
| :------- | :------------------------------------------------ | :---------------------------------- | :--------------------------------------------------------- |
| `ERR-01` | `load-catalog!` construye mapa de lookup          | `error_catalog.edn` 20 entries      | `(count @error-catalog)` = 20                              |
| `ERR-02` | `load-catalog!` falla si falta campo obligatorio  | EDN sin `:context-required`         | `ExceptionInfo` — JVM no arranca                           |
| `ERR-03` | `lookup` retorna entry para código existente      | `:JNS_REF_002`                      | `{:code :JNS_REF_002 :severity :warning :http-status 422}` |
| `ERR-04` | `lookup` lanza para código fuera del catálogo     | `:JNS_INVENTED_999`                 | `ExceptionInfo` "Error code not in catalog"                |
| `ERR-05` | `errors/error` construye `[:error]` completo      | `:JNS_REF_001`, ctx                 | `[:error {:code :JNS_REF_001 :stage :janus}]`              |
| `ERR-06` | `errors/error` log/warn si falta context-required | `:JNS_TX_001` sin `:datahike_error` | `[:error ...]` retornado + warning                         |
| `ERR-07` | `errors/error` nunca lanza con ctx vacío          | `:JNS_TX_001`, `{}`                 | `[:error {:code :JNS_TX_001}]` — sin `throw`               |

```clojure
(deftest catalog-ssot-tests
  (testing "ERR-03: lookup retorna entry correcto"
    (errors/load-catalog!)
    (let [e (errors/lookup :JNS_REF_002)]
      (is (= :JNS_REF_002 (:code e)))
      (is (= :warning     (:severity e)))
      (is (= 422          (:http-status e)))
      (is (= false        (:retryable? e)))))

  (testing "ERR-04: lookup lanza para código no registrado"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Error code not in catalog"
          (errors/lookup :JNS_INVENTED_999))))

  (testing "ERR-07: errors/error nunca lanza — Railway preservado"
    (let [[tag body] (errors/error :JNS_TX_001 {})]
      (is (= :error tag))
      (is (= :JNS_TX_001 (:code body))))))
```

---

### X.3 — `verify-entity-refs` — FK + Type Safety

| ID       | Caso                                    | DB                     | Payload                              | Output                                                   |
| :------- | :-------------------------------------- | :--------------------- | :----------------------------------- | :------------------------------------------------------- |
| `VER-01` | Sin refs → retorno inmediato            | vacía                  | `{:title "WO"}`                      | `[:ok]`                                                  |
| `VER-02` | Todos refs existen y tipo correcto      | `{uuid-loc :location}` | `{:location_id "uuid-loc"}`          | `[:ok]`                                                  |
| `VER-03` | UUID no existe → `JNS_REF_001`          | vacía                  | `{:location_id "ghost"}`             | `[:error {:code :JNS_REF_001}]`                          |
| `VER-04` | UUID tipo incorrecto → `JNS_REF_002`    | `{uuid-inv :invoice}`  | `{:location_id "uuid-inv"}`          | `[:error {:code :JNS_REF_002 :expected_type :location}]` |
| `VER-05` | Múltiples errores → lista `:violations` | vacía                  | `{:location_id "g1" :asset_id "g2"}` | `[:error {:violations [{:field "location_id"} ...]}]`    |
| `VER-06` | `cardinality:many` válido               | `{u1 :user, u2 :user}` | `{:assignees ["u1" "u2"]}`           | `[:ok]`                                                  |
| `VER-07` | `cardinality:many` un UUID inválido     | `{u1 :user, u2 :user}` | `{:assignees ["u1" "ghost"]}`        | `[:error {:violations [{:field "assignees"}]}]`          |
| `VER-08` | **1 sola query** para N refs            | 3 entities             | 7 FK refs                            | `d/q` llamado exactamente 1 vez                          |
| `VER-09` | Campo ref `nil` → skip sin error        | cualquiera             | `{:asset_id nil}`                    | `[:ok]`                                                  |

```clojure
(deftest verify-entity-refs-tests
  (testing "VER-04: type confusion → JNS_REF_002"
    (let [conn   (mem-conn [{:ulid "uuid-inv" :entity-type :invoice}])
          result (verify-entity-refs conn sample-schema {:location_id "uuid-inv"})]
      (is (= :error (first result)))
      (let [v (first (-> result second :violations))]
        (is (= :JNS_REF_002 (:code v)))
        (is (= :location     (:expected_type v)))
        (is (= :invoice      (:actual_type v))))))

  (testing "VER-08: exactamente 1 query para 4 refs"
    (let [cnt (atom 0) orig d/q]
      (with-redefs [d/q (fn [& a] (swap! cnt inc) (apply orig a))]
        (verify-entity-refs
          (mem-conn [{:ulid "l1" :entity-type :location}
                     {:ulid "a1" :entity-type :asset}
                     {:ulid "u1" :entity-type :user}])
          sample-schema
          {:location_id "l1" :asset_id "a1" :assignees ["u1"]}))
      (is (= 1 @cnt)))))
```

---

### X.4 — Spans OTel — Assertions por Fase

| ID        | Caso                                                      | Output esperado                                |
| :-------- | :-------------------------------------------------------- | :--------------------------------------------- |
| `OTEL-01` | Request exitoso → 5 spans                                 | ROOT + 4 hijos por fase                        |
| `OTEL-02` | `tenant.id` y `user.id` en ROOT                           | `attrs["tenant.id"]` = ctx                     |
| `OTEL-03` | Error → `status=:error` + `error.code` en span de la fase | `span.status = :error`                         |
| `OTEL-04` | TX exitosa → `status=:ok` + `datahike.db_after`           | `span.status = :ok`                            |
| `OTEL-05` | `trace_id` del span propagado al `[:error]`               | `(-> result second :trace-id)` = OTel trace-id |

```clojure
(deftest otel-span-tests
  (testing "OTEL-03: error → span :error con error.code"
    (let [span (spy-span) conn (mem-conn [])]
      (with-redefs [otel/current-span (constantly span)]
        (verify-entity-refs conn sample-schema {:location_id "ghost"}))
      (is (= :error @(:status-atom span)))
      (is (= "JNS_REF_001" (get @(:attrs-atom span) "error.code")))))

  (testing "OTEL-05: trace_id del span en el [:error]"
    (let [span   (spy-span)
          result (with-redefs [otel/current-span (constantly span)]
                   (errors/error :JNS_REF_001
                                 {:field "x" :uuid "y" :entity_type "z"
                                  :tenant-id "t" :user-id "u"
                                  :trace-id (otel/trace-id span)}))]
      (is (= "4bf92f3577b34da6a3ce929d0e0e4736" (-> result second :trace-id))))))
```

---

### X.5 — `build-error-dto` — DTO Forense

| ID       | Caso                                                    | Input                      | Output                                      |
| :------- | :------------------------------------------------------ | :------------------------- | :------------------------------------------ |
| `DTO-01` | DTO tiene `trace_id`, `tenant_id`, `user_id`, `code`    | cualquier `[:error]`       | 4 campos presentes en `dto.error`           |
| `DTO-02` | `tenant_id` del ctx Cedar — no del payload              | `ctx.tenant-id="t1"`       | `dto.error.tenant_id = "t1"`                |
| `DTO-03` | Campo `sensitive:true` → `"[REDACTED]"`                 | `{:password "secret"}`     | `dto.error.context.password = "[REDACTED]"` |
| `DTO-04` | Campo no-sensitive → valor original                     | `{:title "WO-001"}`        | `dto.error.context.title = "WO-001"`        |
| `DTO-05` | `correlation_id` = `"REQ-" + primeros 8 chars trace_id` | `trace_id = "4bf92f35..."` | `dto.error.correlation_id = "REQ-4bf92f35"` |

```clojure
(deftest build-error-dto-tests
  (testing "DTO-02: tenant_id del ctx, no del payload"
    (let [dto (build-error-dto
                {:code :JNS_REF_001 :stage :janus :detail "..."}
                (assoc sample-ctx :tenant-id "ctx-tenant")
                sample-schema)]
      (is (= "ctx-tenant" (-> dto :error :tenant_id)))))

  (testing "DTO-03: campo sensitive → [REDACTED]"
    (let [schema+ (assoc sample-schema :attributes
                         [{:name "password" :type "string" :sensitive true}
                          {:name "title"    :type "string"}])
          dto     (build-error-dto
                    {:code :COD_VAL_001 :stage :codice :detail "..."
                     :context {:password "secret123" :title "WO-1"}}
                    sample-ctx schema+)]
      (is (= "[REDACTED]" (-> dto :error :context :password)))
      (is (= "WO-1"       (-> dto :error :context :title))))))
```

---

### X.6 — `sherlog/emit-fault-event!` — Sherlog Pipeline

| ID       | Caso                                                 | Severidad      | Comportamiento esperado                              |
| :------- | :--------------------------------------------------- | :------------- | :--------------------------------------------------- |
| `SHR-01` | `WARNING` → `notify!` llamado 1 vez                  | `:warning`     | `SpyNotifier.calls` = 1, `:code` = `"JNS_REF_002"`   |
| `SHR-02` | `ERROR` → `notify!` llamado 1 vez                    | `:error`       | `SpyNotifier.calls` = 1                              |
| `SHR-03` | `INFO` → **no** llama `notify!`                      | `:info`        | `SpyNotifier.calls` = 0                              |
| `SHR-04` | Evento incluye `tenant_id`, `trace_id`, `error_code` | `:warning`     | 3 campos en el `dto` del spy                         |
| `SHR-05` | Síncrono — completa en < 50ms                        | `:error`       | función retorna + spy recibió llamada                |
| `SHR-06` | `FATAL` → `circuit_breaker: true`                    | `:fatal`       | `dto.error.circuit_breaker = true`                   |
| `SHR-07` | `EventBridgeNotifier` — `detail-type` correcto       | prod           | `"DOMAIN_FAULT_DETECTED"`, `source = "metri-engine"` |
| `SHR-08` | Notifier que lanza — demás siguen ejecutando         | multi-notifier | Spy2 recibe llamada aunque Spy1 lanza                |

```clojure
(deftest sherlog-tests
  (testing "SHR-01: WARNING → SpyNotifier recibe 1 llamada"
    (let [spy (spy-notifier)]
      (sherlog/emit-fault-event!
        {:error {:code "JNS_REF_002" :tenant_id "t" :user_id "u" :trace_id "tr"}}
        :warning [spy])
      (is (= 1 (count @(:calls-atom spy))))
      (is (= "JNS_REF_002" (-> @(:calls-atom spy) first :dto :error :code)))))

  (testing "SHR-03: INFO → cero llamadas a notify!"
    (let [spy (spy-notifier)]
      (sherlog/emit-fault-event! {} :info [spy])
      (is (= 0 (count @(:calls-atom spy))))))

  (testing "SHR-05: emit síncrono — completa con spy < 50ms"
    (let [spy   (spy-notifier)
          start (System/currentTimeMillis)]
      (sherlog/emit-fault-event!
        {:error {:code "JNS_REF_002" :tenant_id "t" :user_id "u" :trace_id "tr"}}
        :warning [spy])
      (is (= 1 (count @(:calls-atom spy))))
      (is (< (- (System/currentTimeMillis) start) 50))))

  (testing "SHR-08: notifier fallido no silencia a los demás"
    (let [failing (reify IFaultNotifier
                    (notify! [_ _ _] (throw (RuntimeException. "boom"))))
          spy     (spy-notifier)]
      (sherlog/emit-fault-event!
        {:error {:code "JNS_TX_001" :tenant_id "t" :user_id "u" :trace_id "tr"}}
        :error [failing spy])
      (is (= 1 (count @(:calls-atom spy))))))

  (testing "SHR-07: EventBridgeNotifier — PutEvents correcto"
    (let [eb-calls (atom [])
          notifier (->EventBridgeNotifier
                     (reify eb/IEventBridgeClient
                       (put-events [_ req] (swap! eb-calls conj req)))
                     "metri-domain-faults")]
      (notify! notifier
        {:error {:code "JNS_REF_002" :tenant_id "t" :user_id "u"
                 :trace_id "tr" :retryable false}}
        :warning)
      (let [entry (-> @eb-calls first :entries first)]
        (is (= "DOMAIN_FAULT_DETECTED" (:detail-type entry)))
        (is (= "metri-engine"         (:source entry)))))))
```

---

### X.7 — Mapa de Cobertura

| Código             | Archivo de test             | Garantías verificadas                                     |
| :----------------- | :-------------------------- | :-------------------------------------------------------- |
| `JNS_REF_001`      | `oltp_test.clj`             | `[:error :JNS_REF_001]` + `span :error`                   |
| `JNS_REF_002`      | `oltp_test.clj`             | `[:error :JNS_REF_002]` + `expected_type` + `actual_type` |
| `JNS_TX_001`       | `oltp_test.clj`             | `[:error :JNS_TX_001]` — **nunca re-throws**              |
| `JNS_CONFLICT_001` | `oltp_test.clj`             | `[:error :JNS_CONFLICT_001 :field :value]`                |
| `JNS_SCOPE_001`    | `codice_gen_test.clj`       | `[:error :JNS_SCOPE_001 :sequence_code]`                  |
| `JNS_SAGA_001`     | `saga_builder_test.clj`     | `[:error :JNS_SAGA_001 :parent_ulid]`                     |
| `JNS_CAL_001`      | `calendar_builder_test.clj` | `[:error :JNS_CAL_001 :parent_ulid]`                      |
| `COD_001`          | `janus_router_test.clj`     | `[:error :COD_001 :entity_type]`                          |
| `COD_VAL_001`      | `janus_router_test.clj`     | `[:error :COD_VAL_001 :field :expected]`                  |
| `COD_KMS_001`      | `codice_kms_test.clj`       | `[:error :COD_KMS_001]` — **nunca re-throws**             |
| `SEC_401`          | `iop_auth_test.clj`         | `[:error :SEC_401 :token_hint]`                           |
| `SEC_403`          | `iop_abac_test.clj`         | `[:error :SEC_403 :policy_violated]`                      |
| `SEC_QTA_001`      | `quota_test.clj`            | `[:error :SEC_QTA_001 :current :limit]`                   |
| `SYS_000`          | `infra_test.clj`            | `[:error :SYS_000]` — **nunca re-throws**                 |
| `SYS_KNS_001`      | `kinesis_test.clj`          | `[:error :SYS_KNS_001 :stream_name]`                      |
| `MCP_503`          | `mcp_test.clj`              | `[:error :MCP_503 :model :timeout_ms]`                    |

> [!IMPORTANT]
> **Regla de oro:** Un error solo está implementado cuando pasan los 4 tests:
>
> 1. ✅ Retorna `[:error {:code :<CODIGO> ...}]` — nunca `nil`, nunca `throw`
> 2. ✅ El span OTel tiene `status = :error` y `attrs["error.code"]` anotado
> 3. ✅ El `[:error]` incluye `tenant-id` y `user-id` del ctx Cedar
> 4. ✅ Todos los campos de `:context-required` del catálogo están en el body
