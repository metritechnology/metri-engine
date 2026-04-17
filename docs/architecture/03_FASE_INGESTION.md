# Fase 03: Orquestación de Ingesta y Ruteo Agnóstico (Élite)

Esta fase define **cinco componentes distintos** con jerarquía explícita de invocación:

| Nivel                 | Componente                            | Manifiesto                                                 | Rol                                                                                                  |
| :-------------------- | :------------------------------------ | :--------------------------------------------------------- | :--------------------------------------------------------------------------------------------------- |
| **Superior**          | **Ingestion Orchestration Pipeline**  | [03A_FASE_IOP.md](03A_FASE_IOP.md)                         | Algoritmo raíz. Compone y ejecuta la cadena de interceptores desacoplados.                           |
| **Interceptor 1**     | **CedarAuthorizer**                   | [06_FASE_CEDAR_AUTHORIZER.md](06_FASE_CEDAR_AUTHORIZER.md) | Motor ABAC Zero-Trust. Autoriza la operación bajo políticas Cedar. Resuelve `tenant_id` y `user_id`. |
| **Interceptor 2**     | **QuotaGuard**                        | [07_FASE_QUOTA_GUARD.md](07_FASE_QUOTA_GUARD.md)           | Barrera económica. Verifica headroom del tenant (requiere `:tenant-id` ya resuelto por Cedar).       |
| **Interno (Etapa 3)** | **Janus Ingestion & Agnostic Router** | [03B_FASE_JANUS_ROUTER.md](03B_FASE_JANUS_ROUTER.md)       | Router puro. Deriva la carga al canal OLTP u OLAP según el Códice.                                   |
| **Etapa 4**           | **Moira EventEmitter**                | [04_FASE_MOIRA.md](04_FASE_MOIRA.md)                       | Cierre del ciclo EDA. Emite CloudEvents downstream de forma asíncrona.                               |
| **Etapa 5**           | **AuditInterceptor**                  | [09_FASE_AUDITORIA.md](09_FASE_AUDITORIA.md)               | Registro OLAP de auditoría — fire-and-forget. Se invoca SIEMPRE (`[:ok]` y `[:error]`).              |

> [!IMPORTANT]
> **Orden de invocación estricto e inmutable:**
>
> ```
> IOP → CedarAuthorizer → QuotaGuard → Janus → [response gRPC] → Moira (async) + AuditInterceptor (async)
> ```
>
> Ninguna etapa puede ser omitida ni reordenada. El fallo en cualquier interceptor cortocircuita la cadena completa.
> **Janus no puede ser invocado directamente** desde el exterior — solo el IOP lo llama, y únicamente después de que `CedarAuthorizer` y `QuotaGuard` hayan emitido `[:ok …]`.
> **AuditInterceptor** se invoca SIEMPRE — incluyendo `[:error]` de Cedar/Quota/Janus — y es el único productor del `audit_log` OLAP.

---

## Mapa de Responsabilidades

```mermaid
graph TD
    Client[Cliente gRPC / Web] --> IOP

    subgraph Fase 03 — Ingestion Orchestration
        IOP[IOP — run-iop] -->|Interceptor 1| CA[CedarAuthorizer]
        CA -->|:ok + tenant-id + role| QG[QuotaGuard]
        QG -->|:ok + quota-reservation| Janus[Janus Agnostic Router]
    end

    Janus -->|OLTP| DH["Datahike (shared table + tenant-guard)"]
    Janus -->|OLAP| KN["Kinesis Firehose (partition=tenant_id)"]

    IOP -->|200 OK inmediato| Client
    IOP -.->|async| Moira[Moira EventEmitter]
    IOP -.->|async — siempre| Audit[AuditInterceptor]
    Moira --> EB[SQS FIFO]
    Audit --> KA["Kinesis → S3 (tenant_id partition) → Athena"]
```

---

## PRINCIPIO TRANSVERSAL: Aislamiento Multitenant por Diseño (Pool Model)

> [!CAUTION]
> **Este principio es IRRENUNCIABLE y aplica a TODOS los componentes de la Fase 03.**
> La violación de cualquier garantía de aislamiento multitenant es una vulnerabilidad **P0 crítica**.
> Ningún dato de un tenant puede ser leído, escrito, consultado o inferido por otro tenant.

### Definición

El Metri Engine opera bajo un modelo **B2B multitenant con infraestructura compartida** (Pool Model) donde todos los tenants comparten las mismas tablas DynamoDB, buckets S3, y databases Athena. El aislamiento se garantiza mediante **filtrado obligatorio por `tenant_id`** en TODAS las operaciones de lectura y escritura — inyectado por `CedarAuthorizer`, nunca por el cliente.

### ¿Por qué Pool Model?

| Criterio | Pool Model (elegido) | Silo Model (descartado) |
| :------- | :------------------- | :---------------------- |
| Escalabilidad | ✅ Sin límite de tenants | ⚠️ Máx. 2,500 tablas DynamoDB/cuenta |
| Costo operativo | ✅ Una tabla, un backup, un alarm | ❌ N tablas × N backups × N alarms |
| Provisioning | ✅ Inmediato — solo registrar tenant | ❌ Crear tabla + schema + DB Athena |
| Throughput | ✅ Compartido — autoescalado global | ⚠️ Per-table provisioning |
| Seguridad | ⚠️ Depende del código (Cedar + IOP) | ✅ Físico — imposible cross-tenant |
| Complejidad | ✅ Baja | ❌ Alta (conn pool, routing) |

> [!IMPORTANT]
> **El Pool Model transfiere la responsabilidad de aislamiento al código.**
> Si un query Datahike o Athena se ejecuta SIN el filtro `tenant_id`, hay data breach.
> Por eso el `tenant_id` se inyecta en el `ctx` por Cedar y es **obligatorio e inmutable**
> durante todo el pipeline — ningún componente puede omitirlo.

### Capas de Aislamiento

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              PRINCIPIO: AISLAMIENTO MULTITENANT (POOL MODEL)               │
│                                                                             │
│  ┌─ CAPA 1: IDENTIDAD (Cedar ABAC — Paso 1 del IOP) ──────────────────┐   │
│  │  • tenant_id NUNCA viene del cliente — Cedar lo resuelve desde       │   │
│  │    Valkey (sesión del token opaco)                                    │   │
│  │  • El payload del cliente que incluya tenant_id → IGNORADO           │   │
│  │  • La identidad es inmutable durante todo el pipeline                │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─ CAPA 2: OLTP — Datahike + DynamoDB (Pool — Filtro Obligatorio) ────┐   │
│  │  • UNA SOLA tabla DynamoDB compartida: metri-dh-shared               │   │
│  │  • UNA SOLA conexión Datahike compartida (inyectada por Integrant)   │   │
│  │  • TODA entidad tiene :tenant/id como atributo obligatorio           │   │
│  │  • TODA query Datalog incluye [:where [?e :tenant/id tenant-id]]     │   │
│  │  • TODA escritura d/transact inyecta :tenant/id desde ctx            │   │
│  │  • Omitir el filtro = vulnerabilidad P0 = data breach                │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─ CAPA 3: OLAP — S3 + Parquet + Athena (Partición por Columna) ──────┐   │
│  │  • UNA SOLA database Athena compartida: metri_analytics              │   │
│  │  • Parquet particionado por tenant_id + fecha:                        │   │
│  │      /tenant_id={tid}/year={Y}/month={M}/day={D}/                    │   │
│  │  • TODA query Athena incluye WHERE tenant_id = ?                     │   │
│  │  • Kinesis Firehose: tenant_id como partition key natural             │   │
│  │  • Partition pruning: Athena solo escanea archivos del tenant         │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─ CAPA 4: EDA — SQS + EventBridge (Segregación Lógica) ─────────────┐   │
│  │  • Cada evento SQS/CloudEvent incluye tenant_id como atributo        │   │
│  │  • EventBridge rules filtran por tenant_id — sin cross-tenant        │   │
│  │  • Audit log particionado por tenant_id en Parquet                   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─ CAPA 5: SESIÓN — Valkey (Namespace por Tenant) ────────────────────┐   │
│  │  • Key pattern: sess:{tenant_id}:{session_token}                     │   │
│  │  • Quotas: quota:{tenant_id}:{dimension}                             │   │
│  │  • Cedar cache: cedar:{tenant_id}:{policy_hash}                      │   │
│  │  • No existe key que cruce tenants                                    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Mecanismo: `tenant-guard` — Gate Obligatorio para Datahike (OLTP)

```clojure
;; ns: metri.infrastructure.tenant-guard
;; SSOT: TODA operación Datahike DEBE pasar por estas funciones.
;; Ningún componente hace d/transact o d/q directamente — siempre via tenant-guard.

(ns metri.infrastructure.tenant-guard
  (:require [datahike.api :as d]))

;; ── Schema Datahike obligatorio para :tenant/id ────────────────────────────
;; Transaccionado en bootstrap — TODA entidad lo tiene.
(def tenant-schema
  [{:db/ident       :tenant/id
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index       true
    :db/doc         "Identificador del tenant propietario. Obligatorio en toda entidad."}])

(defn transact-with-tenant!
  "Escritura aislada — inyecta :tenant/id en CADA entidad del tx-data.
   Si alguna entidad ya tiene :tenant/id distinto al del ctx → error P0."
  [conn tenant-id tx-data]
  {:pre [(some? tenant-id) (string? tenant-id) (seq tx-data)]}
  (let [guarded-tx (mapv
                     (fn [entity]
                       (let [existing (:tenant/id entity)]
                         (when (and existing (not= existing tenant-id))
                           (throw (ex-info "Cross-tenant write attempt blocked"
                                           {:attempted tenant-id
                                            :found     existing
                                            :code      :TENANT_ISOLATION_VIOLATION})))
                         (assoc entity :tenant/id tenant-id)))
                     tx-data)]
    (d/transact conn guarded-tx)))

(defn query-with-tenant
  "Query aislado — inyecta filtro [:where [?e :tenant/id tenant-id]] SIEMPRE."
  [conn tenant-id query-map]
  {:pre [(some? tenant-id) (string? tenant-id)]}
  (let [guarded-query (update query-map :where conj
                              ['?e :tenant/id tenant-id])]
    (d/q guarded-query @conn)))

(defn pull-with-tenant
  "Pull aislado — verifica que la entidad pertenece al tenant.
   Si :tenant/id no coincide → retorna nil (entidad invisible)."
  [conn tenant-id eid pattern]
  {:pre [(some? tenant-id)]}
  (let [entity (d/pull @conn (conj pattern :tenant/id) eid)]
    (when (= tenant-id (:tenant/id entity))
      (dissoc entity :tenant/id))))
```

### Mecanismo: Partición OLAP (S3 + Athena — Pool)

```clojure
;; ns: metri.infrastructure.olap-partition
;; UNA SOLA database Athena compartida — tenant_id como columna de partición.

(defn tenant-s3-prefix
  "Genera el prefix S3 particionado por tenant + entity type + fecha.
   Athena usa partition pruning — solo escanea archivos del tenant solicitado."
  [tenant-id entity-type]
  (let [now (java.time.LocalDate/now)]
    (format "data/tenant_id=%s/entity_type=%s/year=%d/month=%02d/day=%02d/"
            tenant-id entity-type
            (.getYear now) (.getMonthValue now) (.getDayOfMonth now))))

;; Database Athena compartida — NO una por tenant
(def athena-database "metri_analytics")

;; Ejemplo de query Athena (generado por Aegis):
;; SELECT * FROM metri_analytics.work_order
;; WHERE tenant_id = 'acme_corp'
;;   AND year = 2026 AND month = 4;
;;
;; Partition pruning → Athena solo lee:
;;   s3://bucket/data/tenant_id=acme_corp/entity_type=work_order/year=2026/...
```

### Tabla de Garantías por Canal

| Canal | Storage | Modelo | Mecanismo de Aislamiento |
| :---- | :------ | :----- | :----------------------- |
| **OLTP Write** | Datahike + DynamoDB | **Pool** — tabla compartida | `transact-with-tenant!` inyecta `:tenant/id` |
| **OLTP Read** | Datahike + DynamoDB | **Pool** — tabla compartida | `query-with-tenant` inyecta `[:where ... :tenant/id]` |
| **OLAP Write** | Kinesis → S3 Parquet | **Pool** — bucket compartido | `tenant-s3-prefix` como partition key |
| **OLAP Query** | S3 Parquet → Athena | **Pool** — database compartida | `WHERE tenant_id = ?` + partition pruning |
| **Outbox** | SQS FIFO | Pool — cola compartida | `tenant_id` en message attribute |
| **Audit** | Kinesis → S3 Parquet | Pool — bucket compartido | `tenant_id` en partition key |
| **Session** | Valkey | Pool — clúster compartido | `sess:{tenant_id}:*` namespace |
| **Quota** | DynamoDB | Pool — tabla compartida | `quota#{tenant_id}#{dimension}` PK |

> [!IMPORTANT]
> **El `tenant-guard` es el ÚNICO módulo autorizado a ejecutar operaciones Datahike.**
> Ningún componente (Janus, Codice, Aegis) hace `d/transact` o `d/q` directamente.
> TODO pasa por `transact-with-tenant!` / `query-with-tenant` / `pull-with-tenant`.
>
> ```clojure
> ;; ✅ CORRECTO — Janus usa tenant-guard:
> (tenant-guard/transact-with-tenant! conn (:tenant-id ctx) tx-data)
>
> ;; ❌ PROHIBIDO — Janus hace d/transact directo:
> (d/transact conn tx-data)  ;; → sin filtro tenant = data breach
> ```

> [!WARNING]
> **Prohibiciones absolutas:**
> - ❌ Llamar `d/transact` o `d/q` sin pasar por `tenant-guard`
> - ❌ Confiar en el `tenant_id` del payload del cliente
> - ❌ Queries Athena sin `WHERE tenant_id = ?`
> - ❌ Escrituras S3 sin `tenant_id` en el partition key
> - ❌ Keys en Valkey sin el `tenant_id` como parte del namespace
> - ❌ Eventos SQS/CloudEvents sin `tenant_id` como atributo de mensaje

---

## Error Management — Contrato FASE 10

Toda la capa de ingesta cumple el contrato de la [FASE 10 — Gestión de Errores y Observabilidad](10_FASE_GESTION_ERRORES_EDA.md). Los principios se establecen aquí como referencia transversal para todos los sub-documentos de FASE 03.

### Railway Pattern — Contrato Transversal

**Ningún componente de la capa de ingesta lanza excepciones.** Toda función retorna `[:ok valor]` o `[:error mapa]`. El cortocircuito Railway garantiza que un error en cualquier paso detiene el pipeline — no hay ejecución parcial.

```clojure
;; Contrato universal de cualquier paso del IOP (CedarAuthorizer, QuotaGuard, JanusRouter):
;; [:ok  {...ctx enriquecido...}]
;; [:error {:stage  :cedar|:quotas|:janus|:codice
;;          :code   :ABAC_401|:QTA_001|:JNS_VAL_001|...  ← SSOT: error_catalog.edn
;;          :detail "string descriptivo"
;;          :tenant-id "tnt_01J..."   ← si ya fue resuelto por Cedar
;;          :user-id   "usr_01J..."   ← si ya fue resuelto por Cedar
;;          :trace-id  "<otel-trace-id>"}]  ← otel/trace-id del span activo
```

> [!IMPORTANT]
> `errors/error` es el **único** constructor de `[:error]` en toda la capa de ingesta.
> Ningún componente construye `[:error {...}]` inline — todos usan `errors/error :CODIGO ctx`.
> Si el código no existe en `error_catalog.edn`, el constructor lanza en desarrollo y el test falla.
> Esto garantiza que `:retryable?`, `:severity`, el mapeo gRPC y el `:http-status` son
> siempre coherentes con el catálogo.

### Propietarios de Sherlog en FASE 03

| Componente | Cuándo invoca Sherlog | Severity | Código |
| :--------- | :-------------------- | :------- | :----- |
| `JanusRouter` | Códice retorna `[:error]` (schema inválido, tipo desconocido) | `:warning` | `:COD_001`, `:COD_VAL_001` |
| `JanusRouter` | Error de escritura Datahike (`d/transact` falla) | `:error` | `:JNS_TX_001`, `:JNS_OLTP_001` |
| `JanusRouter` | Error de escritura Kinesis (`OLAPChannel`) | `:error` | `:JNS_OLAP_002` |
| `AuditInterceptor` | Error de escritura OLAP (Kinesis falla para audit_log) | `:warning` | `:AUD_001` |
| `handle-unary` (gRPC) | Excepción inesperada — escapa del pipeline | `:error` | `:SYS_000` |

> [!NOTE]
> **`IOP` (`run-iop`) NO invoca Sherlog directamente.**
> Los errores Railway `[:error]` son manejados por sus propietarios (Janus, Cedar, Quota)
> antes de retornar el `[:error]`. El IOP sólo propaga el resultado al transport layer.
> La excepción es `handle-unary` en el gRPC runtime, que captura excepciones inesperadas
> del pipeline completo — ver [01.01_FASE_RUNTIME_GRPC.md](01.01_FASE_RUNTIME_GRPC.md).

### Flujo: Error Railway → DTO Forense → EDA

```
Componente (JanusRouter, Cedar, Quota)
  │
  ├─ errors/error :CODIGO ctx           ← único constructor (SSOT catálogo)
  │    → [:error {:code :X :stage :Y :tenant-id :user-id :trace-id ...}]
  │
  ├─ (cuando severity >= :warning)
  │    sherlog/handle-fault! ← ÚNICO punto de entrada
  │         │
  │         ├─ build-error-dto   → DTO forense (sanitize-context, :retryable? del catálogo)
  │         ├─ emit-fault-event! → EventBridge (SINCRÓNICO, ~ 8-15ms p99)
  │         └─ record-fault!     → OLAPChannel → domain_fault (Kinesis/Parquet)
  │
  └─ retorna [:error] al IOP → propagado al gRPC transport → gRPC Status (derivado del catálogo)
```

### Observabilidad OTel de la capa de ingesta

| Span | Propietario | Atributos clave |
| :--- | :---------- | :-------------- |
| `grpc.{method}` (ROOT) | OTel interceptor Netty | `trace_id` W3C |
| `grpc.handle-unary` | gRPC Runtime | `entity.type`, `operation` |
| `iop.pipeline.start` | IOP | `tenant_id`, `request_id` |
| `iop.step1.cedar.*` | CedarAuthorizer | `tenant_id`, `user_id`, latencia |
| `iop.step2.quota.*` | QuotaGuard | `tenant_id`, `debit`, `remaining` |
| `iop.step3.janus.*` | JanusRouter | `ulid`, `channel`, `entity_type` |
| `janus.route.*` | JanusRouter interno | Ver [03B_FASE_JANUS_ROUTER.md](03B_FASE_JANUS_ROUTER.md) |

---

## Infraestructura Runtime

| Documento | Componente | Vínculo |
| :-------- | :--------- | :------ |
| [01.01_FASE_RUNTIME_GRPC.md](01.01_FASE_RUNTIME_GRPC.md) | Runtime gRPC | Servidor gRPC, main.clj, Integrant system.edn, Protobuf compilation, deployment |

## Referencias Cruzadas de Arquitectura

| Documento | Componente | Vínculo |
| :-------- | :--------- | :------ |
| [03A_FASE_IOP.md](03A_FASE_IOP.md) | IOP | Coordinación del pipeline, `run-iop`, composición de pasos, diagrama de secuencia |
| [03B_FASE_JANUS_ROUTER.md](03B_FASE_JANUS_ROUTER.md) | Janus | Contratos gRPC, ULID, ruteo agnóstico, canal OLTP/OLAP |
| [05.01-JANUS.md](05.01-JANUS.md) | Janus Cerebro | Aislamiento multitenant Pool Model en Janus |
| [06_FASE_CEDAR_AUTHORIZER.md](06_FASE_CEDAR_AUTHORIZER.md) | CedarAuthorizer | `defrecord`, Opaque Token, Hidratación Datalog→Cedar, Malli ATS, RLS Shield |
| [07_FASE_QUOTA_GUARD.md](07_FASE_QUOTA_GUARD.md) | QuotaGuard | `defrecord`, dimensiones, estrategias de reset, TX Functions Datahike |
| [04_FASE_MOIRA.md](04_FASE_MOIRA.md) | Moira | CloudEvents, SQS FIFO, OTEL Span, At-Least-Once delivery |
| [09_FASE_AUDITORIA.md](09_FASE_AUDITORIA.md) | AuditInterceptor | `IAuditInterceptor`, derive-action-type, OLAPChannel, fire-and-forget |
| [10_FASE_GESTION_ERRORES_EDA.md](10_FASE_GESTION_ERRORES_EDA.md) | Error Management | Railway Pattern, Sherlog, error_catalog.edn, domain_fault OLAP |
