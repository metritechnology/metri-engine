# Fase 05 — Motor Analítico Core (Aegis)

> **Fase contenedora:** [01_FASE_ALISTAMIENTO_ENTORNO.md](01_FASE_ALISTAMIENTO_ENTORNO.md)
> **Fase hermana anterior:** [03B_FASE_JANUS_ROUTER.md](03B_FASE_JANUS_ROUTER.md)
> **Fase hermana siguiente:** [06_FASE_CEDAR_AUTHORIZER.md](06_FASE_CEDAR_AUTHORIZER.md)
> **Clientes de infraestructura:** `:infra/datahike` · `:infra/athena` · `:infra/kinesis` · `:infra/tracer`

```
═══════════════════════════════════════════════════════════════════════════════
  05 — AEGIS — MOTOR ANALÍTICO CORE
  Principios: Protocol-First · Railway · DIP (Integrant) · Dual-Speed
═══════════════════════════════════════════════════════════════════════════════
```

---

## MÓDULO I: Rol en el Sistema

El **Aegis Transmuter** es el motor de consultas del Metri Engine. Recibe un Safe ATS del IOP
(post-Cedar) y decide en milisegundos hacia qué canal enrutar la petición:

```
                     Safe ATS (ctx enriquecido por Cedar)
                            │
                     ╔══════╧═══════╗
                     ║  Aegis Core  ║   ← ESTE COMPONENTE
                     ║  Transmuter  ║
                     ╚══════╤═══════╝
                            │
              ┌─────────────┴──────────────┐
              │                            │
   engine: "oltp" (defecto)        engine: "olap"
              │                            │
   ┌──────────▼──────────┐    ┌────────────▼────────────┐
   │   FAST PATH (ms)    │    │  BULK PATH (inter-anual) │
   │ Datahike (Datalog)  │    │  Athena + S3 Iceberg     │
   │ Pool Model + tenant │    │  Kinesis Firehose ingesta │
   └─────────────────────┘    └──────────────────────────┘
```

> [!IMPORTANT]
> Aegis **nunca importa** `metri.infrastructure.*` directamente.
> Recibe `IQueryEngine` e `IStreamWriter` via `#ig/ref` — **SOLID-D garantizado**.
> El `tenant_id` llega en el `ctx` inyectado por Cedar — Aegis lo propaga sin modificarlo.

---

## MÓDULO II: Conexión a Clientes de Infraestructura

### II.1 — Protocolos consumidos

Aegis consume **solo 2 protocolos** de `metri.domain.protocols`:

| Protocol        | Implementación real                        | Stub para tests        |
| :-------------- | :----------------------------------------- | :--------------------- |
| `IQueryEngine`  | `AthenaQueryEngine` (`:infra/athena`)      | `InMemoryQueryEngine`  |
| `IStreamWriter` | `KinesisFirehoseWriter` (`:infra/kinesis`) | `InMemoryStreamWriter` |

Datahike es accedido via `tenant-guard` — **no** via protocol propio (Datahike conn es el API).

### II.2 — Implementación Integrant

```clojure
;; ns: metri.aegis.core
;; SOLID-S: responsabilidad unica — enrutar y compilar queries, sin I/O directo
;; SOLID-D: interpreta ctx.engine y delega a IQueryEngine/IStreamWriter — sin imports de infra
;; DRY: errors/error unico constructor de [:error]
(ns metri.aegis.core
  (:require [integrant.core :as ig]
            [metri.domain.protocols :as proto]
            [metri.common.errors :as errors]
            [metri.infrastructure.tenant-guard :as tenant-guard]))

;; ── Record que encapsula las dependencias de Aegis ────────────────────────
(defrecord AegisTransmuter [datahike-conn query-engine stream-writer tracer]

  ;; IQueryRouter es el protocol de Aegis — definido en domain/protocols.clj
  proto/IQueryRouter

  (transmute! [_ ctx]
    "Enruta el ctx al canal correcto segun el engine del Codice.
     Retorna [:ok {:result ... :channel :oltp|:olap}] | [:error {...}]."
    (let [engine (get-in ctx [:codice :engine] :oltp)]
      (case engine
        :oltp (run-oltp-query datahike-conn ctx)
        :olap (run-olap-query query-engine ctx)
        ;; engine desconocido — falla rapido con Railway
        (errors/error :AEG_001 {:engine engine :ctx ctx})))))

(defmethod ig/init-key :aegis/transmuter
  [_ {:keys [datahike query-engine stream-writer tracer]}]
  (println "    -> Aegis: transmuter inicializado")
  (->AegisTransmuter (:conn datahike)
                     query-engine    ;; IQueryEngine via #ig/ref :infra/athena
                     stream-writer   ;; IStreamWriter via #ig/ref :infra/kinesis
                     tracer))

(defmethod ig/halt-key! :aegis/transmuter
  [_ _]
  (println "    <- Aegis: transmuter liberado"))
```

### II.3 — Fast Path: OLTP via Datahike + tenant-guard

```clojure
;; ns: metri.aegis.core (continuacion)
;; SOLID-S: logica de query OLTP aislada — sin mezclar con OLAP path
;; tenant-guard es el UNICO canal a Datahike — nunca d/q directo

(defn- run-oltp-query
  "Ejecuta un query Datalog en Datahike via tenant-guard.
   El tenant_id llega del ctx inyectado por Cedar — no del cliente.
   Retorna [:ok {:result rows :channel :oltp}] | [:error {:code :AEG_002}]."
  [conn {:keys [tenant-id codice] :as ctx}]
  (try
    (let [query-map (:query codice)
          ;; tenant-guard inyecta [:where [?e :tenant/id tenant-id]] automaticamente
          result    (tenant-guard/query-with-tenant conn tenant-id query-map)]
      [:ok {:result  result
            :channel :oltp
            :tenant-id tenant-id}])
    (catch Exception e
      (errors/error :AEG_002 {:detail (ex-message e) :tenant-id tenant-id}))))

;; ── Pull individual — para rpc GetEntity ──────────────────────────────────
(defn- run-oltp-pull
  "Pull de entidad individual con aislamiento tenant-guard."
  [conn {:keys [tenant-id codice] :as _ctx}]
  (try
    (let [eid     (:entity-id codice)
          pattern (:pull-pattern codice [:*])]
      (if-let [entity (tenant-guard/pull-with-tenant conn tenant-id eid pattern)]
        [:ok {:result entity :channel :oltp :tenant-id tenant-id}]
        ;; entity no existe o no pertenece al tenant — invisible
        (errors/error :AEG_003 {:eid eid :tenant-id tenant-id})))
    (catch Exception e
      (errors/error :AEG_002 {:detail (ex-message e) :tenant-id tenant-id}))))
```

### II.4 — Bulk Path: OLAP via IQueryEngine (Athena)

```clojure
;; ns: metri.aegis.core (continuacion)
;; SOLID-D: consume IQueryEngine — Aegis no sabe si es Athena o InMemoryQueryEngine

(defn- run-olap-query
  "Compila el AST del ctx a SQL y delega al IQueryEngine.
   El WHERE tenant_id = ? es inyectado por compile-sql — NUNCA omitible.
   Retorna [:ok {:execution-id str :channel :olap}] | [:error {:code :AEG_004}]."
  [query-engine {:keys [tenant-id codice] :as _ctx}]
  (let [sql      (compile-olap-sql codice tenant-id)   ;; tenant_id IN el SQL
        database (or (:athena-database codice) "metri_analytics")]
    ;; Delega al IQueryEngine — puede ser Athena real o InMemoryQueryEngine en tests
    (proto/start-query! query-engine sql database)))

(defn- compile-olap-sql
  "Transpila el AST del Codice a SQL Athena.
   INVARIANTE: siempre incluye WHERE tenant_id = '<tenant-id>' (aislamiento Pool Model)."
  [codice tenant-id]
  ;; Implementacion completa — fuera del scope de este modulo
  ;; Ver: metri.aegis.sql-compiler
  (str "SELECT * FROM metri_analytics." (:entity-type codice)
       " WHERE tenant_id = '" tenant-id "'"
       " AND year = " (.getYear (java.time.LocalDate/now))))
```

### II.5 — Bulk Write Path: IStreamWriter (Kinesis Firehose)

```clojure
;; ns: metri.aegis.bulk-ingest
;; Usado por rpc BulkIngest — payloads Arrow/Protobuf hacia S3 Iceberg
;; SOLID-S: responsabilidad unica — enviar batch al IStreamWriter
;; SOLID-D: consume IStreamWriter — no importa metri.infrastructure.kinesis
(ns metri.aegis.bulk-ingest
  (:require [metri.domain.protocols :as proto]
            [metri.common.errors :as errors]))

(defn ingest-batch!
  "Envia un batch de registros al stream OLAP via IStreamWriter.
   partition-key = tenant_id (garantia Pool Model en particion S3).
   Retorna [:ok {:records-sent N}] | [:error {:code :AEG_005}]."
  [stream-writer {:keys [tenant-id entity-type records] :as _ctx}]
  (let [stream-name (str entity-type "-firehose")
        results     (mapv
                      (fn [record]
                        (proto/put-record! stream-writer
                                           stream-name
                                           (serialize-record record)
                                           tenant-id))          ;; partition-key
                      records)
        errors-r    (filter #(= :error (first %)) results)]
    (if (empty? errors-r)
      [:ok {:records-sent (count records) :channel :olap}]
      ;; Alguno fallo — retorna el primer error
      (errors/error :AEG_005 {:failed (count errors-r)
                               :total  (count records)
                               :sample (first errors-r)}))))
```

---

## MÓDULO III: Integrant DAG — Dependencias

```clojure
;; config/system.edn — DAG de Aegis

;; ── CAPA 1: Clientes de infraestructura ───────────────────────────────────
;; (definidos en 01.02_FASE_CLIENTES_INFRAESTRUCTURA.md)
:infra/datahike
  {:store {:backend  :dynamodb
           :table    #env "DATAHIKE_DDB_TABLE"
           :region   #env "AWS_REGION"}}
  ;; → ig/init-key → {:conn conn :config cfg}

:infra/athena
  {:workgroup       #env "ATHENA_WORKGROUP"
   :output-location #env "ATHENA_OUTPUT_LOCATION"
   :region          #env "AWS_REGION"}
  ;; → ig/init-key → AthenaQueryEngine (implements IQueryEngine)

:infra/kinesis
  {:region        #env "AWS_REGION"
   :stream-prefix #env "KINESIS_STREAM_PREFIX"}
  ;; → ig/init-key → KinesisFirehoseWriter (implements IStreamWriter)

:infra/tracer
  {:service-name "metri-aegis"
   :endpoint     #env "OTEL_EXPORTER_OTLP_ENDPOINT"}

;; ── CAPA 2: Aegis ─────────────────────────────────────────────────────────
:aegis/transmuter
  {:datahike     #ig/ref :infra/datahike      ;; Datahike conn para Fast Path
   :query-engine #ig/ref :infra/athena        ;; IQueryEngine para Bulk Path
   :stream-writer #ig/ref :infra/kinesis      ;; IStreamWriter para BulkIngest
   :tracer       #ig/ref :infra/tracer}
  ;; → ig/init-key → AegisTransmuter record
  ;; → ig/halt-key! → nil (los clientes se liberan en CAPA 1)
```

```
┌───────────────────── DAG Integrant — Aegis ──────────────────────────────┐
│                                                                            │
│  :infra/datahike  ──────────────────────────────┐                         │
│  :infra/athena    → IQueryEngine ───────────────┼──► :aegis/transmuter    │
│  :infra/kinesis   → IStreamWriter ──────────────┤                         │
│  :infra/tracer    ──────────────────────────────┘                         │
│                                                                            │
│  Halt order (inverso):                                                     │
│    :aegis/transmuter → :infra/tracer → :infra/kinesis                     │
│                     → :infra/athena  → :infra/datahike                    │
└────────────────────────────────────────────────────────────────────────────┘
```

> [!IMPORTANT]
> `:aegis/transmuter` **nunca** hace `#ig/ref :infra/tenant-guard`.
> El `tenant-guard` es un namespace de funciones puras — no un componente Integrant.
> Se require directamente en `aegis.core` pero **no entra en el DAG**.

---

## MÓDULO IV: system.dev.edn — Config Local (Sin I/O AWS)

```clojure
;; config/system.dev.edn — LocalStack + stubs para desarrollo local

:infra/datahike
  {:store {:backend  :dynamodb
           :table    "metri-dh-shared"
           :region   "us-east-1"
           :endpoint "http://localhost:4566"}}

:infra/athena
  {:workgroup       "primary"
   :output-location "s3://metri-local/athena-results/"
   :region          "us-east-1"
   :endpoint        "http://localhost:4566"}
  ;; Nota: LocalStack Pro para Athena. Ver III.3 de 01.02 para alternativa Trino.

:infra/kinesis
  {:region        "us-east-1"
   :endpoint      "http://localhost:4566"
   :stream-prefix "metri-local"}

:aegis/transmuter
  {:datahike     #ig/ref :infra/datahike
   :query-engine #ig/ref :infra/athena
   :stream-writer #ig/ref :infra/kinesis
   :tracer       #ig/ref :infra/tracer}
```

### Inyección de stubs en tests Unit (sin levantar Docker)

```clojure
;; test/metri/aegis/core_test.clj
;; Liskov: los stubs satisfacen los mismos protocolos que las implementaciones reales

(deftest aegis-olap-happy-path
  (let [athena-stub  (datahike.infra/make-athena-stub)   ;; InMemoryQueryEngine
        kinesis-stub (datahike.infra/make-stream-stub)   ;; InMemoryStreamWriter
        datahike-mem {:conn (dh/connect {:store {:backend :mem}})}
        transmuter   (->AegisTransmuter (:conn datahike-mem)
                                        athena-stub
                                        kinesis-stub
                                        (otel/noop-tracer))
        ctx          {:tenant-id "tnt_01J"
                      :codice    {:engine :olap
                                  :entity-type "work_order"
                                  :athena-database "metri_analytics"}}]
    (let [[status result] (proto/transmute! transmuter ctx)]
      (is (= :ok status))
      (is (= :olap (:channel result))))))
```

---

## MÓDULO V: Variables de Entorno

| Variable                      | Descripción        | Producción                        | Local                      |
| :---------------------------- | :----------------- | :-------------------------------- | :------------------------- |
| `ATHENA_WORKGROUP`            | Workgroup Athena   | `metri-analytics-prod`            | `primary`                  |
| `ATHENA_OUTPUT_LOCATION`      | S3 URI resultados  | `s3://metri-athena-results-prod/` | `s3://metri-local/athena/` |
| `KINESIS_STREAM_PREFIX`       | Prefijo de streams | `metri-firehose-prod`             | `metri-local`              |
| `DATAHIKE_DDB_TABLE`          | Tabla DynamoDB     | `metri-dh-prod`                   | `metri-dh-shared`          |
| `AWS_REGION`                  | Región AWS         | `us-east-1`                       | `us-east-1`                |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Colector OTel      | `http://otel-collector:4317`      | `http://localhost:4317`    |

---

## MÓDULO VI: Observabilidad OTel

| Span                     | Propietario                      | Atributos clave                            |
| :----------------------- | :------------------------------- | :----------------------------------------- |
| `aegis.transmute.start`  | AegisTransmuter                  | `tenant_id`, `engine`, `entity_type`       |
| `aegis.oltp.query`       | `run-oltp-query`                 | `tenant_id`, `rows_returned`, `latency_ms` |
| `aegis.oltp.pull`        | `run-oltp-pull`                  | `tenant_id`, `eid`, `found?`               |
| `aegis.olap.compile_sql` | `compile-olap-sql`               | `tenant_id`, `sql_length`, `table`         |
| `aegis.olap.start_query` | `IQueryEngine.start-query!`      | `tenant_id`, `execution_id`, `latency_ms`  |
| `aegis.olap.get_results` | `IQueryEngine.get-query-results` | `execution_id`, `rows`, `latency_ms`       |
| `aegis.bulk_ingest`      | `ingest-batch!`                  | `tenant_id`, `records_sent`, `failed`      |

> [!NOTE]
> `aegis.olap.*` spans son asíncronos — el span padre **NO** espera `get-query-results`.
> El polling de resultados Athena genera spans propios bajo el trace root del request original.

---

## MÓDULO VII: Matriz TDD

| Test                        | Tipo        | Escenario                         | Esperado                                   |
| :-------------------------- | :---------- | :-------------------------------- | :----------------------------------------- |
| `transmute-oltp-happy`      | Unit        | `engine=:oltp`, tenant válido     | `[:ok {:channel :oltp}]`                   |
| `transmute-olap-happy`      | Unit        | `engine=:olap`, query válido      | `[:ok {:channel :olap :execution-id ...}]` |
| `transmute-unknown-engine`  | Unit        | `engine=:unknown`                 | `[:error {:code :AEG_001}]`                |
| `oltp-tenant-isolation`     | Unit        | 2 tenants en misma tabla          | Tenant B no ve datos de Tenant A           |
| `oltp-pull-not-found`       | Unit        | eid no existe o es de otro tenant | `[:error {:code :AEG_003}]`                |
| `olap-sql-has-tenant-where` | Unit        | `compile-olap-sql`                | SQL incluye `WHERE tenant_id = ?`          |
| `bulk-ingest-all-ok`        | Unit        | 10 records, stub no falla         | `[:ok {:records-sent 10}]`                 |
| `bulk-ingest-partial-fail`  | Unit        | 3/10 fallan en IStreamWriter      | `[:error {:code :AEG_005 :failed 3}]`      |
| `stub-satisfies-protocol`   | Unit        | `(satisfies? IQueryEngine stub)`  | `true`                                     |
| `olap-localstack`           | Integration | LocalStack Athena + S3            | `[:ok {:execution-id ...}]`                |
| `kinesis-localstack`        | Integration | LocalStack Kinesis Firehose       | `[:ok {:sequence-number ...}]`             |

> [!TIP]
> Los tests Unit **nunca hacen I/O**. Usan `make-athena-stub` y `make-stream-stub`
> del módulo `01.02` — sin levantar Docker, sin localstack, sin JVM Athena SDK calls.

---

## MÓDULO VIII: Arquitectura Dual-Speed — Resumen

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    AEGIS — ARQUITECTURA DUAL-SPEED                          │
│                                                                             │
│  FAST PATH                              BULK PATH                           │
│  engine: :oltp                          engine: :olap                       │
│  Latencia: < 50ms                       Latencia: asíncrona (> 1s)          │
│                                                                             │
│  Datahike (DynamoDB)                    Athena (S3 Iceberg)                 │
│    ↑                                      ↑                                 │
│  tenant-guard/query-with-tenant         IQueryEngine.start-query!          │
│  (filtro [:where :tenant/id] auto)      (WHERE tenant_id en SQL)            │
│                                                                             │
│  Colector de resultados: sincrono        Polling: asíncrono (Aegis Faze 05) │
│  Use case: dashboards operacionales     Use case: consolidaciones OLAP      │
│                                                                             │
│  BulkIngest (rpc):                                                          │
│    IStreamWriter.put-record!  →  Kinesis Firehose  →  S3 Parquet  →  Athena│
│    partition-key = tenant_id (Pool Model — sin silo)                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## MÓDULO IX: Referencias Cruzadas

| Documento                                                                        | Vínculo                                                               |
| :------------------------------------------------------------------------------- | :-------------------------------------------------------------------- |
| [01.02_FASE_CLIENTES_INFRAESTRUCTURA.md](01.02_FASE_CLIENTES_INFRAESTRUCTURA.md) | `AthenaQueryEngine`, `KinesisFirehoseWriter`, stubs, protocols        |
| [03B_FASE_JANUS_ROUTER.md](03B_FASE_JANUS_ROUTER.md)                             | OLTPChannel invoca `tenant-guard`, OLAPChannel invoca `IStreamWriter` |
| [03_FASE_INGESTION.md](03_FASE_INGESTION.md)                                     | Contexto del pipeline IOP → Aegis → respuesta gRPC                    |
| [06_FASE_CEDAR_AUTHORIZER.md](06_FASE_CEDAR_AUTHORIZER.md)                       | Cedar inyecta `tenant_id` en `ctx` — Aegis lo consume sin modificarlo |
