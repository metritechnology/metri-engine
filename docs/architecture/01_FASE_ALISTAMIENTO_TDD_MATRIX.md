# FASE 01 — Matriz TDD Maestra

> **SSOT de cobertura** — Cada test ID mapea a un archivo, subfase y módulo documental.  
> Estado: ✅ Implementado · 🔲 Pendiente · ⚠️ Solo en integración

---

## Resumen Ejecutivo

| Suite | Archivo | Tests | Estado |
|:------|:--------|:-----:|:------:|
| **HND** — Lambda Handler / Zero-Trust | `lambda/handler_test.clj` | 8 | ✅ |
| **BST** — Bootstrap Fail-Fast | `grpc/bootstrap_test.clj` | 5 | ✅ |
| **SVC** — gRPC handle-unary | `grpc/service_test.clj` | 7 | ✅ |
| **TRL** — Protobuf Translator | `grpc/translator_test.clj` | 11 | ✅ |
| **RDR** — Config Readers | `grpc/readers_test.clj` | 7 | ✅ |
| **VLK** — Valkey Session Store | `infrastructure/valkey_test.clj` | 7 | ✅ |
| **SQS** — SQS Outbox FIFO | `infrastructure/sqs_test.clj` | 8 | ✅ |
| **EVB** — EventBridge Bus | `infrastructure/eventbridge_test.clj` | 5 | ✅ |
| **IOP** — Pipeline Railway | `iop/pipeline_test.clj` | 7 | ✅ |
| **IOP-CORE** — IOP Orquestador | `iop/iop_core_test.clj` | 6 | ✅ |
| **RES** — Domain Result helpers | `domain/result_test.clj` | 5 | ✅ |
| **SHL** — Sherlog / IFaultNotifier | `iop/sherlog_test.clj` | 8 | ✅ |
| **ERR** — Error Response DTO | `iop/error_response_test.clj` | 9 | ✅ |
| **ICPT** — gRPC Interceptors | `grpc/interceptors_test.clj` | 8 | ✅ |
| **ZT** — Zero-Trust App layer | `application/security/zero_trust_test.clj` | 9 | ✅ |
| **INT** — Integración Docker | `infrastructure_test.clj` | 5 | ⚠️ |
| **ENV** — Env Vars / template.yaml | `lambda/env_test.clj` | 8 | ✅ |

**Total implementados:** 118 tests · **Pendientes:** 0

---

## 1. HND — Lambda Handler & Zero-Trust Token

**Módulo:** `01.01` · **Archivo:** `test/metri/lambda/handler_test.clj`  
**Invariante:** `X-Metri-Origin-Token` valida perimetro. `AuthType: NONE`.

| ID | Descripción | Tipo | Estado |
|:---|:------------|:----:|:------:|
| HND-01 | Request sin `X-Metri-Origin-Token` → 403 Forbidden | Unit | ✅ |
| HND-02 | Token incorrecto → 403 Forbidden | Unit | ✅ |
| HND-03 | Token correcto → `nil` (sin bloqueo, flujo continúa) | Unit | ✅ |
| HND-04 | `METRI_ORIGIN_TOKEN` no configurado (`nil`) → no bloquea | Unit | ✅ |
| HND-05 | Proxy JSON de Function URL tiene `statusCode`, `headers`, `isBase64Encoded`, `body` | Unit | ✅ |
| HND-06 | Trama gRPC-Web Data (flag `0x00`) — estructura 5-byte header correcta | Unit | ✅ |
| HND-07 | Trama gRPC-Web Trailer (flag `0x80`) — flag correcto | Unit | ✅ |
| HND-08 | Base64 encode/decode roundtrip preserva bytes exactos | Unit | ✅ |

---

## 2. BST — Bootstrap Fail-Fast

**Módulo:** `01.01` · **Archivo:** `test/metri/grpc/bootstrap_test.clj`  
**Invariante:** Un fallo en cualquier paso detiene toda la secuencia (`System/exit`).

| ID | Descripción | Tipo | Estado |
|:---|:------------|:----:|:------:|
| BST-01 | `run-step!` completa sin llamar `exit` cuando el paso pasa | Unit | ✅ |
| BST-02 | `run-step!` llama `exit` cuando el paso lanza excepción | Unit | ✅ |
| BST-06 | Si Paso N falla, Paso N+1 **no se ejecuta** (cortocircuito) | Unit | ✅ |
| BST-08 | `load-catalog!` se ejecuta **antes** de `init-grpc-status-map!` | Unit | ✅ |
| BST-FF | Semántica fail-fast: error en paso-2 → paso-3 nunca ejecutado | Unit | ✅ |

---

## 3. SVC — gRPC handle-unary Pattern

**Módulo:** `01.03` · **Archivo:** `test/metri/grpc/service_test.clj`  
**Invariante:** `handle-unary` nunca propaga excepciones al hilo Netty.

| ID | Descripción | Tipo | Estado |
|:---|:------------|:----:|:------:|
| SVC-01 | Pipeline `[:ok]` → `observer.onNext` (1 vez) + `onCompleted` | Unit | ✅ |
| SVC-02 | Pipeline `[:error]` Railway → `onNext` con error serializado (NO `onError`) | Unit | ✅ |
| SVC-03 | Pipeline lanza `Exception` → `observer.onError` con `Status.INTERNAL` | Unit | ✅ |
| SVC-10 | `handle-unary` absorbe toda excepción — retorna `nil` al caller | Unit | ✅ |
| SVC-11 | DIP: cambiar stub del pipeline cambia comportamiento (inyectabilidad) | Unit | ✅ |
| SVC-13 | Exception en pipeline → Sherlog invocado exactamente 1 vez (FASE 10) | Unit | ✅ |
| SVC-14 | Error Railway `[:error]` → Sherlog **no invocado** (solo para excepciones) | Unit | ✅ |

---

## 4. TRL — Protobuf Translator

**Módulo:** `01.03` · **Archivo:** `test/metri/grpc/translator_test.clj`  
**Invariante:** `struct↔map` es un isomorfismo. Errores mapean a gRPC Status canónicos.

| ID | Descripción | Tipo | Estado |
|:---|:------------|:----:|:------:|
| TRL-01 | `struct→map` con primitivos: number, string, bool | Unit | ✅ |
| TRL-02 | `struct→map` con `Struct` anidado | Unit | ✅ |
| TRL-03 | `struct→map` con lista de números → vector Clojure | Unit | ✅ |
| TRL-04 | `struct→map` con `null` value → `nil` | Unit | ✅ |
| TRL-05 | `struct→map → map→struct` roundtrip preserva tipos | Unit | ✅ |
| TRL-06 | `map→struct` con keyword keys → Struct con string keys | Unit | ✅ |
| TRL-14 | `:ABAC_401` → `Status.UNAUTHENTICATED` | Unit | ✅ |
| TRL-15 | `:ABAC_403` → `Status.PERMISSION_DENIED` | Unit | ✅ |
| TRL-16 | `:QTA_001` → `Status.RESOURCE_EXHAUSTED` | Unit | ✅ |
| TRL-17 | `:JNS_VAL_001` → `Status.INVALID_ARGUMENT` | Unit | ✅ |
| TRL-18 | Código desconocido → `Status.INTERNAL` (fallback seguro) | Unit | ✅ |
| TRL-20 | `struct→map` con `Struct` vacío → `{}` | Unit | ✅ |
| TRL-21 | `init-grpc-status-map!` deriva `Status` correcto desde `http-status` del catálogo | Unit | ✅ |

---

## 5. RDR — Config Readers (`#env`, `#env-bool`, `#env-int`)

**Módulo:** `01.01` · **Archivo:** `test/metri/grpc/readers_test.clj`  
**Invariante:** Variables ausentes en dev → placeholder. En producción → fallo explícito.

| ID | Descripción | Tipo | Estado |
|:---|:------------|:----:|:------:|
| RDR-01 | `#env` con variable presente (HOME) → string real | Unit | ✅ |
| RDR-02 | `#env` con variable ausente en dev → `<unset:VAR>` | Unit | ✅ |
| RDR-03 | `#env-bool` con valor no-`"true"` → `false` | Unit | ✅ |
| RDR-04 | `#env-bool` con variable ausente → `false` | Unit | ✅ |
| RDR-05 | `#env-int` con variable ausente → `nil` | Unit | ✅ |
| RDR-06 | `readers/all` contiene exactamente 3 tags: `env`, `env-bool`, `env-int` | Unit | ✅ |
| RDR-07 | Placeholder tiene formato exacto `<unset:VAR_NAME>` | Unit | ✅ |

---

## 6. VLK — Valkey Session Store

**Módulo:** `01.02` · **Archivo:** `test/metri/infrastructure/valkey_test.clj`  
**Protocolo:** `ISessionStore` — `get-session`, `put-session!`, `del-session!`

| ID | Descripción | Tipo | Estado |
|:---|:------------|:----:|:------:|
| VLK-01 | `put-session! → get-session` roundtrip retorna mapa íntegro | Unit | ✅ |
| VLK-02 | `del-session!` → `get-session` retorna `nil` (token revocado) | Unit | ✅ |
| VLK-03 | `get-session` de token inexistente → `nil` | Unit | ✅ |
| VLK-04 | `put-session!` sobreescribe sesión anterior con mismo token | Unit | ✅ |
| VLK-05 | Liskov: `InMemorySessionStore` satisface `ISessionStore` | Unit | ✅ |
| VLK-06 | `put-session!` y `del-session!` retornan `:ok` | Unit | ✅ |
| VLK-07 | Múltiples tokens son aislados entre sí | Unit | ✅ |

---

## 7. SQS — Outbox FIFO

**Módulo:** `01.02` · **Archivo:** `test/metri/infrastructure/sqs_test.clj`  
**Protocolo:** `ISQSBus` — `publish!`, `receive-messages`, `delete-message!`

| ID | Descripción | Tipo | Estado |
|:---|:------------|:----:|:------:|
| SQS-01 | `publish!` happy path → `[:ok {:message-id string}]` | Unit | ✅ |
| SQS-02 | SDK lanza → `[:error {:code :INFRA_SQS_001 :retryable? true}]` | Unit | ✅ |
| SQS-03 | `receive-messages` retorna mensajes publicados | Unit | ✅ |
| SQS-04 | `publish! → receive → delete` → cola vacía | Unit | ✅ |
| SQS-05 | Liskov: `InMemorySQSBus` satisface `ISQSBus` | Unit | ✅ |
| SQS-06 | Mensajes tienen `:message-id`, `:receipt-handle`, `:body` | Unit | ✅ |
| SQS-07 | 3 mensajes → recibidos en orden FIFO | Unit | ✅ |
| SQS-08 | Todas las operaciones retornan `[:ok\|:error ...]` Railway | Unit | ✅ |

---

## 8. EVB — EventBridge Bus

**Módulo:** `01.02` · **Archivo:** `test/metri/infrastructure/eventbridge_test.clj`  
**Protocolo:** `IEventBus` — `put-event!`  
**Bus de faults:** `FAULT_BUS_NAME = "metri-faults"` (ver `template.yaml`)

| ID | Descripción | Tipo | Estado |
|:---|:------------|:----:|:------:|
| EVB-01 | `put-event!` happy path → `[:ok {:event-id string}]` | Unit | ✅ |
| EVB-02 | `put-event!` acumula eventos en el stub | Unit | ✅ |
| EVB-03 | SDK lanza → `[:error {:code :INFRA_EVENTBRIDGE_001}]` Railway | Unit | ✅ |
| EVB-04 | Liskov: `InMemoryEventBus` satisface `IEventBus` | Unit | ✅ |
| EVB-05 | Respuesta es tuple Railway `[:ok\|:error ...]` | Unit | ✅ |

---

## 9. IOP — Pipeline Railway (Motor)

**Módulo:** IOP · **Archivo:** `test/metri/iop/pipeline_test.clj`  
**Invariante:** `pipeline/run` cortocircuita en el primer `[:error]`. Ctx se acumula.

| ID | Descripción | Tipo | Estado |
|:---|:------------|:----:|:------:|
| IOP-01 | `run` con todos los pasos OK → `[:ok ctx-enriquecido]` | Unit | ✅ |
| IOP-02 | `run` cortocircuita en primer error — paso-3 no ejecutado | Unit | ✅ |
| IOP-03 | `run` con un único paso funciona | Unit | ✅ |
| IOP-04 | `run` sin pasos → `[:ok ctx-original]` | Unit | ✅ |
| IOP-05 | Primer paso falla → ningún paso adicional se ejecuta | Unit | ✅ |
| IOP-06 | Ctx se acumula y pasa correctamente entre pasos | Unit | ✅ |
| IOP-07 | Código de error Railway se preserva a través del chain | Unit | ✅ |

---

## 10. IOP-CORE — IOP Orquestador (Cedar → Quota → Janus)

**Módulo:** IOP · **Archivo:** `test/metri/iop/iop_core_test.clj`  
**Invariante:** `AuditInterceptor` siempre se invoca (éxito y error).

| ID | Descripción | Tipo | Estado |
|:---|:------------|:----:|:------:|
| IOP-CORE-01 | Cedar allow → Quota ok → Janus ok → `[:ok {:entity-id ...}]` | Unit | ✅ |
| IOP-CORE-02 | Cedar deniega 401 → `[:error :ABAC_401]`, Quota no ejecutada | Unit | ✅ |
| IOP-CORE-03 | Cedar deniega 403 → `[:error :ABAC_403]` | Unit | ✅ |
| IOP-CORE-04 | Quota agotada → `[:error :QTA_001]`, Janus no invocado | Unit | ✅ |
| IOP-CORE-05 | Janus schema error → `[:error :JNS_VAL_001]` | Unit | ✅ |
| IOP-CORE-06 | `AuditInterceptor` invocado exactamente 1 vez en éxito **y** en error | Unit | ✅ |

---

## 11. INT — Integración Docker Compose

**Archivo:** `test/metri/infrastructure_test.clj`  
**Requiere:** `docker compose up` · **Ejecutar con:** `clojure -M:test -i :integration`

| ID | Descripción | Servicio Docker | Estado |
|:---|:------------|:----------------|:------:|
| INT-01 | Valkey: `put-session! → get-session → del-session!` sin excepciones crudas | `valkey:6379` | ⚠️ |
| INT-02 | SQS: `publish!` retorna `[:ok {:message-id ...}]` | `elasticmq:9324` | ⚠️ |
| INT-03 | DynamoDB: tabla inexistente → `[:error {:code :SYS_000}]` (no excepción cruda) | `dynamodb-local:8000` | ⚠️ |
| INT-04 | EventBridge: `put-event!` retorna `[:ok\|:error ...]` | LocalStack | ⚠️ |
| INT-05 | Kinesis: `put-record!` retorna `[:ok\|:error ...]` | LocalStack | ⚠️ |

---

## 12. ENV — Variables de Entorno

**Archivo:** `test/metri/lambda/env_test.clj`  
**Fuente de verdad:** `template.yaml §8.5`  
**Estrategia:** En local/CI → siempre pasan (warning informativo). En `ENVIRONMENT=production` → validación estricta.

| ID | Variable | Validación | Estado |
|:---|:---------|:-----------|:------:|
| ENV-01 | `OUTBOX_QUEUE_URL` | Presente y es URL SQS válida (`https://sqs.*`) | ✅ |
| ENV-02 | `CEDAR_POLICIES_TABLE` | Presente y no vacío | ✅ |
| ENV-03 | `DATAHIKE_DDB_TABLE` | Presente y no vacío | ✅ |
| ENV-04 | `VALKEY_HOST` | Presente y es hostname/IP válido (sin esquema) | ✅ |
| ENV-05 | `VALKEY_PORT` | Presente, parseable como `int`, rango `[1, 65535]`, valor esperado `6379` | ✅ |
| ENV-06 | `METRI_ORIGIN_TOKEN` | Presente y no vacío — CRÍTICO Zero-Trust | ✅ |
| ENV-CROSS | `VALKEY_HOST` + `PORT` | Ambos presentes o ambos ausentes (consistencia) | ✅ |
| ENV-CROSS-ALL | Todas las vars | Ninguna variable crítica ausente en producción | ✅ |

---

## 13. Mapa de Cobertura por Subfase

```
FASE 01
├── 01.01 Bootstrap & Lambda Handler
│   ├── HND-01..08       ✅  (Zero-Trust token + gRPC-Web framing)
│   ├── BST-01..FF       ✅  (Fail-Fast secuencial)
│   ├── RDR-01..07       ✅  (#env readers)
│   ├── ENV-01..06+CROSS ✅  (Env vars del template.yaml)
│   └── ZT-01..08+INV    ✅  (Zero-Trust app layer — X-Metri-Origin-Token)
│
├── 01.02 Clientes de Infraestructura
│   ├── VLK-01..07  ✅  (ISessionStore stub)
│   ├── SQS-01..08  ✅  (ISQSBus stub)
│   ├── EVB-01..05  ✅  (IEventBus stub)
│   └── INT-01..05  ⚠️  (Docker — manual)
│
├── 01.03 Runtime gRPC
│   ├── SVC-01..14  ✅  (handle-unary pattern)
│   ├── TRL-01..21  ✅  (struct↔map + error→Status)
│   └── ICPT-01..08 ✅  (Interceptores Netty: OTel + Deadline + Logging)
│
├── IOP Pipeline
│   ├── IOP-01..07       ✅  (Railway motor)
│   ├── IOP-CORE-01..06  ✅  (Cedar→Quota→Janus + Audit)
│   ├── SHL-01..08       ✅  (Sherlog / IFaultNotifier + process-fault!)
│   └── ERR-01..09       ✅  (Error Response DTO + sanitize-context)
│
└── Domain
    └── RES-01..05  ✅  (Railway ok? / error? / unwrap)
```

---

## 14. Convenciones

| Convención | Regla |
|:-----------|:------|
| **Stub canónico** | Cada protocolo tiene un `InMemory*` record en su test file |
| **Cero I/O** | Los tests unitarios nunca tocan red, disco ni AWS |
| **Railway shape** | Toda operación retorna `[:ok data]` o `[:error {:code kw ...}]` |
| **Liskov check** | Cada suite incluye un test `satisfies?` para el protocolo |
| **FASE 10 Sherlog** | Excepciones → Sherlog invocado. Errors Railway → Sherlog silenciado |
| **`:test-grpc` alias** | Tests que requieren JARs proto se guardan bajo este alias |
| **`^:integration`** | Tests con Docker marcados — skip en CI por defecto |
