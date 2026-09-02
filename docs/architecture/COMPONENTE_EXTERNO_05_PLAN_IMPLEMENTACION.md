# Plan de Implementación — Metri Schedulers Hub

> **Documento de diseño:** [COMPONENTE_EXTERNO_05_METRI_SCHEDULERS.md](COMPONENTE_EXTERNO_05_METRI_SCHEDULERS.md) — el *qué* y el *por qué*  
> **Anexo IaC:** [COMPONENTE_EXTERNO_05_ANEXO_IAC.md](COMPONENTE_EXTERNO_05_ANEXO_IAC.md) — `template.yaml` validado  
> **Este documento:** el *en qué orden* y *cómo saber que está hecho*

El diseño no se repite aquí. Cada tarea referencia la sección que la justifica; si algo parece
arbitrario, la razón está en el documento padre.

---

## 0. Estado verificado del terreno

Comprobado contra los repositorios reales, no contra la documentación:

| Pieza | Estado real | Consecuencia |
|---|---|---|
| `metri-schedulers/` | **Creado (F1)** — Go 1.24, `github.com/metriops/metri-schedulers`, tres binarios arm64 | Pendiente F2–F4 |
| `metri-event-router/` | Existe, Go, `module github.com/metri/event-router`. Handlers: `sqs_handler`, `watchdog_handler` | Falta `fired_handler` |
| `metri-iot/` | Existe, Go, `module github.com/metriops/metri-iot`. La `Rule` vive en `internal/evaluator/engine.go` y `internal/infra/dynamodb/rule_store.go` | Falta `correlation_id` en todo el recorrido |
| `metri-engine/` | Existe, **Rust** (`Cargo.toml`) | El motor es Rust, no Clojure — la doc de arquitectura arrastra nomenclatura antigua |
| Modelos en runtime | `metri-engine/config/models/` (55 archivos) | **No es** `docs/architecture/models/` |

> **Avance registrado.** F0 y F1 están hechos y verificados:
> - **F0** — atributos portados a `config/models/`; `CodeRegistry` compila los 55 modelos.
>   `SagaBuilder` implementado en Rust (`src/janus_router/saga.rs`) con proyección atómica
>   en la misma `TransactWriteItems`. Suite del motor: **231 tests, 0 fallos**.
> - **F1** — repositorio `metri-schedulers` creado, `sam validate --lint` limpio, los tres
>   binarios compilan, guardas de bitácora activas y ruteo por `EventPattern` verificado
>   sin desplegar (`internal/event/routing_test.go`).
> - **F2** — Chronos operativo: traducción cron POSIX 5→6 campos con `?` y desplazamiento
>   del día-de-semana numérico, `at()` siempre en UTC, rechazo de epoch vencido,
>   `ConflictException → UpdateSchedule`, `ResourceNotFound` como éxito en el borrado,
>   `SUSPENDED → State: DISABLED`, backoff con jitter ante throttling, e Input estático
>   con `tenant_id`, `created_by` y `<aws.scheduler.scheduled-time>`.
>   **Los siete casos de `scheduler` de §15.1 pasan.** Pendiente de despliegue: la prueba
>   manual de cruce de DST, que no es reproducible en local.
> - **F3** — ledger de orden con `UpdateItem` condicional sobre `last_applied_ulid`,
>   activo en Chronos y Kairos antes de tocar AWS. La fila sobrevive al borrado como
>   lápida. Corregido al implementar: **el TTL lo manda la retención de la DLQ (14 días),
>   no el `MaximumEventAgeInSeconds` del target (1 h)** — el default de 7 días dejaba un
>   hueco explotable por el propio runbook de reproceso. Ahora mínimo 15, default 21.
> - **F6** — `correlation_id` propagado en `metri-iot`: `domain.Rule` → `domain.AlertPayload`
>   → evaluador → `rule_store`. Corregido al implementar: **el notifier emitía
>   `detail-type: "metri.alert"`**, que no coincidía con lo que suscriben
>   @metri-notifications ni Iris, ni con el convenio `system.iot.*` del propio repo.
>   Nadie lo consumía, así que el cambio a `system.iot.alert.breach` no rompe nada —
>   pero sin él Iris nunca habría recibido un solo evento.
 - **F5** — Kairos e Iris completos. Kairos parsea la gramática, guarda el contexto
>   **antes** de pedir la regla, emite `provision_requested`/`deprovision_requested` y
>   purga con `REMOVE` conservando la lápida. Iris correlaciona por `correlation_id`,
>   lee el contexto sin tocar el Core y deriva la `idempotency_key` del instante del
>   breach. **El Hub está funcionalmente completo: 86 tests, 0 fallos.**
>
> - **F4** — `FiredHandler` en `metri-event-router`: cuarto punto de entrada Lambda con
>   reautorización Cedar contra `created_by` en `T=0`, guard de idempotencia,
>   `RPC_CALL` → `MetriService.Transact`, `WEBHOOK` por `target_webhook_id` (nunca URL
>   libre), cierre con `suppress_events: true`, y consumo de
>   `provision_requested`/`deprovision_requested`. Añadido `proto/transact.proto`
>   (subconjunto compatible a nivel de cable con `metri.proto`) y el método
>   `Client.Transact`. Registrado en `template.yaml`: `sam validate --lint` limpio.
>
> - **F7** — reconciliación de deriva. `LambdaCensus` enumera las trampas cada hora,
>   pagina el censo y emite la métrica custom `Metri/Schedulers ActiveSchedules` con
>   alarma al 70 % de la cuota (EventBridge Scheduler no publica una métrica nativa
>   de schedules existentes, así que sin ella agotar la cuota se manifiesta como
>   altas que fallan en silencio). El diff y los límites de seguridad están en
>   `internal/census`, probados de forma pura. **Parámetro `ReconcileMode` con
>   default `observe`**: el reconciliador es la única operación capaz de destruir
>   datos, y se despliega reportando sin corregir hasta confirmar que no genera
>   falsos positivos.
>
> **Bloqueadores descubiertos en F6, pendientes de decisión:**
> 1. ~~`LambdaRuleSynchronizer` no declara `Events:`.~~ **RESUELTO:** suscrito a
>    reglas de alerta, suscripciones, perfiles de dispositivo y configuración del
>    harvester. Sin esa sección la función no recibía nada, y una regla que nunca
>    llega al Rule Store es un breach que nunca se dispara — sin error que investigar.
> 2. ~~Tres nombres para el mismo evento.~~ **RESUELTO:** `normalizeRuleEventType`
>    acepta los tres (`system.iot.alert.*` del modelo, `iot_alert_rule.*` del canal
>    OLTP, `rule.*` legacy) y emite el canónico aguas abajo. Sigue el precedente que
>    el propio `cmd/synchronizer/main.go` ya usaba para suscripciones y perfiles.
> 3. ~~El cooldown en RAM sigue sin decidir.~~ **RESUELTO:** implementado como
>    escritura condicional en DynamoDB, ejecutada sólo al detectar breach — coste
>    despreciable porque los breaches son raros por definición. El mapa en RAM queda
>    como pre-filtro. Cubre además la afinidad activo↔contenedor, que el diseño daba
>    por supuesta sin protegerla. Documentado en Componente Externo 04 §4.4.1.
> 4. ~~El Event Router no tiene cliente Valkey.~~ **RESUELTO, y la premisa era
>    errónea:** Valkey no existe en la arquitectura — el `template.yaml` del motor
>    documenta que fue eliminado deliberadamente por coste (80 % de la factura AWS).
>    El guard se implementó con **escritura condicional en DynamoDB** + TTL nativo,
>    que da el mismo compare-and-set atómico sobre infraestructura existente y es el
>    patrón que el sistema ya usa para la blacklist de tokens HMAC. Documentado en
>    §10.7 del diseño.
> 5. ~~El despacho de webhooks está sin implementar.~~ **RESUELTO:** resuelve la
>    entidad `webhook_endpoint` del Core, respeta `is_active` y despacha reutilizando
>    el pool con reintentos del `dispatcher`. Añadida una **guarda SSRF** que el
>    diseño daba por innecesaria: referenciar la entidad da revocabilidad y auditoría,
>    pero la lista de destinos la puebla un usuario, así que no es una allowlist en
>    sentido de seguridad — ver §10.6.
> 6. ~~El comparador del censo está bloqueado por una capacidad que el Core no tiene.~~
>    **RESUELTO.** Se implementó `MetriService.ListEntities` en el motor (fases L1–L3 de
>    [PLAN_IMPLEMENTACION_LIST_ENTITIES.md](PLAN_IMPLEMENTACION_LIST_ENTITIES.md)) y el
>    comparador vive en `metri-event-router/internal/reconcile`, que es donde puede
>    preguntarle al Core. El Hub conserva sólo la mitad productora (`Paginate`).
>    Despliegue en modo `observe` por defecto.

>
> Lo que F0 reveló y este plan no anticipaba: **la proyección de sagas no existía en el
> motor Rust** — no era una reescritura sino una implementación desde cero.

### 0.1 Bloqueador previo: dos directorios de modelos divergentes

`metri-engine/config/models/` es lo que el motor carga en ejecución (confirmado en
`src/eda/tests/moira_tests.rs`). `docs/architecture/models/` es un snapshot documental
**desactualizado**: 50 de 52 modelos compartidos tienen atributos distintos, y `config/`
es sistemáticamente más rico (`asset` tiene 21 atributos que la copia documental no tiene).

**Todos los cambios de modelo descritos en el diseño de Schedulers se aplicaron sobre la copia
documental.** Antes de escribir una línea de Go hay que portarlos a `config/models/`:

| Modelo | Atributos a portar | Justificación |
|---|---|---|
| `scheduled_job.json` | `created_by`, `target_webhook_id`, `last_run_at`, `run_count`, `last_error` | §10.5, §10.6, §10.4 |
| `iot_alert_rule.json` | `correlation_id`, `metric_code` | §7 |

> **Decisión pendiente, y no es de arquitectura:** qué hacer con la divergencia de los otros
> 48 modelos. Las opciones son declarar `config/models/` única fuente de verdad y borrar la
> copia documental, o generar la documental desde la de runtime. Mientras coexistan sin regla,
> cualquier cambio de modelo se aplicará al sitio equivocado la mitad de las veces.

---

## 1. Orden de ejecución

El trabajo cruza cuatro repositorios y no puede hacerse en cualquier orden. Las dependencias
duras son estas:

```
F0  Modelos en config/models/
     │
     ├──────────────┬─────────────────────┐
     ▼              ▼                     ▼
F1  Stack base   F4  Event Router      F6  IoT: correlation_id
    (schedulers)     FiredHandler          (metri-iot)
     │                   │                     │
     ▼                   │                     │
F2  Chronos ─────────────┤                     │
    (CRON/EXACT_TIME)    │                     │
     │                   │                     │
     ▼                   ▼                     │
F3  Ledger ──────► [DEMO 1: CRON extremo a extremo]
     │                                         │
     ▼                                         ▼
F5  Kairos + Iris ◄────────────────────────────┘
     │
     ▼
   [DEMO 2: TELEMETRY extremo a extremo]
     │
     ▼
F7  Reconciliación + observabilidad
```

**Dos hitos demostrables.** `CRON` extremo a extremo (F0→F4) entrega valor sin tocar Metri IoT.
`TELEMETRY` exige además F6, que vive en otro repositorio y otro equipo. Separarlos evita que
el camino temporal quede bloqueado por una dependencia externa.

---

## 2. Fases

### F0 — Modelos (`metri-engine`)

| | |
|---|---|
| **Repo** | `metri-engine` |
| **Entregable** | `config/models/scheduled_job.json` y `config/models/iot_alert_rule.json` actualizados |
| **Depende de** | Nada. Es el prerrequisito de todo lo demás |
| **Riesgo** | `created_by` es `required`: los `scheduled_job` existentes (si los hay) quedan inválidos. Verificar si hay datos antes de marcarlo obligatorio |

Tareas:

1. Portar los cinco atributos a `scheduled_job.json` y los dos a `iot_alert_rule.json`.
2. Añadir `filter_conditions` o dejar constancia de que el guard vive en el consumidor (§10.4).
3. Reescribir `SagaBuilder` según [03B §III.2](03B_FASE_JANUS_ROUTER.md) — hoy proyecta
   atributos que no existen en el modelo (`ulid`, `owner`, `type`, `due_at`) y fallaría
   la validación.
4. Verificar que el Bootstrapper arranca: un JSON malformado impide el arranque del motor.

**Aceptación:** el motor arranca; un `scheduled_job` creado por `shadow_sagas_mapping` desde
`preventive_maintenance` valida contra el modelo y produce N jobs según `prenotify_before_minutes`.

---

### F1 — Stack base (`metri-schedulers`, nuevo)

| | |
|---|---|
| **Entregable** | Repositorio Go con `template.yaml` desplegable y tres binarios que arrancan y no hacen nada |
| **Depende de** | F0 |

Tareas:

1. Crear el repositorio con la estructura de §8.1. Módulo sugerido: `github.com/metriops/metri-schedulers`, coherente con `metri-iot`.
2. Copiar el `template.yaml` del [Anexo IaC](COMPONENTE_EXTERNO_05_ANEXO_IAC.md) — ya pasa `sam validate --lint`.
3. `internal/event/envelope.go`: parseo del sobre (`job_id`, `tenant_id`, `ulid`, `delta`).
4. Tres `cmd/` que reciben el evento, lo loguean y terminan.

**Aceptación:** `sam validate --lint` limpio; `sam deploy` crea los recursos; un
`scheduled_job.created` real llega al CloudWatch Log de la función correcta **y solo a esa**
(verifica que el `EventPattern` filtra por `trigger_type`).

---

### F2 — Chronos: trampas temporales

| | |
|---|---|
| **Entregable** | `CRON` y `EXACT_TIME` se traducen a Schedules de AWS |
| **Depende de** | F1 |

Tareas:

1. `internal/scheduler/expression.go` — traducción cron 5→6 campos con `?`; `at()` con `ScheduleExpressionTimezone: UTC` para `EXACT_TIME` (§5.2).
2. `internal/scheduler/client.go` — `CreateSchedule` con `catch ConflictException → UpdateSchedule`; `DeleteSchedule` con `ResourceNotFound` como éxito (§5.1).
3. `internal/scheduler/target.go` — `Input` **estático** con `tenant_id`, `action_payload` y los atributos de contexto `<aws.scheduler.scheduled-time>` (§10.2).
4. Rechazo de epoch vencido; `SUSPENDED` → `State: DISABLED`.
5. Backoff con jitter ante `ThrottlingException`.

**Aceptación:** los siete casos de `scheduler` en §15.1. Prueba manual obligatoria: un `CRON`
que cruce un cambio de horario de verano dispara a la hora local correcta.

---

### F3 — Ledger de orden

| | |
|---|---|
| **Entregable** | Eventos rezagados y fuera de orden no corrompen el estado |
| **Depende de** | F2 |

Tareas:

1. `internal/ledger/ledger.go` — `UpdateItem` condicional sobre `last_applied_ulid` (§10.3).
2. `internal/ledger/tombstone.go` — la fila sobrevive al borrado con TTL.
3. `internal/event/delta.go` — descarte si el delta solo toca campos de bitácora (§10.4).
4. Verificar que `LEDGER_TTL_DAYS` supera el `MaximumEventAgeInSeconds` del target.

**Aceptación:** `ulid-rezagado-se-descarta`, `lapida-frena-created-tardio`,
`delta-de-bitacora-es-noop` y `delta-de-status-si-reprovisiona` de §15.1.

> **Requisito externo:** el `detail` publicado en EventBridge debe incluir `ulid` y
> `payload.delta`. Si el Event Router los descarta al construir el evento, este ledger no
> puede funcionar. Verificarlo **antes** de empezar F3.

---

### F4 — Event Router: `FiredHandler` (`metri-event-router`)

| | |
|---|---|
| **Entregable** | El Boomerang se ejecuta: el `fired` produce una acción de negocio real |
| **Depende de** | F0, F2 |

Tareas:

1. `cmd/fired_handler/main.go` — cuarto punto de entrada, junto a `sqs_handler` y `watchdog_handler`.
2. `internal/fired/idempotency.go` — `SET NX EX` sobre `idempotency_key` en Valkey.
3. **Reautorización en `T=0`** (§10.5): Cedar contra `created_by`, verificación de `user.status`, y `FAILED` + `DOMAIN_FAULT_DETECTED` si deniega.
4. `RPC_CALL` → `MetriService.Transact` reutilizando el pool gRPC existente.
5. `WEBHOOK` → resolver `target_webhook_id` a la entidad `webhook_endpoint`; **nunca** una URL del payload (§10.6).
6. Cierre con `suppress_events: true` (§4.3).
7. Añadir `FiredHandlerFunction` al `template.yaml` del Event Router.
8. Consumo de `provision_requested` / `deprovision_requested` (necesario para F5).

**Aceptación:** `principal-inactivo-no-ejecuta` de §15.1. **DEMO 1:** crear un `preventive_maintenance`
con recurrencia y ver la orden de trabajo generada en la fecha programada.

---

### F5 — Kairos e Iris: camino telemétrico

| | |
|---|---|
| **Entregable** | Un breach físico dispara una acción de negocio |
| **Depende de** | F3, F4, **F6** |

Tareas:

1. `internal/telemetry/parser.go` — gramática `<METRIC_CODE> <OP> <VALOR>`, con la unidad resuelta desde el `iot_device_profile` (§3.2).
2. Kairos: emisión de `provision_requested` con `tenant_id`, `correlation_id` y `metric_code`; persistencia del contexto.
3. Kairos: `deprovision_requested` al borrar, conservando la lápida.
4. Iris: filtro por `correlation_id` presente, lectura del contexto, `idempotency_key` derivada del `breach_timestamp`.

**Aceptación:** un breach con `correlation_id` produce un `fired` correcto; uno sin él se
ignora. **DEMO 2:** simulador IoT supera un umbral y aparece la orden de mantenimiento.

---

### F6 — Metri IoT: propagación de `correlation_id` (`metri-iot`)

| | |
|---|---|
| **Entregable** | El breach transporta la llave de retorno |
| **Depende de** | F0 |
| **Equipo** | Distinto al de Schedulers — coordinar con antelación |

Tareas:

1. `internal/evaluator/engine.go` — campo `CorrelationID` en `Rule` y en `AlertPayload`, propagado en el sitio de construcción del payload.
2. `internal/infra/dynamodb/rule_store.go` — persistir y leer el campo.
3. `internal/synchronizer/` — consumo de `provision_requested` si la sincronización es responsabilidad de IoT.
4. Verificar que un `correlation_id` vacío no altera el comportamiento de las alertas existentes.

**Aceptación:** una `iot_alert_rule` con `correlation_id` produce un breach que lo contiene;
una sin él se comporta exactamente igual que hoy.

> **Defecto detectado en revisión, decidir antes de F5.** `AssetState.Cooldowns` vive **solo
> en RAM** y se reinicia en cada arranque en frío del contenedor. En una alerta IoT normal eso
> produce ruido; en el flujo de Schedulers produce **órdenes de mantenimiento duplicadas**,
> porque la `idempotency_key` deriva del `breach_timestamp` y dos breaches genuinos son dos
> llaves distintas. Mitigación propuesta: aplicar el cooldown como escritura condicional en
> DynamoDB **solo al detectar breach** — coste casi nulo, porque los breaches son raros.
> Verificar también que `ParallelizationFactor` sea 1: la afinidad activo↔contenedor que
> sostiene el diseño depende de ello y no está documentada.

---

### F7 — Reconciliación y observabilidad

| | |
|---|---|
| **Entregable** | La deriva se detecta antes de que un mantenimiento no se ejecute |
| **Depende de** | F5 |

Tareas:

1. Chronos: invocación programada `rate(1 hour)` con `ListSchedules` y censo **paginado** (§12.2).
2. Event Router `WatchdogHandler`: comparación contra los Jobs `ACTIVE` del Core; corrección en ambos sentidos.
3. Métrica custom `Metri/Schedulers ActiveSchedules` con alarma al 70 % de la cuota.
4. Validar que el dashboard muestra métricas por función, nunca agregadas.

**Aceptación:** el censo reporta cero divergencias en régimen estable; borrar un Schedule a
mano en la consola de AWS se detecta y se corrige en el siguiente ciclo.

> **Riesgo alto:** el reconciliador es la **única operación de todo el sistema capaz de destruir
> datos**. Debe abortar si falta cualquier página del censo. Desplegarlo primero en modo
> observación —que reporte divergencias sin corregirlas— hasta confirmar que no genera falsos positivos.

---

## 3. Riesgos

| Riesgo | Impacto | Mitigación |
|---|---|---|
| Los dos directorios de modelos siguen divergiendo | Cambios aplicados al sitio equivocado | Resolver F0.1 antes de empezar; establecer una única fuente de verdad |
| `metri-iot` es otro equipo y F5 depende de F6 | `TELEMETRY` bloqueado indefinidamente | DEMO 1 (`CRON`) no depende de IoT: entregar valor sin esperar |
| Cooldown en RAM produce acciones duplicadas | Órdenes de mantenimiento repetidas | Decidir antes de F5; no es aceptable para acciones de negocio |
| Cuota de schedules alcanzada en onboarding masivo | Altas fallidas silenciosas | Alarma al 70 % operativa **antes** del primer import masivo |
| `created_by` obligatorio invalida datos existentes | El motor no arranca | Verificar si hay `scheduled_job` en producción antes de F0 |
| Reconciliador con censo incompleto | Borrado de trampas legítimas | Modo observación primero; aborto ante página faltante |

---

## 4. Lo que este plan no cubre

- **Estimaciones de esfuerzo.** No conozco al equipo ni su velocidad; poner números sería inventar.
- **La divergencia de los otros 48 modelos.** Es un problema del repositorio, más amplio que Schedulers.
- **Migración de datos existentes.** No verifiqué si hay `scheduled_job` en producción.
- **El defecto del cooldown en Metri IoT.** Está señalado, pero su corrección pertenece al Componente 04.
