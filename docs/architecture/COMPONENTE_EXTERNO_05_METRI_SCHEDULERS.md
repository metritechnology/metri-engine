# Componente Externo 05 — Metri Schedulers Hub

> **Estado:** Diseño / Documentación  
> **Responsable:** Arquitectura Metri Engine  
> **Naturaleza:** Componente Externo — AWS SAM independiente  
> **Bus de integración:** Amazon EventBridge (eventos desde Metri Engine Core)  
> **Funciones del stack:** Chronos (temporal) · Kairos (telemétrico) · Iris (retorno) — §1.1  
> **Ejecutor del retorno:** Metri Event Router y @metri-notifications — §4.2
> **Plan de implementación:** [COMPONENTE_EXTERNO_05_PLAN_IMPLEMENTACION.md](COMPONENTE_EXTERNO_05_PLAN_IMPLEMENTACION.md) — orden de ejecución, dependencias cruzadas entre repos y bloqueadores verificados

> **Autorización diferida:** un Job es una autorización que viaja al futuro. `created_by` persiste el principal y el ejecutor reevalúa Cedar en `T=0`; `WEBHOOK` referencia `webhook_endpoint` en vez de una URL libre — §10.5 y §10.6.

---

## Guía de lectura

Este documento describe un componente que **aún no existe**: es especificación, no descripción. Según para qué vengas:

| Si vienes a… | Lee |
|---|---|
| Entender qué hace y por qué | §0 a §2 — el Patrón Boomerang y su posición en el ecosistema |
| Implementarlo | §1.1 (las tres funciones) → §8 (estructura y topología) → [Anexo IaC](COMPONENTE_EXTERNO_05_ANEXO_IAC.md) → §15 (qué probar) |
| Integrar otro componente con él | §4 (contrato del evento `fired` y ejecutores) y §7 (contrato telemétrico) |
| Auditar sus garantías | §10 completa — idempotencia, orden, realimentación, autorización diferida |
| Operarlo en producción | §12 (reconciliación y runbook) y §8.4 (límites de AWS) |

**Los tres modos de fallo que dan forma a este diseño.** Casi todo lo no obvio del documento existe por uno de ellos:

| Modo de fallo | Por qué es peligroso | Dónde se resuelve |
|---|---|---|
| **El fallo diferido** | Se crea sin error y revienta meses después, en `T=0`, sin nadie mirando | §5.2, §8.4, §10.6 |
| **El fallo silencioso** | No hay excepción ni alarma: simplemente no ocurre nada el día que tocaba | §10.2, §12.1 |
| **El privilegio congelado** | La autorización se evalúa al crear, se ejecuta medio año después | §10.5 |

> **Decisiones revocadas.** Varias afirmaciones de versiones anteriores eran falsas y **se dejaron señaladas en el punto exacto donde alguien podría reintroducirlas**, en vez de agruparlas al final: `CreateSchedule` no es idempotente (§5.1), las DLQ no pueden ser FIFO (§10.1), retención no es reintento (§10.1), el `tx_id` no viaja en el sobre (§10.3), y `reading_value` no es la gramática telemétrica (§3.2). Si una de esas notas te parece obvia, es que está haciendo su trabajo.

---

## 0. Principio Rector

Metri Schedulers **no vive dentro del ciclo de vida de Janus**. Es un componente externo en su propio AWS SAM Stack cuya única responsabilidad es: **recibir eventos del bus de Metri Engine y convertirlos en trampas temporales o telemétricas en AWS**. Cuando la trampa se dispara, devuelve ciegamente el `action_payload` original al bus para que **el ejecutor sancionado** lo ejecute (§4.2). El Hub no ejecuta acciones de negocio ni escribe en el motor.

Este es el **Patrón Boomerang**: Metri Engine lanza el payload hacia el futuro; Metri Schedulers lo sostiene durmiente; AWS lo despierta en `T=0` y lo devuelve íntegro.

---

## 1. Posición en el Ecosistema Metri

```
┌─────────────────────────────────────────────────────────────────────┐
│                    METRI ENGINE CORE (SAM Stack)                    │
│                                                                     │
│  Janus Router ──► Crea/Actualiza/Elimina scheduled_job en EAV (DDB)  │
│                                  │                                  │
│                    EventBridge Bus emite:                           │
│                    system.scheduled_job.created                     │
│                    system.scheduled_job.updated                     │
│                    system.scheduled_job.deleted                     │
│                                  │                                  │
└──────────────────────────────────┼──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│               METRI SCHEDULERS HUB (SAM Stack Externo)              │
│                                                                     │
│  Ruteo por event pattern sobre detail.trigger_type:                 │
│                                                                     │
│   CRON / EXACT_TIME ──► Lambda CHRONOS                              │
│                         CreateSchedule en EventBridge Scheduler     │
│                                                                     │
│   TELEMETRY ──────────► Lambda KAIROS                               │
│                         provision_requested + HubStateTable      │
│                                                                     │
│   iot.alert.breach ───► Lambda IRIS                                 │
│                         correlaciona y publica el Boomerang         │
│                                  │                                  │
│                    Disparo en T=0:                                  │
│                    Publica action_payload → EventBridge Bus         │
│                                  │                                  │
└──────────────────────────────────┼──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     EJECUTORES DEL BOOMERANG                        │
│                                                                     │
│  RPC_CALL / WEBHOOK ────► Event Router (FiredHandler)               │
│                           gRPC MetriService.Transact / POST externo │
│  DISPATCH_NOTIFICATION ─► @metri-notifications (Dispatcher)         │
│  EDA_BROADCAST ─────────► Regla nativa de EventBridge (cero compute)│
│                                                                     │
│  Metri Engine Core NO consume el bus: toda escritura entra por      │
│  gRPC atravesando Cedar y QuotaGuard. Ver §4.2.                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Flujo de integración: Metri Engine → bus EDA → Metri Schedulers → AWS → bus EDA → Metri Engine.**  
Metri Schedulers **nunca llama al almacén EAV (DynamoDB) ni al storage de ningún otro SAM**, ni en escritura ni en lectura. Es un listener y actuador puro: su único estado propio es la tabla de correlación privada que necesita el trigger `TELEMETRY` (§7, §8).

### 1.1 Las tres funciones del Hub

El Hub **no es una Lambda**. Es un stack con tres funciones de responsabilidad única, sin invocación entre ellas: se coordinan exclusivamente por el bus y por la tabla de correlación.

| Función | Responsabilidad única | Se activa con | Escala con |
|---|---|---|---|
| **Chronos** | Traduce `CRON` / `EXACT_TIME` a Schedules de AWS. Es el único que habla con la API de EventBridge Scheduler | `system.scheduled_job.*` **filtrado** por `trigger_type ∈ {CRON, EXACT_TIME}` | Tasa de altas/bajas de jobs temporales (fría, ráfagas de import masivo) |
| **Kairos** | Traduce `TELEMETRY` a una `iot_alert_rule` vía bus y guarda el contexto de retorno | `system.scheduled_job.*` **filtrado** por `trigger_type = TELEMETRY` | Tasa de altas/bajas de jobs telemétricos (muy fría) |
| **Iris** | Correlaciona el breach físico con su Job y lanza el Boomerang de vuelta | `system.iot.alert.breach` con `correlation_id` presente | Tasa de breaches de planta (caliente, impredecible) |

**El ruteo lo hace el bus, no un `if`.** El filtrado por `trigger_type` vive en el `EventPattern` de cada regla de EventBridge (content-based filtering sobre `detail.trigger_type`). Ninguna función recibe un evento que no le corresponda, y añadir un cuarto `trigger_type` en el futuro es una regla nueva, no una rama nueva dentro de una función existente.

**Por qué separarlas — y no es dogma SOLID:**

| Razón | Consecuencia concreta |
|---|---|
| **Radio de fallo** | Un bug en el parser de `trigger_expression` telemétrico (Kairos) no puede tumbar el aprovisionamiento de mantenimientos programados (Chronos) |
| **Perfiles de carga opuestos** | Iris es el camino caliente: un evento de planta puede llegar en ráfaga. Chronos es frío salvo en imports masivos. Concurrencia reservada y tuning de memoria independientes |
| **Least Privilege real** | Iris jamás obtiene `scheduler:*` ni escritura del payload de correlación — sólo lectura. Kairos jamás obtiene `scheduler:*`. Chronos accede a DynamoDB **sólo** para el ledger de versión (§10.3), nunca al payload de correlación. Con una sola función, el permiso más peligroso del conjunto lo heredaría todo el código |
| **Despliegue independiente** | Corregir la correlación de breaches no exige redesplegar el componente que sostiene trampas dormidas desde hace meses |

> **Nota de nomenclatura:** el nombre **Hermes** está reservado en Metri para el *Unificador Analítico Multitipo* de Janus ([05.04-HERMES.md](05.04-HERMES.md)), que resuelve consultas cross-domain entre el motor OLTP y Athena. No guarda relación alguna con scheduling. Las funciones de este stack **nunca deben llamarse Hermes**: dos componentes homónimos en stacks distintos hacen ambiguos los logs, los dashboards, las alarmas y las conversaciones de incidente.

---

## 2. Por qué el Bus EDA nativo elimina el Outbox propio de Schedulers

El modelo `scheduled_job.json` declara `event_rules` nativos que el motor de Metri Engine emite automáticamente al bus EventBridge:

| Evento del motor OLTP | detail-type emitido al bus |
|---|---|
| `on_create` de `scheduled_job` | `system.scheduled_job.created` |
| `on_update` de `scheduled_job` | `system.scheduled_job.updated` |
| `on_delete` de `scheduled_job` | `system.scheduled_job.deleted` |

Dado que Metri Engine **ya garantiza** la emisión del evento EDA a través del patrón Outbox persistiendo la entidad `outbox_event` en la misma transacción ACID de DynamoDB EAV (el motor EDA/Moira es parte del ciclo de escritura transaccional, no una llamada de red externa asíncrona no confiable), **no existe Dual Write Problem**. El patrón Transactional Outbox queda resuelto de forma nativa por la arquitectura transaccional de Metri Engine Core.

> **Decisión de diseño:** Metri Schedulers escucha directamente el bus EventBridge de Metri Engine (alimentado por Moira/SQS FIFO). No necesita DynamoDB Outbox Table ni DynamoDB Streams propios en su stack. La única tabla del stack es `HubStateTable` (§8), que no es un outbox sino estado interno del Hub: el ledger de orden de todos los trigger (§10.3) y el contexto de correlación de los `TELEMETRY` (§7). Esto reduce la infraestructura del SAM Stack a su mínimo irreducible.

---

## 3. El Modelo de Datos — `scheduled_job.json`

La entidad `scheduled_job` es `is_system: true` y `track_history: true`. Toda su historia de mutaciones queda registrada en el almacén transaccional EAV de DynamoDB con histórico inmutable.

### 3.1 Taxonomía completa de atributos

| Atributo | Tipo | Rol arquitectónico |
|---|---|---|
| `parent_entity_ref` | `uuid (indexed)` | ID camaleónico del originador: un Maintenance Order, un Reminder, una Invoice. Permite al motor encontrar el contexto sin hardcodear relaciones |
| `created_by` | `reference → user (is_system, indexed)` | **Principal que engendró el Job.** Cedar lo reevalúa en `T=0` (§10.5): la trampa dispara meses después de crearse, cuando el creador pudo ser desactivado o perder permisos. Sin él la autorización queda congelada en el instante de creación |
| `trigger_type` | `enum` | Naturaleza de la trampa: `CRON`, `EXACT_TIME`, o `TELEMETRY` |
| `trigger_expression` | `string` | Motor universal de expresión (ver §3.2) |
| `iana_timezone` | `string` | Zona horaria canónica IANA (ej. `America/Bogota`). Delega el DST mundial a AWS vía `ScheduleExpressionTimezone`. **Aplica sólo a `CRON`** — en `EXACT_TIME` el epoch es UTC absoluto (§5.2) |
| `action_type` | `enum` | Determina **qué ejecutor** recibe el evento `fired` (§4.2). El Hub no ejecuta la acción, sólo la devuelve: `RPC_CALL`, `DISPATCH_NOTIFICATION`, `WEBHOOK`, `EDA_BROADCAST` |
| `target_user_id` | `reference → user` | Protección topológica: El Códice bloquea la eliminación del usuario si tiene un Job futuro apuntando a él |
| `target_group_id` | `reference → user_group` | Idem para grupos de usuarios (broadcasting masivo) |
| `target_role_id` | `reference → role` | Resolución de notificaciones por rol orgánico de planta |
| `target_webhook_id` | `reference → webhook_endpoint` | Destino cuando `action_type = WEBHOOK`; **obligatorio en ese caso**. La URL nunca viaja en el `action_payload` (§10.5) |
| `action_payload` | `json` | **El Boomerang Ciego.** Devuelto íntegro al bus cuando la trampa dispara. Contiene todo el contexto necesario para que **el ejecutor** (§4.2) actúe sin consultar estado adicional. Su tamaño serializado se valida en la creación: viaja dentro del límite de 256 KB por entry de `PutEvents` (§8.4) |
| `idempotency_hash` | `string (is_system)` | Huella dactilar **estática por Job**. No se usa directamente como llave de deduplicación: el ejecutor deduplica por `idempotency_key`, que combina este hash con el instante de la ocurrencia (§10.2) |
| `status` | `enum (is_dimension)` | `ACTIVE → COMPLETED / FAILED / SUSPENDED`. El modelo declara `default_value: ACTIVE`, por lo que **`PENDING` no se alcanza nunca** por la vía normal de creación. `COMPLETED` sólo aplica a `EXACT_TIME` (§4.3); `SUSPENDED` mapea a `State: DISABLED` del Schedule, no a su borrado (§5.2) |
| `last_run_at` | `epoch (is_system)` | Instante del último disparo ejecutado. **Campo de bitácora** |
| `run_count` | `long (is_system)` | Número de disparos ejecutados; `default_value: 0`. **Campo de bitácora** |
| `last_error` | `string (is_system)` | Causa del último fallo cuando `status = FAILED`: epoch vencido (§5.2), `action_payload` sobre 256 KB (§8.4), `ConflictException` no resuelto (§5.1). **Campo de bitácora** |

#### Los tres campos de bitácora

`last_run_at`, `run_count` y `last_error` son **los únicos atributos que muta el ejecutor**, no el usuario. Existen por dos razones distintas:

1. **Observabilidad.** Sin ellos no hay forma de responder "¿cuándo disparó por última vez?" ni "¿por qué dejó de disparar?" sobre una trampa que lleva meses dormida. `last_error` convierte un fallo silencioso en `T=0` en un dato consultable.
2. **Correctitud.** Son la lista blanca que hace posible el guard de §10.4: si el `delta` de un `system.scheduled_job.updated` toca **únicamente** estos tres campos, Chronos y Kairos descartan el evento sin llamar a AWS. Sin un conjunto cerrado y nombrado de campos de bitácora, ese guard no se puede escribir.

> **No existe `next_run_at`.** Sería el campo más pedido por la UI, y es justamente el que no puede existir: sólo AWS conoce la próxima ocurrencia de un Schedule, y escribirla de vuelta en el EAV crearía la dependencia bidireccional Hub→Core que §5 prohíbe. La UI debe derivarla del `trigger_expression` y el `iana_timezone`, que sí son suyos.

### 3.2 Los tres `trigger_type` explicados

#### `CRON` — Recurrencia periódica
`trigger_expression` contiene una expresión cron estándar:
```
"0 8 * * MON"   → Todos los lunes a las 8am (en iana_timezone)
"0 0 1 */6 *"   → El 1ero de cada semestre a medianoche
```
AWS EventBridge Scheduler gestiona el DST automáticamente al recibir el `iana_timezone` del Job en `ScheduleExpressionTimezone`. Chronos traduce la expresión de 5 a 6 campos antes de enviarla (§5.2) y registra la trampa con `FlexibleTimeWindow: {Mode: OFF}` para ejecución puntual.

#### `EXACT_TIME` — Disparo único en fecha absoluta
`trigger_expression` contiene un timestamp Epoch Unix:
```
"1735689600"   → 2026-01-01T00:00:00Z
```
Chronos crea el schedule con `ScheduleExpression: at(2026-01-01T00:00:00)` y **`ScheduleExpressionTimezone: "UTC"`** — nunca el `iana_timezone` del Job (§5.2). AWS lo elimina automáticamente post-ejecución con `ActionAfterCompletion: DELETE`.

#### `TELEMETRY` — Disparo por condición telemétrica
`trigger_expression` contiene una expresión de umbral con la gramática canónica **`<METRIC_CODE> <OPERADOR> <VALOR>`**:
```
"VIBRATION > 5000"     → Cuando la vibración del activo supere 5,000
"TEMPERATURE >= 90"    → Cuando la temperatura alcance o supere 90
```

| Componente | Valores admitidos |
|---|---|
| `METRIC_CODE` | Código de variable Metri declarado en el `iot_device_profile` (ej. `VIBRATION`, `TEMPERATURE`) |
| `OPERADOR` | `>`, `<`, `>=`, `<=`, `==` — mapeados a `GT`, `LT`, `GTE`, `LTE`, `EQ` de `iot_alert_rule` |
| `VALOR` | Decimal. La unidad se resuelve desde el `iot_device_profile` del asset, nunca desde la expresión |

> **Gramática única:** ésta es la única forma válida de `trigger_expression` para `TELEMETRY` en todo el documento. Cualquier referencia a `reading_value` pertenece a una versión anterior del contrato y queda derogada.

Este `trigger_type` **no usa AWS EventBridge Scheduler** y **no comparte storage con Metri IoT**. La integración ocurre exclusivamente a través del bus EventBridge de Metri Engine:

1. Kairos parsea `trigger_expression` y emite `system.iot.alert_rule.provision_requested` al bus, con el `job_id` como `correlation_id`. Metri Engine Core crea la `iot_alert_rule` real.
2. Kairos persiste `{correlation_id, action_payload, idempotency_hash}` en su **tabla de correlación privada** (`HubStateTable`, §8) — nunca en storage de Metri IoT.
3. Metri IoT gestiona la regla de forma nativa (DynamoDB Rule Store + Kinesis Stream + Local RAM cache), como cualquier otra `iot_alert_rule`.
4. Cuando el Harvester de Metri IoT detecta un breach, emite `system.iot.alert.breach` al bus incluyendo el `correlation_id`.
5. **Iris escucha ese evento**, lee el `action_payload` de la tabla de correlación por `correlation_id`, y publica `system.scheduled_job.fired`.

Los dos SAMs nunca comparten bases de datos. El bus es el único contrato entre ellos.

> **Principio clave:** `trigger_type: TELEMETRY` convierte un `ScheduledJob` en una `iot_alert_rule` con un `action_payload` asociado. Metri IoT detecta el momento físico; Metri Schedulers ejecuta la acción de negocio.

---

## 4. Los cuatro `action_type` — Lo que sucede al disparar

Cuando la trampa despierta se publica al bus de Metri Engine un `detail-type: "system.scheduled_job.fired"` con el `action_payload` íntegro. **Quién lo publica depende del trigger:** en `CRON`/`EXACT_TIME` lo hace AWS EventBridge Scheduler directamente, asumiendo el `SchedulerExecutionRole` — ninguna Lambda del Hub se ejecuta en `T=0`; en `TELEMETRY` lo publica Iris tras correlacionar el breach. El evento lo consume el ejecutor que corresponda a su `action_type` (§4.2):

### 4.1 El contrato del evento `fired`

Todo disparo publica exactamente este evento. **Metri Schedulers no ejecuta ninguna acción de negocio**: sólo devuelve el Boomerang.

```json
{
  "source":      "metri.schedulers",
  "detail-type": "system.scheduled_job.fired",
  "detail": {
    "job_id":           "uuid del scheduled_job",
    "tenant_id":        "tnt_01J...",
    "action_type":      "RPC_CALL | DISPATCH_NOTIFICATION | WEBHOOK | EDA_BROADCAST",
    "created_by":       "uuid del principal — el ejecutor reautoriza con él (§10.5)",
    "target_webhook_id": "uuid | null — sólo si action_type = WEBHOOK",
    "action_payload":   { },
    "idempotency_key":  "sha256(idempotency_hash + scheduled_time)",
    "scheduled_time":   "2026-01-01T08:00:00-05:00",
    "traceparent":      "00-<trace_id>-<span_id>-01"
  }
}
```

> El `source` de este stack es **`metri.schedulers`**, coherente con el `source` que ya esperan los consumidores del bus (Componente Externo 07 §10).

> **`tenant_id` se propaga, no se implementa.** El Hub **no particiona, no consulta ni autoriza por tenant**: el `job_id` es un UUID globalmente único, así que ni el nombre del Schedule (`metri-job-{uuid}`) ni la PK de `HubStateTable` necesitan discriminar inquilino. `scheduled_job.json` tampoco declara `tenant_id`, y hace bien: en las entidades de negocio el tenant es la clave de partición del EAV (`T#<tenant_id>#E#<id>`), no un atributo. El Hub es un mensajero ciego también en esto.
>
> Lo único exigible es **no dejarlo caer**, porque el sobre de todo evento del bus lo lleva de forma obligatoria y el ejecutor lo necesita para `MetriService.Transact`. Tres puntos de relevo:
>
> | Punto | Qué debe hacer |
> |---|---|
> | **Chronos**, al provisionar | Embeber `tenant_id` en el `Input` **estático** del target. Es el caso crítico: en `T=0` no corre ningún código nuestro — AWS construye el `fired` a partir de ese `Input` escrito meses antes. Si falta ahí, no hay runtime que lo rellene |
> | **Kairos**, al provisionar | Relevar `tenant_id` en `provision_requested`, o la `iot_alert_rule` aterriza en la partición equivocada |
> | **Iris**, al correlacionar | Relevar el `tenant_id` del evento de breach al construir el `fired` |
>
> **Endurecimiento opcional (no bloqueante):** Iris resuelve el `action_payload` por `correlation_id`, un UUID no adivinable proveniente de `metri.iot`, que es un emisor confiable. Guardar el `tenant_id` junto a la fila de correlación y contrastarlo con el del breach añadiría defensa en profundidad ante un evento mal formado, pero no cubre un agujero abierto.

### 4.2 Quién ejecuta cada `action_type`

**No existe ni se construirá un consumidor de bus dentro de Metri Engine Core.** El Core es un servicio gRPC: toda escritura entra por `MetriService.Transact`, atravesando el interceptor Cedar y QuotaGuard. Un consumidor de EventBridge que escribiera directamente en el EAV bypasearía ambos, y eso está prohibido.

La pata de retorno la ejecutan **componentes que ya consumen el bus y ya son clientes gRPC del Core**:

| `action_type` | Ejecutor real | Cómo lo recibe | Qué hace |
|---|---|---|---|
| `RPC_CALL` | **Metri Event Router** (`FiredHandler`) | Regla EventBridge: `detail.action_type = RPC_CALL` | Invoca `MetriService.Transact` sobre el Core. Ya es cliente gRPC (`MoiraRoutingService`); reutiliza el pool y el retry existentes |
| `DISPATCH_NOTIFICATION` | **@metri-notifications** (`Dispatcher`) | Añade `system.scheduled_job.fired` con `detail.action_type = DISPATCH_NOTIFICATION` a su patrón | Resuelve `target_user_id` / `target_group_id` / `target_role_id` y despacha por sus canales |
| `WEBHOOK` | **Metri Event Router** (`FiredHandler`) | Regla EventBridge: `detail.action_type = WEBHOOK` | Resuelve `target_webhook_id` → `webhook_endpoint` y hace POST a su `target_url`, con el `auth_token` descifrado y su `max_retries`. **Nunca a una URL del payload** (§10.5). Es el único componente autorizado a tocar la red pública |
| `EDA_BROADCAST` | **Regla nativa de EventBridge** | Regla con Input Transformer sobre el propio bus | Re-publica el `action_payload` como evento nuevo. Cero compute |

**Por qué el Event Router y no un componente nuevo:** ya consume el bus, ya mantiene un pool gRPC contra el Core, y ya es por diseño *"el único componente del sistema que toca la red pública de telecomunicaciones"* (Componente Externo 01). Crear un ejecutor aparte duplicaría las tres capacidades y abriría una segunda vía de egreso.

### 4.3 Cierre del ciclo sin realimentación

Tras ejecutar, el `FiredHandler` actualiza el Job llamando a `Transact` con **`suppress_events: true`**:

```
TransactionRequest{
  tenant_id, entity_type: "scheduled_job", entity_id: job_id,
  action: UPDATE,
  payload: {status: "COMPLETED" | "FAILED", last_run_at, run_count},
  suppress_events: true        ← imprescindible
}
```

Sin esa bandera, el `event_rule` `SCHEDULED_JOB_UPDATED` (que tiene `filter_conditions: []`) emitiría `system.scheduled_job.updated`, Chronos haría `UpdateSchedule` y Kairos re-aprovisionaría la `iot_alert_rule` — un ciclo de realimentación en cada disparo. La bandera ya existe en `TransactionRequest` (`metri.proto`) precisamente para censurar la tupla Outbox.

> **`COMPLETED` sólo aplica a `EXACT_TIME`.** Un job `CRON` permanece `ACTIVE` entre disparos; marcarlo completo tras la primera ejecución apagaría una recurrencia semestral en su primer ciclo.

El `action_payload` debe ser **auto-contenido**: incluye todos los parámetros que el ejecutor necesita sin consultar estado en el almacén EAV. Esto garantiza la naturaleza ciega del Boomerang.

---

## 5. El Patrón UUID Determinista — Anti-Race Condition

Metri prohíbe almacenar el ARN devuelto por AWS (`arn:aws:scheduler:...`) en el almacén EAV. Hacerlo crearía una dependencia de escritura de vuelta desde el Hub hacia el motor, introduciendo el anti-patrón de acoplamiento bidireccional.

**Solución:** Chronos construye el nombre del Schedule usando el UUID nativo del `scheduled_job`:

```
schedule_name = "metri-job-{scheduled_job_id}"
# Ejemplo: "metri-job-6f96a3b2-1c4d-4e8a-b2f1-09d7c3e45812"
```

Esto garantiza:
- **UPDATE:** `UpdateSchedule(Name="metri-job-{id}")` — O(1), sin lookup de ARN
- **DELETE:** `DeleteSchedule(Name="metri-job-{id}")` — O(1), sin estado previo en el almacén EAV
- **Fantasmas imposibles:** Si el Job se elimina en el almacén EAV antes de que AWS lo ejecute, Chronos destruye el Schedule por nombre; no puede disparar un Job huérfano

### 5.1 `CreateSchedule` NO es idempotente

Un nombre determinista hace la operación **repetible**, no idempotente. `CreateSchedule` sobre un nombre existente devuelve **`ConflictException`**, no un éxito silencioso. Como EventBridge entrega al menos una vez, Chronos verá reintentos del mismo `created` y debe absorberlos explícitamente:

```go
_, err := client.CreateSchedule(ctx, in)
var conflict *types.ConflictException
switch {
case err == nil:
    return nil
case errors.As(err, &conflict):
    // Reintento del mismo evento, o carrera entre dos entregas.
    // El nombre ya es del Job correcto: converger con Update.
    _, err = client.UpdateSchedule(ctx, toUpdateInput(in))
    return err
default:
    return err   // error real → DLQ
}
```

Simétricamente, `DeleteSchedule` sobre un nombre inexistente devuelve `ResourceNotFoundException`, que en el camino de borrado **es el estado deseado** y debe tratarse como éxito, no como fallo.

> El parámetro `ClientToken` de `CreateSchedule` sólo deduplica reintentos de *la misma petición* dentro de su ventana; no resuelve el caso de dos entregas del bus separadas en el tiempo. El `catch ConflictException → Update` es obligatorio.

### 5.2 Traducción de expresiones — el modelo no habla AWS

`trigger_expression` guarda cron POSIX de 5 campos; EventBridge Scheduler exige 6 campos y prohíbe que día-del-mes y día-de-semana estén ambos definidos: uno debe ser `?`. La traducción es responsabilidad de Chronos:

| `trigger_type` | Valor en el modelo | Lo que envía Chronos |
|---|---|---|
| `CRON` | `"0 8 1 */6 *"` (min h dom mes dow) | `ScheduleExpression: "cron(0 8 1 */6 ? *)"` — se añade el campo año `*` y el `?` sustituye al campo no usado |
| `CRON` | `"0 8 * * MON"` | `cron(0 8 ? * MON *)` — aquí el `?` cae en día-del-mes |
| `EXACT_TIME` | `"1735689600"` (epoch **UTC**) | `ScheduleExpression: "at(2026-01-01T00:00:00)"` con `ScheduleExpressionTimezone: "UTC"` |

> **Trampa de zona horaria en `EXACT_TIME`.** El epoch es un instante absoluto en UTC. Pasar el `iana_timezone` del Job junto a un `at()` derivado de ese epoch desplaza el disparo —5 horas para `America/Bogota`. `iana_timezone` aplica **sólo** a `CRON`, donde la intención humana es "las 8am locales"; en `EXACT_TIME` el timezone del schedule es siempre `UTC`.
>
> El nombre correcto del parámetro es **`ScheduleExpressionTimezone`**, no `Timezone`.

**Epoch en el pasado:** `EXACT_TIME` con un instante ya vencido hace que AWS rechace o nunca dispare el schedule. Chronos debe detectarlo antes de llamar a la API y marcar el Job como `FAILED` con causa explícita, en vez de crear una trampa muerta que nadie volverá a mirar.

**`SUSPENDED`:** el enum de `status` incluye `SUSPENDED`, pero suspender no es borrar. Chronos lo mapea a `UpdateSchedule` con `State: DISABLED`, conservando la trampa para poder reactivarla sin recalcular la expresión.

---

## 6. Flujo End-to-End — Orden de Mantenimiento Semestral

```
[Usuario en Metri UI]
   │ Crea Maintenance Order con recurrencia: "Cada 6 meses, lunes 8am"
   ▼
[Janus Router → EAV (DynamoDB)]
   │ Transacción ACID: crea MaintenanceOrder + ScheduledJob
   │   trigger_type: CRON
   │   trigger_expression: "0 8 1 */6 *"   (1ero de cada semestre, 8am)
   │   iana_timezone: "America/Bogota"
   │   action_type: RPC_CALL
   │   action_payload: {method: "maintenance.execute", order_id: "...", asset_id: "..."}
   │   idempotency_hash: SHA256(order_id + trigger_expression)   ← estático, por Job
   │ Emite (vía Outbox/Moira): system.scheduled_job.created → EventBridge Bus
   │ Responde HTTP 200 OK al usuario (hilo liberado)
   ▼
[Lambda Chronos — Metri Schedulers SAM]
   │ Recibe event: system.scheduled_job.created
   │
   ├─ GUARD 1 — delta de bitácora (§10.4)
   │    ¿claves(delta) ⊆ {last_run_at, run_count, last_error}?
   │    → SÍ: descartar como no-operación. No se llama a AWS.
   │    (en un 'created' no aplica; en cada 'updated' posterior, sí)
   │
   ├─ GUARD 2 — ledger de orden (§10.3)
   │    UpdateItem(HubStateTable, Key={job_id},
   │      ConditionExpression: "attribute_not_exists(job_id)
   │                            OR last_applied_ulid < :ulid")
   │    → ConditionalCheckFailed: evento rezagado. Descartar como ÉXITO.
   │    → OK: este evento es el más nuevo. Continuar.
   │
   │ Construye schedule_name = "metri-job-{id}"
   │ SDK AWS: CreateSchedule(
   │   Name: "metri-job-{id}",
   │   ScheduleExpression: "cron(0 8 1 */6 ? *)",
   │   ScheduleExpressionTimezone: "America/Bogota",
   │   Target: {EventBridgeBus, Input: {tenant_id, action_payload,
   │            idempotency_key: sha256(hash + <aws.scheduler.scheduled-time>)}},
   │   FlexibleTimeWindow: {Mode: OFF}
   │ )
   │ El Input es ESTÁTICO: en T=0 no corre código nuestro. Lo que falte aquí
   │ no lo rellena nadie seis meses después (§4.1).
   │ En ConflictException: converge con UpdateSchedule (§5.1)
   │ En ThrottlingException: backoff con jitter
   │ Agotado el RetryPolicy del target: el evento cae a ChronosDLQ (§10.1)
   ▼
[Dormido en AWS EventBridge Scheduler — meses de latencia pasiva]
   ▼
[T=0 — 1ero del semestre, 8am hora Bogotá]
   │ AWS EventBridge Scheduler dispara
   │ Publica a EventBridge Bus: detail-type="system.scheduled_job.fired"
   │   detail: {action_type: "RPC_CALL", action_payload: {...},
   │            idempotency_key: sha256(idempotency_hash + <aws.scheduler.scheduled-time>)}
   ▼
[Event Router — FiredHandler]        ← NO el Core: el Core no consume el bus (§4.2)
   │ Recibe fired event (regla: detail.action_type = RPC_CALL)
   │ Reserva idempotency_key en DynamoDB (attribute_not_exists) — duplicados fuera
   │ Invoca gRPC MetriService.Transact → Cedar + QuotaGuard en el interceptor
   │   maintenance.execute(order_id, asset_id)
   │ Actualiza el Job vía Transact con suppress_events: true (§4.3)
   │   CRON permanece ACTIVE; sólo EXACT_TIME pasa a COMPLETED
   ▼
[Usuario en Metri UI]
   Recibe notificación: "Mantenimiento ejecutado exitosamente"
```

---

## 7. Flujo de Disparo Telemétrico — `trigger_type: TELEMETRY`

### El problema del diseño ingenuo

Un diseño incorrecto haría que el Lambda Harvester de Metri IoT leyera una DynamoDB Table perteneciente al SAM de Metri Schedulers. Esto crea **acoplamiento estructural**: dos componentes externos independientes compartiendo storage. Si el SAM de Schedulers se degrada, la telemetría de IoT también falla. Si el schema de la tabla cambia, ambos SAMs deben actualizarse en sincronía. Es el anti-patrón de microservicios más clásico.

### La regla de oro

> **Dos SAMs independientes nunca comparten bases de datos. El bus EventBridge es el único contrato entre ellos.**

### El diseño correcto: `iot_alert_rule` como implementación del trigger telemétrico

Cuando un `ScheduledJob` de tipo `TELEMETRY` se crea, Kairos **no escribe en el storage de Metri IoT ni consulta el almacén EAV**. En cambio, **delega la detección del evento físico al componente que ya sabe hacerlo**: Metri IoT, y retiene únicamente en su propia tabla privada el contexto que necesitará para armar el Boomerang cuando llegue el breach. Lo hace a través del bus, creando una `iot_alert_rule` real en Metri Engine Core, con el `job_id` como `correlation_id`.

```
[Usuario en Metri UI]
   │ Configura Job: "Si VIBRACIÓN > 5000 RPM → ejecutar Maintenance Order"
   │   asset_id: 99
   │   trigger_type: TELEMETRY
   │   trigger_expression: "VIBRATION > 5000"  ← métrica + operador + valor
   │   action_type: RPC_CALL
   │   action_payload: {method: "maintenance.execute", order_id: "...", asset_id: "99"}
   ▼
[Janus Router → EAV (DynamoDB)]
   │ Transacción ACID: crea ScheduledJob
   │ Emite (vía Outbox/Moira): system.scheduled_job.created → EventBridge Bus
   │   detail: {job_id, trigger_type: TELEMETRY, trigger_expression, action_payload, ...}
   ▼
[Lambda Kairos — Metri Schedulers SAM]
   │ Detecta trigger_type = TELEMETRY
   │
   ├─ GUARD 1 — delta de bitácora (§10.4)
   │    Si el delta sólo toca {last_run_at, run_count, last_error} → descartar.
   │    Sin esto, cada disparo reaprovisionaría una iot_alert_rule DUPLICADA.
   │
   ├─ GUARD 2 — ledger de orden (§10.3)
   │    UpdateItem condicional sobre last_applied_ulid en HubStateTable.
   │    Rezagado → descartar como éxito.
   │
   │ Parsea trigger_expression:
   │   metric    = "VIBRATION"
   │   operator  = GT
   │   threshold = 5000.0
   │
   │ NO crea EventBridge Scheduler
   │ NO escribe en storage de Metri IoT
   │
   │ Persiste en HubStateTable (tabla privada de este SAM):
   │   {correlation_id: job_id, action_payload, idempotency_hash, ttl}
   │
   │ Emite al bus: system.iot.alert_rule.provision_requested
   │   detail: {
   │     tenant_id: <relevado del evento de entrada>,
   │     correlation_id: job_id,          ← la llave de retorno
   │     asset_id: 99,
   │     metric_code: "VIBRATION",        ← required en iot_alert_rule
   │     threshold_operator: "GT",
   │     threshold_value: 5000.0,
   │     unit_of_measure: <resuelto del iot_device_profile del asset>,
   │     alert_severity: "CRITICAL",
   │     notify_users: [],
   │     notify_groups: []
   │   }
   ▼
[Event Router — bus → gRPC]          ← única vía de escritura sancionada (§4.2)
   │ Recibe system.iot.alert_rule.provision_requested
   │ Invoca MetriService.Transact(entity_type: "iot_alert_rule", action: CREATE)
   │ Crea iot_alert_rule en el almacén EAV con correlation_id = job_id
   │   Cedar y QuotaGuard se aplican como en cualquier escritura
   │ El motor EDA (Moira) emite: system.iot.alert.created → Metri IoT activa la regla
   │ (Kinesis Stream + RAM cache hot-reload en <300ms — ver Componente 04)
   ▼
[Lambda Harvester — Metri IoT SAM]
   │ Recibe MQTT: asset_id=99, VIBRATION=5200 RPM
   │ Evalúa reglas desde RAM local: {operator: GT, threshold: 5000, severity: CRITICAL}
   │ Evalúa: 5200 > 5000 → TRUE → BREACH
   │
   │ Emite: system.iot.alert.breach → EventBridge Bus
   │   detail: {
   │     asset_id: 99,
   │     metric: "VIBRATION",
   │     value: 5200,
   │     threshold: 5000,
   │     severity: "CRITICAL",
   │     correlation_id: job_id   ← el mismo job_id que Kairos inyectó
   │   }
   ▼
[Lambda Iris — Metri Schedulers SAM]
   │ Escucha también system.iot.alert.breach
   │ Filtra: solo eventos con correlation_id presente
   │ Lee action_payload + idempotency_hash de HubStateTable (PK = correlation_id)
   │ Deriva idempotency_key = sha256(hash + breach_timestamp)  — §10.2
   │   Sin round-trip al Core: el Boomerang sigue siendo ciego y sin latencia de red en T=0
   │ Publica: system.scheduled_job.fired → EventBridge Bus
   │   detail: {action_type: RPC_CALL, action_payload,
   │            idempotency_key: sha256(idempotency_hash + breach_timestamp)}
   ▼
[Event Router — FiredHandler]
   │ Reserva idempotency_key en DynamoDB (attribute_not_exists) — duplicados fuera
   │ Invoca gRPC MetriService.Transact: maintenance.execute(order_id, asset_id)
   │ Actualiza el Job con suppress_events: true (§4.3)
```

### Por qué este diseño es correcto

| Principio | Cumplimiento |
|---|---|
| **Sin storage compartido** | Ninguna función del Hub lee ni escribe tablas de Metri IoT; el Harvester nunca lee tablas de Schedulers. Cada SAM es dueño exclusivo de sus tablas |
| **Estado mínimo y privado** | El Hub mantiene una sola tabla (`HubStateTable`), escrita por Chronos y Kairos y leída por Iris — las tres del mismo SAM. No es storage compartido: es estado interno del componente, con TTL atado al ciclo de vida del Job |
| **Bus como único contrato** | La integración es: Kairos emite → Metri Engine crea regla → Metri IoT detecta → Iris consume breach |
| **Reutilización del motor de reglas** | La `iot_alert_rule` es una entidad real en el almacén EAV con histórico, auditoría y lifecycle completo |
| **Desacoplamiento de fallos** | Si Metri Schedulers cae, Metri IoT sigue operando sus propias alertas normalmente |
| **Eliminación limpia** | Al eliminar el ScheduledJob, Kairos emite `system.iot.alert_rule.deprovision_requested` → Metri Engine elimina la `iot_alert_rule` correspondiente. La fila **no se borra**: se purga el `action_payload` pero sobrevive como lápida del ledger con TTL, para que un `created` rezagado no reviva la regla (§10.3) |

### Estado del contrato con Metri IoT

Las tres piezas que este flujo necesita **ya están declaradas** en los contratos de los otros componentes:

| Pieza | Dónde vive ahora |
|---|---|
| `correlation_id` en la regla de alerta | `models/iot_alert_rule.json` — `uuid`, `is_system`, indexado, nulo en reglas creadas por el usuario |
| Propagación hasta el evento de breach | `Rule.CorrelationID` → `AlertPayload.CorrelationID` en el Harvester (Componente Externo 04 §3). El Harvester la transporta sin interpretarla |
| `metric_code` para discriminar la variable | `models/iot_alert_rule.json` — `required`, con terminología `urn:api:system:metri:variables` |
| `provision_requested` / `deprovision_requested` | Declarados como eventos consumidos en Componente Externo 04 §2.2 |

> **Alternativa sin `correlation_id`:** correlacionar por `rule_id`. Requiere que Kairos capture el `rule_id` devuelto al crear la `iot_alert_rule` e indexe `HubStateTable` por `rule_id` en vez de por `job_id`. Se descartó porque obliga a Kairos a esperar la respuesta de una creación asíncrona.

---

## 8. Arquitectura del Proyecto e Infraestructura SAM

### 8.1 Estructura de carpetas y archivos

Tres binarios Go independientes sobre un núcleo compartido. Los paquetes de `internal/` existen porque **más de una función los necesita**; ninguno se creó por simetría.

```
metri-schedulers/
├── template.yaml                    ← IaC SAM — todos los recursos AWS (§8.3)
├── samconfig.toml                   ← perfiles de despliegue por entorno
├── Makefile                         ← build, test, deploy, local (§8.4)
├── go.mod
├── go.sum
│
├── cmd/                             ← un binario por función; sin lógica de negocio
│   ├── chronos/
│   │   └── main.go                  ← Chronos — trampas temporales (CRON / EXACT_TIME)
│   ├── kairos/
│   │   └── main.go                  ← Kairos  — provisión telemétrica (TELEMETRY)
│   └── iris/
│       └── main.go                  ← Iris    — correlación de retorno (breach → fired)
│
├── internal/
│   ├── event/                       ← sobre entrante del bus
│   │   ├── envelope.go              ← parse de detail: job_id, tenant_id, ulid, delta
│   │   └── delta.go                 ← ¿el delta sólo toca campos de bitácora? (§10.4)
│   │
│   ├── ledger/                      ← garantía de orden — usado por Chronos y Kairos
│   │   ├── ledger.go                ← UpdateItem condicional sobre last_applied_ulid (§10.3)
│   │   └── tombstone.go             ← lápida de borrado con TTL: mata al 'created' rezagado
│   │
│   ├── correlation/                 ← contexto TELEMETRY — Kairos escribe, Iris lee
│   │   └── store.go                 ← action_payload + idempotency_hash por correlation_id
│   │
│   ├── scheduler/                   ← única frontera con la API de EventBridge Scheduler
│   │   ├── client.go                ← Create/Update/Delete + ConflictException → Update (§5.1)
│   │   ├── expression.go            ← cron 5→6 campos, at(), ScheduleExpressionTimezone (§5.2)
│   │   └── target.go                ← Input del target con <aws.scheduler.scheduled-time> (§10.2)
│   │
│   ├── telemetry/
│   │   └── parser.go                ← gramática <METRIC_CODE> <OP> <VALOR> → GT/LT/... (§3.2)
│   │
│   ├── bus/
│   │   └── publisher.go             ← PutEvents: provision_requested, deprovision, fired (§4.1)
│   │
│   ├── idempotency/
│   │   └── key.go                   ← sha256(idempotency_hash + instante de la ocurrencia)
│   │
│   └── otel/
│       └── tracer.go                ← OpenTelemetry; propaga traceparent al evento fired
│
└── docs/
    └── architecture/ → (este documento)
```

> **No hay carpeta `proto/`.** El Hub **no habla gRPC con nadie**: recibe del bus y publica al bus. La única escritura hacia Metri Engine Core la ejecuta el Event Router (§4.2). Si en el futuro aparece un `proto/` aquí, es señal de que alguien rompió el desacople.

**Reparto de paquetes por función** — cada binario enlaza sólo lo que usa, y eso se refleja en su IAM:

| Paquete | Chronos | Kairos | Iris |
|---|:---:|:---:|:---:|
| `event` | ✅ | ✅ | ✅ |
| `ledger` | ✅ | ✅ | — |
| `scheduler` | ✅ | — | — |
| `telemetry` | — | ✅ | — |
| `correlation` | — | ✅ escribe | ✅ lee |
| `bus` | — | ✅ | ✅ |
| `idempotency` | ✅ (en el target) | — | ✅ (en el breach) |

### 8.2 Topología de recursos AWS

```
                     EventBridge Bus (metri-event-bus) — propiedad del Core
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        │ Rule                        │ Rule                        │ Rule
        │ trigger_type ∈ CRON,        │ trigger_type = TELEMETRY    │ source=metri.iot
        │ EXACT_TIME                  │                             │ correlation_id exists
        ▼                             ▼                             ▼
   ┌──────────┐                  ┌──────────┐                  ┌──────────┐
   │ CHRONOS  │                  │  KAIROS  │                  │   IRIS   │
   │ 128 MB   │                  │ 128 MB   │                  │ 128 MB   │
   │          │                  │          │                  │ conc. 50 │
   └────┬─────┘                  └────┬─────┘                  └────┬─────┘
        │                             │                             │
        │ scheduler:*                 │ ledger + correlación        │ GetItem
        │ + ledger                    │ + PutEvents                 │ + PutEvents
        ▼                             ▼                             ▼
 ┌──────────────────┐          ┌───────────────────────────────────────────┐
 │ EventBridge      │          │  HubStateTable (DynamoDB, PK job_id)      │
 │ Scheduler        │          │  · last_applied_ulid  → ledger (§10.3)    │
 │ ScheduleGroup:   │          │  · action_payload     → correlación (§7)  │
 │ metri-jobs-{env} │          │  · expires_at         → TTL / lápidas     │
 │                  │          └───────────────────────────────────────────┘
 │ metri-job-{uuid} │
 └────────┬─────────┘
          │ T=0 — asume SchedulerExecutionRole
          │ (ninguna Lambda del Hub se ejecuta aquí)
          ▼
   EventBridge Bus ──► system.scheduled_job.fired ──► ejecutores (§4.2)

   Fault tolerance: una DLQ estándar por función — ChronosDLQ / KairosDLQ / IrisDLQ
   alimentadas por el DeadLetterConfig del target, no por la Lambda (§10.1)
```

**Inventario de recursos del stack:**

| Recurso | Tipo | Nota |
|---|---|---|
| `LambdaChronos` / `LambdaKairos` / `LambdaIris` | `AWS::Serverless::Function` | Go AOT sobre `provided.al2023`, `arm64` |
| `ChronosDLQ` / `KairosDLQ` / `IrisDLQ` | `AWS::SQS::Queue` | **Estándar**, no FIFO (§10.1) |
| `HubStateTable` | `AWS::DynamoDB::Table` | PK `job_id`, TTL `expires_at`, PAY_PER_REQUEST |
| `MetriJobsScheduleGroup` | `AWS::Scheduler::ScheduleGroup` | Uno por entorno, **no por tenant** (§8.4) |
| `SchedulerExecutionRole` | `AWS::IAM::Role` | Único principal que publica el `fired` en `T=0` |
| Alarmas + Dashboard | `AWS::CloudWatch::*` | Cuota de schedules, profundidad de DLQ, throttling |

**El Hub no crea el bus.** `metri-event-bus` pertenece al stack del Core y llega como parámetro. Este stack sólo añade reglas sobre él.

### 8.3 `template.yaml` y Makefile → anexo

El `template.yaml` completo y el Makefile viven en **[COMPONENTE_EXTERNO_05_ANEXO_IAC.md](COMPONENTE_EXTERNO_05_ANEXO_IAC.md)**. Se extrajeron porque son implementación: cambian con cada ajuste de despliegue sin que la arquitectura se mueva, y 350 líneas de YAML entre la topología y las garantías del componente hacían ilegible el documento.

Lo que el anexo materializa, y dónde se argumenta cada cosa:

| Recurso del stack | Garantía que sostiene |
|---|---|
| `LambdaChronos` / `LambdaKairos` / `LambdaIris` | Responsabilidad única y radio de fallo aislado (§1.1) |
| `EventPattern` con `detail.trigger_type` | El bus rutea; ninguna función ramifica (§1.1) |
| Políticas IAM por función | Chronos sin acceso al payload de correlación; Iris sólo lectura (§10) |
| `RetryPolicy` + `DeadLetterConfig` por target | Reintento real, no retención (§10.1) |
| `ChronosDLQ` / `KairosDLQ` / `IrisDLQ` — estándar | EventBridge no admite FIFO como DLQ (§10.1) |
| `HubStateTable` con TTL `expires_at` | Ledger de orden y lápidas de borrado (§10.3) |
| `MetriJobsScheduleGroup` | Agrupación por entorno, nunca por tenant (§8.4) |
| `SchedulerExecutionRole` | Único principal que publica el `fired` en `T=0` (§4.2) |
| Alarmas de DLQ y latencia de Iris | Un mensaje en DLQ nadie lo reintenta solo (§10.1) |

### 8.4 Límites de AWS que condicionan el diseño

El documento asumía capacidad infinita. No la hay, y dos de estos límites son alcanzables con el volumen previsto de un CMMS multitenant.

| Límite | Por qué importa aquí | Mitigación |
|---|---|---|
| **Schedules por cuenta y región** (cuota por defecto en el orden del millón, ajustable) | Un tenant con miles de activos y mantenimientos preventivos semestrales genera varios `scheduled_job` por activo — y el fan-out de pre-notificación **multiplica por cada offset**: `[1440, 30]` triplica el conteo | Métrica de schedules activos en el dashboard con alarma al 70 % de la cuota vigente, y solicitud de aumento antes de onboarding masivo |
| **Tasa de `CreateSchedule`** | [Bulk CSV Upload](12_FASE_BULK_CSV_UPLOAD_INTEGRATION.md) puede dar de alta miles de planes de mantenimiento en una sola operación. Cada uno detona un `created` → una llamada a la API. El throttling es seguro (`ThrottlingException`) pero satura la DLQ si no se absorbe | Backoff exponencial con jitter dentro de Chronos ante `ThrottlingException`, y concurrencia reservada que acote el paralelismo del propio Chronos |
| **Tamaño del evento: 256 KB por entry de `PutEvents`** | El `action_payload` viaja dos veces: en el `Input` del target y en el `fired`. Un payload grande **crea el Job sin error y falla en `T=0`**, meses después, sin nadie mirando | Validar el tamaño serializado **en la creación** del `scheduled_job` y rechazar con error de dominio, no en el disparo |
| **Cuota de Schedule Groups** | Es mucho más baja que la de schedules. Un grupo por tenant no escala en un SaaS multitenant | Agrupar por entorno y tipo de trigger, no por tenant. No hace falta aislar por tenant: el `job_id` es un UUID globalmente único (§5). Para atribución de costos basta un **tag** `tenant_id` en el schedule |
| **Granularidad mínima: 1 minuto** | `CRON` no puede expresar recurrencias sub-minuto | Validar en la creación; un `scheduled_job` no es un scheduler de tiempo real |

> **Modelo de costo:** EventBridge Scheduler se factura por invocación, no por schedule almacenado. Una trampa dormida durante seis meses no cuesta nada hasta que dispara — lo que hace del Patrón Boomerang una opción barata frente a un poller que consulta continuamente.

> **Verificar antes de implementar:** las cuotas de AWS cambian y varían por cuenta. Los valores vigentes deben confirmarse en Service Quotas para la región de despliegue; lo que este documento fija es **qué límites vigilar**, no sus números.

---

## 9. Variables de Entorno

Inyectadas en §8.3. La columna de alcance importa: **una variable que una función no necesita no se le entrega**, por el mismo motivo por el que su IAM está segregado.

| Variable | Descripción | Chronos | Kairos | Iris | Ejemplo |
| :--- | :--- | :---: | :---: | :---: | :--- |
| `ENVIRONMENT` | Entorno de ejecución | ✅ | ✅ | ✅ | `production` |
| `HUB_STATE_TABLE` | Tabla privada del Hub: ledger de orden (§10.3) y correlación `TELEMETRY` (§7) | ✅ | ✅ | ✅ | `metri-scheduler-state-production` |
| `LEDGER_TTL_DAYS` | Vida de las lápidas del ledger. Debe superar el `MaximumEventAgeInSeconds` del target | ✅ | ✅ | ⚪ | `7` |
| `SCHEDULE_GROUP_NAME` | Schedule Group donde se crean las trampas. Uno por entorno, nunca por tenant (§8.4) | ✅ | — | — | `metri-jobs-production` |
| `SCHEDULER_EXECUTION_ROLE_ARN` | Role que AWS asume para publicar el `fired` en `T=0`. Chronos lo pasa con `PassRole`, no lo asume | ✅ | — | — | `arn:aws:iam::123:role/metri-scheduler-execution-production` |
| `METRI_EVENT_BUS_ARN` | ARN del bus. En Chronos es el **destino del target** del Schedule, no un `PutEvents` propio | ✅ | — | — | `arn:aws:events:us-east-1:123:event-bus/metri-event-bus` |
| `METRI_EVENT_BUS_NAME` | Bus al que Kairos publica `provision_requested` e Iris publica `fired` | — | ✅ | ✅ | `metri-event-bus` |
| `AWS_REGION` | Región AWS (inyectada automáticamente por Lambda) | ✅ | ✅ | ✅ | `us-east-1` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Endpoint OTEL para traces (⚪ opcional) | ⚪ | ⚪ | ⚪ | `https://otel.internal:4317` |

> **Chronos no publica al bus.** Recibe el ARN para incrustarlo como *target* del Schedule; quien ejecuta el `PutEvents` en `T=0` es AWS con el `SchedulerExecutionRole`. Por eso su política IAM no incluye `events:PutEvents` — ver §8.3.

> **`DLQ_URL` no existe como variable.** Las DLQ se enganchan por `DeadLetterConfig` en el *target* de EventBridge (§10.1); ninguna función necesita conocer la URL de su cola en tiempo de ejecución.

---

## 10. Seguridad y Garantías

| Principio | Implementación |
|---|---|
| **Idempotencia por ocurrencia** | `idempotency_key = sha256(idempotency_hash + scheduled_time)` en todo evento `fired`. El ejecutor deduplica con escritura condicional en DynamoDB (`attribute_not_exists` + TTL) — ver §10.7. El hash estático solo era insuficiente — ver §10.2 |
| **Sin ARN en EAV** | Patrón UUID determinista: `metri-job-{id}`. Elimina dependencia bidireccional Hub→EAV |
| **Fantasmas imposibles** | DELETE de Job → `DeleteSchedule(Name="metri-job-{id}")` O(1). No puede disparar un Job eliminado |
| **Fault Tolerance** | `RetryPolicy` en el *target* de EventBridge + `DeadLetterConfig` a una SQS **estándar** por función. Ver §10.1 |
| **Sin privilegios congelados** | `created_by` persiste el principal; el ejecutor reevalúa Cedar y `user.status` en `T=0`. Un Job cuyo creador perdió permisos falla ruidosamente, no se ejecuta (§10.5) |
| **Sin SSRF diferido** | `WEBHOOK` referencia `webhook_endpoint`: allowlist por construcción, credencial cifrada e interruptor `is_active`. Nunca una URL del `action_payload` (§10.6) |
| **Least Privilege** | Segregado por función: Chronos sin DynamoDB, Iris sólo `GetItem`, Kairos sin `scheduler:*`. `SchedulerExecutionRole` sólo puede `events:PutEvents` al bus, con guard `aws:SourceAccount`, y el `iam:PassRole` está acotado por `iam:PassedToService` (§8) |
| **No Dual Write** | El bus EDA de Metri Engine es atómico con la escritura EAV a través de `outbox_event` y Moira. No hay Outbox table externa en Schedulers |
| **DST resuelto** | `iana_timezone` delega el horario de verano/invierno a AWS vía `ScheduleExpressionTimezone`. Aplica sólo a `CRON` — ver §5.2 |

### 10.1 Reintentos: retención ≠ reintento

Una versión anterior afirmaba que *"AWS Lambda reintenta hasta 14 días"*. **Es falso y peligroso**: los 14 días son el `MessageRetentionPeriod` de SQS, es decir cuánto tiempo un mensaje **ya fallido** sobrevive esperando reproceso. No es una política de reintento.

Los reintentos automáticos los gobierna el *target* de la regla de EventBridge, no la Lambda:

```yaml
Target:
  RetryPolicy:
    MaximumRetryAttempts: 20          # tope duro del servicio: 185
    MaximumEventAgeInSeconds: 3600    # tope duro del servicio: 86400 (24 h)
  DeadLetterConfig:
    Arn: !GetAtt ChronosDLQ.Arn       # SQS ESTÁNDAR — ver abajo
```

Agotados los intentos **o** superada la edad máxima, EventBridge deposita el evento en la DLQ. Ahí se detiene la automatización: los 14 días de retención son la ventana para que un operador (o un reproceso manual) lo rescate. Sin un consumidor que drene la DLQ, un evento en ella **nunca se reintenta solo**.

> **Por qué las DLQ no pueden ser FIFO.** EventBridge no admite colas SQS FIFO ni como target ni como destino de `DeadLetterConfig`. Una cola FIFO además exige `MessageGroupId` en cada mensaje, valor que EventBridge no tiene forma de proporcionar. El diseño original declaraba `metri-scheduler-dlq.fifo`: no era construible.

**Consecuencia sobre el orden.** Al perder FIFO se pierde también cualquier ilusión de ordenamiento — que EventBridge nunca garantizó de todas formas. La mitigación se detalla en §10.3.

### 10.2 El `idempotency_hash` estático no sobrevive a un `CRON`

`idempotency_hash` es `sha256(parent_id + trigger_expression + offset)`: **constante durante toda la vida del Job**. Combinado con un guard `SET NX EX 86400`, un cron que dispare más de una vez cada 24 h ejecutaría **sólo su primer disparo**; todos los siguientes se descartarían como duplicados. Un job horario se ejecuta una vez y muere en silencio.

La llave debe identificar la *ocurrencia*, no el Job. EventBridge Scheduler inyecta atributos de contexto en el `Input` del target:

```
<aws.scheduler.scheduled-time>   ← instante programado de ESTA ocurrencia
<aws.scheduler.execution-id>     ← identificador único de ESTA ejecución
<aws.scheduler.attempt-number>   ← nº de intento (no invalida la deduplicación)
```

Chronos los incrusta al construir el target, y el evento `fired` viaja con:

```
idempotency_key = sha256(idempotency_hash + scheduled_time)
```

Dos entregas de la misma ocurrencia comparten llave y se deduplican; dos ocurrencias distintas del mismo cron no colisionan jamás. En el camino `TELEMETRY`, donde no hay Scheduler de AWS, Iris usa el `timestamp` del breach como discriminante.

### 10.3 "Fantasmas imposibles" exige un ledger de versión

La garantía de §5 —*"si el Job se elimina antes de que AWS lo ejecute, Chronos destruye el Schedule; no puede disparar un Job huérfano"*— **sólo se sostiene si las mutaciones se aplican en orden**. EventBridge no lo garantiza, y sin FIFO tampoco hay red de seguridad. Dos escenarios rompen la promesa:

| Escenario | Resultado sin protección |
|---|---|
| `created` y `deleted` invertidos | `DeleteSchedule` sobre un nombre inexistente → tratado como éxito (§5.1); luego llega el `created` viejo → **Schedule fantasma permanente** que disparará un Job borrado |
| Dos `updated` invertidos | Prevalece la expresión antigua; la trampa queda programada con la recurrencia equivocada, y nada lo delata hasta el disparo |

**La llave de orden ya existe.** El sobre de Moira transporta un `ulid` por mutación, y los ULID son monotónicamente crecientes y ordenables lexicográficamente. No hace falta inventar un número de versión: basta con propagarlo al `detail` del evento y compararlo.

**El ledger.** Antes de tocar la API de AWS, la función aplica una escritura condicional sobre `HubStateTable` (§8):

```
UpdateItem(
  Key: {job_id},
  UpdateExpression:    "SET last_applied_ulid = :u, expires_at = :ttl",
  ConditionExpression: "attribute_not_exists(job_id) OR last_applied_ulid < :u"
)
```

- **Condición satisfecha** → el evento es más nuevo que todo lo aplicado: procede con `CreateSchedule` / `UpdateSchedule` / `DeleteSchedule`.
- **`ConditionalCheckFailedException`** → el evento es un rezagado. Se descarta **silenciosamente y como éxito**: no es un fallo, no va a la DLQ.

DynamoDB resuelve el compare-and-set de forma atómica, así que dos entregas concurrentes no pueden ambas ganar.

**El caso del borrado es el que exige el ledger.** Un `deleted` no elimina su fila: la deja como **lápida** con el `ulid` del borrado y un TTL. Un `created` rezagado que llegue después encuentra un `last_applied_ulid` mayor y se descarta. Sin la lápida, el fantasma es inevitable: no hay estado contra el cual comparar.

> **El TTL de la lápida lo manda la DLQ, no el target.** Es tentador dimensionarlo contra el `MaximumEventAgeInSeconds` del target (1 h), porque pasada esa ventana EventBridge deja de entregar. Es insuficiente: el evento no desaparece, **cae a la DLQ y ahí sobrevive 14 días**, y el runbook (§12.3) contempla reprocesarla a mano. Un `created` rescatado al día 10 contra una lápida de 7 días recrearía la trampa de un Job ya borrado. El TTL debe superar la retención de la DLQ — `LedgerTtlDays` tiene mínimo 15 y valor por defecto 21.

> **Corrección de una afirmación previa.** Una versión anterior de este documento proponía comparar el `tx_id` del datom. **No es implementable:** el sobre de Moira no transporta `tx_id`, y esa misma versión declaraba a Chronos como función sin estado, de modo que no tenía dónde guardar el último valor aplicado. El ledger con `ulid` corrige ambos errores.

> **Requisito de contrato sobre el Event Router.** El `detail` publicado en EventBridge **debe** incluir `ulid` y `payload.delta`. Moira ya los produce; el publicador debe dejar de descartarlos al construir el evento — ver Componente Externo 01.

### 10.4 El ciclo de realimentación de `on_update`

`SCHEDULED_JOB_UPDATED` declara `filter_conditions: []`: **toda** mutación del Job emite el evento, incluida la que escribe el propio ejecutor al registrar el resultado del disparo. Sin protección, cada ejecución reprovisionaría la trampa — y en `TELEMETRY`, Kairos crearía una `iot_alert_rule` duplicada por disparo.

Dos barreras, deliberadamente redundantes porque fallan de formas distintas:

1. **`suppress_events: true`** en el `Transact` del ejecutor (§4.3). Corta el evento en origen. Falla si algún escritor futuro olvida la bandera.
2. **Inspección del `delta`** en Chronos y Kairos. Si el `payload.delta` toca **únicamente** campos de bitácora, el evento se descarta como no-operación:

```
CAMPOS_DE_BITACORA = {last_run_at, run_count, last_error}
si claves(delta) ⊆ CAMPOS_DE_BITACORA  →  descartar (éxito, sin llamar a AWS)
```

La segunda barrera es la que realmente cierra el agujero: protege frente a cualquier escritor, presente o futuro, sin depender de su disciplina.

> **Lo que NO debe filtrarse.** Un cambio de `status` a `SUSPENDED` **sí** debe reprovisionar (§5.2: `State: DISABLED`), igual que cualquier cambio en `trigger_type`, `trigger_expression`, `iana_timezone` o `action_payload`. El filtro es por *campos de bitácora*, no por "cambios de estado".

### 10.5 Autorización diferida: el problema del principal congelado

Un `scheduled_job` es una **autorización que viaja al futuro**. Se crea hoy y se ejecuta en seis meses. Entre esos dos instantes el creador puede haber sido desactivado, cambiado de rol o abandonado la empresa — y sin un principal persistido, la acción se ejecutaría igual, con privilegios que su dueño ya no tiene. Es escalada de privilegios por el paso del tiempo, sin atacante.

**`created_by` cierra el ciclo.** El ejecutor (`FiredHandler`) reevalúa Cedar en `T=0` con ese principal antes de invocar `Transact`:

```
principal = created_by del evento fired
si user.status ≠ ACTIVE                       → FAILED, no ejecutar
si Cedar deniega (principal, action, resource) → FAILED, no ejecutar
en ambos casos: last_error explícito + DOMAIN_FAULT_DETECTED al bus
```

Esto no añade una vía de autorización nueva: `Transact` ya atraviesa el interceptor Cedar (§4.2). Lo que `created_by` aporta es **contra quién** evaluar, en lugar de contra el servicio que llama.

> **El fallo debe ser ruidoso.** Marcar `FAILED` en silencio convierte una revocación de permisos en un mantenimiento preventivo que dejó de ejecutarse sin que nadie se enterase. Por eso emite `DOMAIN_FAULT_DETECTED` ([10_FASE_GESTION_ERRORES_EDA.md](10_FASE_GESTION_ERRORES_EDA.md)): un Job que muere por autorización es un evento operativo, no un detalle técnico.

**Compromiso operativo que hay que asumir conscientemente.** Reautorizar contra `created_by` significa que **cuando alguien sale de la empresa, sus Jobs programados dejan de dispararse**. Para un recordatorio personal es lo correcto; para el mantenimiento semestral de una planta entera, no. Dos mitigaciones, y la elección es de negocio, no de arquitectura:

| Mitigación | Cuándo |
|---|---|
| Transferencia de propiedad en el offboarding: reasignar `created_by` de los Jobs del saliente | Procesos de planta que deben sobrevivir a la persona |
| Crear el Job bajo un principal de servicio del tenant en vez de un humano | Jobs proyectados por `SagaBuilder` desde una entidad madre organizacional |

Lo que **no** es aceptable es la alternativa que había antes: no reevaluar nada.

### 10.6 `WEBHOOK` sin URL libre

El diseño original ponía la URL de destino dentro del `action_payload`. Eso es un **SSRF diferido**: un JSON creado por un usuario, sin allowlist, ejecutado meses después por el único componente del sistema con salida a la red pública — que desde dentro de la VPC alcanza metadata de instancia y servicios internos.

`target_webhook_id` lo sustituye por una referencia a `webhook_endpoint`, entidad que ya existía en el modelo y que aporta cuatro cosas que un string no puede:

| Propiedad de `webhook_endpoint` | Qué resuelve |
|---|---|
| `target_url` gestionado como entidad | Sólo se alcanzan destinos dados de alta y autorizados por Cedar: da **revocabilidad y auditoría**, no inmunidad al SSRF (ver abajo) |
| `auth_token` con Envelope Encryption | La credencial no vive en claro dentro de un `action_payload` con `track_history` |
| `is_active` | Cortar un destino comprometido apaga **todos** los Jobs que lo usan, sin tocarlos uno a uno |
| `max_retries` | Política de reintento del destino, no inventada por el ejecutor |

> **Referenciar la entidad NO basta contra el SSRF.** Es tentador llamar a esto una
> allowlist, y no lo es en sentido de seguridad: la lista de destinos **la puebla un
> usuario**. Un `webhook_endpoint` dado de alta apuntando a `169.254.169.254`
> alcanzaría las credenciales de la instancia desde el único componente con salida a
> la red pública. El ejecutor resuelve el nombre y **rechaza todo destino que apunte
> a una dirección interna** —loopback, rangos privados, link-local— porque un dominio
> público puede resolver a una IP privada. Lo que la entidad sí aporta es
> revocabilidad (`is_active`), credencial cifrada en reposo y trazabilidad de alta.

**Regla de validación:** `action_type = WEBHOOK` con `target_webhook_id` nulo es un Job inválido y debe rechazarse **en la creación**. Aceptarlo produce exactamente el fallo diferido que este documento persigue en §8.4: se crea sin error y revienta en `T=0`, meses después.

### 10.7 El guard de idempotencia no usa Valkey

Versiones anteriores de este documento especificaban `SET NX EX` sobre Valkey. **Valkey no existe en la arquitectura**: el `template.yaml` de Metri Engine documenta que fue eliminado deliberadamente junto con la VPC y los Interface Endpoints, porque representaba el 80 % de la factura AWS sin añadir protección real (Lambda ya está tras CloudFront + WAF, con IAM Least Privilege y KMS).

Reintroducirlo por un guard de deduplicación revertiría una decisión de coste ya tomada y desplegada.

**La sustitución no es un apaño**: DynamoDB ofrece exactamente la misma garantía que se necesita —compare-and-set atómico— con la infraestructura que ya existe.

```
PutItem(
  Item: {idempotency_key, reserved_at, expires_at},
  ConditionExpression: "attribute_not_exists(idempotency_key)"
)
```

- **Condición satisfecha** → la ocurrencia es nueva: ejecutar.
- **`ConditionalCheckFailedException`** → ya se ejecutó: descartar sin error.
- **Cualquier otro error** → **nunca** se concede la reserva. Tratar un fallo de infraestructura como "llave libre" ejecutaría la acción de negocio sin ninguna protección.

Es además el patrón que el propio sistema ya usa para la blacklist de tokens HMAC revocados (`REVOKED#<jti>` con TTL nativo), así que no introduce un mecanismo nuevo.

> **El TTL debe superar el `MaximumEventAgeInSeconds` del target.** Mientras EventBridge pueda reentregar el evento, la llave tiene que seguir reservada. Con el target a 1 h, 24 h da holgura de sobra.

---

## 11. Diagrama de Flujo Maestro (El Boomerang)

```mermaid
sequenceDiagram
    participant FE as Frontend / API
    participant JR as Janus Router + EAV (DynamoDB)
    participant BUS as EventBridge Bus (Metri Engine)
    participant CH as Lambda Chronos (Schedulers SAM)
    participant KA as Lambda Kairos (Schedulers SAM)
    participant IR as Lambda Iris (Schedulers SAM)
    participant EB as AWS EventBridge Scheduler
    participant DT as HubStateTable (privada de Schedulers)
    participant IOT as Lambda Harvester (IoT SAM)
    participant ER as Event Router (FiredHandler)

    FE->>JR: Crea ScheduledJob (CRON / EXACT_TIME / TELEMETRY)
    JR->>JR: Transacción ACID — EAV (DynamoDB)
    JR->>BUS: system.scheduled_job.created (vía Moira)
    JR-->>FE: HTTP 200 OK (hilo liberado)

    Note over BUS: El EventPattern rutea por detail.trigger_type

    alt trigger_type = CRON o EXACT_TIME
        BUS->>CH: Evento entregado sólo a Chronos
        CH->>CH: Guard §10.4 — ¿delta sólo de bitácora? → descartar
        CH->>DT: Guard §10.3 — UpdateItem condicional (last_applied_ulid < ulid)
        alt evento rezagado
            DT-->>CH: ConditionalCheckFailed
            Note over CH: Descartado como ÉXITO. AWS no se toca.
        else evento más nuevo
            DT-->>CH: OK
            CH->>EB: CreateSchedule(Name="metri-job-{id}", expr, tz, Input estático)
            Note over EB: Trampa temporal durmiente en AWS
            Note over CH: Chronos termina aquí. No participa en T=0.
            EB-->>BUS: T=0 → system.scheduled_job.fired (vía SchedulerExecutionRole)
        end
    else trigger_type = TELEMETRY
        BUS->>KA: Evento entregado sólo a Kairos
        KA->>KA: Guard §10.4 — ¿delta sólo de bitácora? → descartar
        KA->>DT: Guard §10.3 + persiste {correlation_id, action_payload, hash}
        KA->>BUS: system.iot.alert_rule.provision_requested (correlation_id=job_id)
        Note over KA: Kairos termina aquí. No espera el breach.
        BUS->>ER: provision_requested
        ER->>JR: gRPC Transact — crea iot_alert_rule con correlation_id
        JR->>BUS: system.iot.alert.created (vía Moira) → Metri IoT activa la regla
        Note over IOT: Harvester evalúa telemetría MQTT normalmente
        IOT->>BUS: system.iot.alert.breach (correlation_id=job_id)
        BUS->>IR: Entregado sólo a Iris (filtro: correlation_id exists)
        IR->>DT: Lee action_payload por correlation_id (sin tocar el Core)
        IR->>BUS: system.scheduled_job.fired + action_payload
    end

    BUS->>ER: Consume fired event (el Core no escucha el bus — §4.2)
    ER->>ER: Reserva idempotency_key (DynamoDB condicional)
    ER->>JR: gRPC Transact — ejecuta la acción (Cedar + QuotaGuard)
    ER->>JR: Transact status + suppress_events:true (evita realimentación)
    Note over JR: CRON sigue ACTIVE; sólo EXACT_TIME pasa a COMPLETED

    opt Borrado del Job
        JR->>BUS: system.scheduled_job.deleted
        BUS->>CH: Entregado a Chronos
        CH->>DT: Escribe LÁPIDA (last_applied_ulid del borrado + TTL)
        CH->>EB: DeleteSchedule — ResourceNotFound se trata como éxito (§5.1)
        Note over DT: Sin la lápida, un 'created' rezagado<br/>recrearía un Schedule fantasma (§10.3)
    end
```

---

## 12. Operación: reconciliación de deriva y runbook

### 12.1 El ledger detecta el fantasma; nada detecta su inverso

§10.3 impide que un evento rezagado cree un Schedule para un Job borrado. Pero existe la divergencia contraria y el documento no la cubría: **un Job `ACTIVE` en el EAV sin Schedule en AWS**. Se produce por caminos perfectamente normales:

| Causa | Resultado |
|---|---|
| El evento agotó su `RetryPolicy` y cayó a la DLQ (§10.1) | El Job existe, la trampa nunca se creó |
| `ThrottlingException` sostenida en un import masivo (§8.4) | Idem, para un lote entero |
| Alguien borró un Schedule a mano en la consola de AWS | La trampa desaparece sin que el EAV se entere |

En los tres casos **el sistema no falla: simplemente no ocurre nada** el día del mantenimiento. Es el peor modo de fallo posible en un CMMS, porque no hay error que investigar.

### 12.2 Censo periódico — quién puede compararlo

Nadie ve los dos lados a la vez, y eso es deliberado: el Hub no consulta el EAV (§1) y el Core no consume el bus (§4.2). La reconciliación **no puede vivir en ninguno de los dos**.

Vive donde ya existe el patrón: el **`WatchdogHandler` del Event Router**, que ya se dispara cada 5 minutos por EventBridge Scheduler para recuperar `outbox_event` huérfanos, y que ya es cliente gRPC del Core.

```
[Chronos — invocación programada, rate(1 hour)]
   │ scheduler:ListSchedules sobre metri-jobs-{env}
   │ Publica system.scheduler.census (paginado)
   │   detail: {page, total_pages, job_ids: [...]}
   ▼
[Event Router — WatchdogHandler]
   │ gRPC al Core: Jobs con status=ACTIVE y trigger_type ∈ {CRON, EXACT_TIME}
   │
   ├─ Job ACTIVE sin Schedule  → deriva por pérdida
   │    Reemite system.scheduled_job.updated → Chronos reprovisiona
   │    El ledger (§10.3) hace la operación segura aunque no hubiera deriva
   │
   └─ Schedule sin Job ACTIVE  → deriva por fantasma
        Emite system.scheduled_job.deleted → Chronos destruye la trampa
```

Ambas correcciones son **idempotentes por construcción**: pasan por los mismos guards que cualquier mutación, así que un reconciliador que se ejecute de más no hace daño.

> **Dependencia no satisfecha.** El paso «gRPC al Core: Jobs con `status=ACTIVE`» **no es implementable hoy**: `MetriService` expone `Discovery`, `Explore`, `Query`, `Transact`, `BulkIngest` y `MatchRoutingRulesBatch`, y ninguno lista entidades filtradas por un atributo. `Explore` devuelve valores distintos de un atributo, no las entidades que lo tienen; `Query` es la consulta analítica de dashboards. El comparador necesita un RPC de listado ligero en el Core antes de poder construirse — ver el plan de implementación, bloqueador 6.
>
> La otra mitad de esta sección —el censo, la métrica `ActiveSchedules` y la alarma de cuota— **sí está operativa** y aporta valor sin el comparador.

> **El censo se pagina.** Un `PutEvents` admite 256 KB por entry (§8.4). Un tenant grande supera ese límite en identificadores, así que el censo viaja en páginas numeradas y el comparador espera a tener todas antes de concluir que algo falta. Comparar contra un censo incompleto **borraría trampas legítimas**: es la única operación de este documento capaz de destruir datos, y por eso el reconciliador aborta si falta cualquier página.

### 12.3 Runbook

| Síntoma | Causa probable | Acción |
|---|---|---|
| `ChronosDLQ` con mensajes | Throttling en import masivo, o `trigger_expression` inválida | Inspeccionar el mensaje. Si es throttling, reprocesar la cola; si es expresión inválida, el Job es irrecuperable: marcar `FAILED` con `last_error` |
| `IrisDLQ` con mensajes | Fila de correlación ausente o caducada por TTL | El Job telemétrico perdió su contexto. Reaprovisionar reemitiendo `scheduled_job.updated` |
| Divergencias recurrentes en el censo | El `RetryPolicy` es insuficiente para la tasa de altas | Subir `MaximumRetryAttempts` y la concurrencia reservada de Chronos antes que ampliar el censo |
| Job `FAILED` con `last_error` de autorización | El creador perdió permisos o fue desactivado (§10.5) | Decisión de negocio: transferir `created_by` o dar de baja el Job. No reactivar sin reasignar |
| Alarma de cuota de schedules al 70 % | Crecimiento orgánico, o fan-out de pre-notificación multiplicando | Solicitar aumento de cuota **antes** del siguiente onboarding; revisar si los offsets de `prenotify` son todos necesarios |
| Un mantenimiento no se ejecutó y no hay error | Deriva no detectada, o `idempotency_key` mal construida (§10.2) | Verificar el censo más reciente y el `run_count` del Job |

> **Ningún mensaje en DLQ se reintenta solo** (§10.1). Una DLQ con profundidad estable no es un sistema en reposo: es trabajo perdido esperando a que alguien lo mire.

---

## 13. Tabla de Decisiones de Diseño

| Decisión | Alternativa Descartada | Razón |
|---|---|---|
| Bus EDA nativo como trigger del Hub | DynamoDB Outbox + Streams propio | El motor transaccional de Metri Engine ya guarda los eventos en EAV atómicamente (outbox_event) para Moira; Outbox propio en Schedulers es redundante |
| UUID determinista como schedule name | Guardar ARN en EAV | Elimina acoplamiento bidireccional Hub→Core; DELETE y UPDATE son O(1) |
| `TELEMETRY` vía provisión de `iot_alert_rule` + tabla de correlación **privada** | Trigger store propio consultado directamente por el Harvester de Metri IoT | Reutiliza el motor de reglas existente sin acoplar storage entre SAMs. La tabla del Hub es estado interno, no un contrato compartido (§7) |
| `action_payload` auto-contenido (Boomerang) | El Hub consulta el almacén EAV vía gRPC en el disparo | Metri Schedulers no puede depender de latencia de red hacia el Core en `T=0`. Para `CRON`/`EXACT_TIME` el payload viaja en el target del Schedule; para `TELEMETRY` vive en la tabla de correlación local |
| Tres funciones de responsabilidad única (Chronos / Kairos / Iris) | Una sola Lambda que ramifica por `trigger_type` | Radio de fallo aislado, IAM mínimo real por función (Chronos sin DynamoDB, Iris sólo lectura), y escalado independiente entre el camino frío de aprovisionamiento y el camino caliente de breaches |
| Ruteo por `EventPattern` sobre `detail.trigger_type` | `switch (trigger_type)` dentro del handler | El bus entrega a cada función sólo lo suyo. Un `trigger_type` nuevo es una regla nueva, no una rama nueva en código ya desplegado |
| Una DLQ estándar por función, 14 días de retención | Una DLQ FIFO compartida por todo el stack | Un mensaje envenenado en el camino telemétrico no bloquea el aprovisionamiento temporal. **Además, EventBridge no admite colas FIFO como target ni como DLQ**, por lo que la FIFO compartida del diseño anterior no era construible |
| Nombres propios por función (Chronos / Kairos / Iris) | Reutilizar el nombre `Hermes` | `Hermes` ya identifica al Unificador Analítico de Janus (05.04). Dos componentes homónimos degradan logs, dashboards y alarmas |
| `CreateSchedule` con `catch ConflictException → UpdateSchedule` | Asumir que un nombre determinista hace la creación idempotente | `CreateSchedule` falla con `ConflictException` sobre un nombre existente. Con entrega at-least-once, el reintento es inevitable |
| `idempotency_key` = hash + `<aws.scheduler.scheduled-time>` | `idempotency_hash` estático como llave de deduplicación | Un hash constante con TTL de 24 h silencia todos los disparos de un cron sub-diario tras el primero |
| Ledger `last_applied_ulid` con `UpdateItem` condicional | Confiar en el orden de entrega del bus, o comparar un `tx_id` que el sobre no transporta | EventBridge no garantiza orden y sin FIFO no hay red. El ULID de Moira ya es monotónico; DynamoDB da el compare-and-set atómico. La **lápida** en el borrado es lo único que impide un Schedule fantasma |
| Descartar eventos cuyo `delta` sólo toca campos de bitácora | Confiar únicamente en `suppress_events: true` | La bandera depende de la disciplina del escritor. La inspección del `delta` protege frente a cualquier escritor presente o futuro |
| Go en `arm64` (Graviton) | Rust / Node.js / Python | Cold-start <15ms, excelente ecosistema (AWS SDK v2 maduro), bajo consumo de RAM y rendimiento nativo óptimo en Graviton sin el costo de desarrollo de Rust |

---

## 14. Fases de Implementación (Roadmap)

| Fase | Componente | Descripción |
|---|---|---|
| **SCH-1** | SAM Stack base | Tres DLQ **estándar**, IAM roles segregados por función con `Resource` acotado y guard de `iam:PassRole`, EventBridge Rules con `EventPattern` filtrado por `trigger_type` y `RetryPolicy` + `DeadLetterConfig` por target |
| **SCH-2** | **Chronos** — `CRON` / `EXACT_TIME` | UUID determinista, `catch ConflictException → Update` (§5.1), traducción de expresiones a 6 campos y `ScheduleExpressionTimezone` (§5.2), `SUSPENDED` → `State: DISABLED`, rechazo de epoch vencido, backoff ante `ThrottlingException`, guard de ledger por `ulid` (§10.3) y descarte de deltas de bitácora (§10.4) |
| **SCH-3** | `HubStateTable` | Tabla DynamoDB privada (PK `job_id`) con TTL. Dos responsabilidades: **ledger de orden** con `UpdateItem` condicional para todos los trigger (§10.3, incluye lápidas de borrado) y **contexto de correlación** para `TELEMETRY` (§7) |
| **SCH-4a** | **Kairos** — provisión telemétrica | Parseo de `trigger_expression`, emisión de `provision_requested` / `deprovision_requested`, escritura de `HubStateTable`. Cero acceso cruzado a storage entre SAMs |
| **SCH-4b** | **Iris** — correlación de retorno | Consumo de `system.iot.alert.breach` filtrado por `correlation_id`, lectura de `HubStateTable` y publicación de `system.scheduled_job.fired`. Desplegable de forma independiente de SCH-4a |
| **SCH-4c** | Guard de orden y realimentación | `UpdateItem` condicional sobre `last_applied_ulid` en Chronos y Kairos, lápidas en el borrado, y descarte de eventos cuyo `delta` sólo toca campos de bitácora (§10.3, §10.4) |
| **SCH-4d** | Reautorización en `T=0` | El `FiredHandler` evalúa Cedar contra `created_by` y verifica `user.status` antes de `Transact`; `FAILED` + `DOMAIN_FAULT_DETECTED` si deniega (§10.5). Resolución de `target_webhook_id` en vez de URL libre (§10.6) |
| **SCH-5** | Idempotency Guard en el ejecutor | Escritura condicional en DynamoDB sobre `idempotency_key` (hash + `scheduled-time`), no sobre el hash estático (§10.2). Vive en el `FiredHandler` del Event Router — ver §10.7 |
| **SCH-6** | Observabilidad + CloudWatch Dashboard | Métricas **segregadas por función**: errores de Chronos / Kairos / Iris, profundidad de las 3 DLQ, fired rate, latencia breach→fired. **Alarma de schedules activos al 70 % de la cuota** y contador de `ThrottlingException` (§8.4) |

---

## 15. Matriz de Verificación

El roadmap dice qué construir; esta matriz dice **cómo saber que está bien**. Cada caso corresponde a un fallo concreto que este documento identificó, no a cobertura por cobertura: son las pruebas que separan un Hub que parece correcto de uno que lo es.

### 15.1 Casos límite — los que no se ven en el camino feliz

| Caso | Paquete | Entrada | Resultado esperado | Garantía |
|---|---|---|---|---|
| `conflict-converge-con-update` | `scheduler` | `CreateSchedule` sobre un nombre existente | Captura `ConflictException` y converge con `UpdateSchedule`; no propaga error | §5.1 |
| `delete-inexistente-es-exito` | `scheduler` | `DeleteSchedule` sobre nombre ausente | `ResourceNotFoundException` tratada como éxito; no va a DLQ | §5.1 |
| `cron-5-a-6-campos` | `scheduler` | `"0 8 1 */6 *"` | `cron(0 8 1 */6 ? *)`; el `?` cae en el campo no usado | §5.2 |
| `cron-dow-usa-interrogante-en-dom` | `scheduler` | `"0 8 * * MON"` | `cron(0 8 ? * MON *)` | §5.2 |
| `exact-time-siempre-utc` | `scheduler` | epoch + `iana_timezone: America/Bogota` | `ScheduleExpressionTimezone: "UTC"`, **no** el del Job | §5.2 |
| `epoch-vencido-no-crea-trampa` | `scheduler` | `EXACT_TIME` en el pasado | No llama a AWS; Job a `FAILED` con `last_error` | §5.2 |
| `suspended-deshabilita-no-borra` | `scheduler` | `status: SUSPENDED` | `UpdateSchedule` con `State: DISABLED`; la trampa sobrevive | §5.2 |
| `ulid-rezagado-se-descarta` | `ledger` | `ulid` menor que `last_applied_ulid` | `ConditionalCheckFailed` → descarte **como éxito**; AWS no se toca | §10.3 |
| `lapida-frena-created-tardio` | `ledger` | `deleted` y luego `created` con `ulid` anterior | La lápida bloquea el `created`; no se crea Schedule fantasma | §10.3 |
| `delta-de-bitacora-es-noop` | `event` | delta = `{run_count, last_run_at}` | Descarte sin llamar a AWS ni a DynamoDB | §10.4 |
| `delta-de-status-si-reprovisiona` | `event` | delta = `{status}` | **No** se descarta: `SUSPENDED` debe llegar a AWS | §10.4 |
| `idempotency-key-por-ocurrencia` | `idempotency` | dos ocurrencias del mismo `CRON` | Llaves distintas; ninguna deduplica a la otra | §10.2 |
| `idempotency-key-dedup-reentrega` | `idempotency` | dos entregas de la misma ocurrencia | Misma llave; la segunda se descarta | §10.2 |
| `payload-sobre-256kb-se-rechaza` | `event` | `action_payload` > 256 KB | Rechazo **en la creación**, no en `T=0` | §8.4 |
| `webhook-sin-destino-es-invalido` | `event` | `action_type: WEBHOOK`, `target_webhook_id: null` | Rechazo en la creación | §10.6 |
| `principal-inactivo-no-ejecuta` | ejecutor | `created_by` con `user.status ≠ ACTIVE` | `FAILED` + `DOMAIN_FAULT_DETECTED`; no invoca `Transact` | §10.5 |
| `censo-incompleto-aborta` | reconciliador | falta una página del censo | Aborta sin borrar nada | §12.2 |

### 15.2 Criterios de aceptación por fase

| Fase | Se considera terminada cuando |
|---|---|
| **SCH-1** | `sam validate --lint` limpio; las tres DLQ existen y son **estándar**; cada regla tiene `RetryPolicy` y `DeadLetterConfig`; ninguna política IAM usa `Resource: "*"` |
| **SCH-2** | Pasan los siete casos de `scheduler` de §15.1; un `CRON` semestral dispara en la fecha correcta cruzando un cambio de DST |
| **SCH-3** | Pasan `ulid-rezagado-se-descarta` y `lapida-frena-created-tardio`; el TTL configurado supera el `MaximumEventAgeInSeconds` del target |
| **SCH-4a/b** | Un breach con `correlation_id` produce un `fired` con el `action_payload` correcto; un breach sin él se ignora |
| **SCH-4c** | Pasan los dos casos de `delta` y los dos de `ledger`; reprocesar un evento duplicado no crea `iot_alert_rule` duplicadas |
| **SCH-4d** | `principal-inactivo-no-ejecuta` pasa; ningún `WEBHOOK` alcanza una URL ausente de `webhook_endpoint` |
| **SCH-5** | Pasan los dos casos de `idempotency`; un `CRON` horario ejecuta **las 24 veces** en un día |
| **SCH-6** | El dashboard muestra métricas por función, nunca agregadas; el censo de §12.2 corre y reporta cero divergencias en régimen estable |

> **La prueba que más importa** es `idempotency-key-por-ocurrencia`. Su ausencia fue el defecto más silencioso del diseño original: un `CRON` horario ejecutaba una vez y moría, sin error, sin alarma y sin nadie mirando.

