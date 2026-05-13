# Componente Externo 04 — Módulo Metri IoT y Eventos Telemétricos

> **Estado:** Diseño / Documentación  
> **Responsable:** Arquitectura Metri Engine  
> **Naturaleza:** Componente Externo — AWS SAM independiente  
> **Bus de integración:** Amazon EventBridge (eventos desde Metri Engine Core)

---

## 0. Principio Rector

La telemetría en Metri Engine **no satura las líneas vitales operativas**. Todo evento físico de maquinaria entra a través del **Componente Metri IoT**, separado orgánicamente como Serverless en su propio AWS SAM Stack. Este componente **recibe sus instrucciones operativas a través del bus de eventos de Metri Engine** (EventBridge), convirtiendo cada acción OLTP en una acción autónoma de infraestructura real-time.

---

## 1. Posición en el Ecosistema Metri

```
┌─────────────────────────────────────────────────────────────────┐
│                    METRI ENGINE CORE (SAM Stack)                │
│  ┌────────────┐   EventBridge Bus    ┌───────────────────────┐  │
│  │  Janus     │ ──────────────────► │   Metri IoT           │  │
│  │  Router    │                     │   (SAM Stack Externo)  │  │
│  └────────────┘                     └───────────────────────┘  │
│                                               │                  │
│  Entidades OLTP (Datahike):                   │ Controla        │
│  • iot_subscription         ◄─────────────────┤                  │
│  • iot_alert_rule           ◄─────────────────┤ AWS IoT Core    │
│  • iot_device_command       ◄─────────────────┤ MQTT Broker     │
│  • iot_device_profile                         │                  │
│  • iot_harvester_config     ◄─────────────────┤ REST Polling    │
└─────────────────────────────────────────────────────────────────┘
```

**Metri IoT es un listener puro del bus de Metri Engine.** No posee API propia expuesta al frontend. Sus acciones son 100% reactivas a eventos del bus y a telemetría física de dispositivos.

---

## 2. El Catálogo Maestro IoT (Datahike en Metri Engine Core)

El Catálogo separa el Activo Físico (`asset`) de su conexión MQTT, protegiendo el inventario en rotación de hardware de proveedores.

### 2.1 Entidad `iot_subscription`

Vincula un `asset` a un Thing de AWS IoT Core. Hereda la auditoría Time-Travel de Datahike por ser `engine: oltp` con `track_history: true`.

| Atributo               | Tipo                       | Rol                                                                |
| ---------------------- | -------------------------- | ------------------------------------------------------------------ |
| `asset_id`             | `reference → asset`        | Dimensión principal (multitenant)                                  |
| `aws_thing_name`       | `string (unique identity)` | Identificador en AWS IoT Registry                                  |
| `topic_pattern`        | `string`                   | Ej: `metri/telemetry/{tenant_id}/{asset_id}/#`                     |
| `subscription_status`  | `enum`                     | `PENDING_PROVISIONING → ACTIVE → SUSPENDED/ERROR`                  |
| `metric_mapping`       | `json`                     | Mapeo PLC → columnas `meter_reading` (ej. `TMP_VAL → TEMPERATURE`) |
| `batch_window_seconds` | `integer (default: 300)`   | Granularidad de micro-batching SQS (0 = crudo instantáneo)         |

**Eventos EDA que emite al bus:**

- `system.iot.subscription.created` → aprovisiona el Thing en AWS IoT Core
- `system.iot.subscription.updated` → actualiza certificados o topic patterns
- `system.iot.subscription.deleted` → desvincula el dispositivo físico

### 2.2 Entidad `iot_alert_rule`

Define umbrales matemáticos evaluados en tiempo real por el **Stateful Multiplexer** en RAM. Es la alternativa al límite de 1,000 AWS IoT Rules por cuenta.

| Atributo             | Tipo                             | Rol                                           |
| -------------------- | -------------------------------- | --------------------------------------------- |
| `asset_id`           | `reference → asset`              | El equipo bajo vigilancia                     |
| `threshold_operator` | `enum`                           | `GT, LT, EQ, GTE, LTE`                        |
| `threshold_value`    | `decimal`                        | Punto de quiebre algorítmico                  |
| `unit_of_measure`    | `string (terminology UN/CEFACT)` | Evita ambigüedad: `CEL` vs `FAH`              |
| `alert_severity`     | `enum`                           | `WARNING / CRITICAL / FATAL`                  |
| `notify_users`       | `reference[] → user`             | Notificación individual directa               |
| `notify_groups`      | `reference[] → role`             | Notificación grupal (ej. "Mantenimiento Sur") |

**Eventos EDA que emite al bus:**

- `system.iot.alert.created` → **inyecta la regla en memoria del Stateful Mux**
- `system.iot.alert.updated` → **reemplaza el umbral in-flight, sin downtime**
- `system.iot.alert.deleted` → **purga la regla del Mux, apagando monitoreo inmediato**

### 2.3 Entidad `iot_device_command`

Registro auditable de cada comando enviado a maquinaria física. Ciclo de vida trazado por ACKs MQTT.

| Atributo       | Tipo                | Rol                                                            |
| -------------- | ------------------- | -------------------------------------------------------------- |
| `asset_id`     | `reference → asset` | Receptor del comando                                           |
| `command_type` | `enum`              | `SHUTDOWN, REBOOT, ACTUATE_VALVE, FIRMWARE_UPDATE, SET_STATE`  |
| `payload`      | `json`              | Parámetros técnicos (ej. `{'target_rpm': 1000}`)               |
| `status`       | `enum`              | `PENDING → SENT → DELIVERED / FAILED`                          |
| `expires_at`   | `instant`           | TTL del comando: si el dispositivo no se conecta antes, CADUCA |

**Eventos EDA que emite al bus:**

- `system.iot.command.issued` → Lambda Command Dispatcher envía al Desired State del Shadow
- `system.iot.command.updated` → mapea ACKs físicos cuando el Shadow reporta estado `Reported`

### 2.4 Entidad `iot_device_profile`

Define la gramática de traducción bidireccional entre el mundo PLC y el mundo semántico Metri.

| Vector                       | Dirección   | Función                                                                                                         |
| ---------------------------- | ----------- | --------------------------------------------------------------------------------------------------------------- |
| `inbound_metric_rules`       | PLC → Metri | Transmuta llaves numéricas crudas (`tmp_val`) a medidas UN/CEFACT (`TEMPERATURE` en `CEL`) con `math_modifier`  |
| `inbound_metadata_rules`     | PLC → Metri | Extrae metadatos de texto libre (`err_code`) hacia el MAP del Data Lake                                         |
| `outbound_command_templates` | Metri → PLC | Transmuta comandos abstractos UI (`TURN_OFF`) al JSON exacto que el PLC espera (`{'cmd_id': 0, 'force': true}`) |

### 2.5 Entidad `iot_harvester_config`

Configura el polling REST para activos sin MQTT nativo. Las credenciales son `sensitive: true`, protegidas por KMS Envelope Encryption automática del motor.

| Atributo                   | Tipo                     | Rol                                                        |
| -------------------------- | ------------------------ | ---------------------------------------------------------- |
| `asset_id`                 | `reference → asset`      | Activo a sondear                                           |
| `target_url`               | `string`                 | Endpoint REST del activo                                   |
| `polling_interval_seconds` | `integer (default: 300)` | Frecuencia de polling                                      |
| `auth_type`                | `enum`                   | `BEARER_TOKEN, BASIC_AUTH, API_KEY, NONE`                  |
| `credentials`              | `string (sensitive)`     | Cifradas con KMS; descifradas sub-milisegundo en Lambda    |
| `payload_format`           | `enum`                   | `JSON, XML, GRPC` — selecciona el decodificador del Worker |
| `mapping_directive`        | `json`                   | Regla de aplanado para JSON irregular de terceros          |

---

## 3. Arquitectura del SAM Stack — Metri IoT

### 3.1 Diagrama General de Componentes

```
                     ┌──────────────────────────────────────────────────────┐
                     │              METRI IOT SAM STACK                     │
                     │                                                      │
EventBridge Bus ────►│  ┌─────────────────────────────────────────────┐    │
(Metri Engine)       │  │         LAMBDA: Provisioner                 │    │
  system.iot.        │  │  Escucha: subscription.created/updated/      │    │
  subscription.*     │  │          deleted                             │    │
                     │  │  Acciones: RegisterThing, AttachPolicy,      │    │
                     │  │  CreateCert, DeleteThing en IoT Core         │    │
                     │  └─────────────────────────────────────────────┘    │
                     │                                                      │
EventBridge Bus ────►│  ┌─────────────────────────────────────────────┐    │
(Metri Engine)       │  │    LAMBDA: Rule Engine Synchronizer         │    │
  system.iot.        │  │  Escucha: alert.created/updated/deleted      │    │
  alert.*            │  │  Acción: Escribe/actualiza/purga la regla    │    │
                     │  │  en DynamoDB IoT Rules Table (Rule Store)    │    │
                     │  └───────────────────────┬─────────────────────┘    │
                     │                          │ Notifica                 │
                     │                          ▼                          │
                     │  ┌─────────────────────────────────────────────┐    │
                     │  │    LAMBDA: Stateful Multiplexer Updater     │    │
                     │  │  Recibe el stream de DynamoDB Streams       │    │
                     │  │  Actualiza la regla activa en Valkey        │    │
                     │  │  (in-memory rule cache) sin downtime        │    │
                     │  └─────────────────────────────────────────────┘    │
                     │                                                      │
                     │         ╔═══════════════════════════════╗           │
                     │         ║   VALKEY — RULE CACHE         ║           │
                     │         ║  "rules:{tenant}:{asset}"     ║           │
                     │         ║  Compartido por ambos paths   ║           │
                     │         ╚═══════════╤═══════════════════╝           │
                     │                    │                                │
                     │           carga reglas (ambos paths)                │
                     │                    │                                │
                     │          ┌─────────┴──────────┐                    │
                     │          │                    │                    │
AWS IoT Core MQTT ──►│  ┌───────┴────────┐  ┌───────┴───────────────┐    │
(SQS → batch)        │  │ LAMBDA:        │  │ LAMBDA: HTTP          │◄───┤ EventBridge
                     │  │ Harvester      │  │ Harvester Worker      │    │ Scheduler
                     │  │ (Stateful Mux) │  │                       │    │ (polling)
                     │  │                │  │                       │    │
                     │  │ ENTRY POINT A  │  │ ENTRY POINT B         │    │
                     │  │ MQTT nativos   │  │ JSON/XML/gRPC APIs    │    │
                     │  │                │  │ (iot_harvester_cfg)   │    │
                     │  │ ① Normaliza    │  │ ① Decode protocolo    │    │
                     │  │ ② Eval Valkey  │  │ ② Eval Valkey         │    │
                     │  │ ③ Breach→bus  │  │ ③ Breach→bus         │    │
                     │  │ ④ Escribe S3   │  │ ④ Escribe S3          │    │
                     │  └───────┬────────┘  └───────┬───────────────┘    │
                     │          └─────────┬──────────┘                    │
                     │                   │                                │
                     │    system.iot.alert.breach → EventBridge Bus       │
                     │    system.iot.reading.ingested → EventBridge Bus   │
                     │                                                     │
EventBridge Bus ────►│  ┌─────────────────────────────────────────────┐   │
(Metri Engine)       │  │    LAMBDA: Command Dispatcher               │   │
  system.iot.        │  │  Escucha: command.issued                    │   │
  command.*          │  │  Descifra credenciales vía KMS              │   │
                     │  │  Publica al Desired State del Device Shadow │   │
                     │  │  Escucha ACKs y actualiza command.status    │   │
                     │  └─────────────────────────────────────────────┘   │
                     └─────────────────────────────────────────────────────┘
```

### 3.2 Los Dos Puntos de Entrada — Un Solo Evaluador

Todo dato telemétrico entra a Metri IoT por uno de dos caminos. Sin importar el origen, ambos pasan por el **mismo motor de evaluación de reglas** (Valkey) antes de ser inyectados en las bases de datos de Metri Engine.

|                   | Entry Point A — AWS IoT Core                                                    | Entry Point B — HTTP Harvester                         |
| ----------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------ |
| **Fuente**        | Dispositivos físicos con AWS IoT SDK (MQTT)                                     | APIs REST/XML/gRPC de activos sin MQTT nativo          |
| **Disparo**       | Dispositivo publica al broker → Kinesis Data Stream → Metri IoT Rules Evaluator | EventBridge Scheduler según `polling_interval_seconds` |
| **Ejemplos**      | Turbinas, compresores, PLCs con firmware IoT                                    | SCADA legacy, APIs de fabricante, OPC-UA sobre HTTP    |
| **Normalización** | `metric_mapping` de `iot_subscription`                                          | `inbound_metric_rules` de `iot_device_profile`         |
| **Evaluación**    | Estado local en RAM del Lambda (sin cache externo)                              | **Idéntica al Entry Point A**                          |
| **S3 Data Lake**  | Kinesis Data Firehose (automático, filtra RULE_CHANGE)                          | Lambda escribe directo a S3                            |

**Por qué un evaluador compartido:**

- **Paridad de cobertura:** Una regla `iot_alert_rule` aplica al asset sin importar si el dato llegó por MQTT o polling. No existen activos de "primera clase" (MQTT) y "segunda clase" (HTTP).
- **Single Source of Truth:** Las reglas viven en el stream Kinesis y DynamoDB, actualizadas por un único camino (Rule Synchronizer). El estado reside exclusivamente en la RAM del Evaluator.
- **Cooldown unificado:** El cooldown de breach en la memoria local aplica a nivel de regla. Si MQTT y HTTP poll llegan casi simultáneos del mismo asset, el cooldown suprime el breach duplicado.

---

## 4. Motor de Reglas Dinámicas — El Problema y la Solución

### 4.1 Por qué AWS IoT Rules no es suficiente

AWS IoT Rules presenta limitaciones estructurales insalvables para una plataforma SaaS multitenant:

| Limitación                                                   | Impacto en Metri                                                            |
| ------------------------------------------------------------ | --------------------------------------------------------------------------- |
| **1,000 reglas máximo por cuenta AWS**                       | Un tenant con 500 activos y 3 alertas por activo = 1,500 reglas → imposible |
| **Costo por millón de mensajes evaluados**                   | Escala lineal con el volumen de telemetría; inviable a escala               |
| **Sin hot-reload**: modificar una regla = eliminar y recrear | Downtime en monitoreo de turbinas, compresores, válvulas críticas           |
| **SQL estático sin contexto de negocio**                     | No puede evaluar umbrales por unidad de medida UN/CEFACT                    |
| **Sin aislamiento multitenant nativo**                       | Toda la lógica de enrutamiento debe ser externa                             |

### 4.2 Solución: Kinesis + Stateful Rule Engine

La arquitectura reemplaza AWS IoT Rules con Kinesis Data Streams como transporte y un motor propio de evaluación:

```
TRANSPORTE (Kinesis Data Streams):
  • Una sola AWS IoT Rule por tenant → Kinesis Data Stream
  • El stream lleva DOS tipos de registros:
      READING:     {type:READING, asset_id, metric, value}    ← telemetría MQTT
      RULE_CHANGE: {type:RULE_CHANGE, asset_id, rule_id, op} ← cambios de reglas
  • PartitionKey = asset_id en ambos tipos
      → mismo shard → mismo Lambda → mismo estado en RAM
  • Dos consumidores del stream:
      1. Metri IoT Rules Evaluator → evaluación de reglas en RAM local
      2. Kinesis Data Firehose     → S3 Data Lake (solo filtra records READING)
  • Replay posible hasta 365 días

ESTADO DE EVALUACIóN (Lambda local):
  • El Metri IoT Rules Evaluator mantiene en RAM:
      rules_cache: Map<tenant:asset → []Rule>  ← alimentado por RULE_CHANGE records
      cooldowns:   Map<tenant:asset:rule_id → epoch_ms>  ← anti-tormenta de breaches
  • Cold start: una única Query a DynamoDB por asset (solo al inicio del contenedor)
  • Hot path: cero llamadas externas — evaluación pura en RAM
```

### 4.3 Flujo de creación de una regla (CRUD en tiempo real)

```
Usuario crea iot_alert_rule en Metri UI
        │
        ▼
Metri Engine Core guarda en Datahike (OLTP + Time-Travel)
        │
        ▼
Emite evento: system.iot.alert.created → EventBridge Bus
        │
        ▼
Lambda Rule Engine Synchronizer recibe el evento
        │ 1. Escribe item en DynamoDB IoT Rules Table
        │ 2. Publica evento RULE_CHANGE al Kinesis Data Stream del tenant
        │
        ▼
REGLA ACTIVA EN RAM DE LOS EVALUADORES:
        │ El consumer de Kinesis procesa RULE_CHANGE → actualiza Map local
        │ Hot-reload: el próximo batch de telemetría usa la regla nueva
        ▼
REGLA ACTIVA EN <500ms desde la acción del usuario
```

### 4.4 Evaluación de umbrales en el Harvester

```
Por cada record Kinesis recibido:

  IF type == RULE_CHANGE:
    IF op == UPSERT: state.rules["tenant:asset"] = upsert(rule)
    IF op == DELETE: state.rules["tenant:asset"] = remove(rule_id)
    → Sin emisión de eventos. Solo actualiza RAM.

  IF type == READING:
    IF asset no bootstrapped:
      → Query DynamoDB (solo esta vez) → carga reglas en state.rules
    FOR EACH rule en state.rules["tenant:asset"]:
      a. Verificar unit_of_measure coincide con metric recibida
      b. Aplicar math_modifier del iot_device_profile si aplica
      c. Evaluar: valor {operator} threshold_value
      d. Si TRUE → verificar cooldown en RAM
         Si cooldown activo: suprimir silenciosamente
         Si NO cooldown: → BREACH → PutEvents EventBridge + actualizar cooldown
    Siempre: PutEvents system.iot.reading.ingested → Datahike OLTP
    (S3: Kinesis Firehose gestiona automáticamente los READING records)
```

---

## 5. Lambda: Provisioner (iot_subscription lifecycle)

**Trigger:** EventBridge — eventos `system.iot.subscription.*`

### Flujo `subscription.created`

1. Recibe `aws_thing_name`, `topic_pattern`, `asset_id`, `tenant_id`
2. Llama a AWS IoT Core API:
   - `CreateThing(thingName=aws_thing_name)`
   - `CreateKeysAndCertificate()` → guarda el certificado ARN en Datahike (vía Metri Engine gRPC)
   - `AttachThingPrincipal(thingName, certificateArn)`
   - `AttachPolicy(policyName="MetriIoTDevicePolicy", target=certificateArn)`
3. Crea **una sola** AWS IoT Rule de entrada (enrutamiento al Kinesis Data Stream del tenant):
   ```sql
   SELECT *, topic() as raw_topic, clientid() as device_id
   FROM 'metri/telemetry/{tenant_id}/#'
   -- Una regla por tenant (no por activo), eliminando la explosión de cuotas
   -- Action: KinesisAction → StreamName="metri-telemetry-{tenant_id}"
   --         PartitionKey="${asset_id}"  ← garantiza orden por activo dentro del shard
   ```
4. Crea el **Kinesis Data Stream** del tenant (si no existe):
   - `StreamName: metri-telemetry-{tenant_id}`
   - `ShardCount: ceil(assets_count / 500)` ← ~500 activos por shard
5. Crea el **Kinesis Data Firehose** del tenant:
   - Source: el Kinesis Data Stream del tenant
   - Destination: S3 `s3://metri-datalake/{tenant_id}/readings/`
   - Format: Parquet (con schema del `meter_reading`)
   - Buffer: 5 minutos o 128MB (lo que ocurra primero)
6. Actualiza `subscription_status = ACTIVE` vía evento de vuelta al bus

### Flujo `subscription.deleted`

1. Revoca y elimina el certificado del Thing
2. Llama `DeleteThing(thingName)`
3. Si no quedan activos del tenant:
   - Elimina la AWS IoT Rule del tenant
   - Elimina el Kinesis Data Stream del tenant
   - Elimina el Kinesis Data Firehose del tenant
4. Actualiza `subscription_status = SUSPENDED`

---

## 6. Lambda: Command Dispatcher (iot_device_command lifecycle)

**Trigger:** EventBridge — eventos `system.iot.command.*`

### Flujo `command.issued`

1. Recibe `asset_id`, `command_type`, `payload`, `expires_at`
2. Consulta `iot_device_profile` del asset para obtener `outbound_command_templates`
3. Traduce `command_type` abstracto → payload PLC exacto via `device_payload_template`
4. Descifra credenciales de acceso vía KMS (si son necesarias para el integrador)
5. Publica al **AWS IoT Device Shadow** (Desired State):
   ```json
   {
     "state": {
       "desired": { "<device_payload_template traducido>" }
     }
   }
   ```
6. Actualiza `command.status = SENT` vía evento al bus

### Flujo de ACK (`command.updated` desde Shadow Callback)

1. AWS IoT Core notifica al SQS cuando el Shadow reporta estado `Reported`
2. Lambda compara `desired` vs `reported`:
   - Si coinciden: `status = DELIVERED`
   - Si `expires_at` ha pasado: `status = FAILED`
3. Emite `system.iot.command.updated` → Metri Engine actualiza el registro

---

## 7. Lambda: HTTP Harvester Worker — Ingesta desde APIs Externas

### 7.1 Rol y Propósito

El **HTTP Harvester Worker** es el sub-componente de Metri IoT dedicado exclusivamente a la **ingesta activa de datos telemétricos desde activos que no hablan MQTT**. Su trabajo es salir a buscar los datos donde viven — en APIs REST, feeds XML industriales, o servicios gRPC propietarios — y normalizarlos antes de inyectarlos en las bases de datos de Metri Engine.

No es un componente reactivo. Es un **agente de polling activo y programado**: conoce la URL, las credenciales, el formato de respuesta, y la frecuencia de cada activo que tiene bajo su responsabilidad. Actúa como un adaptador universal entre el mundo heterogéneo de APIs de terceros y el modelo semántico homogéneo de Metri Engine.

```
[API Externa del Activo]                    [Metri Engine]
  JSON REST   ─────┐                      ┌─► Datahike (meter_reading OLTP)
  XML Feed    ─────┤ HTTP Harvester  ─────┤
  gRPC API    ─────┘  (normaliza)         └─► S3 Data Lake (Parquet / OLAP)
```

### 7.2 El Modelo `iot_harvester_config` — La Ficha de Configuración

Cada activo que el Harvester debe sondear tiene su propia `iot_harvester_config` en Datahike. Esta entidad es `track_history: true`: cualquier cambio de URL, credencial o intervalo queda auditado con Time-Travel inmutable.

| Atributo                   | Tipo                     | Rol                                                                                           |
| -------------------------- | ------------------------ | --------------------------------------------------------------------------------------------- |
| `asset_id`                 | `reference → asset`      | El activo físico propietario de esta configuración                                            |
| `target_url`               | `string`                 | Endpoint completo de la API del activo (ej. `https://plc-siemens-01.planta.com/api/readings`) |
| `polling_interval_seconds` | `integer (default: 300)` | Frecuencia de consulta. 300 = cada 5 minutos. El Harvester respeta este contrato al segundo   |
| `auth_type`                | `enum`                   | Mecanismo de autenticación: `BEARER_TOKEN`, `BASIC_AUTH`, `API_KEY`, `NONE`                   |
| `credentials`              | `string (sensitive)`     | Credenciales crudas protegidas por KMS Envelope Encryption. Nunca viajan en texto plano       |
| `payload_format`           | `enum`                   | Formato de respuesta esperado: `JSON`, `XML`, `GRPC`. Selecciona el decodificador correcto    |
| `mapping_directive`        | `json`                   | Regla de aplanado/transformación para APIs con estructura no estándar                         |

**Eventos EDA que emite al bus:**

- `system.iot.harvester.created` → registra el schedule de polling en EventBridge Scheduler
- `system.iot.harvester.updated` → actualiza la frecuencia o reconfigura el schedule
- `system.iot.harvester.deleted` → elimina el schedule; el Harvester deja de sondear el activo

### 7.3 El Pipeline de Ingesta — 6 Etapas

```
┌────────────────────────────────────────────────────────────────────────┐
│                    HTTP HARVESTER PIPELINE                             │
│                                                                        │
│  [1] SCHEDULE     EventBridge Scheduler dispara según                  │
│      TRIGGER  ──► polling_interval_seconds del iot_harvester_config    │
│                                   │                                    │
│  [2] CREDENTIAL   Lee iot_harvester_config de DynamoDB cache           │
│      DECRYPTION ──► KMS.Decrypt(credentials) → token/password en RAM  │
│                   Jamás toca disco. Jamás llama a Secrets Manager.     │
│                                   │                                    │
│  [3] HTTP         Construye la request HTTP según auth_type:           │
│      REQUEST  ──► BEARER_TOKEN: Authorization: Bearer {token}          │
│                   BASIC_AUTH:   Authorization: Basic base64(u:p)       │
│                   API_KEY:      Header X-Api-Key: {key} o ?key={key}   │
│                   NONE:         Request sin autenticación              │
│                                   │                                    │
│  [4] PROTOCOL     Selecciona el decodificador por payload_format:      │
│      DECODER  ──► JSON | XML | GRPC                                    │
│                                   │                                    │
│  [5] NORMALIZE    Aplica iot_device_profile.inbound_metric_rules:      │
│      & MAP    ──► fuente cruda (tmp_val) → UN/CEFACT (TEMPERATURE/CEL) │
│                   Aplica math_modifier si la escala requiere ajuste    │
│                   Aplica mapping_directive para aplanado estructural   │
│                                   │                                    │
│  [6] INJECT   ──► Inyecta READING a Kinesis Stream                    │
└────────────────────────────────────────────────────────────────────────┘
```

### 7.4 Los Tres Decodificadores de Protocolo

#### Decodificador JSON (REST)

El más común. El Harvester hace `GET {target_url}`, recibe JSON, y aplica `mapping_directive` para aplanar estructuras anidadas irregulares de terceros.

#### Decodificador XML (Feeds industriales)

Muchos sistemas SCADA y equipos industriales legacy exponen feeds XML. El Harvester usa un parser SAX streaming para evitar cargar el XML completo en memoria, extrayendo solo los nodos definidos en `mapping_directive`.

#### Decodificador gRPC

Para activos modernos. El Harvester usa el proto definido en `iot_device_profile` para deserializar la respuesta.

### 7.5 Inyección a las Bases de Datos de Metri Engine

**Mecanismo de inyección del HTTP Harvester:**

```
[HTTP Harvester — Metri IoT SAM]
   │ Normaliza el payload → meter_reading estructurado
   │ Escribe directamente al Kinesis Data Stream del tenant como record:
   │   {type: READING, asset_id, data: ...}

[Metri IoT Rules Evaluator]
   │ Consume el Kinesis Stream (automático)
   │ 1. Evalúa reglas in-memory (RAM local)
   │ 2. Emite system.iot.reading.ingested → EventBridge Bus
   │ 3. (S3 Firehose sigue capturando los READINGS del stream)

[Metri Engine Core]
   │ Consume "system.iot.reading.ingested"
   │ Escribe meter_reading en Datahike (OLTP)
```

### 7.6 Scheduling — Cómo se programa el Polling

El Harvester no usa un loop interno ni un cron en el servidor. **Delega el scheduling a EventBridge Scheduler**, aprovechando la misma infraestructura que Metri Schedulers Hub (Componente 05).

### 7.7 Manejo de Errores y Resiliencia

| Escenario                             | Comportamiento                                                                                                                                                                        |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **API externa no responde (timeout)** | Lambda registra el fallo en CloudWatch, no escribe nada. El siguiente ciclo de polling reintentará.                                                                                   |
| **Credenciales expiradas**            | KMS.Decrypt falla → Lambda emite `system.iot.harvester.error` al bus con `error_code: AUTH_FAILURE`.                                                                                 |
| **Respuesta malformada**              | El decodificador lanza excepción → registro en CloudWatch + evento de error al bus.                                                                                                  |
| **Carga alta de activos**             | Lambda escala horizontalmente. Cada invocación de EventBridge Schedule es independiente y stateless.                                                                                 |

---

## 8. Infraestructura del SAM Stack — Recursos AWS

```yaml
# Recursos lógicos del template.yaml de Metri IoT SAM

Resources:
  # --- COMPUTE ---
  LambdaProvisioner:          # Lifecycle de iot_subscription + crea KDS + Firehose por tenant
  LambdaRuleSynchronizer:     # Sincroniza iot_alert_rule → DynamoDB + PutRecord Kinesis (RULE_CHANGE)
  MetriIoTRulesEvaluator:     # Evaluation Engine: RAM local, sin cache externo
  LambdaCommandDispatcher:    # Lifecycle de iot_device_command
  LambdaHttpHarvester:        # Polling REST/XML/gRPC para iot_harvester_config

  # --- STREAMING (Entry Point A — MQTT) ---
  KinesisDataStream: # Un stream por tenant: "metri-telemetry-{tenant_id}"
    # PartitionKey = asset_id (orden garantizado por activo)
    # Trigger del Metri IoT Rules Evaluator (batch, iterador)

  KinesisDataFirehose: # Un Firehose por tenant: source=KDS, dest=S3 Parquet
    # Buffer: 5min o 128MB. Sin código. Sin Lambda.
    # Convierte a Parquet automáticamente

  # --- STORAGE ---
  DynamoDBIoTRuleStore:       # Fuente de verdad de reglas (solo consultada en cold start)
  DynamoDBCommandQueue:       # Estado de comandos pendientes (con TTL por expires_at)

  # --- SECURITY ---
  KMSIoTKey: # Clave KMS para Envelope Encryption de credentials
  IoTDevicePolicy: # IAM Policy mínima para Things: pub/sub solo a su topic

  # --- IOT CORE ---
  # Una sola IoT Rule por tenant → KinesisAction (no SQS)
  # Creada dinámicamente por LambdaProvisioner en el primer asset del tenant

  # --- OBSERVABILITY ---
  CloudWatchDashboardIoT: # Métricas: breach rate, harvest latency, Kinesis lag, Firehose delivery
  AlertSNSTopic: # Notificaciones de errores de infraestructura del SAM
```

---

## 9. Seguridad y Zero-Trust

| Principio                   | Implementación                                                                                                        |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| **Envelope Encryption**     | `iot_harvester_config.credentials` cifradas con KMS. Lambda descifra en invocación, jamás persiste en texto plano     |
| **Least Privilege IoT**     | Cada Thing tiene política que restringe publish/subscribe SOLO a `metri/telemetry/{tenant_id}/{asset_id}/#`           |
| **Aislamiento multitenant** | Clave de Valkey incluye `tenant_id`: `rules:{tenant_id}:{asset_id}` — imposible cross-tenant                          |
| **DynamoDB TTL**            | `iot_device_command` con `expires_at` → TTL automático, comandos caducados son purgados                               |
| **No AWS Secrets Manager**  | Credenciales bajo KMS directo: microsegundos de latencia vs. cientos de ms del Secrets Manager, sin costo por llamada |
| **Token de origen**         | Lambdas del SAM validan `X-Metri-Origin-Token` en eventos del bus para evitar inyección externa                       |

---

## 10. Flujo End-to-End: Turbina bajo vibración crítica

```
[Turbina física]
   │ Publica MQTT: metri/telemetry/tenant-42/asset-99/VIBRATION → valor: 12.5
   ▼
[AWS IoT Core — Una regla de tenant]
   │ Enruta al SQS Harvester Queue
   ▼
[Metri IoT Rules Evaluator]
   │ 1. Parsea topic → tenant_id=42, asset_id=99, metric=VIBRATION
   │ 2. HGETALL "rules:42:99" → carga reglas desde Valkey (microsegundos)
   │    Encuentra: {operator: GT, threshold: 10.0, unit: "VIB_MS", severity: FATAL}
   │ 3. Aplica math_modifier=1.0 del iot_device_profile
   │ 4. Evalúa: 12.5 GT 10.0 → TRUE → BREACH
   │ 5. Emite a EventBridge: system.iot.alert.breach
   │    {asset_id: 99, tenant_id: 42, severity: FATAL, value: 12.5, threshold: 10.0}
   │ 6. Escribe meter_reading=12.5 VIB_MS al Data Lake
   ▼
[Metri Engine Core — consume breach event]
   │ Notifica a notify_users + notify_groups via @metri-notifications
   │ Crea registro de alerta en historial del asset (Time-Travel Datahike)
   ▼
[Usuario en Metri UI]
   │ Ve la alerta FATAL en tiempo real
   │ Emite comando: SHUTDOWN para asset-99
   ▼
[Metri Engine Core]
   │ Crea iot_device_command → emite system.iot.command.issued
   ▼
[Lambda Command Dispatcher]
   │ Traduce SHUTDOWN → {'cmd_id': 0, 'force': true} via iot_device_profile
   │ Publica a AWS Device Shadow Desired State
   ▼
[Turbina física — AWS IoT SDK]
   │ Recibe Desired State → ejecuta shutdown
   │ Reporta Reported State → DELIVERED
   ▼
[Lambda Command Dispatcher — ACK]
   │ Detecta desired == reported
   │ Emite system.iot.command.updated → status=DELIVERED
   ▼
[Metri Engine Core]
   Actualiza iot_device_command.status = DELIVERED (Time-Travel registrado)
```

**Latencia total estimada: < 800ms desde la lectura física hasta la notificación al usuario.**

---

## 11. Tabla de Decisiones de Diseño

| Decisión                                  | Alternativa Descartada             | Razón                                                     |
| ----------------------------------------- | ---------------------------------- | --------------------------------------------------------- |
| Una AWS IoT Rule por tenant               | Una regla por activo/alerta        | Límite de 1,000 reglas por cuenta AWS                     |
| Evaluación de umbrales en Lambda (Valkey) | AWS IoT Rules SQL                  | Sin cuotas, hot-reload, contexto UN/CEFACT, multitenant   |
| KMS Envelope Encryption                   | AWS Secrets Manager                | Microsegundos vs. milisegundos, sin costo por llamada API |
| DynamoDB Streams → Valkey                 | Polling periódico de reglas        | Actualización de reglas en <500ms, sin downtime           |
| Abstracción de Device Shadow              | AWS IoT Device Shadow directo      | Control total del ciclo de vida, trazabilidad Datahike    |
| `iot_device_profile` como traductor       | Payload hardcodeado por fabricante | Soporta múltiples fabricantes PLC sin cambiar código      |
| `batch_window_seconds` configurable       | Ingesta siempre en tiempo real     | Balance costo/granularidad según SLA del cliente          |

---

## 12. Fases de Implementación (Roadmap)

| Fase      | Componente                      | Descripción                                                            |
| --------- | ------------------------------- | ---------------------------------------------------------------------- |
| **IoT-1** | DynamoDB Rule Store + Valkey    | Infraestructura base del motor de reglas                               |
| **IoT-2** | Lambda Rule Engine Synchronizer | Consume eventos de alerta del bus → escribe en Rule Store              |
| **IoT-3** | Lambda Mux Updater              | DynamoDB Streams → hot-reload en Valkey                                |
| **IoT-4** | Lambda Provisioner              | Lifecycle completo de iot_subscription + AWS IoT Core                  |
| **IoT-5** | Metri IoT Rules Evaluator       | Evaluación de telemetría (MQTT + HTTP poll) + emisión de breach events |
| **IoT-6** | Lambda Command Dispatcher       | Envío de comandos + ACK lifecycle                                      |
| **IoT-7** | Lambda HTTP Harvester Worker    | Polling REST con KMS + decodificadores                                 |
| **IoT-8** | Observabilidad + Dashboard      | CloudWatch metrics, alertas de infraestructura                         |
