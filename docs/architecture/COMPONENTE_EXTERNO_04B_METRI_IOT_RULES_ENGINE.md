# Componente Externo 04B — Metri IoT Rules Engine

> **Estado:** Diseño / Documentación
> **Padre:** Componente Externo 04 — Metri IoT
> **Responsabilidad única:** Evaluación de umbrales telemétricos en tiempo real y emisión de alertas

---

## 0. Propósito

El **Metri IoT Rules Engine** es el sub-sistema dentro del SAM Stack de Metri IoT que:

1. **Sincroniza** las reglas de alerta (`iot_alert_rule`) desde Metri Engine Core hacia el stream de Kinesis
2. **Evalúa** cada lectura telemétrica contra esas reglas en RAM local del Lambda — sin cache externo
3. **Emite** eventos de breach al bus EventBridge cuando se cruza un umbral

No usa Valkey. No usa ElastiCache. La evaluación es **Kinesis-native** con estado local en RAM del Lambda.

**Las reglas viven en dos lugares con roles distintos:**
- **DynamoDB** → fuente permanente. Guarda todas las reglas sin importar cuándo se crearon. No caduca.
- **RAM del Lambda** → estado de trabajo. Se carga desde DynamoDB al arrancar, y se actualiza en caliente vía Kinesis mientras el Lambda está vivo.

Kinesis **no almacena reglas**. Solo transporta notificaciones de cambios en tiempo real.

---

## 1. Por qué AWS IoT Rules no es suficiente

| Limitación AWS IoT Rules | Impacto en Metri SaaS Multitenant |
|---|---|
| 1,000 reglas máximo por cuenta | 500 activos × 3 alertas = 1,500 reglas → imposible |
| Costo por millón de mensajes evaluados | Escala lineal con telemetría; inviable a escala |
| Modificar una regla = eliminar y recrear | Downtime de monitoreo en turbinas y válvulas críticas |
| SQL estático sin contexto de negocio | No evalúa unidades UN/CEFACT ni math_modifier |
| Sin aislamiento multitenant nativo | Toda lógica de tenant debe vivir en el cliente |

---

## 2. El Principio Fundamental: Kinesis Partition Key = asset_id

Todo el diseño del Rules Engine descansa sobre una propiedad de Kinesis que ya está definida en la arquitectura:

```
AWS IoT Rule → Kinesis Data Stream
               PartitionKey = "${asset_id}"
```

**Consecuencia:**
> Todo lo relacionado con un activo — su telemetría y los cambios a sus reglas — siempre va al **mismo shard**, procesado **en orden** por la **misma instancia del Lambda Evaluator**.

Esto elimina la necesidad de un cache externo compartido (Valkey). El estado puede vivir en la memoria del proceso Lambda porque ese proceso es el único que ve ese activo.

---

## 3. Arquitectura: Dual Record Type en el mismo Stream

El Kinesis Data Stream ya existente transporta **dos tipos de registros**:

```
┌───────────────────────────────────────────────────────────────────────┐
│                    KINESIS DATA STREAM                                │
│                   "metri-telemetry-{tenant_id}"                       │
│                                                                       │
│  TIPO A — READING (viene de AWS IoT Core → IoT Rule → Kinesis)       │
│  {                                                                    │
│    type:      "READING",                                              │
│    asset_id:  "99",                                                   │
│    tenant_id: "42",                                                   │
│    metric:    "VIBRATION",                                            │
│    value:     12.5,                                                   │
│    unit:      "VIB_MS",                                               │
│    timestamp: 1735689600                                              │
│  }                                                                    │
│                                                                       │
│  TIPO B — RULE_CHANGE (viene del Lambda Rule Synchronizer)           │
│  {                                                                    │
│    type:       "RULE_CHANGE",                                         │
│    asset_id:   "99",      ← mismo partition key → mismo shard        │
│    tenant_id:  "42",                                                  │
│    rule_id:    "rule-6f96",                                           │
│    operation:  "UPSERT" | "DELETE",                                   │
│    threshold_operator: "GT",                                          │
│    threshold_value:    10.0,                                          │
│    unit_of_measure:    "VIB_MS",                                      │
│    alert_severity:     "FATAL",                                       │
│    notify_users:       ["user-1"],                                    │
│    notify_groups:      ["group-maint"],                               │
│    correlation_id:     "job-abc123"  ← solo si viene de TELEMETRY    │
│  }                                                                    │
│                                                                       │
│  PartitionKey en ambos tipos = asset_id                               │
│  → Garantía: mismo shard → mismo Lambda → mismo estado en RAM        │
└───────────────────────────────────────────────────────────────────────┘
```

---

## 4. Los 2 Lambdas del Rules Engine

### 4.1 Lambda Rule Synchronizer

**Trigger:** EventBridge — `system.iot.alert.created` / `updated` / `deleted`

Convierte cada mutación de `iot_alert_rule` en Datahike en dos escrituras atómicas:

| Evento del bus | DynamoDB Rule Store | Kinesis Data Stream |
|---|---|---|
| `system.iot.alert.created` | `PutItem` con todos los atributos | `PutRecord` tipo `RULE_CHANGE / UPSERT` |
| `system.iot.alert.updated` | `UpdateItem` | `PutRecord` tipo `RULE_CHANGE / UPSERT` |
| `system.iot.alert.deleted` | `DeleteItem` | `PutRecord` tipo `RULE_CHANGE / DELETE` |

**Por qué también en DynamoDB:**
DynamoDB es el **único almacén permanente de reglas**. Kinesis retiene mensajes máximo 7 días. Una regla creada hace 6 meses ya no existe en el stream. DynamoDB la tiene para siempre, y el Evaluator la carga desde ahí en cold start sin importar cuándo fue creada.

```
Garantía temporal: Desde que el usuario crea una regla hasta que el Evaluator la aplica: < 300ms

Usuario → Datahike → bus EDA → Rule Synchronizer → DynamoDB + Kinesis (RULE_CHANGE)
  t=0      t=10ms     t=30ms        t=80ms           t=150ms    t=200ms
                                                           ↓
                              RULE_CHANGE record entra al mismo shard que el asset
                                    ↓
                              Evaluator procesa RULE_CHANGE antes del próximo READING
```

### 4.2 Metri IoT Rules Evaluator

**Trigger:** Kinesis Data Stream `metri-telemetry-{tenant_id}` (batch iterator por shard)

Es el núcleo del motor. Mantiene estado local en RAM. No depende de ningún servicio externo durante la evaluación en caliente.

#### Estado interno del Evaluator (Go struct por execution environment)

```go
type EvaluatorState struct {
    // Reglas por asset — cargadas en cold start desde DynamoDB,
    // actualizadas en caliente via RULE_CHANGE records del stream
    rules map[string][]Rule  // key: "tenant_id:asset_id"

    // Cooldown por regla — evita tormenta de breach events
    // cuando un sensor fluctúa en torno al umbral
    // key: "tenant_id:asset_id:rule_id", value: unix epoch del último breach
    cooldowns map[string]int64

    // Assets ya cargados en cold start (evita re-consultar DynamoDB)
    bootstrapped map[string]bool  // key: "tenant_id:asset_id"
}
```

#### Algoritmo de procesamiento por batch

```
Para cada record en el batch Kinesis:

  IF type == "RULE_CHANGE":
    IF operation == "UPSERT":
      → state.rules["tenant:asset"] = upsert(rule_id, payload)
    IF operation == "DELETE":
      → state.rules["tenant:asset"] = remove(rule_id)
    → No emite eventos. Solo actualiza el estado local.

  IF type == "READING":
    asset_key = "{tenant_id}:{asset_id}"

    IF NOT state.bootstrapped[asset_key]:
      → Consulta DynamoDB: Query(PK="{tenant_id}#{asset_id}")
      → Carga TODAS las reglas del asset en state.rules[asset_key]
      → state.bootstrapped[asset_key] = true
      (esta consulta solo ocurre UNA VEZ por asset por ciclo de vida del contenedor)

    FOR EACH rule IN state.rules[asset_key]:
      IF rule.unit_of_measure != reading.metric: SKIP
      IF evaluate(reading.value, rule.operator, rule.threshold):
        cooldown_key = "{tenant}:{asset}:{rule_id}"
        IF cooldown activo (now - state.cooldowns[cooldown_key] < rule.cooldown_seconds):
          SKIP (supresión silenciosa)
        ELSE:
          → PutEvents EventBridge: system.iot.alert.breach
          → state.cooldowns[cooldown_key] = now

    → PutEvents EventBridge: system.iot.reading.ingested
    (S3 Data Lake: Kinesis Firehose lo escribe automáticamente)
```

#### Garantías de consistencia

```
Escenario: Regla creada mientras llega telemetría

Stream (mismo shard, asset_id=99):
  t=100ms  READING  {asset_id:99, VIBRATION, value:12.5}  ← evalúa sin reglas aún
  t=150ms  RULE_CHANGE {asset_id:99, rule_id:"r1", GT, 10.0}  ← regla entra
  t=200ms  READING  {asset_id:99, VIBRATION, value:11.2}  ← evalúa CON la regla → BREACH

Kinesis garantiza orden dentro del shard.
El RULE_CHANGE siempre es procesado antes de los READING posteriores.
No hay race condition posible.
```

---

## 5. Cold Start — Cómo se Cargan Reglas Histricas y Recientes

**El problema central:** Un Lambda arranca hoy con la RAM vacía. Las reglas pueden haber sido creadas hace un año. Kinesis no tiene esos registros del pasado — solo retiene 7 días. DynamoDB los tiene para siempre.

```
[Lambda Evaluator arranca — contenedor nuevo, RAM vacía]
   │  state = EvaluatorState{} → sin reglas
   │
   ▼
[Primer batch del shard — llegan READING de varios assets]
   │
   ├─ Por cada asset no bootstrapped (primera vez que se ve):
   │    → DynamoDB Query: PK="{tenant}#{asset}"
   │    → Devuelve TODAS las reglas del asset (incluso las de hace 1 año)
   │    → Carga en state.rules[asset_key]
   │    → state.bootstrapped[asset_key] = true
   │    (esta consulta ocurre UNA Única VEZ por asset por ciclo de vida)
   │
   └─ Por cada RULE_CHANGE en el batch (cambios recientes, últimos 7 días):
        → Aplica directamente sobre state.rules

[Batches siguientes — mismo contenedor]
   │  state.rules tiene todas las reglas: las del año pasado (DynamoDB)
   │  y las de los últimos 7 días (Kinesis RULE_CHANGE)
   │  CERO consultas externas durante la evaluación
   ▼
Evaluación pura en RAM
```

### Flujo completo: regla creada hace 1 año, medición llega hoy

```
Hace 1 año:
  Operador creó regla: VIBRATION GT 10.0 FATAL para turbina-99
  Rule Synchronizer → PutItem DynamoDB (sigue ahí, no caduca)
  Rule Synchronizer → PutRecord Kinesis (ya no existe, caduó en 7 días)

Hoy, 09:00 AM — Lambda arranca (cold start):
  state.rules = {}  ← vacío

Hoy, 09:00:15 AM — turbina-99 manda VIBRATION = 12.5:
  Lambda recibe READING
  ¿bootstrapped["42:turbina-99"]? → NO
  → Query DynamoDB PK="42#turbina-99"
  → Encuentra la regla de hace 1 año: GT 10.0 FATAL
  → state.rules["42:turbina-99"] = [{r1: GT 10.0 FATAL}]
  → bootstrapped = true
  Evalúa: 12.5 > 10.0 = TRUE → BREACH FATAL emitido ✅

Hoy, 09:00:16 AM — turbina-99 manda VIBRATION = 11.2:
  ¿bootstrapped? → SÍ
  Evalúa directo en RAM: 11.2 > 10.0 = TRUE
  ¿Cooldown activo? → SÍ (breach hace 1 segundo, cooldown 300s)
  → Suprimido silenciosamente
```

### Los dos caminos que alimentan la RAM

| Origen | Cuándo aplica | Qué contiene |
|---|---|---|
| **DynamoDB** | Cold start (primera vez que el Lambda ve ese asset) | Todas las reglas: creadas hace 1 día, 6 meses, 3 años |
| **Kinesis RULE_CHANGE** | Lambda ya en caliente (running) | Solo cambios ocurridos mientras el Lambda está vivo |

Ambos caminos cargan exactamente el mismo formato en `state.rules`. El Evaluator no distingue si una regla llegó de DynamoDB o de Kinesis — para él son datos en un mapa Go.

**En condiciones normales (contenedor warm), el Evaluator es completamente independiente de cualquier servicio externo.** La RAM tiene todo lo que necesita.

---

## 6. Flujo CRUD de Reglas — Tiempo Real

### CREATE — Nueva regla activa

```
[Metri UI]
  Usuario define: asset=Turbina-99, VIBRATION GT 10.0 VIB_MS, severity=FATAL
  ▼
[Datahike — Metri Engine Core]
  Transacción ACID → crea iot_alert_rule id="rule-6f96"
  Emite: system.iot.alert.created → EventBridge Bus
  ▼
[Lambda Rule Synchronizer]
  ├─ PutItem DynamoDB: PK="42#99" SK="rule-6f96" {GT, 10.0, VIB_MS, FATAL, ...}
  └─ PutRecord Kinesis: {type:"RULE_CHANGE", asset_id:"99", op:"UPSERT", ...}
     PartitionKey="99" → mismo shard que la telemetría del activo
  ▼
[Metri IoT Rules Evaluator — procesando el shard de asset 99]
  Recibe RULE_CHANGE → state.rules["42:99"] = upsert("rule-6f96", payload)
  ▼
[Próximo READING de asset 99]
  Evalúa contra rule-6f96. Si valor > 10.0 → BREACH
```

### UPDATE — Cambio de umbral sin downtime

```
[Rule Synchronizer]
  ├─ UpdateItem DynamoDB: threshold_value=8.0
  └─ PutRecord Kinesis: {type:"RULE_CHANGE", op:"UPSERT", threshold_value:8.0}
  ▼
[Evaluator]
  Recibe RULE_CHANGE → sobreescribe rule-6f96 en state.rules con threshold_value=8.0
  Próximo READING evalúa contra 8.0. Sin reinicio. Sin downtime.
```

### DELETE — Regla eliminada instantáneamente

```
[Rule Synchronizer]
  ├─ DeleteItem DynamoDB: PK="42#99" SK="rule-6f96"
  └─ PutRecord Kinesis: {type:"RULE_CHANGE", op:"DELETE", rule_id:"rule-6f96"}
  ▼
[Evaluator]
  Recibe RULE_CHANGE DELETE → elimina rule-6f96 de state.rules["42:99"]
  Ningún READING posterior puede disparar esa regla. Imposible breach fantasma.
```

---

## 7. Modelo `iot_alert_rule` — Atributos en el Evaluator

| Atributo | Tipo | Uso en el Evaluation Engine |
|---|---|---|
| `asset_id` | `reference → asset` | Partition key del Kinesis record. Define el shard y el Lambda que lo evalúa |
| `threshold_operator` | `enum GT/LT/EQ/GTE/LTE` | Selecciona la función de comparación |
| `threshold_value` | `decimal` | El valor de quiebre |
| `unit_of_measure` | `string (UN/CEFACT)` | Filtro previo: solo evalúa si el metric coincide |
| `alert_severity` | `enum WARNING/CRITICAL/FATAL` | Incluida en el breach event |
| `notify_users` | `reference[] → user` | Lista en el breach event para notificaciones |
| `notify_groups` | `reference[] → role` | Idem para grupos |

---

## 8. Cooldown sin Valkey

El cooldown anti-tormenta vive en `state.cooldowns` (Go map en RAM):

```
En BREACH detectado para rule_id:

1. cooldown_key = "{tenant_id}:{asset_id}:{rule_id}"
2. state.cooldowns[cooldown_key] = unix_now_ms

En evaluación posterior:
3. last_breach = state.cooldowns[cooldown_key]  → 0 si no existe
4. IF (now - last_breach) < cooldown_seconds: SKIP (supresión)
   ELSE: emitir breach y actualizar cooldown
```

**Por qué funciona sin Valkey:**
Todo el tráfico de un asset va al mismo shard → mismo Lambda → mismo `state.cooldowns`. No hay posibilidad de que otro Lambda procese el mismo asset y genere un breach duplicado.

**Si el contenedor Lambda se reinicia:**
El cooldown se resetea. Peor caso: un breach extra en el momento del reinicio. Para severidades FATAL, este es un comportamiento aceptable y preferible a depender de un cache externo que también puede fallar.

---

## 9. Aislamiento Multitenant

Las claves del mapa interno incluyen `tenant_id`:

```go
state.rules["42:99"]    // tenant 42, asset 99
state.rules["43:99"]    // tenant 43, asset 99 (diferente tenant, mismo asset_id)
```

Imposible cross-tenant por diseño del mapa. El `tenant_id` viene del registro Kinesis (extraído del topic MQTT original).

---

## 10. Infraestructura del Rules Engine

```yaml
# Recursos en template.yaml de Metri IoT SAM

Resources:

  # --- RULE STORE (bootstrap de cold start) ---
  IoTRulesTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: metri-iot-rules
      BillingMode: PAY_PER_REQUEST
      KeySchema:
        - AttributeName: pk   # tenant_id#asset_id
          KeyType: HASH
        - AttributeName: sk   # rule_id
          KeyType: RANGE
      # Sin StreamSpecification: no se usan DynamoDB Streams
      # Sin ElastiCache: no se usa Valkey
      GlobalSecondaryIndexes:
        - IndexName: rule-id-index
          KeySchema:
            - AttributeName: sk
              KeyType: HASH

  # --- STREAMING (transporte de telemetría y rule changes) ---
  KinesisDataStream:
    # Un stream por tenant: "metri-telemetry-{tenant_id}"
    # PartitionKey = asset_id (orden garantizado, mismo shard por asset)
    # Lleva dos tipos de records: READING y RULE_CHANGE

  KinesisDataFirehose:
    # Un Firehose por tenant: source=KDS, dest=S3 Parquet
    # Solo consume records tipo READING (filtra RULE_CHANGE)
    # Sin código. Sin Lambda. Automático.

  # --- LAMBDAS ---
  LambdaRuleSynchronizer:
    # Trigger: EventBridge (system.iot.alert.created/updated/deleted)
    # Escribe a DynamoDB Rule Store + PutRecord Kinesis (RULE_CHANGE)
    Policies:
      - dynamodb:PutItem / UpdateItem / DeleteItem (→ IoTRulesTable)
      - kinesis:PutRecord (→ KinesisDataStream)
      - events:PutEvents (→ MetriEventBus)

  MetriIoTRulesEvaluator:
    # Trigger: Kinesis Data Stream (iterator por shard)
    # Estado local en RAM: rules_cache + cooldowns (sin Valkey)
    # Cold start: Query DynamoDB una sola vez por asset por ciclo de vida
    # Evaluación en caliente: cero llamadas externas
    Policies:
      - kinesis:GetRecords / GetShardIterator (→ KinesisDataStream)
      - dynamodb:Query (→ IoTRulesTable, solo cold-start path)
      - events:PutEvents (→ MetriEventBus)
      # Sin elasticache:Connect — Valkey eliminado

  # --- SIN VALKEY / SIN ELASTICACHE ---
  # El estado de evaluación vive en RAM del Lambda Evaluator
  # Alimentado por RULE_CHANGE records del stream Kinesis
  # Bootstrap por DynamoDB en cold start
```

---

## 11. Métricas de Observabilidad

| Métrica CloudWatch | Descripción |
|---|---|
| `RuleSyncLatency` | Tiempo desde evento EDA hasta escritura en DynamoDB + PutRecord Kinesis |
| `EvaluatorColdStartLoads` | Número de consultas DynamoDB por cold start (debe ser bajo) |
| `RuleEvaluationDuration` | Tiempo de evaluación por batch (p50/p99) — debe ser sub-ms |
| `BreachesEmitted` | Breaches emitidos por tenant y severity |
| `BreachSuppressed` | Breaches suprimidos por cooldown (en RAM) |
| `KinesisRuleChangeLag` | Lag entre PutRecord RULE_CHANGE y procesamiento por Evaluator |
| `UnknownTenantRecords` | Records descartados por tenant_id no reconocido |

---

## 12. Tabla de Decisiones de Diseño

| Decisión | Alternativa descartada | Razón |
|---|---|---|
| **Estado en RAM del Lambda** (sin Valkey) | ElastiCache Valkey | Valkey: costo fijo, volátil, componente extra que puede fallar. RAM del Lambda: gratuita, local, sin red, sin latencia |
| **RULE_CHANGE records en el mismo Kinesis stream** | Stream separado para reglas | Mismo stream con mismo PartitionKey garantiza orden causal entre cambios de regla y telemetría del mismo asset |
| **DynamoDB solo para cold start** | DynamoDB en hot path | En hot path sería 5-20ms por lookup. Solo en cold start (una vez por contenedor) es aceptable |
| **Cooldown en RAM local** | Cooldown en DynamoDB o Valkey | El partition key garantiza que el mismo Lambda procesa todo el tráfico de un asset. Cooldown local es exacto y gratuito |
| **Kinesis PartitionKey = asset_id** | PartitionKey aleatorio | Con PartitionKey aleatorio no hay garantía de shard fijo → necesitaríamos cache externo. Con asset_id, el shard es determinista |
| **Firehose filtra RULE_CHANGE** | Escribir RULE_CHANGE a S3 | Los records de reglas no son datos de serie de tiempo; contaminarían el Data Lake |
