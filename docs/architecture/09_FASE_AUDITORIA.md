# Fase 09 — Auditoría Asertiva y Criptografía Time-Travel (Rust & metri-panel)

**Fase contenedora:** Core Engine  
**Depende de:** [03A_FASE_IOP.md](03A_FASE_IOP.md), [06_FASE_CEDAR_AUTHORIZER.md](06_FASE_CEDAR_AUTHORIZER.md), [10_FASE_GESTION_ERRORES_EDA.md](10_FASE_GESTION_ERRORES_EDA.md)  
**Consumida por:** `IOP Pipeline`, `Sherlog` (Fase 10), `QuotaGuard` (Fase 7), `CedarAuthorizer` (Fase 6), `metri-panel` (BI Analytics Dashboard & Detail timelines)

> [!IMPORTANT]
> La auditoría en Metri opera en **dos capas ortogonales** diseñadas para equilibrar el rendimiento transaccional de escritura y la potencia analítica de consulta:
>
> | Capa | Motor de Persistencia | Eventos Soportados | Propósito / Costo Query |
> | :--- | :-------------------- | :----------------- | :---------------------- |
> | **OLTP Time-Travel** | DynamoDB EAV (Datoms) | `CREATE`, `UPDATE`, `DELETE` | Reconstrucción as-of e historial de cambios por entidad en tiempo real / O(1) vía PK |
> | **OLAP Asíncrono** | Kinesis Firehose → S3 → Athena | `READ`, `ACCESS_DENIED`, `QUOTA_EXHAUSTED`, `WRITE_ERROR`, `UNKNOWN` | Análisis de cumplimiento forense, seguridad y SLOs globales / $0.001 por GB escaneado en Athena |

---

## MÓDULO I: Auditoría OLTP — Time-Travel Nativo sobre EAV DynamoDB

En el motor transaccional EAV de Metri, **la transacción es un hecho histórico inmutable**. En lugar de almacenar logs estructurados en tablas de auditoría clásicas que añaden overhead en el Write Path, el sistema utiliza un modelo de persistencia solo-append en DynamoDB donde cada atributo de una entidad se representa como un Datom individual.

### I.1 — Persistencia de Datoms con EAV y Control Histórico
Cada datom escrito en la tabla transaccional `metri-eav` consta de la siguiente clave de ordenación binaria (SK):
```
SK binario: [attr_id: 2 Bytes][tx_id: 8 Bytes][op: 1 Byte (Assert=1 / Retract=0)]
```
Esta ordenación binaria permite que DynamoDB ordene cronológicamente y por atributo las modificaciones de cada entidad de forma nativa. 

El Write Path transaccional ACID (`OltpChannel` / `EavWriter`) escribe metadatos de auditoría directamente en el datom (anotando la transacción con el ID del usuario actor y el ID de la transacción en la cabecera), garantizando que todo cambio de negocio tenga trazabilidad total sin doble escritura.

### I.2 — Query de Historial (Time-Travel native) en Rust
El lector del motor EAV (`EavReader` en `src/eav/reader/pull.rs`) expone el método `history` para recuperar la traza de cambios completa (tanto aserciones como retracciones) de cualquier activo de negocio.

```rust
// src/eav/reader/pull.rs
/// Entrada del historial de un atributo en una entidad.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub attr_name: String,
    pub value:     Option<DatomValue>,
    pub tx_id:     u64,
    pub op:        bool, // true = assert (creación/mutación), false = retract (eliminación)
}

impl EavReader {
    /// History query — retorna TODOS los datoms históricos de una entidad incluyendo retracciones.
    /// Ideal para auditorías forenses y visualizaciones de timelines de cambio en metri-panel.
    /// [PORTED_FROM: (defn history-query [db entity-id])]
    pub async fn history(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attr_name: Option<&str>,
    ) -> Result<Vec<HistoryEntry>, DomainError> {
        let pk = format!("T#{}#E#{}", tenant_id, entity_id);

        let key_condition = "#pk = :pk".to_string();
        let mut attr_names  = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "PK".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk));

        // Ejecutar query en DynamoDB indexado de forma natural
        let raw_items = self.ddb
            .query(&self.table, None, &key_condition, attr_names, attr_values, true, None)
            .await
            .map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("history falló: {e:?}")))?;

        // Mapear datoms crudos al vector de historial estructurado
        let entries = raw_items
            .into_iter()
            .filter_map(|item| {
                let sk = match item.get("SK") {
                    Some(AttributeValue::B(blob)) => blob.as_ref(),
                    _ => return None,
                };
                if sk.len() != 11 { return None; }
                let attr_id = u16::from_be_bytes(sk[0..2].try_into().unwrap());
                let tx_id   = u64::from_be_bytes(sk[2..10].try_into().unwrap());
                let op      = sk[10] != 0;

                let attr = crate::codice::global().get_attr_name(attr_id).unwrap_or("unknown_attr").to_string();
                if let Some(filter) = attr_name {
                    if attr != filter { return None; }
                }
                
                let value = extract_datom_value(&item);
                Some(HistoryEntry {
                    attr_name: attr,
                    value,
                    tx_id,
                    op,
                })
            })
            .collect();

        Ok(entries)
    }
}
```

### I.3 — Snapshot Reads (`as-of`)
De forma adicional, `EavReader::pull_as_of` permite reconstruir de forma instantánea el estado exacto de una entidad en un punto de transacción pasado $T$. Esto se logra aplicando una query acotada con el operador `SK <= [attr_id][as_of_tx][0x01]` y tomando el estado de aserción más reciente.

---

## MÓDULO II: Auditoría OLAP — Modelo de Eventos `audit_log`

Para análisis agregados e informes de cumplimiento regulatorio, el almacenamiento OLTP por datoms individuales es ineficiente de escanear. Por ello, el sistema utiliza un canal asíncrono que escribe registros denegados o de sólo lectura en un flujo OLAP denormalizado.

### II.1 — Modelo Códice del Log
La estructura de este registro se define centralizadamente en `config/models/audit_log.json`, con motor analítico asignado y desactivando el bus EDA para prevenir loops circulares infinitos de eventos de auditoría (`disable_eda: true`):

```json
{
  "entity": "audit_log",
  "engine": "olap",
  "partition_strategy": "YYYY-MM-DD",
  "disable_eda": true,
  "attributes": [
    { "name": "tenant_id", "type": "reference", "entityRef": "tenant", "required": true, "is_dimension": true },
    { "name": "user_id", "type": "reference", "entityRef": "user", "required": true, "is_dimension": true },
    { "name": "action_type", "type": "enum", "options": ["READ", "WRITE", "DELETE", "ACCESS_DENIED", "QUOTA_EXHAUSTED", "PLUGIN_REJECTED"], "required": true, "is_dimension": true },
    { "name": "resource_domain", "type": "string", "required": true, "is_dimension": true, "doc": "Tipo de entidad accedida (e.g., 'asset')" },
    { "name": "resource_id", "type": "uuid", "doc": "ID del registro consultado o mutado." },
    { "name": "client_ip", "type": "string", "is_dimension": true },
    { "name": "security_context", "type": "json", "doc": "Snapshot de evaluación Cedar y claims del JWT." },
    { "name": "execution_time_ms", "type": "long", "is_measure": true },
    { "name": "plugin_telemetry", "type": "json", "doc": "Tiempos de ejecución de plugins inyectados." }
  ]
}
```

### II.2 — Flujo Ingesta OLAP
```
[IopOrchestrator (Rust)]
         │
         ▼ (tokio::spawn)
[IAuditInterceptor] ──(Value JSON)──> [OlapChannel] ──(Kinesis Firehose)──> [S3 Data Lake] ──> [Athena Engine]
```
Los logs de auditoría se almacenan en S3 particionados por fecha para optimizar las consultas columnares en Athena:
`s3://metri-audit-olap/audit_log/year=YYYY/month=MM/day=DD/`

### II.3 — Consultas de Compliance en Athena (SLO & Seguridad)
```sql
-- Detección de posibles ataques de fuerza bruta o escaneos:
SELECT client_ip, user_id, COUNT(*) as denegaciones
FROM audit_log
WHERE tenant_id = 'uuid-tenant-acme' AND action_type = 'ACCESS_DENIED'
  AND year = '2026' AND month = '05'
GROUP BY client_ip, user_id HAVING COUNT(*) > 10;
```

---

## MÓDULO III: `IAuditInterceptor` — Implementación en Rust

El módulo de auditoría OLAP se desacopla del flujo del pipeline principal mediante traits asíncronos nativos.

### III.1 — Protocolo de Auditoría (`src/domain/audit/protocol.rs`)
El contrato define un comportamiento no invasivo, puramente asíncrono y de exclusión de fallos (*fault-tolerant*):

```rust
// domain/audit/protocol.rs
use async_trait::async_trait;
use serde_json::Value;

/// Contrato para el interceptor de auditoría.
/// INVARIANTES:
/// 1. `audit` siempre se ejecuta tras enviar la respuesta gRPC al cliente (post-response).
/// 2. Nunca propaga excepciones; absorbe fallos de infraestructura y notifica a Sherlog.
/// 3. No altera el contexto original ni el payload del request.
#[async_trait]
pub trait IAuditInterceptor: Send + Sync {
    async fn audit(&self, request: &Value, succeeded: bool);
}
```

### III.2 — Derivación Pura del ActionType (`src/domain/audit/action_type.rs`)
La clasificación analítica del evento se delega a una función pura libre de efectos colaterales e I/O:

```rust
// domain/audit/action_type.rs
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionType {
    Write,
    AccessDenied,
    QuotaExhausted,
    WriteError,
    Unknown,
}

impl ActionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ActionType::Write          => "WRITE",
            ActionType::AccessDenied   => "ACCESS_DENIED",
            ActionType::QuotaExhausted => "QUOTA_EXHAUSTED",
            ActionType::WriteError     => "WRITE_ERROR",
            ActionType::Unknown        => "UNKNOWN",
        }
    }
}

pub fn derive_action_type(succeeded: bool, error_stage: Option<&str>) -> ActionType {
    if succeeded {
        return ActionType::Write;
    }
    match error_stage {
        Some("cedar") | Some("auth") => ActionType::AccessDenied,
        Some("quota")                 => ActionType::QuotaExhausted,
        Some("janus")                 => ActionType::WriteError,
        _                             => ActionType::Unknown,
    }
}
```

### III.3 — Implementación Concreta (`src/infrastructure/audit/interceptor.rs`)
La implementación inyecta el `IWriteChannel` (apuntando a Kinesis Firehose) y ejecuta la auditoría asíncronamente aislando el hilo transaccional gRPC principal:

```rust
// infrastructure/audit/interceptor.rs
pub struct AuditInterceptorImpl {
    olap_channel: Arc<dyn IWriteChannel>,
}

impl AuditInterceptorImpl {
    pub fn new(olap_channel: Arc<dyn IWriteChannel>) -> Self {
        Self { olap_channel }
    }

    fn build_security_snapshot(&self, request: &Value) -> Value {
        request.get("metadata").cloned().unwrap_or(json!({}))
    }
}

#[async_trait::async_trait]
impl IAuditInterceptor for AuditInterceptorImpl {
    async fn audit(&self, request: &Value, succeeded: bool) {
        let action_type = derive_action_type(succeeded, None);
        
        let tenant_id = request.get("tenant_id").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
        let user_id   = request.get("user_id").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
        let entity    = request.get("entity_type").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");

        let audit_payload = json!({
            "action_type":      action_type.as_str(),
            "resource_domain":  entity,
            "tenant_id":        tenant_id,
            "user_id":          user_id,
            "timestamp":        Utc::now().timestamp_millis(),
            "security_context": self.build_security_snapshot(request),
            "request_payload":  request.get("payload").cloned().unwrap_or(Value::Null),
            "status":           if succeeded { "SUCCESS" } else { "FAILURE" }
        });

        // Crear contexto artificial para Janus e inyectar de forma asíncrona
        let mut req_map = serde_json::Map::new();
        req_map.insert("data".to_string(), Value::Array(vec![audit_payload]));
        
        let ctx = IopContext::new(tenant_id, user_id, "audit_log", "BULK_CREATE", req_map);
        let olap = Arc::clone(&self.olap_channel);

        // Tokio-spawn: Desacoplamiento asíncrono no bloqueante
        tokio::spawn(async move {
            if let Err(e) = olap.route(ctx).await {
                // FASE 10: Sherlog Fault Event (AUD_001)
                tracing::error!("[AuditInterceptor] Falla al escribir en canal OLAP (Kinesis): {:?}", e);
            }
        });
    }
}
```

### III.4 — Integración con el Pipeline IOP (`src/iop/core.rs`)
La orquestación de la auditoría se acopla al ciclo ferroviario (Railway) del `IopOrchestrator`. Nótese cómo la auditoría se ejecuta **siempre**, garantizando el registro de fallas de seguridad y cuotas:

```rust
// iop/core.rs -> Extracto de IIopOrchestrator::run()
let result = self.run_steps(ctx).await;

// ... lógicas de Moira posterior al procesamiento ...

// Invocación asíncrona del interceptor (Ok y Err)
if let Some(audit) = &self.audit_interceptor {
    let succeeded = result.is_ok();
    let req_val   = Value::Object(request_clone);
    audit.audit(&req_val, succeeded).await;
}
```

---

## MÓDULO IV: Integración con metri-panel (Dashboard de Auditoría y Time-Travel)

La auditoría en la nueva arquitectura de Metri está diseñada para ser expuesta y aprovechada directamente por la interfaz de usuario en `metri-panel` mediante dos patrones clave de frontend:

### IV.1 — Dashboard OLAP de Auditoría Forense (gRPC Universal)
Para pintar paneles forenses agregados, `metri-panel` realiza peticiones `Query` universales apuntando a la entidad virtual `audit_log`. 

A continuación se muestra un ejemplo real de cómo configurar visualizaciones modernas de auditoría en la estructura de `BIAnalyticsView.vue` de metri-panel:

```typescript
// metri-panel/src/views/BIAuditAnalyticsView.vue
const AUDIT_DASHBOARD_LAYOUT = [
  // 1. KPI Card: Accesos Denegados hoy
  {
    i: 'kpi-security-denials', title: 'Accesos Denegados (Hoy)', type: 'kpi', x: 0, y: 0, w: 4, h: 5,
    queries: [{
      key: 'denials-count-q',
      entity: 'audit_log',
      output: 'kpi',
      metrics: [{ fn: 'count', field: 'id', name: 'Denegaciones' }],
      filters: [{
        criteria: { field: 'action_type', op_ref: 'EQ', value: { string_val: 'ACCESS_DENIED' } }
      }],
      timeframe: { type: 'TODAY', timezone: 'America/Bogota' }
    }],
    format: { style: 'decimal' }
  },
  // 2. Pie Chart: Distribución global de tipos de eventos de acceso
  {
    i: 'chart-audit-distribution', title: 'Distribución de Eventos de Acceso', type: 'chart', x: 4, y: 0, w: 8, h: 10,
    queries: [{
      key: 'distribution-q',
      entity: 'audit_log',
      output: 'pie',
      dimensions: [{ field: 'action_type', labelTemplate: 'Acción: {{action_type}}' }],
      metrics: [{ fn: 'count', field: 'id', name: 'Total' }],
      timeframe: { type: 'LAST_N_DAYS', n_value: 30, timezone: 'America/Bogota' }
    }]
  },
  // 3. Table: Tabla detallada forense de actividades
  {
    i: 'table-audit-trail', title: 'Bitácora Histórica Forense (OLAP)', type: 'pivot', x: 0, y: 10, w: 12, h: 12,
    queries: [{
      key: 'forensic-table-q',
      entity: 'audit_log',
      output: 'table',
      limit: 100,
      selectTree: { user_id: true, action_type: true, resource_domain: true, client_ip: true, execution_time_ms: true },
      dimensions: [
        { field: 'user_id', labelTemplate: 'Usuario: {{user_id}}' },
        { field: 'action_type' },
        { field: 'resource_domain' },
        { field: 'client_ip' },
        { field: 'execution_time_ms' }
      ]
    }]
  }
];
```

### IV.2 — Timeline OLTP de Cambios de Activos (Git-like Time-Travel)
Cuando un analista o técnico examina un registro individual (por ejemplo, un `asset` o un `work_order`) en metri-panel, el sistema no sólo muestra el estado actual, sino que permite renderizar una **línea de tiempo visual del historial del activo**.

#### Flujo de Consulta e Interacción
El frontend hace una llamada gRPC de proyección usando el sistema de `select_tree` extendido con el nodo de `history` mapeando directamente al método `EavReader::history`:

```
metri-panel ── rpc Query(entity: "asset", select_tree: { id: "uuid-asset", _history: true }) ──> metri-engine
```

El motor Rust procesa la consulta devolviendo los datoms históricos que el componente visual de metri-panel formatea y renderiza de forma premium:

```vue
<!-- metri-panel/src/components/audit/AssetHistoryTimeline.vue -->
<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useMetriClient } from '@/composables/useMetriClient'

const props = defineProps<{ entityId: string, entityType: string }>()
const timelineEntries = ref<any[]>([])
const loading = ref(true)

const fetchTimeline = async () => {
  const client = useMetriClient()
  try {
    // LLamada gRPC mapeada a EavReader::history en el backend Rust
    const resp = await client.query({
      tenantId: 'tenant-active-id',
      entity: props.entityType,
      selectTree: {
        id: props.entityId,
        _history: true // Flag semántico de Aegis
      }
    })
    timelineEntries.value = resp.data.rows // [{ attr_name, value, tx_id, op, user_id, timestamp }]
  } catch (err) {
    console.error('Error cargando timeline de auditoría:', err)
  } finally {
    loading.value = false
  }
}

onMounted(fetchTimeline)
</script>

<template>
  <div class="p-6 bg-white dark:bg-zinc-900 rounded-2xl shadow-sm border border-zinc-100 dark:border-zinc-800">
    <h3 class="text-lg font-bold text-zinc-900 dark:text-white mb-6 flex items-center gap-2">
      <span class="p-1.5 rounded-lg bg-indigo-50 dark:bg-indigo-950/50 text-indigo-600 dark:text-indigo-400">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
      </span>
      Línea de Tiempo de Cambios (Time-Travel)
    </h3>
    
    <div v-if="loading" class="animate-pulse space-y-4">
      <div v-for="i in 3" :key="i" class="h-16 bg-zinc-50 dark:bg-zinc-800 rounded-xl"></div>
    </div>
    
    <div v-else class="relative border-l-2 border-zinc-100 dark:border-zinc-800 ml-4 space-y-8">
      <div v-for="entry in timelineEntries" :key="entry.tx_id" class="relative pl-6 group">
        <!-- Indicador de cambio de estado interactivo (Git Style node) -->
        <span class="absolute -left-[9px] top-1.5 w-4 h-4 rounded-full border-2 border-white dark:border-zinc-900 flex items-center justify-center transition-all group-hover:scale-125"
          :class="entry.op ? 'bg-indigo-500 shadow-sm shadow-indigo-200' : 'bg-rose-500 shadow-sm shadow-rose-200'">
        </span>
        
        <div class="flex flex-col md:flex-row md:items-center justify-between gap-2 p-4 bg-zinc-50 dark:bg-zinc-800/40 rounded-xl border border-zinc-100/50 dark:border-zinc-800/30 transition-all hover:bg-zinc-100/30 dark:hover:bg-zinc-800/80">
          <div>
            <span class="text-xs font-semibold px-2 py-0.5 rounded-md"
              :class="entry.op ? 'bg-indigo-50 dark:bg-indigo-950/30 text-indigo-600' : 'bg-rose-50 dark:bg-rose-950/30 text-rose-600'">
              {{ entry.op ? 'MUTACIÓN / ASERCIÓN' : 'ELIMINACIÓN / RETRACCIÓN' }}
            </span>
            <div class="mt-2 text-sm text-zinc-900 dark:text-zinc-200">
              Atributo <code class="px-1.5 py-0.5 bg-zinc-200 dark:bg-zinc-700 rounded font-mono text-xs">{{ entry.attr_name }}</code> modificado a:
              <strong class="font-semibold ml-1 text-indigo-600 dark:text-indigo-400">{{ entry.value }}</strong>
            </div>
          </div>
          
          <div class="text-right flex flex-row md:flex-col items-center md:items-end justify-between md:justify-center gap-2 border-t md:border-t-0 border-zinc-100 dark:border-zinc-800/40 pt-2 md:pt-0">
            <div class="flex items-center gap-1.5 text-xs text-zinc-500 dark:text-zinc-400">
              <span class="w-5 h-5 rounded-full bg-zinc-200 dark:bg-zinc-700 flex items-center justify-center text-[10px] font-bold text-zinc-600 dark:text-zinc-300">U</span>
              ID Usuario: {{ entry.user_id || 'Servicio' }}
            </div>
            <span class="text-[10px] font-mono text-zinc-400 dark:text-zinc-500 mt-1">Tx: #{{ entry.tx_id }}</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
```

---

## MÓDULO V: Principios SOLID y DRY en la Implementación de Rust

| Principio | Aplicación Práctica en la Base de Código Rust |
| :-------- | :-------------------------------------------- |
| **S (Responsabilidad Única)** | `AuditInterceptorImpl` tiene una sola razón de cambio: interactuar con la infraestructura del canal analítico. La derivación lógica de los códigos analíticos se encapsula en la función matemática pura `derive_action_type`. |
| **O (Abierto / Cerrado)** | Si se define un nuevo evento del sistema en la capa Cedar o de cuotas, basta con agregar el caso en la función pura `derive_action_type` y en el modelo JSON analítico. El código de la infraestructura del interceptor no requiere cambios. |
| **L (Sustitución de Liskov)** | La suite de pruebas de auditoría aplica el mismo contrato a stubs analíticos virtuales y a la implementación real de base. Todos los stubs de test (`NoOpAuditInterceptor`, `SpyAuditInterceptor`) heredan directamente del trait principal `IAuditInterceptor` de forma intercambiable. |
| **I (Segregación de Interfaces)** | `IAuditInterceptor` declara exclusivamente el método asíncrono `audit()`. No hereda ni fuerza el acoplamiento con lógicas de envío de fallas o de compresión del canal. |
| **D (Inversión de Dependencias)** | El `IopOrchestrator` recibe una referencia abstracta e inyectable de tipo `Arc<dyn IAuditInterceptor>`, lo que permite mockear la auditoría con stubs cero-op de forma segura y veloz durante la suite de pruebas locales sin iniciar conexiones a AWS Kinesis. |

---

## MÓDULO VI: Catálogo de Errores TOML

De acuerdo al control canónico centralizado de errores del motor, los fallos asociados a la fase de interceptación analítica de auditoría se registran en `config/errors/error_catalog.toml` bajo el siguiente estándar nativo:

```toml
# config/errors/error_catalog.toml
# ── Familia AUD — Auditoría del Motor Analítico ─────────────────────────────────

[[errors]]
code             = "AUD_001"
family           = "aud"
stage            = "audit-interceptor"
severity         = "warning"
http_status      = 500
grpc_status      = "INTERNAL"
description      = "AuditInterceptor.audit failed to write to OLAPChannel — audit record lost"
context_required = ["tenant_id", "user_id", "action_type", "cause"]
retryable        = false

[[errors]]
code             = "AUD_002"
family           = "aud"
stage            = "audit-interceptor"
severity         = "warning"
http_status      = 500
grpc_status      = "INTERNAL"
description      = "audit/derive_action_type returned unknown — fallback to WRITE applied"
context_required = ["tenant_id", "operation", "result_stage"]
retryable        = false
```

---

## MÓDULO VII: Estructura de Carpetas Rust

```
metri-engine/
├── config/
│   ├── errors/
│   │   └── error_catalog.toml             ← Agregar errores AUD_001 y AUD_002
│   └── models/
│       └── audit_log.json                 ← SSOT del modelo analítico de auditoría
│
├── src/
│   ├── domain/
│   │   └── audit/
│   │       ├── mod.rs
│   │       ├── protocol.rs        [RUST]  ← Trait IAuditInterceptor
│   │       └── action_type.rs     [RUST]  ← Función pura derive_action_type + tests inline
│   │
│   ├── infrastructure/
│   │   └── audit/
│   │       ├── mod.rs
│   │       └── interceptor.rs     [RUST]  ← Implementación real y asíncrona (tokio::spawn)
│   │
│   ├── eav/
│   │   └── reader/
│   │       └── pull.rs            [RUST]  ← EavReader::history para Time-Travel
│   │
│   ├── iop/
│   │   └── core.rs                [RUST]  ← IopOrchestrator: Invocación post-response
│   │
│   └── grpc/
│       └── server.rs              [RUST]  ← Inicialización e inyección del AuditInterceptorImpl
```

---

## MÓDULO VIII: Matriz TDD (Rust Inline Tests)

En Rust, los tests unitarios y de lógica pura se programan de forma nativa en la sección `#[cfg(test)]` al final del mismo archivo.

### VIII.1 — Tests de Lógica Pura (`src/domain/audit/action_type.rs`)
| ID | Caso | Entrada | Salida Esperada |
| :--- | :--- | :--- | :--- |
| **AUD_DAT_01** | Transacción Exitosa | `succeeded = true` | `ActionType::Write` |
| **AUD_DAT_02** | Falla en Cedar | `succeeded = false, stage = "cedar"` | `ActionType::AccessDenied` |
| **AUD_DAT_03** | Falla en QuotaGuard | `succeeded = false, stage = "quota"` | `ActionType::QuotaExhausted` |
| **AUD_DAT_04** | Falla en Janus | `succeeded = false, stage = "janus"` | `ActionType::WriteError` |
| **AUD_DAT_05** | Falla de Infraestructuras | `succeeded = false, stage = "infra"` | `ActionType::Unknown` |

### VIII.2 — Pruebas de Integración y Fallos (`src/infrastructure/audit/interceptor.rs`)
Se valida que el interceptor no bloquee el hilo gRPC ante fallos simulados del stream analítico de Kinesis (absorbiendo de forma segura y reportando la falla `AUD_001` sin propagar excepción al cliente).

---

## MÓDULO IX: Observabilidad (OpenTelemetry & SLOs)

### IX.1 — Árbol de Spans de OpenTelemetry
```
Span Raíz (gRPC Request): "metri.MetriService/Transact"           [Trace_Id: W3C Traceparent]
  ├── "iop.pipeline.start"
  │     ├── "cedar.authorize"
  │     ├── "quota.check"
  │     └── "janus.route"
  └── "audit.interceptor" (Async Spawn)   [kind: Producer]     ← Desacoplado vía Tokio
        attrs:
          - audit.action_type
          - audit.tenant_id
          - audit.resource_domain
```

### IX.2 — Dashboard de Salud del Canal de Auditoría (Athena SLO)
Este query analiza la tasa de éxito del canal analítico computando la diferencia entre los registros indexados en `audit_log` contra los eventos de falla `AUD_001` capturados por Sherlog en el log de fallas del sistema:

```sql
WITH total_logs AS (
  SELECT COUNT(*) as total FROM audit_log 
  WHERE tenant_id = 'tenant-acme' AND year = '2026'
),
failed_logs AS (
  SELECT COUNT(*) as fallas FROM domain_fault 
  WHERE error_code = 'AUD_001' AND tenant_id = 'tenant-acme' AND year = '2026'
)
SELECT 
  total,
  fallas,
  ROUND(100.0 * (total - fallas) / total, 4) as audit_reliability_pct
FROM total_logs, failed_logs;
```
