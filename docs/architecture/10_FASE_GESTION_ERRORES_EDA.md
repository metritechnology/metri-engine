# FASE 10: GESTIÓN DE ERRORES Y OBSERVABILIDAD

Cuatro pilares garantizan que cada anomalía en el motor `metri-engine` sea un **activo analizable**:

1. **Railway Pattern** — Zero-Exception en el núcleo funcional de Rust usando `Result<T, DomainError>`.
2. **Rich Context Error DTO** — Empaquetado forense en formato JSON con información de usuario, tenant y causa.
3. **OpenTelemetry (Metri Trace)** — Integración de spans mediante el ecosistema de `tracing` en Rust.
4. **Sherlog Pipeline** — Tratamiento de errores críticos (`WARNING` y superiores) como eventos EDA a través de EventBridge y almacenamiento persistente OLAP.

---

## MÓDULO I: Railway Pattern — Zero-Exception

Ninguna capa de lógica de negocio lanza excepciones o pánicos (`panic!`). Las librerías de terceros y llamadas a servicios externos se capturan en las fronteras funcionales y se convierten a una estructura unificada y controlada.

### I.1 — Contrato Railway en Rust

En Rust, el Railway Pattern se materializa mediante el uso de `Result<T, DomainError>` y el operador `?`.

```rust
// Representación de un error de dominio con contexto estructurado
#[derive(Debug, Clone, Error, PartialEq, Serialize, Deserialize)]
#[error("{code:?}: {detail}")]
pub struct DomainError {
    pub code: ErrorCode,
    pub stage: String,
    pub detail: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
}
```

> [!IMPORTANT]
> El campo `context` debe almacenar metadatos que expliquen el fallo (por ejemplo, el campo inválido o el valor conflictivo), mientras que la correlación con el `tenant_id` y `user_id` ocurre al empaquetar el error en la capa del orquestador IOP.

### I.2 — Frontera de captura (Ejemplo en OLTPChannel)

Cualquier error de AWS SDK u otra llamada de infraestructura es interceptado y mapeado de inmediato:

```rust
// src/infrastructure/dynamodb.rs
fn map_sdk_error<E: std::fmt::Debug>(
    err: SdkError<E>,
    default: ErrorCode,
    table_name: &str,
) -> DomainError {
    let msg = format!("{err:?}");
    let code = if msg.contains("AccessDeniedException") {
        ErrorCode::Infra001
    } else {
        default
    };
    DomainError::infra(code, format!("DynamoDB error en tabla '{table_name}': {msg}"))
}
```

---

## MÓDULO II: OpenTelemetry — Un Span por Fase

Cada petición que cruza el Metri Engine genera un árbol de spans correlacionados por un `trace_id` (W3C traceparent). Esto se logra mediante el framework `tracing` en Rust.

### II.1 — Jerarquía de Spans (Caso Transact)

```
Span ROOT: "iop.pipeline.start"              [trace_id: a1b2c3...]
  │  attrs: tenant_id, user_id, entity_type, operation
  ├── "janus.load-schema"
  ├── "janus.validate-payload"
  ├── "janus.verify-entity-refs"
  └── "janus.execute-transaction"
```

### II.2 — Declaración de Spans con `tracing`

La macro de instrumentación automatiza la creación del span y la inyección de atributos contextuales:

```rust
// src/iop/core.rs
#[tracing::instrument(
    name = "iop.pipeline.start",
    skip(self, ctx),
    fields(tenant_id = %ctx.tenant_id, request_id)
)]
async fn run(&self, ctx: IopContext) -> Value {
    // Código funcional del pipeline...
}
```

---

## MÓDULO III: Catálogo de Errores (SSOT)

Un único archivo TOML en la configuración del proyecto funciona como la fuente autoritativa (Single Source of Truth) para todos los errores del sistema.

### III.1 — Catálogo en TOML (`config/errors/error_catalog.toml`)

Cada entrada define metadatos de comportamiento para gRPC, HTTP y la política de reintentos:

```toml
[[errors]]
code             = "JNS_TX_001"
family           = "jns"
stage            = "janus"
severity         = "error"
http_status      = 500
grpc_status      = "INTERNAL"
description      = "EAV transact failed — full transaction rolled back (entity + all index projections)"
context_required = ["tx_data_size", "eav_error"]
retryable        = true
```

### III.2 — Carga e Inicialización Singleton (`OnceLock`)

El catálogo se valida al arrancar y se expone de manera segura y eficiente mediante `OnceLock`:

```rust
// src/domain/error_catalog.rs
static GLOBAL_CATALOG: OnceLock<ErrorCatalog> = OnceLock::new();

pub fn init_global(catalog: ErrorCatalog) {
    GLOBAL_CATALOG.set(catalog).expect("ErrorCatalog already initialized");
}

pub fn global() -> &'static ErrorCatalog {
    GLOBAL_CATALOG.get().expect("ErrorCatalog not initialized")
}
```

---

## MÓDULO IV: Rich Context Error DTO

Cuando un `DomainError` es capturado en la salida gRPC, el orquestador IOP construye un DTO enriquecido para facilitar la investigación forense, incluyendo la clasificación del componente origen.

### IV.1 — Estructura del JSON DTO

```json
{
  "status": "error",
  "error": {
    "code": "JNS_REF_002",
    "description": "Referenced entity UUID exists but has wrong entity type (type confusion attack blocked)",
    "correlation_id": "REQ-4bf92f35",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "span_id": "0000000000000000",
    "tenant_id": "system",
    "user_id": "usr_system_bff",
    "timestamp": 1718221000,
    "stage": "janus",
    "component": "metri-engine",
    "retryable": false,
    "context": {
      "field": "location_id",
      "expected_type": "location",
      "actual_type": "invoice"
    }
  }
}
```

El campo `"component"` clasifica el microservicio o componente donde se originó la falla (por ejemplo: `metri-auth`, `metri-engine`, `metri-event-router`, `metri-notifications`), permitiendo búsquedas estructuradas inmediatas en dashboards y sistemas de telemetría.

### IV.2 — Sanitización de Privacidad (Capa Códice)

El constructor verifica si hay campos `sensitive: true` definidos en el Códice para reemplazarlos con `"[REDACTED]"`:

```rust
// src/iop/error_response.rs
fn sanitize_context(context: Value, model: Option<&EntityModel>) -> Value {
    let Some(model) = model else { return context; };
    let Some(obj) = context.as_object() else { return context; };

    let sensitive_keys: std::collections::HashSet<&str> = model
        .attributes
        .iter()
        .filter(|a| a.sensitive)
        .map(|a| a.name.as_str())
        .collect();

    let sanitized = obj
        .iter()
        .map(|(k, v)| {
            if sensitive_keys.contains(k.as_str()) {
                (k.clone(), Value::String("[REDACTED]".to_string()))
            } else {
                (k.clone(), v.clone())
            }
        })
        .collect::<serde_json::Map<_, _>>();

    Value::Object(sanitized)
}
```

---

## MÓDULO V: Sherlog — Pipeline EDA de Errores

Los fallos de severidad superior a `WARNING` se consideran **eventos operacionales** del sistema.

### V.1 — Umbral de Severidad

| Severidad | Canal EDA | Destino Físico |
| :--- | :--- | :--- |
| `Info` | Ninguno (solo log local) | CloudWatch Logs |
| `Warning` | EventBridge / SQS | `domain_fault` (canal OLAP) |
| `Error` | EventBridge (escalación a PagerDuty) | `domain_fault` (canal OLAP) |
| `Fatal` | EventBridge (alerta + auto-restart) | `domain_fault` (canal OLAP) |

### V.2 — IFaultNotifier en Rust

El subsistema utiliza un trait asíncrono para enviar notificaciones:

```rust
// src/iop/sherlog.rs
#[async_trait::async_trait]
pub trait IFaultNotifier: Send + Sync {
    async fn notify(&self, error_dto: &Value, severity: &FaultSeverity) -> Result<(), DomainError>;
}
```

### V.3 — Despacho y Doble Acción (EDA + OLAP)

`process_fault` coordina de forma segura (fire-and-forget) la publicación en EventBridge y el volcado OLAP al stream de auditoría forense (`domain_fault`):

```rust
// src/iop/sherlog.rs
pub async fn process_fault(
    notifier: &dyn IFaultNotifier,
    error: &DomainError,
    error_dto: &Value,
) {
    let code_str = format!("{:?}", error.code);
    
    // 1. Lookup de severidad en el catálogo global
    let severity = error_catalog::try_global()
        .and_then(|c| c.get(&code_str))
        .map(|entry| FaultSeverity::from_catalog_str(&entry.severity))
        .unwrap_or(FaultSeverity::Warning);

    if !severity.requires_notification() {
        return;
    }

    // 2. Disparo asíncrono seguro
    if let Err(e) = notifier.notify(error_dto, &severity).await {
        error!("[Sherlog] EventBridge dispatch falló: {e:?}");
    }
}
```

---

## MÓDULO VI: Integración con Echo

El indicador `retryable` derivado del catálogo instruye a la cola intermedia Echo si el error es recuperable mediante reintentos con backoff exponencial.

```rust
// src/domain/errors.rs
impl ErrorCode {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ErrorCode::Aeg003      // Timeout de Athena
                | ErrorCode::Eav001  // Caída de transacciones DynamoDB
                | ErrorCode::Infra001
        )
    }
}
```

---

## MÓDULO VII: Estructura de Archivos del Subsistema

El motor en Rust organiza el módulo de la siguiente manera:

```
metri-engine/
├── config/errors/
│   └── error_catalog.toml             ← Catálogo de Errores SSOT
│
├── src/
│   ├── domain/
│   │   ├── errors.rs                  ← Enums de ErrorCode y estructura DomainError
│   │   └── error_catalog.rs           ← OnceLock del catálogo TOML
│   │
│   ├── otel/
│   │   ├── mod.rs
│   │   └── tracer.rs                  ← Atributos de spans y trace_id OTel
│   │
│   ├── iop/
│   │   ├── error_response.rs          ← build_error_dto y sanitize_context
│   │   └── sherlog.rs                 ← Trait IFaultNotifier y process_fault
│   │
│   └── grpc/
│       └── service.rs                 ← gRPC Gate Handler que devuelve el DTO
```

---

## MÓDULO VIII: Checklist de Implementación

**Defensa Inquebrantable:**
- [ ] No usar `panic!`, `unwrap()` o `expect()` en lógica operacional del engine.
- [ ] Retornar siempre `Result<T, DomainError>`.
- [ ] Validar la presencia del código en `error_catalog.toml` durante el bootstrap.

**OpenTelemetry:**
- [ ] Anotar `tenant_id` y `user_id` en el span raíz de cada llamada.
- [ ] En caso de fallo, marcar el span como `Error` y asociar el atributo `error.code`.

**Sherlog / EDA:**
- [ ] No propagar errores de EventBridge o Kinesis hacia el cliente de la API (fire-and-forget).
- [ ] Garantizar el redactado de campos marcados con `sensitive: true` en el payload JSON antes de la ingesta en Athena.
