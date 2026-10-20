//! Sobre de evento del contrato `metri-contracts` v1.0 (lado productor).
//!
//! Espejo en Rust de `metri-schedulers/internal/event/envelope.go`. Los nombres de
//! campo constituyen el contrato de interoperabilidad verificado en `golden_test.rs`.
//!
//! Política fail-closed: `DomainEnvelope::from_attributes` rechaza cualquier sobre sin
//! `job_id`, `tenant_id`, `ulid` o campos obligatorios de disparador, clasificándolo
//! como `EnvelopeError::Poison` (no reintentable).

use super::delta::Delta;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Versión canónica del esquema del sobre de eventos `metri-contracts`.
pub const SCHEMA_VERSION: &str = "1.0";

/// Tipo de operación del ciclo de vida de un `scheduled_job`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledJobOp {
    /// Creación de una nueva tarea programada (`created`).
    Created,
    /// Actualización de atributos de una tarea programada existente (`updated`).
    Updated,
    /// Eliminación o baja lógica de una tarea programada (`deleted`).
    Deleted,
}

impl ScheduledJobOp {
    /// Representación textual en minúsculas según el contrato (`"created"`, `"updated"`, `"deleted"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            ScheduledJobOp::Created => "created",
            ScheduledJobOp::Updated => "updated",
            ScheduledJobOp::Deleted => "deleted",
        }
    }

    /// Retorna el `detail-type` canónico para enrutamiento en el bus de eventos (`system.scheduled_job.<op>`).
    pub fn detail_type(&self) -> String {
        format!("system.scheduled_job.{}", self.as_str())
    }

    /// Parsea la operación desde el verbo del canal transaccional o el literal del contrato.
    pub fn parse(s: &str) -> Result<Self, EnvelopeError> {
        match s {
            "CREATE" | "created" => Ok(ScheduledJobOp::Created),
            "UPDATE" | "updated" => Ok(ScheduledJobOp::Updated),
            "DELETE" | "deleted" => Ok(ScheduledJobOp::Deleted),
            other => Err(EnvelopeError::Poison(format!(
                "E-EVT-001: op '{}' fuera del contrato (esperado created|updated|deleted)",
                other
            ))),
        }
    }
}

/// Error de construcción o validación del sobre de evento de dominio.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    /// El sobre viola el contrato: reintentar no lo arregla (DLQ).
    #[error("{0}")]
    Poison(String),
}

/// El sobre de una mutación de `scheduled_job`, listo para publicar.
///
/// Serializa exactamente a los fixtures de `metri-contracts/golden/`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DomainEnvelope {
    /// Versión del esquema del contrato de eventos (`"1.0"`).
    pub schema_version: String,
    /// Operación ejecutada sobre la tarea programada (`"created"`, `"updated"`, `"deleted"`).
    pub op: String,
    /// Identificador único de la entidad `scheduled_job`.
    pub job_id: String,
    /// Identificador del tenant propietario de la tarea programada.
    pub tenant_id: String,
    /// Identificador ULID monotónico de la mutación para ordenamiento causal en el ledger.
    pub ulid: String,
    /// Tipo de disparador (`"CRON"`, `"EXACT_TIME"`, `"TELEMETRY"`).
    pub trigger_type: String,
    /// Expresión de programación o condición de disparo (ej. expresión cron o regla de telemetría).
    pub trigger_expression: String,
    /// Zona horaria IANA opcional para evaluación de expresiones temporales (ej. `"America/Bogota"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iana_timezone: Option<String>,
    /// Tipo de acción a ejecutar al dispararse la tarea (`"RPC_CALL"`, `"DISPATCH_NOTIFICATION"`, `"WEBHOOK"`, `"EDA_BROADCAST"`).
    pub action_type: String,
    /// Objeto JSON con la carga útil que se entregará al ejecutor de la acción.
    pub action_payload: Value,
    /// Hash determinista de idempotencia de la definición del trabajo.
    pub idempotency_hash: String,
    /// Identificador del usuario o actor del sistema que creó la tarea programada.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    /// Identificador del webhook destino cuando `action_type` es `"WEBHOOK"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_webhook_id: Option<String>,
    /// Estado operativo actual de la tarea (`"PENDING"`, `"ACTIVE"`, `"SUSPENDED"`, `"COMPLETED"`, `"FAILED"`).
    pub status: String,
    /// Identificador de correlación distribuida para trazabilidad extremo a extremo.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    /// Identificador de la entidad o evento causante que originó esta mutación.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    /// Cabecera W3C Trace Context (`traceparent`) para propagación OpenTelemetry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// Mapa diferencial de atributos modificados (`before`/`after`) en operaciones de actualización.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<Delta>,
}

/// Entrada para construir el sobre desde los atributos de la entidad.
///
/// `attributes` es el payload aplanado de la entidad (lo que hoy viaja en el
/// detail del evento). `action_payload` puede llegar como objeto (payload JSON
/// del canal) o como string JSON (así lo coacciona el validador Códice) — se
/// normaliza a objeto.
pub struct ScheduledJobEventInput<'a> {
    /// Operación del ciclo de vida ejecutada sobre el `scheduled_job`.
    pub op: ScheduledJobOp,
    /// Identificador de la entidad `scheduled_job`.
    pub entity_id: &'a str,
    /// Identificador del tenant propietario.
    pub tenant_id: &'a str,
    /// Identificador ULID de la transacción o mutación.
    pub mutation_ulid: &'a str,
    /// Mapa de atributos de la entidad en representación JSON.
    pub attributes: &'a serde_json::Map<String, Value>,
    /// Diferencial opcional de atributos modificados en la transacción.
    pub delta: Option<Delta>,
    /// Identificador opcional de la entidad o evento causante.
    pub causation_id: Option<String>,
}

const TRIGGER_TYPES: [&str; 3] = ["CRON", "EXACT_TIME", "TELEMETRY"];
const ACTION_TYPES: [&str; 4] = [
    "RPC_CALL",
    "DISPATCH_NOTIFICATION",
    "WEBHOOK",
    "EDA_BROADCAST",
];
const JOB_STATUSES: [&str; 5] = ["PENDING", "ACTIVE", "SUSPENDED", "COMPLETED", "FAILED"];

impl DomainEnvelope {
    /// Construye y valida bajo política fail-closed un `DomainEnvelope` a partir de los atributos de la mutación.
    pub fn from_attributes(input: ScheduledJobEventInput<'_>) -> Result<Self, EnvelopeError> {
        let attrs = input.attributes;
        let poison = |code: &str, field: &str| {
            EnvelopeError::Poison(format!(
                "{code}: sobre '{field}' inválido — el Job necesita corrección, no reintento"
            ))
        };

        if input.entity_id.is_empty() {
            return Err(poison("E-EVT-002", "job_id"));
        }
        if input.tenant_id.is_empty() {
            return Err(poison("E-EVT-003", "tenant_id"));
        }
        if input.mutation_ulid.is_empty() {
            return Err(poison("E-EVT-004", "ulid"));
        }

        let str_attr = |name: &str| -> Option<String> {
            match attrs.get(name) {
                Some(Value::String(s)) => Some(s.clone()),
                Some(other) => Some(other.to_string()),
                None => None,
            }
        };

        let trigger_type = str_attr("trigger_type").unwrap_or_default();
        if !TRIGGER_TYPES.contains(&trigger_type.as_str()) {
            return Err(poison("E-EVT-002", "trigger_type"));
        }
        let action_type = str_attr("action_type").unwrap_or_default();
        if !ACTION_TYPES.contains(&action_type.as_str()) {
            return Err(poison("E-EVT-002", "action_type"));
        }
        let status = str_attr("status").unwrap_or_else(|| "PENDING".to_string());
        if !JOB_STATUSES.contains(&status.as_str()) {
            return Err(poison("E-EVT-002", "status"));
        }
        let trigger_expression = str_attr("trigger_expression").unwrap_or_default();
        if trigger_expression.is_empty() {
            return Err(poison("E-EVT-002", "trigger_expression"));
        }
        let idempotency_hash = str_attr("idempotency_hash").unwrap_or_default();
        if idempotency_hash.is_empty() {
            return Err(poison("E-EVT-002", "idempotency_hash"));
        }

        let action_payload = match attrs.get("action_payload") {
            None => Value::Object(serde_json::Map::new()),
            Some(Value::String(s)) => serde_json::from_str(s).map_err(|e| {
                EnvelopeError::Poison(format!("E-EVT-001: action_payload no parseable: {e}"))
            })?,
            Some(v @ Value::Object(_)) => v.clone(),
            Some(_) => return Err(poison("E-EVT-001", "action_payload")),
        };

        Ok(DomainEnvelope {
            schema_version: SCHEMA_VERSION.to_string(),
            op: input.op.as_str().to_string(),
            job_id: input.entity_id.to_string(),
            tenant_id: input.tenant_id.to_string(),
            ulid: input.mutation_ulid.to_string(),
            trigger_type,
            trigger_expression,
            iana_timezone: str_attr("iana_timezone"),
            action_type,
            action_payload,
            idempotency_hash,
            created_by: str_attr("created_by"),
            target_webhook_id: str_attr("target_webhook_id"),
            status,
            correlation_id: str_attr("correlation_id"),
            causation_id: input.causation_id,
            traceparent: str_attr("traceparent"),
            delta: input.delta,
        })
    }

    /// detail-type canónico del contrato (el Hub enruta por esto).
    pub fn detail_type(&self) -> String {
        format!("system.scheduled_job.{}", self.op)
    }

    /// Serializa el sobre de dominio a un `serde_json::Value` listo para inyectar en el campo `detail` del bus.
    #[allow(clippy::expect_used)]
    pub fn to_detail(&self) -> Value {
        serde_json::to_value(self).expect("DomainEnvelope es serializable por construcción")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn attrs(extra: Value) -> serde_json::Map<String, Value> {
        let base = json!({
            "trigger_type": "CRON",
            "trigger_expression": "0 8 * * 1-5",
            "iana_timezone": "America/Bogota",
            "action_type": "RPC_CALL",
            "action_payload": {"entity_type": "asset"},
            "idempotency_hash": "abc123",
            "created_by": "01USER",
            "status": "PENDING"
        });
        let mut merged = base.as_object().cloned().unwrap();
        if let Some(extra_obj) = extra.as_object() {
            for (k, v) in extra_obj {
                merged.insert(k.clone(), v.clone());
            }
        }
        merged
    }

    #[test]
    fn sobre_valido_se_construye_y_detalla() {
        let a = attrs(json!({}));
        let env = DomainEnvelope::from_attributes(ScheduledJobEventInput {
            op: ScheduledJobOp::Created,
            entity_id: "01JOB",
            tenant_id: "tnt_1",
            mutation_ulid: "01MUT",
            attributes: &a,
            delta: None,
            causation_id: Some("01PM".to_string()),
        })
        .expect("sobre válido");

        assert_eq!(env.detail_type(), "system.scheduled_job.created");
        assert_eq!(env.schema_version, SCHEMA_VERSION);
        let detail = env.to_detail();
        assert_eq!(detail["job_id"], json!("01JOB"));
        assert_eq!(detail["ulid"], json!("01MUT"));
        assert_eq!(detail["action_payload"]["entity_type"], json!("asset"));
    }

    #[test]
    fn action_payload_como_string_json_se_normaliza() {
        // El validador Códice coacciona los atributos json a string — el sobre
        // no debe publicar un string donde el Hub espera un objeto.
        let a = attrs(json!({"action_payload": "{\"entity_type\":\"asset\"}"}));
        let env = DomainEnvelope::from_attributes(ScheduledJobEventInput {
            op: ScheduledJobOp::Created,
            entity_id: "01JOB",
            tenant_id: "tnt_1",
            mutation_ulid: "01MUT",
            attributes: &a,
            delta: None,
            causation_id: None,
        })
        .expect("sobre válido");
        assert_eq!(env.action_payload["entity_type"], json!("asset"));
    }

    #[test]
    fn fail_closed_sin_campos_obligatorios() {
        let cases = [
            ("job_id", json!({})),
            ("trigger_type", json!({"trigger_type": "QUARTZ"})),
            ("status", json!({"status": "RUNNING"})),
            ("idempotency_hash", json!({"idempotency_hash": ""})),
        ];
        for (nombre, extra) in cases {
            let a = attrs(extra);
            let res = DomainEnvelope::from_attributes(ScheduledJobEventInput {
                op: ScheduledJobOp::Created,
                entity_id: if nombre == "job_id" { "" } else { "01JOB" },
                tenant_id: "tnt_1",
                mutation_ulid: "01MUT",
                attributes: &a,
                delta: None,
                causation_id: None,
            });
            assert!(res.is_err(), "{nombre} ausente/inválido debe ser Poison");
        }
    }

    #[test]
    fn ulid_vacio_es_poison_eevt004() {
        let a = attrs(json!({}));
        let err = DomainEnvelope::from_attributes(ScheduledJobEventInput {
            op: ScheduledJobOp::Updated,
            entity_id: "01JOB",
            tenant_id: "tnt_1",
            mutation_ulid: "",
            attributes: &a,
            delta: None,
            causation_id: None,
        })
        .unwrap_err();
        assert!(err.to_string().contains("E-EVT-004"));
    }

    #[test]
    fn op_parsea_desde_operacion_del_canal() {
        assert_eq!(
            ScheduledJobOp::parse("CREATE").unwrap(),
            ScheduledJobOp::Created
        );
        assert_eq!(
            ScheduledJobOp::parse("DELETE").unwrap(),
            ScheduledJobOp::Deleted
        );
        assert!(ScheduledJobOp::parse("UPSERT").is_err());
    }
}
