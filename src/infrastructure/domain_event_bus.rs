//! Domain event bus adapters — EventBridge real and outbox.
//!
//! domain_event_bus — adaptadores del puerto application::ports::DomainEventPublisher.
//!
//! Dos implementaciones, una misma frontera:
//! - ContractEventPublisher: EventBridge real (producción).
//! - NoopEventPublisher: tests y composition roots sin AWS.
//!
//! El adaptador conoce EventBridge; el canal OLTP sólo conoce el trait. Cambiar
//! de bus — o publicar vía outbox — no toca el write path.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::application::ports::{DomainEventPublisher, PublishError};
use crate::domain::protocols::IEventBus;

pub struct ContractEventPublisher {
    bus: Arc<dyn IEventBus>,
    bus_name: String,
    source: String,
}

impl ContractEventPublisher {
    pub fn new(bus: Arc<dyn IEventBus>, bus_name: impl Into<String>) -> Self {
        Self {
            bus,
            bus_name: bus_name.into(),
            source: "metri.engine".to_string(),
        }
    }
}

#[async_trait]
impl DomainEventPublisher for ContractEventPublisher {
    async fn publish(&self, detail_type: &str, detail: &Value) -> Result<(), PublishError> {
        self.bus
            .put_event(&self.bus_name, &self.source, detail_type, detail.clone())
            .await
            .map(|_| ())
            .map_err(|e| {
                // Los fallos de PutEvents son de infraestructura: reintentables.
                // Un sobre inválido jamás llega aquí — el builder del contrato
                // lo rechaza antes, como Poison, sin tocar la red.
                PublishError::Transient(e.to_string())
            })
    }
}

pub struct NoopEventPublisher;

#[async_trait]
impl DomainEventPublisher for NoopEventPublisher {
    async fn publish(&self, _detail_type: &str, _detail: &Value) -> Result<(), PublishError> {
        Ok(())
    }
}

/// Adaptador fire-and-forget que reutiliza el emit histórico del canal
/// (tokio::spawn + PutEvents). Latencia cero para el write path; los fallos
/// los registra el emisor de fondo. El sucesor natural es un adaptador por
/// outbox que devuelva errores reales y reintente con backoff.
#[derive(Default)]
pub struct DetachedEventBridgePublisher;

#[async_trait]
impl DomainEventPublisher for DetachedEventBridgePublisher {
    async fn publish(&self, detail_type: &str, detail: &Value) -> Result<(), PublishError> {
        let bus_name =
            std::env::var("EVENTBRIDGE_BUS_NAME").unwrap_or_else(|_| "metri-events".to_string());
        super::eventbridge::publish_domain_event_async(
            bus_name,
            "metri.engine".to_string(),
            detail_type.to_string(),
            detail.clone(),
        );
        Ok(())
    }
}

/// Construye la publicación del contrato metri-contracts para una mutación de
/// `scheduled_job`: `(detail_type, detail)` listos para el bus.
///
/// Devuelve None (con log) cuando el sobre no cumple el contrato — Poison:
/// no se publica un sobre que el Hub rechazaría a DLQ. En DELETE usa los
/// atributos previos que rescató el writer, porque tras el retract ya no hay
/// trigger_type ni action_payload en el almacén.
pub fn build_scheduled_job_detail(
    result: &crate::eav::writer::TransactResult,
    operation: &str,
    payload: &Value,
    tenant_id: &str,
) -> Option<(String, Value)> {
    use crate::domain::events::{DomainEnvelope, ScheduledJobEventInput, ScheduledJobOp};

    let op = match ScheduledJobOp::parse(operation) {
        Ok(op) => op,
        Err(e) => {
            tracing::error!(error = %e, job = %result.entity_id, "[events] op fuera de contrato");
            return None;
        }
    };

    let attrs: serde_json::Map<String, Value> = match op {
        ScheduledJobOp::Deleted => {
            let deleted = match &result.deleted_attrs {
                Some(m) if !m.is_empty() => m,
                _ => {
                    tracing::error!(
                        job = %result.entity_id,
                        "[events] DELETE sin atributos previos — el Hub no podría identificar la trampa"
                    );
                    return None;
                }
            };
            deleted
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        crate::eav::writer::outbox::datom_value_to_json(v),
                    )
                })
                .collect()
        }
        _ => match payload.as_object() {
            Some(obj) => obj.clone(),
            None => {
                tracing::error!(job = %result.entity_id, "[events] payload no es objeto");
                return None;
            }
        },
    };

    let mut envelope = match DomainEnvelope::from_attributes(ScheduledJobEventInput {
        op,
        entity_id: &result.entity_id,
        tenant_id,
        mutation_ulid: &result.mutation_ulid,
        attributes: &attrs,
        delta: result.delta.clone(),
        causation_id: None,
    }) {
        Ok(env) => env,
        Err(e) => {
            tracing::error!(
                error = %e,
                job = %result.entity_id,
                "[events] sobre contract inválido — NO se publica (Poison, ve a corrección manual)"
            );
            return None;
        }
    };

    // La correlación del loop completo: el padre de la saga (la pauta
    // preventive_maintenance) viaja como correlation_id — de la pauta al fired
    // a la OT generada.
    if envelope.correlation_id.is_none() {
        envelope.correlation_id = attrs
            .get("parent_entity_ref")
            .and_then(|v| v.as_str())
            .map(String::from);
    }

    Some((envelope.detail_type(), envelope.to_detail()))
}

#[cfg(test)]
mod tests {
    use super::build_scheduled_job_detail;
    use crate::eav::writer::{TransactOp, TransactPayload, TransactResult};
    use serde_json::json;
    use std::collections::HashMap;

    fn result(entity_id: &str) -> TransactResult {
        TransactResult {
            entity_id: entity_id.to_string(),
            tx_id: 1,
            datoms: 1,
            outbox_count: 0,
            mutation_ulid: "01HZZZZZZZZZZZZZZZZZZZZZ02".to_string(),
            delta: None,
            deleted_attrs: None,
        }
    }

    #[test]
    fn create_de_job_produce_sobre_contract_con_correlacion() {
        let r = result("01HZZZZZZZZZZZZZZZZZZZZZ01");
        let payload = json!({
            "trigger_type": "CRON",
            "trigger_expression": "0 8 * * 1-5",
            "iana_timezone": "America/Bogota",
            "action_type": "RPC_CALL",
            "action_payload": {"entity_type": "asset"},
            "idempotency_hash": "abc123",
            "status": "PENDING",
            "parent_entity_ref": "01PM"
        });
        let (detail_type, detail) = build_scheduled_job_detail(&r, "CREATE", &payload, "tnt_1")
            .expect("el sobre debe construirse");
        assert_eq!(detail_type, "system.scheduled_job.created");
        assert_eq!(detail["job_id"], json!("01HZZZZZZZZZZZZZZZZZZZZZ01"));
        assert_eq!(detail["ulid"], json!("01HZZZZZZZZZZZZZZZZZZZZZ02"));
        // La correlación del loop: la pauta madre viaja con el sobre.
        assert_eq!(detail["correlation_id"], json!("01PM"));
    }

    #[test]
    fn update_con_delta_de_bitacora_viaja_en_el_sobre() {
        let mut r = result("01JOB");
        r.delta = Some(crate::domain::events::compute_delta(
            json!({"run_count": 41}).as_object().unwrap(),
            json!({"run_count": 42}).as_object().unwrap(),
        ));
        let payload = json!({
            "trigger_type": "CRON",
            "trigger_expression": "0 8 * * 1-5",
            "action_type": "RPC_CALL",
            "action_payload": {},
            "idempotency_hash": "abc123",
            "status": "ACTIVE"
        });
        let (detail_type, detail) = build_scheduled_job_detail(&r, "UPDATE", &payload, "tnt_1")
            .expect("el sobre debe construirse");
        assert_eq!(detail_type, "system.scheduled_job.updated");
        assert_eq!(detail["delta"]["run_count"]["after"], json!(42));
    }

    #[test]
    fn delete_sin_atributos_previos_es_poison_no_publica() {
        let r = result("01JOB");
        let payload = json!({ "id": "01JOB" });
        assert!(
            build_scheduled_job_detail(&r, "DELETE", &payload, "tnt_1").is_none(),
            "sin trigger_type el Hub no identificaría la trampa: no publicar"
        );
    }

    #[test]
    fn delete_con_atributos_rescatados_publica_sobre_completo() {
        let mut r = result("01JOB");
        let mut deleted = HashMap::new();
        deleted.insert(
            "trigger_type".to_string(),
            crate::eav::types::datom::DatomValue::Str("TELEMETRY".to_string()),
        );
        deleted.insert(
            "trigger_expression".to_string(),
            crate::eav::types::datom::DatomValue::Str("VIBRATION > 5000".to_string()),
        );
        deleted.insert(
            "action_type".to_string(),
            crate::eav::types::datom::DatomValue::Str("RPC_CALL".to_string()),
        );
        deleted.insert(
            "action_payload".to_string(),
            crate::eav::types::datom::DatomValue::Str("{}".to_string()),
        );
        deleted.insert(
            "idempotency_hash".to_string(),
            crate::eav::types::datom::DatomValue::Str("hash1".to_string()),
        );
        deleted.insert(
            "status".to_string(),
            crate::eav::types::datom::DatomValue::Str("ACTIVE".to_string()),
        );
        r.deleted_attrs = Some(deleted);

        let (detail_type, detail) =
            build_scheduled_job_detail(&r, "DELETE", &json!({"id": "01JOB"}), "tnt_1")
                .expect("con atributos previos el sobre debe construirse");
        assert_eq!(detail_type, "system.scheduled_job.deleted");
        assert_eq!(detail["trigger_type"], json!("TELEMETRY"));
        assert_eq!(detail["action_payload"], json!({}));
    }

    // Silencia el unused en tests: TransactOp/TransactPayload se usan vía
    // TransactResult en el writer; este import documenta la frontera.
    #[allow(dead_code)]
    fn _frontera(_p: TransactPayload, _o: TransactOp) {}
}
