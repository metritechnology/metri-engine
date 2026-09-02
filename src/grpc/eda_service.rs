// grpc/eda_service.rs — Servidor gRPC MoiraRoutingService
// SRP: Expone extremos asíncronos para el Event Router en Golang.

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument};

use crate::aegis::oltp::executor::OltpExecutor;
use crate::grpc::pb::eda::v1::moira_routing_service_server::MoiraRoutingService;
use crate::grpc::pb::eda::v1::{
    MatchRoutingRulesBatchRequest, MatchRoutingRulesBatchResponse, ResetOrphanedEventsRequest,
    ResetOrphanedEventsResponse,
};
use crate::grpc::service::fc_to_fbs_filter_node;
use crate::iop::core::MoiraEmitter;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct EdaMatchRequest {
    pub outbox_id: String,
    pub tenant_id: String,
    pub entity_name: String,
    pub trigger_type: String,
    pub cdc_payload_json: Vec<u8>,
}

#[derive(Debug, serde::Serialize)]
pub struct EdaWebhookTarget {
    pub webhook_url: String,
    pub secret_token_cipher: String,
    pub prepared_payload_base64: String,
}

#[derive(Debug, serde::Serialize)]
pub struct EdaRoutingResult {
    pub outbox_id: String,
    pub detail_type_output: String,
    pub destinations: Vec<EdaWebhookTarget>,
}

#[derive(Debug, serde::Serialize)]
pub struct EdaResponseJson {
    pub routing_results: Vec<EdaRoutingResult>,
    pub unmatched_outbox_ids: Vec<String>,
}

pub struct EdaGrpcService {
    moira_emitter: Arc<dyn MoiraEmitter>,
    oltp_executor: OltpExecutor,
}

impl EdaGrpcService {
    pub fn new(moira_emitter: Arc<dyn MoiraEmitter>, oltp_executor: OltpExecutor) -> Self {
        Self {
            moira_emitter,
            oltp_executor,
        }
    }
}

#[tonic::async_trait]
impl MoiraRoutingService for EdaGrpcService {
    #[instrument(name = "eda.match_rules.batch", skip(self, request))]
    async fn match_routing_rules_batch(
        &self,
        request: Request<MatchRoutingRulesBatchRequest>,
    ) -> Result<Response<MatchRoutingRulesBatchResponse>, Status> {
        let req = request.into_inner();

        // 1. Descomprimir el batch de MessagePack
        let requests: Vec<EdaMatchRequest> = rmp_serde::from_slice(&req.messagepack_encoded_batch)
            .map_err(|e| Status::invalid_argument(format!("MessagePack inválido: {e}")))?;

        let mut routing_results = Vec::new();
        let mut unmatched_outbox_ids = Vec::new();

        for req_item in requests {
            let tenant_id = &req_item.tenant_id;
            let entity_name = &req_item.entity_name;
            let trigger_type = &req_item.trigger_type;
            let outbox_id = &req_item.outbox_id;

            // 2. Decodificar el CDC payload JSON
            let cdc_payload: serde_json::Value =
                match serde_json::from_slice(&req_item.cdc_payload_json) {
                    Ok(val) => val,
                    Err(e) => {
                        return Err(Status::invalid_argument(format!(
                            "Payload JSON inválido para outbox_id={outbox_id}: {e}"
                        )));
                    }
                };

            // 3. Consultar las reglas activas para esta entidad y gatillo
            let query_rules = serde_json::json!({
                "entity": "event_routing_rule",
                "select": ["id", "rule_code", "description", "target_entity_name", "event_trigger_type", "filter_conditions", "detail_type_output"],
                "where": [
                    "and",
                    ["=", "target_entity_name", entity_name],
                    ["=", "event_trigger_type", trigger_type]
                ],
                "limit": 100
            });

            let rules_rows = match self
                .oltp_executor
                .run_oltp_query(tenant_id, &query_rules)
                .await
            {
                Ok(val) => val,
                Err(e) => {
                    return Err(Status::internal(format!(
                        "Fallo al consultar reglas de ruteo: {e:?}"
                    )));
                }
            };

            let rules = rules_rows.as_array().cloned().unwrap_or_default();

            // 4. Consultar webhooks activos del tenant
            let query_webhooks = serde_json::json!({
                "entity": "webhook_endpoint",
                "select": ["id", "name", "target_url", "http_method", "authentication_type", "auth_token", "subscribed_rule_ids", "max_retries", "is_active"],
                "where": [
                    "and",
                    ["=", "is_active", true]
                ],
                "limit": 100
            });

            let webhooks_rows = match self
                .oltp_executor
                .run_oltp_query(tenant_id, &query_webhooks)
                .await
            {
                Ok(val) => val,
                Err(_) => serde_json::Value::Array(vec![]),
            };

            let active_webhooks = webhooks_rows.as_array().cloned().unwrap_or_default();

            let mut matched_any_rule = false;
            let mut destinations = Vec::new();
            let mut matched_detail_type = String::new();

            for rule in rules {
                let rule_id = rule.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let detail_type_output = rule
                    .get("detail_type_output")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // 5. Evaluación AST de condiciones
                if let Some(fc) = rule.get("filter_conditions") {
                    if let Some(fbs_node) = fc_to_fbs_filter_node(fc) {
                        let passes =
                            crate::aegis::oltp::filter::eval_filter_node(&cdc_payload, &fbs_node);
                        if !passes {
                            continue;
                        }
                    }
                }

                matched_any_rule = true;
                matched_detail_type = detail_type_output.to_string();

                // 6. Mapear Webhooks asociados
                for webhook in &active_webhooks {
                    let subscribed_ids = webhook.get("subscribed_rule_ids");
                    let mut matches_rule = false;
                    if let Some(val) = subscribed_ids {
                        if let Some(arr) = val.as_array() {
                            matches_rule = arr.iter().any(|v| v.as_str() == Some(rule_id));
                        } else if let Some(s) = val.as_str() {
                            matches_rule = s == rule_id;
                        }
                    }

                    if matches_rule {
                        let target_url = webhook
                            .get("target_url")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let secret_token_cipher = webhook
                            .get("auth_token")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();

                        // Base64 encoding del payload original
                        let prepared_payload_base64 = base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            &req_item.cdc_payload_json,
                        );

                        destinations.push(EdaWebhookTarget {
                            webhook_url: target_url,
                            secret_token_cipher,
                            prepared_payload_base64,
                        });
                    }
                }
            }

            if matched_any_rule {
                routing_results.push(EdaRoutingResult {
                    outbox_id: outbox_id.to_string(),
                    detail_type_output: matched_detail_type,
                    destinations,
                });
            } else {
                unmatched_outbox_ids.push(outbox_id.to_string());
            }
        }

        let resp_json = EdaResponseJson {
            routing_results,
            unmatched_outbox_ids,
        };

        let json_bytes = serde_json::to_vec(&resp_json)
            .map_err(|e| Status::internal(format!("Fallo al serializar respuesta JSON: {e}")))?;

        Ok(Response::new(MatchRoutingRulesBatchResponse {
            json_routing_results: json_bytes,
        }))
    }

    #[instrument(name = "eda.watchdog.reset", skip(self, request))]
    async fn reset_orphaned_events(
        &self,
        request: Request<ResetOrphanedEventsRequest>,
    ) -> Result<Response<ResetOrphanedEventsResponse>, Status> {
        let req = request.into_inner();
        let ttl_ms = req.ttl_ms;

        // Determinar los tenants sobre los cuales correr el watchdog
        let mut tenants = Vec::new();
        if let Ok(active_tenants_env) = std::env::var("ACTIVE_TENANTS") {
            for t in active_tenants_env.split(',') {
                let trimmed = t.trim();
                if !trimmed.is_empty() {
                    tenants.push(trimmed.to_string());
                }
            }
        }

        // Si no hay especificado, usar por defecto el tenant local estándar
        if tenants.is_empty() {
            tenants.push("tnt_01".to_string());
        }

        let mut total_reset = 0;
        let mut reset_event_ids = Vec::new();

        for tenant_id in tenants {
            info!(tenant_id = %tenant_id, ttl_ms = %ttl_ms, "[Watchdog] Iniciando barrido de huérfanos");
            match self
                .moira_emitter
                .reset_orphaned_processing(&tenant_id, ttl_ms)
                .await
            {
                Ok(count) => {
                    if count > 0 {
                        info!(tenant_id = %tenant_id, count = %count, "[Watchdog] Eventos huérfanos reseteados");
                        total_reset += count as i32;
                        // Nota: El emisor no retorna las IDs directamente, así que agregamos un log descriptivo
                        reset_event_ids.push(format!("tnt_{tenant_id}_count_{count}"));
                    }
                }
                Err(e) => {
                    error!(tenant_id = %tenant_id, error = ?e, "[Watchdog] Fallo en barrido de huérfanos");
                }
            }
        }

        Ok(Response::new(ResetOrphanedEventsResponse {
            reset_count: total_reset,
            reset_event_ids,
        }))
    }
}
