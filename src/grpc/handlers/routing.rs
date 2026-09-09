//! MatchRoutingRulesBatch RPC body.
use crate::grpc::handlers::query_support::{fc_to_fbs_filter_node, fc_to_proto_filter_node};
use crate::grpc::pb::{
    MatchRoutingRulesBatchRequest, MatchRoutingRulesBatchResponse, MatchRoutingRulesResponse,
    MatchedRule, WebhookTarget,
};
use crate::grpc::service::MetriGrpcService;
use tonic::{Request, Response, Status};
use tracing::error;

// Handlers del MetriGrpcService — fase 2: service.rs delega, aquí vive el cuerpo.

impl MetriGrpcService {
    pub(crate) async fn match_routing_rules_batch_impl(
        &self,
        request: Request<MatchRoutingRulesBatchRequest>,
    ) -> Result<Response<MatchRoutingRulesBatchResponse>, Status> {
        let (session_tenant_id, session_user_id) = {
            let session = request
                .extensions()
                .get::<crate::grpc::interceptors::AuthenticatedSession>()
                .ok_or_else(|| Status::unauthenticated("Petición no autenticada [Fail-Closed]"))?;
            (session.tenant_id.clone(), session.user_id.clone())
        };

        if session_tenant_id != "system" {
            error!(
                "[gRPC RoutingRules] Acceso denegado: Se requiere rol de sistema. Solicitante={}",
                session_user_id
            );
            return Err(Status::permission_denied(
                "Acceso denegado: Se requiere rol de sistema para la consulta de ruteo",
            ));
        }

        let batch = request.into_inner();
        let mut responses = Vec::with_capacity(batch.requests.len());

        for req in batch.requests {
            let tenant_id = &req.tenant_id;
            let entity_name = &req.entity_name;
            let trigger_type = &req.trigger_type;

            // 1. Decodificar CDC payload
            let cdc_payload: serde_json::Value = match serde_json::from_slice(&req.cdc_payload_json)
            {
                Ok(val) => val,
                Err(e) => {
                    return Err(Status::invalid_argument(format!(
                        "Payload JSON inválido: {e}"
                    )));
                }
            };

            // 2. Escanear reglas del tenant activas para la entidad y trigger
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

            // 3. Consultar todos los webhook endpoints activos para filtrar por suscripción en memoria
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

            let mut matched_rules = Vec::new();

            for rule in rules {
                let rule_id = rule.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let rule_code = rule.get("rule_code").and_then(|v| v.as_str()).unwrap_or("");
                let description = rule
                    .get("description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let detail_type_output = rule
                    .get("detail_type_output")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // 4. Evaluación nativa de FilterNode
                if let Some(fc) = rule.get("filter_conditions") {
                    if let Some(fbs_node) = fc_to_fbs_filter_node(fc) {
                        let passes =
                            crate::aegis::oltp::filter::eval_filter_node(&cdc_payload, &fbs_node);
                        if !passes {
                            continue;
                        }
                    }
                }

                // 5. Mapear Webhooks suscritos
                let mut matched_webhooks = Vec::new();
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
                        let http_method = webhook
                            .get("http_method")
                            .and_then(|v| v.as_str())
                            .unwrap_or("POST")
                            .to_string();
                        let auth_type = webhook
                            .get("authentication_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("NONE")
                            .to_string();
                        let resolved_auth_secret = webhook
                            .get("auth_token")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let max_retries = webhook
                            .get("max_retries")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(3) as i32;

                        matched_webhooks.push(WebhookTarget {
                            target_url,
                            http_method,
                            auth_type,
                            resolved_auth_secret,
                            max_retries,
                            timeout_seconds: 10,
                            headers: std::collections::HashMap::new(),
                        });
                    }
                }

                let condition_proto = rule
                    .get("filter_conditions")
                    .and_then(fc_to_proto_filter_node);

                matched_rules.push(MatchedRule {
                    rule_code: rule_code.to_string(),
                    detail_type_output: detail_type_output.to_string(),
                    webhooks: matched_webhooks,
                    priority: 1,
                    name: description.to_string(),
                    condition: condition_proto,
                });
            }

            responses.push(MatchRoutingRulesResponse {
                status: Some(crate::grpc::pb::Status {
                    success: true,
                    error_code: String::new(),
                    error_message: String::new(),
                    error_context: None,
                }),
                matched_rules,
            });
        }

        Ok(Response::new(MatchRoutingRulesBatchResponse {
            status: Some(crate::grpc::pb::Status {
                success: true,
                error_code: String::new(),
                error_message: String::new(),
                error_context: None,
            }),
            responses,
        }))
    }
}
