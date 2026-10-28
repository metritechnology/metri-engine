//! Servidor gRPC `MoiraRoutingService` (`EdaGrpcService`) para el Event Router en Go.
//!
//! Expone `MatchRoutingRulesBatch` (MessagePack) y `ResetOrphanedEvents` (watchdog
//! de eventos huérfanos en el transactional outbox).

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument};

use crate::aegis::oltp::executor::OltpExecutor;
use crate::grpc::handlers::query_support::fc_to_fbs_filter_node;
use crate::grpc::pb::eda::v1::moira_routing_service_server::MoiraRoutingService;
use crate::grpc::pb::eda::v1::{
    MatchRoutingRulesBatchRequest, MatchRoutingRulesBatchResponse, ResetOrphanedEventsRequest,
    ResetOrphanedEventsResponse,
};
use crate::iop::core::MoiraEmitter;

#[rustfmt::skip] #[doc = "Elemento individual de evaluación de ruteo dentro de un lote MessagePack del Event Router."] #[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct EdaMatchRequest {
    #[doc = "Identificador único del evento en el transactional outbox."] pub outbox_id: String,
    #[doc = "Identificador del tenant propietario del evento."] pub tenant_id: String,
    #[doc = "Nombre de la entidad de negocio que originó el evento CDC."] pub entity_name: String,
    #[doc = "Tipo de disparador o mutación (`CREATE`, `UPDATE`, `DELETE`, etc.)."] pub trigger_type: String,
    #[doc = "Carga útil CDC serializada en JSON (bytes UTF-8)."] pub cdc_payload_json: Vec<u8>,
}

#[rustfmt::skip] #[doc = "Destino de webhook resuelto para un evento que coincidió con una regla de ruteo."] #[derive(Debug, serde::Serialize)]
pub struct EdaWebhookTarget {
    #[doc = "URL HTTP/HTTPS de destino del endpoint de webhook."] pub webhook_url: String,
    #[doc = "Token o secreto de autenticación asociado al webhook."] pub secret_token_cipher: String,
    #[doc = "Carga útil del evento codificada en Base64 lista para su entrega."] pub prepared_payload_base64: String,
}

#[rustfmt::skip] #[doc = "Resultado de evaluación de ruteo para un evento del outbox que coincidió con al menos una regla."] #[derive(Debug, serde::Serialize)]
pub struct EdaRoutingResult {
    #[doc = "Identificador del registro de outbox evaluado."] pub outbox_id: String,
    #[doc = "Tipo de detalle de salida (`detail-type`) determinado por la regla coincidente."] pub detail_type_output: String,
    #[doc = "Lista de webhooks activos suscritos a las reglas coincidentes."] pub destinations: Vec<EdaWebhookTarget>,
}

#[rustfmt::skip] #[doc = "Respuesta JSON consolidada de la evaluación por lotes de reglas de ruteo EDA."] #[derive(Debug, serde::Serialize)]
pub struct EdaResponseJson {
    #[doc = "Resultados de ruteo para los eventos que coincidieron con reglas activas."] pub routing_results: Vec<EdaRoutingResult>,
    #[doc = "Identificadores de outbox que no coincidieron con ninguna regla de ruteo."] pub unmatched_outbox_ids: Vec<String>,
}

#[rustfmt::skip] #[doc = "Implementación del servicio gRPC `MoiraRoutingService` para evaluación de reglas y watchdog del outbox."] pub struct EdaGrpcService {
    moira_emitter: Arc<dyn MoiraEmitter>,
    oltp_executor: OltpExecutor,
}

#[rustfmt::skip] impl EdaGrpcService {
    #[doc = "Construye una nueva instancia de `EdaGrpcService` con el emisor Moira y el ejecutor OLTP dados."] pub fn new(moira_emitter: Arc<dyn MoiraEmitter>, oltp_executor: OltpExecutor) -> Self {
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
        let req = Self::authorize_system_caller(request)?;

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
        let req = Self::authorize_system_caller(request)?;
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

impl EdaGrpcService {
    fn authorize_system_caller<T>(request: Request<T>) -> Result<T, Status> {
        let s = request
            .extensions()
            .get::<crate::grpc::interceptors::AuthenticatedSession>()
            .ok_or_else(|| Status::unauthenticated("Petición no autenticada [Fail-Closed]"))?;
        if !crate::cedar::is_master_tenant(&s.tenant_id) && s.user_id != "usr_system_bff" {
            return Err(Status::permission_denied(
                "Acceso denegado: Se requiere rol de sistema para la consulta de ruteo",
            ));
        }
        Ok(request.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::interceptors::AuthenticatedSession;

    #[test]
    fn eda_rechaza_peticion_sin_sesion_y_de_tenant_regular() {
        let unauth_req = Request::new(());
        assert_eq!(
            EdaGrpcService::authorize_system_caller(unauth_req)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        let mut regular_req = Request::new(());
        regular_req.extensions_mut().insert(AuthenticatedSession {
            tenant_id: "tnt_01".to_string(),
            user_id: "usr_regular".to_string(),
            jti: "jti_1".to_string(),
        });
        assert_eq!(
            EdaGrpcService::authorize_system_caller(regular_req)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let mut sys_req = Request::new(());
        sys_req.extensions_mut().insert(AuthenticatedSession {
            tenant_id: "system".to_string(),
            user_id: "usr_system_bff".to_string(),
            jti: "jti_sys".to_string(),
        });
        assert!(EdaGrpcService::authorize_system_caller(sys_req).is_ok());
    }
}
