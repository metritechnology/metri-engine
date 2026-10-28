//! CompositeTransact RPC — instanciación atómica (Fase 9 de
//! PLAN_REFACTORIZACION_MODELO).
//!
//! N entidades en UNA `TransactWriteItems`: la escritura es todo-o-nada.
//! `entities[0]` es la raíz (ej. `work_order_procedure`); el resto, sus hijos
//! (ej. `work_order_procedure_field`). Contrato: TODOS los `entity_id` van
//! preasignados por el cliente — los hijos referencian ids antes de que el
//! motor mintee ninguno.
//!
//! Reuso estricto (DRY): la seguridad por entidad es la misma de Transact
//! (`validate_single_mutation`), la validación estructural es la del Códice
//! (`validator::validate_payload`) y la atomicidad es la de la saga
//! (`EavWriter::transact_with_projections`). Este handler solo compone.
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::writer::transact::{TransactOp, TransactPayload};
use crate::grpc::pb::{
    CompositeTransactRequest, CompositeTransactResponse, EntityWriteResult, TransactionRequest,
};
use crate::grpc::service::MetriGrpcService;
use crate::grpc::translator;
use crate::janus_router::saga::SagaProjection;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

impl MetriGrpcService {
    pub(crate) async fn composite_transact_impl(
        &self,
        request: Request<CompositeTransactRequest>,
    ) -> Result<Response<CompositeTransactResponse>, Status> {
        let principal = crate::cedar::get_principal_data(
            &request,
            self.valkey_store.as_ref(),
            self.oltp_executor.pull_reader(),
            self.principal_cache.as_ref(),
            self.dev_auth_bypass,
        )
        .await
        .map_err(|err| Status::unauthenticated(format!("Authentication failed: {}", err.detail)))?;

        let mut req = request.into_inner();
        req.tenant_id = if principal.tenant_id == "system" {
            if req.tenant_id.is_empty() {
                "system".to_string()
            } else {
                req.tenant_id.clone()
            }
        } else {
            principal.tenant_id.clone()
        };

        if req.entities.is_empty() {
            return Err(Status::invalid_argument(
                "CompositeTransact exige al menos una entidad",
            ));
        }
        if req.entities.len() > 100 {
            return Err(Status::invalid_argument(format!(
                "CompositeTransact supera el máximo de 100 entidades por transacción ({})",
                req.entities.len()
            )));
        }

        // Planificación: seguridad + validación estructural por entidad.
        // Todo rechazo sale ANTES de tocar la base: el composite que falla en
        // planificación deja exactamente 0 filas.
        let mut payloads: Vec<TransactPayload> = Vec::new();
        let mut identidad: Vec<(String, String)> = Vec::new(); // (type, id)
        for (i, entity) in req.entities.iter().enumerate() {
            let entity: &TransactionRequest = entity;
            let tenant_id = if principal.tenant_id == "system" && entity.tenant_id.is_empty() {
                req.tenant_id.clone()
            } else if principal.tenant_id == "system" {
                entity.tenant_id.clone()
            } else {
                principal.tenant_id.clone()
            };
            if tenant_id != req.tenant_id {
                return Err(Status::permission_denied(format!(
                    "entities[{i}]: Conflicto de tenant en transacción compuesta ({} vs {})",
                    tenant_id, req.tenant_id
                )));
            }

            if entity.action != 1 {
                // OperationAction::CREATE — un composite es una instanciación.
                return Err(Status::invalid_argument(format!(
                    "entities[{i}]: CompositeTransact solo soporta CREATE; \
                     los updates van por Transact"
                )));
            }
            if entity.entity_id.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "entities[{i}]: entity_id preasignado obligatorio — los hijos \
                     referencian ids antes de que el motor mintea ninguno"
                )));
            }
            if entity.entity_type.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "entities[{i}]: entity_type vacío"
                )));
            }

            let payload_json = if let Some(struct_payload) = entity.payload.as_ref() {
                translator::struct_to_value(struct_payload.clone())
            } else {
                serde_json::Value::Object(serde_json::Map::new())
            };
            let payload_json = match payload_json.get("data") {
                Some(data) => data.clone(),
                None => payload_json,
            };

            if entity.entity_type == "role" {
                if let Some(grants) = payload_json.get("grants") {
                    self.validate_role_grants(grants)?;
                }
            }

            // Seguridad por entidad: aislamiento de tenant, grants self-service
            // y ABAC Cedar — la misma puerta que Transact, sin atajos.
            self.validate_single_mutation(
                &tenant_id,
                &entity.entity_type,
                "CREATE",
                &payload_json,
                &principal,
            )
            .await?;

            if entity.entity_type == "user_group" {
                self.validate_user_group_hierarchy_cycle(
                    &tenant_id,
                    &entity.entity_id,
                    &payload_json,
                )
                .await?;
            }

            // Estructura + coerción contra el Códice (misma validación del
            // camino normal: falla ANTES de planear datoms).
            let Some(model) = crate::codice::global().get_model(&entity.entity_type) else {
                return Err(Status::from(DomainError::eav(
                    ErrorCode::Eav004,
                    format!("entity_type '{}' no en registry", entity.entity_type),
                )));
            };
            let attrs =
                crate::codice::validator::validate_payload(model, &payload_json, &tenant_id, true)
                    .map_err(|e| Status::invalid_argument(e.detail))?;

            info!(
                "Composite entity[{i}] | tenant: {} | entity: {} | id: {}",
                tenant_id, entity.entity_type, entity.entity_id
            );
            identidad.push((entity.entity_type.clone(), entity.entity_id.clone()));
            payloads.push(TransactPayload {
                tenant_id,
                entity_id: Some(entity.entity_id.clone()),
                entity_type: entity.entity_type.clone(),
                attrs,
                op: TransactOp::Create,
                suppress_events: req.suppress_events,
            });
        }

        // Control de cuotas transaccional para CompositeTransact (salvo bypass de desarrollo,
        // tenant maestro o cuentas de sistema).
        let mut debited_quotas: Vec<(String, i64)> = Vec::new();
        let quota_exempt_caller = self.dev_auth_bypass.is_bypass()
            || crate::cedar::is_master_tenant(&req.tenant_id)
            || principal.user_id == "usr_system_bff"
            || principal.user_id == "usr_master";

        if !quota_exempt_caller {
            use crate::quota::{DebitOutcome, QuotaCounter, QuotaResolver};
            let mut counts_by_domain: std::collections::BTreeMap<String, i64> =
                std::collections::BTreeMap::new();
            for (entity_type, _) in &identidad {
                if !crate::cedar::SystemSecurityRules::is_quota_exempt(entity_type) {
                    *counts_by_domain.entry(entity_type.clone()).or_insert(0) += 1;
                }
            }
            if !counts_by_domain.is_empty() {
                let resolver = QuotaResolver::new(self.oltp_executor.clone());
                let ledger = self.eav_writer.quota_ledger();
                for (domain, units) in counts_by_domain {
                    let active_quota = resolver
                        .active_quota(&req.tenant_id, &domain, "WRITE_COUNT")
                        .await
                        .map_err(Status::from)?;
                    let Some(quota) = active_quota else {
                        for (cid, amt) in &debited_quotas {
                            let _ = ledger.release(&req.tenant_id, cid, *amt).await;
                        }
                        return Err(Status::from(DomainError::new(
                            ErrorCode::Quota001,
                            format!(
                                "No quota configured for tenant={} resource_domain={} limit_type=WRITE_COUNT",
                                req.tenant_id, domain
                            ),
                        )));
                    };
                    match ledger
                        .try_debit(
                            &req.tenant_id,
                            &quota.counter_id,
                            units,
                            quota.max_limit,
                            quota.seed_usage,
                        )
                        .await
                    {
                        Ok(DebitOutcome::Debited { .. }) => {
                            debited_quotas.push((quota.counter_id, units));
                        }
                        Ok(DebitOutcome::Exhausted { .. }) => {
                            for (cid, amt) in &debited_quotas {
                                let _ = ledger.release(&req.tenant_id, cid, *amt).await;
                            }
                            return Err(Status::from(DomainError::new(
                                ErrorCode::Quota001,
                                "Quota exhausted — upgrade plan or wait for reset",
                            )));
                        }
                        Err(e) => {
                            for (cid, amt) in &debited_quotas {
                                let _ = ledger.release(&req.tenant_id, cid, *amt).await;
                            }
                            return Err(Status::from(e));
                        }
                    }
                }
            }
        }

        // Madre = entities[0]; hijos = proyecciones de la MISMA transacción.
        // (R6 del Patrón Result: el vacío ya se rechazó arriba con Status;
        // aquí no hay pánico — se degrada a error de dominio por si el
        // contrato cambia mañana.)
        let mut iter = payloads.into_iter();
        let madre = iter.next().ok_or_else(|| {
            Status::invalid_argument("CompositeTransact exige al menos una entidad")
        })?;
        let proyecciones: Vec<SagaProjection> = iter
            .map(|p| {
                warn!(
                    entity = %p.entity_type,
                    "[Composite] hijo atómico de {}: {}", madre.entity_type, p.entity_type
                );
                SagaProjection {
                    entity_id: p.entity_id.unwrap_or_default(),
                    entity_type: p.entity_type,
                    attrs: p.attrs,
                }
            })
            .collect();

        let result = match self
            .eav_writer
            .transact_with_projections(madre, proyecciones, Some(&principal.user_id))
            .await
        {
            Ok(res) => res,
            Err(e) => {
                if !debited_quotas.is_empty() {
                    use crate::quota::QuotaCounter;
                    let ledger = self.eav_writer.quota_ledger();
                    for (cid, amt) in &debited_quotas {
                        let _ = ledger.release(&req.tenant_id, cid, *amt).await;
                    }
                }
                warn!(error = ?e, "[Composite] abortado: 0 filas escritas");
                return Err(Status::from(e));
            }
        };

        // Invalidación de caches de aplicación, una vez por entidad escrita.
        for (entity_type, entity_id) in &identidad {
            self.evict_caches_after_mutation(
                req.tenant_id.clone(),
                entity_type.clone(),
                entity_id.clone(),
                &serde_json::Value::Null,
            );
        }

        let results = identidad
            .iter()
            .map(|(entity_type, entity_id)| EntityWriteResult {
                entity_type: entity_type.clone(),
                entity_id: entity_id.clone(),
                tx_id: result.tx_id,
            })
            .collect();

        Ok(Response::new(CompositeTransactResponse {
            status: Some(crate::grpc::pb::Status {
                success: true,
                error_code: String::new(),
                error_message: String::new(),
                error_context: None,
            }),
            results,
            datoms: result.datoms as u64,
            outbox_count: result.outbox_count as u64,
        }))
    }
}
