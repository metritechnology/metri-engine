// authorizer/evaluator/mutational.rs — Camino mutacional (PDP).
// CREATE/UPDATE/DELETE/UPSERT: evaluación Cedar real por cada boundary del
// principal — basta con que UNA política permita.

use cedar_policy::{Context, EntityUid, Request};

use crate::cedar::engine::CedarAuthorizer;
use crate::cedar::evaluator::action_registry::cedar_schema;
use crate::cedar::evaluator::entities_builder::{mutational_entities, resource_attrs};
use crate::cedar::evaluator::grants::{collect_user_grants, known_domains};
use crate::cedar::ports::PolicyStore;
use crate::cedar::types::PrincipalData;
use crate::domain::errors::{DomainError, ErrorCode};

pub(crate) fn step4_mutational(
    cedar_engine: &CedarAuthorizer,
    policy_cache: &dyn PolicyStore,
    principal: &PrincipalData,
    action: &str,
    resource: &serde_json::Value,
) -> Result<serde_json::Value, DomainError> {
    let principal_uid = format!("Metri::User::\"{}\"", principal.user_id)
        .parse::<EntityUid>()
        .map_err(|e| {
            DomainError::new(ErrorCode::Auth401, format!("Invalid principal UID: {e}"))
                .with_stage("cedar")
        })?;
    let action_uid = format!("Metri::Action::\"{}\"", action)
        .parse::<EntityUid>()
        .map_err(|e| {
            DomainError::new(ErrorCode::Auth401, format!("Invalid action UID: {e}"))
                .with_stage("cedar")
        })?;

    let resource_id = resource
        .get("entity_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let resource_uid = format!("Metri::MetriResource::\"{}\"", resource_id)
        .parse::<EntityUid>()
        .map_err(|e| {
            DomainError::new(ErrorCode::Auth401, format!("Invalid resource UID: {e}"))
                .with_stage("cedar")
        })?;

    let schema = cedar_schema();
    let user_grants = collect_user_grants(principal, &known_domains());
    let entity_type = resource
        .get("entity_type")
        .and_then(|v| v.as_str())
        .unwrap_or("project");
    let entities = mutational_entities(
        principal,
        &user_grants.into_iter().collect::<Vec<_>>(),
        resource_id,
        resource_attrs(principal, entity_type, action, resource),
    )?;

    let mut allowed = false;

    if principal.roles_boundaries.is_empty() {
        if let Some(policy_set) = policy_cache
            .policy_for("admin")
            .or_else(|| policy_cache.any_policy())
        {
            let request = new_request(&principal_uid, &action_uid, &resource_uid, schema)?;
            if cedar_engine.is_authorized(policy_set, &entities, &request)? {
                allowed = true;
            }
        }
    } else {
        for boundary in &principal.roles_boundaries {
            let policy_set = policy_cache
                .policy_for(&boundary.role_id)
                .or_else(|| policy_cache.policy_for("admin"))
                .or_else(|| policy_cache.policy_for("tenant-admin"))
                .or_else(|| policy_cache.any_policy())
                .ok_or_else(|| {
                    DomainError::new(ErrorCode::Auth403, "PolicySet not compiled for role")
                        .with_stage("cedar")
                })?;

            let request = new_request(&principal_uid, &action_uid, &resource_uid, schema)?;
            if cedar_engine.is_authorized(policy_set, &entities, &request)? {
                allowed = true;
                break;
            }
        }
    }

    if !allowed {
        return Err(DomainError::new(
            ErrorCode::Auth403,
            format!("Cedar DENY (Mutational ABAC Failed for action={action})"),
        )
        .with_stage("cedar"));
    }

    Ok(serde_json::json!({}))
}

fn new_request(
    principal_uid: &EntityUid,
    action_uid: &EntityUid,
    resource_uid: &EntityUid,
    schema: &cedar_policy::Schema,
) -> Result<Request, DomainError> {
    Request::new(
        Some(principal_uid.clone()),
        Some(action_uid.clone()),
        Some(resource_uid.clone()),
        Context::empty(),
        Some(schema),
    )
    .map_err(|e| DomainError::new(ErrorCode::Auth403, e.to_string()).with_stage("cedar"))
}
