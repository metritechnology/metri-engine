// authorizer/evaluator/entities_builder.rs — Ensamblado de entidades Cedar.
//
// La base de acciones es una sola instancia precompilada (el loader de Cedar
// añade todas las acciones del schema — el JSON de acciones que el camino
// mutacional reconstruía por petición era 100 % redundante). Por petición se
// clona la base y se le añaden exactamente dos entidades tipadas: el User con
// sus grants y el Resource con el grant requerido.

use std::collections::{HashMap, HashSet};

use cedar_policy::{Entity, Entities, EntityUid, RestrictedExpression};

use crate::cedar::authorizer::PrincipalData;
use crate::cedar::authorizer::evaluator::action_registry::{base_action_entities, cedar_schema};
use crate::domain::errors::{DomainError, ErrorCode};

/// Atributos del Resource para el camino mutacional: el grant que la política
/// exige y, si existe, la empresa asignada (hydration ABAC del intercept).
pub(crate) fn resource_attrs(
    principal: &PrincipalData,
    entity_type: &str,
    action: &str,
    resource: &serde_json::Value,
) -> serde_json::Value {
    let required_grant = format!("{}:{}", entity_type, action);

    let mut attrs = serde_json::json!({
        "tenant_id": principal.tenant_id,
        "entity_type": entity_type,
        "required_grant": required_grant,
    });

    if let Some(company_id) = resource.get("assigned_company_id").and_then(|v| v.as_str()) {
        attrs
            .as_object_mut()
            .unwrap()
            .insert("assigned_company_id".to_string(), serde_json::json!(company_id));
    }

    attrs
}

/// Entidades de una evaluación mutacional: base de acciones + User + Resource.
pub(crate) fn mutational_entities(
    principal: &PrincipalData,
    user_grants: &[String],
    resource_id: &str,
    resource_attrs: serde_json::Value,
) -> Result<Entities, DomainError> {
    let user = user_entity(principal, user_grants)?;
    let resource = resource_entity(resource_id, resource_attrs)?;

    base_action_entities()
        .clone()
        .add_entities([user, resource], Some(cedar_schema()))
        .map_err(|e| {
            DomainError::new(
                ErrorCode::Auth403,
                format!("Failed to create Cedar entities: {e}"),
            )
            .with_stage("cedar")
        })
}

/// `Entity::new` evalúa los atributos restringidos — un fallo aquí es un
/// error de autorización, no de infraestructura.
fn new_entity(
    uid: EntityUid,
    attrs: HashMap<String, RestrictedExpression>,
) -> Result<Entity, DomainError> {
    Entity::new(uid, attrs, HashSet::new()).map_err(|e| {
        DomainError::new(
            ErrorCode::Auth403,
            format!("Failed to build Cedar entity: {e}"),
        )
        .with_stage("cedar")
    })
}

fn str_attr(value: &str) -> RestrictedExpression {
    RestrictedExpression::new_string(value.to_string())
}

fn set_attr<'a, I: IntoIterator<Item = &'a String>>(values: I) -> RestrictedExpression {
    RestrictedExpression::new_set(
        values
            .into_iter()
            .map(|v| RestrictedExpression::new_string(v.clone()))
            .collect::<Vec<_>>(),
    )
}

fn user_entity(principal: &PrincipalData, grants: &[String]) -> Result<Entity, DomainError> {
    let uid = format!("Metri::User::\"{}\"", principal.user_id)
        .parse::<EntityUid>()
        .map_err(|e| {
            DomainError::new(ErrorCode::Auth401, format!("Invalid principal UID: {e}"))
                .with_stage("cedar")
        })?;

    let mut attrs = HashMap::new();
    attrs.insert("tenant_id".to_string(), str_attr(&principal.tenant_id));
    attrs.insert("user_id".to_string(), str_attr(&principal.user_id));
    attrs.insert("roles".to_string(), set_attr(&principal.roles));
    attrs.insert("user_type".to_string(), str_attr(&principal.user_type));
    attrs.insert("company_id".to_string(), str_attr(&principal.company_id));
    attrs.insert("grants".to_string(), set_attr(grants));
    attrs.insert("query_scope".to_string(), str_attr("ALL"));
    attrs.insert("status".to_string(), str_attr(&principal.status));

    new_entity(uid, attrs)
}

fn resource_entity(
    resource_id: &str,
    attrs_json: serde_json::Value,
) -> Result<Entity, DomainError> {
    let uid = format!("Metri::MetriResource::\"{}\"", resource_id)
        .parse::<EntityUid>()
        .map_err(|e| {
            DomainError::new(ErrorCode::Auth401, format!("Invalid resource UID: {e}"))
                .with_stage("cedar")
        })?;

    let mut attrs = HashMap::new();
    if let Some(obj) = attrs_json.as_object() {
        for (key, value) in obj {
            if let Some(s) = value.as_str() {
                attrs.insert(key.clone(), str_attr(s));
            }
        }
    }

    new_entity(uid, attrs)
}
