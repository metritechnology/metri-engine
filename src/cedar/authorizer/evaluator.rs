// authorizer/evaluator.rs — evaluación Cedar (fase 2).
// step4: decisiones para camino analítico y mutacional, esquema Cedar y
// recolección de grants del principal.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use cedar_policy::{Context, Entities, EntityUid, PolicySet, Request, Schema};

use crate::cedar::authorizer::{CedarAuthorizer, PrincipalData};
use crate::domain::errors::{DomainError, ErrorCode};

pub fn step4_evaluate_cedar(
    cedar_engine: &CedarAuthorizer,
    policy_cache: &HashMap<String, PolicySet>,
    principal: &PrincipalData,
    action: &str,
    resource: &serde_json::Value,
    body: &serde_json::Value,
) -> Result<serde_json::Value, DomainError> {
    if principal.user_id == "usr_system_bff" {
        if is_mutational_action(action) {
            return Ok(serde_json::json!({}));
        } else {
            let mut domain_dict = serde_json::Map::new();
            if let Some(domains) = resource.get("domains").and_then(|v| v.as_array()) {
                for d_val in domains {
                    let domain = d_val.as_str().unwrap_or("");
                    domain_dict.insert(
                        domain.to_string(),
                        serde_json::json!([{
                            "query_scope": "ALL",
                            "permitted_locations": Vec::<String>::new(),
                            "permitted_assets": Vec::<String>::new(),
                        }]),
                    );
                }
            }
            return Ok(serde_json::Value::Object(domain_dict));
        }
    }

    if is_mutational_action(action) {
        step4_mutational(
            cedar_engine,
            policy_cache,
            principal,
            action,
            resource,
            body,
        )
    } else {
        step4_analytical(cedar_engine, principal, action, resource)
    }
}

static CEDAR_SCHEMA: std::sync::OnceLock<Schema> = std::sync::OnceLock::new();

fn get_cedar_schema() -> &'static Schema {
    CEDAR_SCHEMA.get_or_init(|| {
        let schema_src = include_str!("../../../config/policies/cedar-schema.json");
        Schema::from_str(schema_src).expect("Failed to parse cedar-schema.json")
    })
}

fn collect_user_grants(principal: &PrincipalData) -> HashSet<String> {
    let mut grants_set = HashSet::new();

    let all_domains: Vec<String> = if let Some(registry) = crate::codice::registry::global_opt() {
        registry.entity_names().map(|s| s.to_string()).collect()
    } else {
        vec![
            "project".to_string(),
            "work_order".to_string(),
            "asset".to_string(),
            "location".to_string(),
            "user".to_string(),
            "user_group".to_string(),
            "role".to_string(),
            "tenant".to_string(),
            "company".to_string(),
            "api_key".to_string(),
            "audit_log".to_string(),
            "form_template".to_string(),
            "provider".to_string(),
            "webhook_endpoint".to_string(),
            "dashboardBI".to_string(),
            "domain_plugin".to_string(),
            "domain_quota".to_string(),
            "tenant_plugin".to_string(),
        ]
    };

    let all_actions: Vec<String> = vec!["VIEW", "CREATE", "UPDATE", "DELETE", "EXECUTE", "EXPORT"]
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    for boundary in &principal.roles_boundaries {
        for grant in &boundary.grants {
            if let Some(domain_val) = grant.get("domain").and_then(|v| v.as_str()) {
                let domains = if domain_val == "*" {
                    all_domains.clone()
                } else {
                    vec![domain_val.to_string()]
                };

                let actions =
                    if let Some(actions_arr) = grant.get("actions").and_then(|v| v.as_array()) {
                        let mut acts = Vec::new();
                        for act_val in actions_arr {
                            if let Some(act_str) = act_val.as_str() {
                                if act_str == "*" {
                                    acts = all_actions.clone();
                                    break;
                                } else {
                                    acts.push(act_str.to_string());
                                }
                            }
                        }
                        acts
                    } else if let Some(action_str) = grant.get("actions").and_then(|v| v.as_str()) {
                        if action_str == "*" {
                            all_actions.clone()
                        } else {
                            vec![action_str.to_string()]
                        }
                    } else {
                        vec![]
                    };

                for d in &domains {
                    for a in &actions {
                        grants_set.insert(format!("{}:{}", d, a));
                    }
                }
            }
        }
    }

    grants_set
}

pub(crate) fn step4_mutational(
    cedar_engine: &CedarAuthorizer,
    policy_cache: &HashMap<String, PolicySet>,
    principal: &PrincipalData,
    action: &str,
    resource: &serde_json::Value,
    _body: &serde_json::Value,
) -> Result<serde_json::Value, DomainError> {
    let mut allowed = false;

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

    let schema = get_cedar_schema();

    let user_grants = collect_user_grants(principal);
    let user_grants_json = serde_json::Value::Array(
        user_grants
            .into_iter()
            .map(serde_json::Value::String)
            .collect(),
    );

    let principal_roles_json = serde_json::Value::Array(
        principal
            .roles
            .iter()
            .map(|r| serde_json::Value::String(r.clone()))
            .collect(),
    );

    let entity_type = resource
        .get("entity_type")
        .and_then(|v| v.as_str())
        .unwrap_or("project");
    let required_grant = format!("{}:{}", entity_type, action);

    let mut resource_attrs = serde_json::json!({
        "tenant_id": principal.tenant_id,
        "entity_type": entity_type,
        "required_grant": required_grant,
    });

    if let Some(company_id) = resource.get("assigned_company_id").and_then(|v| v.as_str()) {
        resource_attrs.as_object_mut().unwrap().insert(
            "assigned_company_id".to_string(),
            serde_json::json!(company_id),
        );
    }

    let entities_json = serde_json::json!([
        {
            "uid": {
                "type": "Metri::User",
                "id": &principal.user_id
            },
            "attrs": {
                "tenant_id": &principal.tenant_id,
                "user_id": &principal.user_id,
                "roles": &principal_roles_json,
                "user_type": &principal.user_type,
                "company_id": &principal.company_id,
                "grants": &user_grants_json,
                "query_scope": "ALL",
                "status": &principal.status
            },
            "parents": []
        },
        {
            "uid": {
                "type": "Metri::MetriResource",
                "id": &resource_id
            },
            "attrs": resource_attrs,
            "parents": []
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "CREATE"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"mutational\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "UPDATE"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"mutational\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "DELETE"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"mutational\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "UPSERT"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"mutational\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "GET"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"mutational\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "QueryKpi"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"analytical\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "QueryTimeseries"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"analytical\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "QueryTable"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"analytical\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "QueryPie"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"analytical\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "QueryBubble"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"analytical\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "QueryCsvExport"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"analytical\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "QueryMetrics"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"analytical\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "DiscoverSchema"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"analytical\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "ExploreSchema"
            },
            "attrs": {},
            "parents": [
                {
                    "type": "Metri::Action",
                    "id": "ActionGroup::\"analytical\""
                }
            ]
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "ActionGroup::\"mutational\""
            },
            "attrs": {},
            "parents": []
        },
        {
            "uid": {
                "type": "Metri::Action",
                "id": "ActionGroup::\"analytical\""
            },
            "attrs": {},
            "parents": []
        }
    ]);

    let entities =
        Entities::from_json_str(&entities_json.to_string(), Some(schema)).map_err(|e| {
            DomainError::new(
                ErrorCode::Auth403,
                format!("Failed to create Cedar entities: {e}"),
            )
            .with_stage("cedar")
        })?;

    if principal.roles_boundaries.is_empty() {
        if let Some(policy_set) = policy_cache
            .get("admin")
            .or_else(|| policy_cache.values().next())
        {
            let request = Request::new(
                Some(principal_uid.clone()),
                Some(action_uid.clone()),
                Some(resource_uid.clone()),
                Context::empty(),
                Some(schema),
            )
            .map_err(|e| DomainError::new(ErrorCode::Auth403, e.to_string()).with_stage("cedar"))?;

            let is_ok = cedar_engine.is_authorized(policy_set, &entities, &request)?;
            if is_ok {
                allowed = true;
            }
        }
    } else {
        for boundary in &principal.roles_boundaries {
            let policy_set = policy_cache
                .get(&boundary.role_id)
                .or_else(|| policy_cache.get("admin"))
                .or_else(|| policy_cache.get("tenant-admin"))
                .or_else(|| policy_cache.values().next())
                .ok_or_else(|| {
                    DomainError::new(ErrorCode::Auth403, "PolicySet not compiled for role")
                        .with_stage("cedar")
                })?;

            let request = Request::new(
                Some(principal_uid.clone()),
                Some(action_uid.clone()),
                Some(resource_uid.clone()),
                Context::empty(),
                Some(schema),
            )
            .map_err(|e| DomainError::new(ErrorCode::Auth403, e.to_string()).with_stage("cedar"))?;

            let is_ok = cedar_engine.is_authorized(policy_set, &entities, &request)?;

            if is_ok {
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

fn grant_allows_action(grant: &serde_json::Value, action: &str) -> bool {
    let check = |act: &str| {
        act == "*"
            || act == action
            || (action == "BulkIngestData" && (act == "CREATE" || act == "UPDATE"))
    };
    if let Some(actions_arr) = grant.get("actions").and_then(|v| v.as_array()) {
        actions_arr.iter().any(|act_val| {
            if let Some(act_str) = act_val.as_str() {
                check(act_str)
            } else {
                false
            }
        })
    } else if let Some(action_str) = grant.get("actions").and_then(|v| v.as_str()) {
        check(action_str)
    } else {
        false
    }
}

pub(crate) fn step4_analytical(
    _cedar_engine: &CedarAuthorizer,
    principal: &PrincipalData,
    action: &str,
    resource: &serde_json::Value,
) -> Result<serde_json::Value, DomainError> {
    let mut domain_dict = serde_json::Map::new();

    if let Some(domains) = resource.get("domains").and_then(|v| v.as_array()) {
        for d_val in domains {
            let domain = d_val.as_str().unwrap_or("");
            let mut boundaries_json = Vec::new();

            let master_tenant_id =
                std::env::var("METRI_MASTER_TENANT_ID").unwrap_or_else(|_| "system".to_string());
            if (domain == "tenant" || domain == "domain_quota" || domain == "quota")
                && principal.tenant_id != master_tenant_id
            {
                // Auto-authorize to pass intercept phase, service-level check (SystemSecurityRules)
                // will enforce PermissionDenied with the correct system message.
                boundaries_json.push(serde_json::json!({
                    "query_scope": "ALL",
                    "permitted_locations": Vec::<String>::new(),
                    "permitted_assets": Vec::<String>::new(),
                }));
            } else {
                for boundary in &principal.roles_boundaries {
                    for grant in &boundary.grants {
                        let grant_domain = grant.get("domain").and_then(|v| v.as_str());
                        if grant_domain == Some(domain) || grant_domain == Some("*") {
                            if grant_allows_action(grant, action) {
                                let scope = grant
                                    .get("scope")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("NONE");
                                boundaries_json.push(serde_json::json!({
                                    "query_scope": scope,
                                    "permitted_locations": boundary.permitted_locations,
                                    "permitted_assets": boundary.permitted_assets,
                                }));
                            }
                        }
                    }
                }
            }

            if boundaries_json.is_empty() {
                return Err(DomainError::new(
                    ErrorCode::Auth403,
                    format!(
                        "Cedar DENY: No role authorized for action {action} on domain {domain}"
                    ),
                )
                .with_stage("cedar"));
            }

            domain_dict.insert(
                domain.to_string(),
                serde_json::Value::Array(boundaries_json),
            );
        }
    }

    Ok(serde_json::Value::Object(domain_dict))
}

fn is_mutational_action(action: &str) -> bool {
    matches!(action, "CREATE" | "UPDATE" | "DELETE" | "UPSERT")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cedar::authorizer::RoleBoundary;

    fn principal_with_grants(grants: Vec<serde_json::Value>) -> PrincipalData {
        PrincipalData {
            user_id: "usr_test".to_string(),
            tenant_id: "tnt_01".to_string(),
            status: "ACTIVE".to_string(),
            user_type: "INTERNAL".to_string(),
            company_id: String::new(),
            roles: ["role_test".to_string()].into_iter().collect(),
            roles_boundaries: vec![RoleBoundary {
                role_id: "role_test".to_string(),
                grants,
                permitted_locations: vec![],
                permitted_assets: vec![],
            }],
            time_restrictions: vec![],
            group_allowed_locations: vec![],
            group_allowed_assets: vec![],
            groups: HashSet::new(),
        }
    }

    fn resource(domains: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "entity_type": "project",
            "entity_id": "res_1",
            "domains": domains
        })
    }

    // ── Caracterización: collect_user_grants ────────────────────────────────

    #[test]
    fn char_grants_wildcard_domain_expands_to_registry_domains() {
        let principal = principal_with_grants(vec![serde_json::json!({
            "domain": "*", "actions": ["VIEW"], "scope": "ALL"
        })]);
        let grants = collect_user_grants(&principal);
        assert!(grants.contains("project:VIEW"));
        assert!(grants.contains("work_order:VIEW"));
        assert!(grants.contains("tenant:VIEW"));
        assert!(!grants.contains("project:DELETE"));
    }

    #[test]
    fn char_grants_wildcard_action_expands_to_all_actions() {
        let principal = principal_with_grants(vec![serde_json::json!({
            "domain": "project", "actions": ["*"], "scope": "ALL"
        })]);
        let grants = collect_user_grants(&principal);
        for action in ["VIEW", "CREATE", "UPDATE", "DELETE", "EXECUTE", "EXPORT"] {
            assert!(grants.contains(&format!("project:{action}")), "falta project:{action}");
        }
    }

    #[test]
    fn char_grants_action_as_string_is_single_grant() {
        let principal = principal_with_grants(vec![serde_json::json!({
            "domain": "asset", "actions": "VIEW", "scope": "ALL"
        })]);
        let grants = collect_user_grants(&principal);
        assert!(grants.contains("asset:VIEW"));
        assert_eq!(grants.len(), 1);
    }

    #[test]
    fn char_grants_multiple_boundaries_accumulate() {
        let mut principal = principal_with_grants(vec![serde_json::json!({
            "domain": "project", "actions": ["VIEW"]
        })]);
        principal.roles_boundaries.push(RoleBoundary {
            role_id: "role_other".to_string(),
            grants: vec![serde_json::json!({"domain": "asset", "actions": ["VIEW"]})],
            permitted_locations: vec![],
            permitted_assets: vec![],
        });
        let grants = collect_user_grants(&principal);
        assert!(grants.contains("project:VIEW"));
        assert!(grants.contains("asset:VIEW"));
    }

    // ── Caracterización: grant_allows_action ────────────────────────────────

    #[test]
    fn char_grant_allows_exact_and_wildcard() {
        let exact = serde_json::json!({"actions": ["VIEW"]});
        let wildcard = serde_json::json!({"actions": ["*"]});
        let string_form = serde_json::json!({"actions": "VIEW"});
        assert!(grant_allows_action(&exact, "VIEW"));
        assert!(!grant_allows_action(&exact, "DELETE"));
        assert!(grant_allows_action(&wildcard, "DELETE"));
        assert!(grant_allows_action(&string_form, "VIEW"));
        assert!(!grant_allows_action(&string_form, "EXPORT"));
    }

    #[test]
    fn char_grant_bulk_ingest_maps_to_create_and_update() {
        let create = serde_json::json!({"actions": ["CREATE"]});
        let update = serde_json::json!({"actions": ["UPDATE"]});
        let view = serde_json::json!({"actions": ["VIEW"]});
        assert!(grant_allows_action(&create, "BulkIngestData"));
        assert!(grant_allows_action(&update, "BulkIngestData"));
        assert!(!grant_allows_action(&view, "BulkIngestData"));
    }

    // ── Caracterización: enrutamiento de step4_evaluate_cedar ───────────────

    #[test]
    fn char_system_bff_mutational_returns_empty_dict() {
        let mut principal = principal_with_grants(vec![]);
        principal.user_id = "usr_system_bff".to_string();
        let engine = crate::cedar::authorizer::CedarAuthorizer::new();
        let policy_cache = HashMap::new();
        let res = step4_evaluate_cedar(
            &engine,
            &policy_cache,
            &principal,
            "CREATE",
            &resource(&["project"]),
            &serde_json::json!({}),
        );
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), serde_json::json!({}));
    }

    #[test]
    fn char_system_bff_analytical_returns_all_scope_per_domain() {
        let mut principal = principal_with_grants(vec![]);
        principal.user_id = "usr_system_bff".to_string();
        let engine = crate::cedar::authorizer::CedarAuthorizer::new();
        let policy_cache = HashMap::new();
        let res = step4_evaluate_cedar(
            &engine,
            &policy_cache,
            &principal,
            "VIEW",
            &resource(&["project", "asset"]),
            &serde_json::json!({}),
        );
        assert!(res.is_ok());
        let dict = res.unwrap();
        for domain in ["project", "asset"] {
            let boundaries = dict[domain].as_array().unwrap();
            assert_eq!(boundaries[0]["query_scope"], "ALL");
        }
    }

    #[test]
    fn char_action_routing_mutational_vs_analytical() {
        // UPSERT es mutacional; QueryMetrics y DiscoverSchema no lo son.
        assert!(is_mutational_action("UPSERT"));
        assert!(is_mutational_action("DELETE"));
        assert!(!is_mutational_action("QueryMetrics"));
        assert!(!is_mutational_action("DiscoverSchema"));
        assert!(!is_mutational_action("BulkIngestData"));
    }

    // ── Caracterización: bordes de step4_analytical ─────────────────────────

    #[test]
    fn char_analytical_no_matching_grant_is_auth403() {
        let principal = principal_with_grants(vec![serde_json::json!({
            "domain": "project", "actions": ["VIEW"], "scope": "OWN"
        })]);
        let engine = crate::cedar::authorizer::CedarAuthorizer::new();
        let res = step4_analytical(&engine, &principal, "DELETE", &resource(&["project"]));
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code, ErrorCode::Auth403);
    }

    #[test]
    fn char_analytical_scope_and_boundaries_flow_to_output() {
        let mut principal = principal_with_grants(vec![serde_json::json!({
            "domain": "project", "actions": ["VIEW"], "scope": "OWN"
        })]);
        principal.roles_boundaries[0].permitted_locations = vec!["loc_1".to_string()];
        principal.roles_boundaries[0].permitted_assets = vec!["asset_1".to_string()];

        let engine = crate::cedar::authorizer::CedarAuthorizer::new();
        let res = step4_analytical(&engine, &principal, "VIEW", &resource(&["project"]));
        assert!(res.is_ok());
        let dict = res.unwrap();
        let boundaries = dict["project"].as_array().unwrap();
        assert_eq!(boundaries[0]["query_scope"], "OWN");
        assert_eq!(boundaries[0]["permitted_locations"][0], "loc_1");
        assert_eq!(boundaries[0]["permitted_assets"][0], "asset_1");
    }

    #[test]
    fn char_analytical_wildcard_domain_grant_covers_any_domain() {
        let principal = principal_with_grants(vec![serde_json::json!({
            "domain": "*", "actions": ["VIEW"], "scope": "ALL"
        })]);
        let engine = crate::cedar::authorizer::CedarAuthorizer::new();
        let res = step4_analytical(&engine, &principal, "VIEW", &resource(&["webhook_endpoint"]));
        assert!(res.is_ok());
        let dict = res.unwrap();
        assert_eq!(dict["webhook_endpoint"][0]["query_scope"], "ALL");
    }

    #[test]
    fn char_analytical_resource_without_domains_is_empty_ok() {
        let principal = principal_with_grants(vec![]);
        let engine = crate::cedar::authorizer::CedarAuthorizer::new();
        let res = step4_analytical(&engine, &principal, "VIEW", &serde_json::json!({}));
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), serde_json::json!({}));
    }
}
