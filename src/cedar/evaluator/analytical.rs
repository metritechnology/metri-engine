//! Analytical path — grants as per-domain boundaries.
//!
//! Camino analítico (PDP).
//! Consultas: sin motor Cedar — los grants del principal se traducen a
//! boundaries por dominio (`query_scope` + perímetros), que los handlers
//! aplican como FLS. Auto-autorización acotada a dominios de sistema para
//! dejar pasar el intercept: el chequeo de servicio (SystemSecurityRules)
//! impone el PermissionDenied con el mensaje canónico.

use crate::cedar::evaluator::grants::grant_allows_action;
use crate::cedar::rules::is_master_tenant;
use crate::cedar::types::PrincipalData;
use crate::domain::errors::{DomainError, ErrorCode};

pub(crate) fn step4_analytical(
    principal: &PrincipalData,
    action: &str,
    resource: &serde_json::Value,
) -> Result<serde_json::Value, DomainError> {
    let mut domain_dict = serde_json::Map::new();

    if let Some(domains) = resource.get("domains").and_then(|v| v.as_array()) {
        for d_val in domains {
            let domain = d_val.as_str().unwrap_or("");
            let mut boundaries_json = Vec::new();

            if (domain == "tenant" || domain == "domain_quota" || domain == "quota")
                && !is_master_tenant(&principal.tenant_id)
            {
                boundaries_json.push(serde_json::json!({
                    "query_scope": "ALL",
                    "permitted_locations": Vec::<String>::new(),
                    "permitted_assets": Vec::<String>::new(),
                }));
            } else {
                for boundary in &principal.roles_boundaries {
                    for grant in &boundary.grants {
                        let grant_domain = grant.get("domain").and_then(|v| v.as_str());
                        if (grant_domain == Some(domain) || grant_domain == Some("*"))
                            && grant_allows_action(grant, action)
                        {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cedar::types::RoleBoundary;
    use std::collections::HashSet;

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

    #[test]
    fn char_analytical_no_matching_grant_is_auth403() {
        let principal = principal_with_grants(vec![serde_json::json!({
            "domain": "project", "actions": ["VIEW"], "scope": "OWN"
        })]);
        let res = step4_analytical(&principal, "DELETE", &resource(&["project"]));
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

        let res = step4_analytical(&principal, "VIEW", &resource(&["project"]));
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
        let res = step4_analytical(&principal, "VIEW", &resource(&["webhook_endpoint"]));
        assert!(res.is_ok());
        let dict = res.unwrap();
        assert_eq!(dict["webhook_endpoint"][0]["query_scope"], "ALL");
    }

    #[test]
    fn char_analytical_resource_without_domains_is_empty_ok() {
        let principal = principal_with_grants(vec![]);
        let res = step4_analytical(&principal, "VIEW", &serde_json::json!({}));
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), serde_json::json!({}));
    }
}
