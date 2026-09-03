// authorizer/evaluator.rs — PDP Cedar: enrutamiento de decisiones.
//
// La entrada única es `step4_evaluate_cedar`: atajo system-bff y, según la
// acción, camino mutacional (mutational.rs — motor Cedar real) o analítico
// (analytical.rs — boundaries por dominio). Los grants viven en grants.rs, y
// el registro de acciones y la base de entidades precompilada en
// action_registry.rs / entities_builder.rs.

mod action_registry;
mod analytical;
mod entities_builder;
mod grants;
mod mutational;

pub(crate) use analytical::step4_analytical;
pub(crate) use mutational::step4_mutational;

use crate::cedar::authorizer::{CedarAuthorizer, PolicyStore, PrincipalData};
use crate::domain::errors::DomainError;

pub fn step4_evaluate_cedar(
    cedar_engine: &CedarAuthorizer,
    policy_cache: &dyn PolicyStore,
    principal: &PrincipalData,
    action: &str,
    resource: &serde_json::Value,
) -> Result<serde_json::Value, DomainError> {
    use action_registry::is_mutational_action;

    // La cuenta BFF de sistema decide sin políticas: mutaciones allow directo,
    // consultas reciben ALL-scope sobre los dominios pedidos.
    if principal.user_id == "usr_system_bff" {
        if is_mutational_action(action) {
            return Ok(serde_json::json!({}));
        }
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

    if is_mutational_action(action) {
        step4_mutational(cedar_engine, policy_cache, principal, action, resource)
    } else {
        step4_analytical(cedar_engine, principal, action, resource)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cedar::authorizer::RoleBoundary;
    use crate::domain::errors::ErrorCode;
    use std::collections::{HashMap, HashSet};

    fn system_bff() -> PrincipalData {
        PrincipalData {
            user_id: "usr_system_bff".to_string(),
            tenant_id: "system".to_string(),
            status: "ACTIVE".to_string(),
            user_type: "SYSTEM".to_string(),
            company_id: String::new(),
            roles: ["system-bff".to_string()].into_iter().collect(),
            roles_boundaries: vec![],
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
    fn char_system_bff_mutational_returns_empty_dict() {
        let principal = system_bff();
        let engine = CedarAuthorizer::new();
        let policy_cache = HashMap::new();
        let res = step4_evaluate_cedar(
            &engine,
            &policy_cache,
            &principal,
            "CREATE",
            &resource(&["project"]),
        );
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), serde_json::json!({}));
    }

    #[test]
    fn char_system_bff_analytical_returns_all_scope_per_domain() {
        let principal = system_bff();
        let engine = CedarAuthorizer::new();
        let policy_cache = HashMap::new();
        let res = step4_evaluate_cedar(
            &engine,
            &policy_cache,
            &principal,
            "VIEW",
            &resource(&["project", "asset"]),
        );
        assert!(res.is_ok());
        let dict = res.unwrap();
        for domain in ["project", "asset"] {
            let boundaries = dict[domain].as_array().unwrap();
            assert_eq!(boundaries[0]["query_scope"], "ALL");
        }
    }

    #[test]
    fn char_un_principal_normal_enruta_por_tipo_de_accion() {
        // Un principal sin grants: el camino analítico responde Auth403.
        let principal = PrincipalData {
            user_id: "usr_normal".to_string(),
            tenant_id: "tnt_01".to_string(),
            status: "ACTIVE".to_string(),
            user_type: "INTERNAL".to_string(),
            company_id: String::new(),
            roles: ["role_x".to_string()].into_iter().collect(),
            roles_boundaries: vec![RoleBoundary {
                role_id: "role_x".to_string(),
                grants: vec![],
                permitted_locations: vec![],
                permitted_assets: vec![],
            }],
            time_restrictions: vec![],
            group_allowed_locations: vec![],
            group_allowed_assets: vec![],
            groups: HashSet::new(),
        };
        let engine = CedarAuthorizer::new();
        let policy_cache = HashMap::new();

        let res = step4_evaluate_cedar(
            &engine,
            &policy_cache,
            &principal,
            "VIEW",
            &resource(&["project"]),
        );
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code, ErrorCode::Auth403);
    }
}
