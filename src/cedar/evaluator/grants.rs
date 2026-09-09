//! Principal grants expansion — the plain domain:action set.
//!
//! Expansión de grants del principal.
//! Funciones puras: el JSON de grants de los boundaries se convierte en el
//! conjunto plano `dominio:acción` que consume la evaluación Cedar.

use std::collections::HashSet;

use crate::cedar::types::PrincipalData;

/// Acciones de negocio posibles cuando un grant declara wildcard.
pub const ALL_ACTIONS: &[&str] = &["VIEW", "CREATE", "UPDATE", "DELETE", "EXECUTE", "EXPORT"];

/// Fallback cuando el Códice no tiene registro compilado (tests, arranque
/// temprano). En producción la lista de dominios la provee el registry.
pub const FALLBACK_DOMAINS: &[&str] = &[
    "project",
    "asset",
    "location",
    "user",
    "user_group",
    "role",
    "tenant",
    "company",
    "api_key",
    "audit_log",
    "form_template",
    "webhook_endpoint",
    "dashboardBI",
    "domain_plugin",
    "domain_quota",
    "tenant_plugin",
];

/// Dominios de negocio conocidos: del Códice si existe, del fallback si no.
pub fn known_domains() -> Vec<String> {
    match crate::codice::registry::global_opt() {
        Some(registry) => registry.entity_names().map(|s| s.to_string()).collect(),
        None => FALLBACK_DOMAINS.iter().map(|s| s.to_string()).collect(),
    }
}

/// Expande los grants de todos los boundaries al conjunto `dominio:acción`.
/// `all_domains` se resuelve UNA vez por evaluación (antes se clonaba por
/// grant con wildcard).
pub fn collect_user_grants(principal: &PrincipalData, all_domains: &[String]) -> HashSet<String> {
    let mut grants_set = HashSet::new();

    for boundary in &principal.roles_boundaries {
        for grant in &boundary.grants {
            let Some(domain_val) = grant.get("domain").and_then(|v| v.as_str()) else {
                continue;
            };

            let domains: Vec<String> = if domain_val == "*" {
                all_domains.to_vec()
            } else {
                vec![domain_val.to_string()]
            };

            let actions: Vec<String> = match grant.get("actions") {
                Some(serde_json::Value::Array(actions_arr)) => {
                    let mut acts = Vec::new();
                    for act_val in actions_arr {
                        if let Some(act_str) = act_val.as_str() {
                            if act_str == "*" {
                                acts = ALL_ACTIONS.iter().map(|s| s.to_string()).collect();
                                break;
                            }
                            acts.push(act_str.to_string());
                        }
                    }
                    acts
                }
                Some(serde_json::Value::String(action_str)) => {
                    if action_str == "*" {
                        ALL_ACTIONS.iter().map(|s| s.to_string()).collect()
                    } else {
                        vec![action_str.clone()]
                    }
                }
                _ => vec![],
            };

            for d in &domains {
                for a in &actions {
                    grants_set.insert(format!("{}:{}", d, a));
                }
            }
        }
    }

    grants_set
}

/// ¿Un grant individual habilita la acción pedida? `BulkIngestData` cuenta
/// como CREATE o UPDATE (legacy: el bulk muta en dos fases).
pub fn grant_allows_action(grant: &serde_json::Value, action: &str) -> bool {
    let check = |act: &str| {
        act == "*"
            || act == action
            || (action == "BulkIngestData" && (act == "CREATE" || act == "UPDATE"))
    };
    match grant.get("actions") {
        Some(serde_json::Value::Array(actions_arr)) => actions_arr
            .iter()
            .filter_map(|act_val| act_val.as_str())
            .any(check),
        Some(serde_json::Value::String(action_str)) => check(action_str),
        _ => false,
    }
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

    #[test]
    fn wildcard_de_dominio_expande_todos_los_dominios_inyectados() {
        let principal = principal_with_grants(vec![
            serde_json::json!({"domain": "*", "actions": ["VIEW"]}),
        ]);
        let domains = vec!["a".to_string(), "b".to_string()];
        let grants = collect_user_grants(&principal, &domains);
        assert_eq!(
            grants,
            ["a:VIEW", "b:VIEW"].into_iter().map(String::from).collect()
        );
    }

    #[test]
    fn grant_sin_domain_se_ignora_sin_panico() {
        let principal = principal_with_grants(vec![serde_json::json!({"actions": ["VIEW"]})]);
        assert!(collect_user_grants(&principal, &[]).is_empty());
    }
}
