//! Tests for `cedar::rules.rs`.
// cedar/tests/rules.rs — Reglas de sistema, ventana horaria y dominios maestros.

use crate::cedar::evaluator::step4_analytical;
use crate::cedar::rules::{step3b_validate_time_window, SystemSecurityRules};
use crate::cedar::types::{PrincipalData, RoleBoundary, TimeRestriction};
use crate::domain::errors::ErrorCode;
use chrono::{TimeZone, Utc};
use std::collections::HashSet;

#[test]
fn test_time_window_validation() {
    let principal = PrincipalData {
        user_id: "usr_test".to_string(),
        tenant_id: "tnt_01".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: HashSet::new(),
        roles_boundaries: vec![],
        time_restrictions: vec![TimeRestriction {
            days_of_week: vec![1, 2, 3, 4, 5],
            start_minute: 480,
            end_minute: 1080,
        }],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: HashSet::new(),
    };

    // Monday 9:00 AM
    let monday_ok = Utc.with_ymd_and_hms(2026, 5, 25, 9, 0, 0).unwrap();
    let res = step3b_validate_time_window(&principal, monday_ok);
    assert!(res.is_ok());

    // Saturday 9:00 AM
    let saturday_err = Utc.with_ymd_and_hms(2026, 5, 30, 9, 0, 0).unwrap();
    let res = step3b_validate_time_window(&principal, saturday_err);
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code, ErrorCode::Auth403);
}

#[test]
fn test_tenant_domain_matrix_system_vs_normal() {
    // Case A: System Tenant user with matching grant
    let system_principal_ok = PrincipalData {
        user_id: "usr_sys_ok".to_string(),
        tenant_id: "system".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: vec!["role_admin".to_string()].into_iter().collect(),
        roles_boundaries: vec![RoleBoundary {
            role_id: "role_admin".to_string(),
            grants: vec![
                serde_json::json!({"domain": "tenant", "actions": ["VIEW"], "scope": "ALL"}),
            ],
            permitted_locations: vec![],
            permitted_assets: vec![],
        }],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: HashSet::new(),
    };
    let res = step4_analytical(
        &system_principal_ok,
        "VIEW",
        &serde_json::json!({
            "entity_type": "tenant",
            "entity_id": "",
            "domains": ["tenant"]
        }),
    );
    assert!(res.is_ok());
    let boundaries = res.unwrap().get("tenant").cloned().unwrap();
    assert_eq!(
        boundaries[0].get("query_scope").unwrap().as_str().unwrap(),
        "ALL"
    );

    // Case B: System Tenant user without matching grant -> Should fail
    let system_principal_err = PrincipalData {
        user_id: "usr_sys_err".to_string(),
        tenant_id: "system".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: vec!["role_regular".to_string()].into_iter().collect(),
        roles_boundaries: vec![RoleBoundary {
            role_id: "role_regular".to_string(),
            grants: vec![
                serde_json::json!({"domain": "work_order", "actions": ["VIEW"], "scope": "ALL"}),
            ],
            permitted_locations: vec![],
            permitted_assets: vec![],
        }],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: HashSet::new(),
    };
    let res = step4_analytical(
        &system_principal_err,
        "VIEW",
        &serde_json::json!({
            "entity_type": "tenant",
            "entity_id": "",
            "domains": ["tenant"]
        }),
    );
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code, ErrorCode::Auth403);

    // Case C: Non-System Tenant user without matching grant -> Should be auto-authorized
    // Case C: Non-System Tenant user without matching grant -> Should be auto-authorized
    let normal_principal = PrincipalData {
        user_id: "usr_normal".to_string(),
        tenant_id: "tnt_01".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: HashSet::new(),
        roles_boundaries: vec![],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: HashSet::new(),
    };
    let res = step4_analytical(
        &normal_principal,
        "VIEW",
        &serde_json::json!({
            "entity_type": "tenant",
            "entity_id": "",
            "domains": ["tenant"]
        }),
    );
    assert!(res.is_ok());
    let boundaries = res.unwrap().get("tenant").cloned().unwrap();
    assert_eq!(
        boundaries[0].get("query_scope").unwrap().as_str().unwrap(),
        "ALL"
    );
}

#[test]
fn test_system_entities_authorization() {
    let system_entities = vec!["domain_plugin", "domain_quota", "tenant_plugin"];

    for entity in &system_entities {
        assert!(SystemSecurityRules::is_master_only_entity(entity));
        assert!(SystemSecurityRules::is_quota_exempt(entity));

        // System tenant user should pass check_crud_authorization
        let sys_res =
            SystemSecurityRules::check_crud_authorization(entity, "system", "usr_sys", "mutate");
        assert!(
            sys_res.is_ok(),
            "Expected system tenant to be authorized for {}",
            entity
        );

        // Non-system tenant user should be rejected by check_crud_authorization
        let reg_res = SystemSecurityRules::check_crud_authorization(
            entity,
            "tnt_tenant1",
            "usr_reg",
            "mutate",
        );
        assert!(
            reg_res.is_err(),
            "Expected non-system tenant to be rejected for {}",
            entity
        );
        assert_eq!(reg_res.unwrap_err().code, ErrorCode::Auth403);
    }

    // Test Cedar step4 evaluation for system vs non-system tenant with grants

    // System tenant user WITH grant for domain_plugin
    let principal_sys_with_grant = PrincipalData {
        user_id: "usr_sys_admin".to_string(),
        tenant_id: "system".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: vec!["role_sys_admin".to_string()].into_iter().collect(),
        roles_boundaries: vec![RoleBoundary {
            role_id: "role_sys_admin".to_string(),
            grants: vec![serde_json::json!({
                "domain": "domain_plugin",
                "actions": ["VIEW", "CREATE", "UPDATE", "DELETE"],
                "scope": "ALL"
            })],
            permitted_locations: vec![],
            permitted_assets: vec![],
        }],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: HashSet::new(),
    };

    // System tenant user WITHOUT grant for domain_plugin
    let principal_sys_no_grant = PrincipalData {
        user_id: "usr_sys_user".to_string(),
        tenant_id: "system".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: vec!["role_basic".to_string()].into_iter().collect(),
        roles_boundaries: vec![RoleBoundary {
            role_id: "role_basic".to_string(),
            grants: vec![serde_json::json!({
                "domain": "asset",
                "actions": ["VIEW"],
                "scope": "ALL"
            })],
            permitted_locations: vec![],
            permitted_assets: vec![],
        }],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: HashSet::new(),
    };

    let resource_domain_plugin = serde_json::json!({
        "entity_type": "domain_plugin",
        "entity_id": "dp_01",
        "domains": ["domain_plugin"]
    });

    // 1. System tenant user with grant -> authorized
    let view_res = step4_analytical(&principal_sys_with_grant, "VIEW", &resource_domain_plugin);
    assert!(
        view_res.is_ok(),
        "System user with grant should view domain_plugin"
    );

    // 2. System tenant user without grant -> unauthorized
    let view_no_grant_res =
        step4_analytical(&principal_sys_no_grant, "VIEW", &resource_domain_plugin);
    assert!(
        view_no_grant_res.is_err(),
        "System user without grant should be rejected"
    );
    assert_eq!(view_no_grant_res.unwrap_err().code, ErrorCode::Auth403);
}
