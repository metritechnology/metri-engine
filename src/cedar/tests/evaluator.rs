//! Tests for `cedar::evaluator.rs`.
// cedar/tests/evaluator.rs — PDP Cedar: aislamiento por compañía, grants y CSV.

use crate::cedar::engine::CedarAuthorizer;
use crate::cedar::evaluator::{step4_analytical, step4_mutational};
use crate::cedar::types::{PrincipalData, RoleBoundary};
use crate::domain::errors::ErrorCode;
use cedar_policy::{Context, Entities, EntityUid, PolicySet, Request, Schema};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

#[test]
fn test_cedar_contractor_isolation() {
    // 1. Load policies
    let policies_src = include_str!("../../../config/policies/metri.cedar");
    let policies = PolicySet::from_str(policies_src).expect("Failed to parse metri.cedar");

    // 2. Load schema
    let schema_src = include_str!("../../../config/policies/cedar-schema.json");
    let schema = Schema::from_str(schema_src).expect("Failed to parse cedar-schema.json");

    // 3. Define entities JSON
    let entities_json = r#"[
        {
            "uid": {
                "type": "Metri::User",
                "id": "usr_internal"
            },
            "attrs": {
                "tenant_id": "tnt_01",
                "user_id": "usr_internal",
                "roles": [],
                "email": "internal@metri.com",
                "user_type": "INTERNAL",
                "company_id": "",
                "grants": ["project:QueryMetrics", "work_order:QueryMetrics"],
                "query_scope": "ALL",
                "status": "ACTIVE"
            },
            "parents": []
        },
        {
            "uid": {
                "type": "Metri::User",
                "id": "usr_contractor_a"
            },
            "attrs": {
                "tenant_id": "tnt_01",
                "user_id": "usr_contractor_a",
                "roles": [],
                "email": "contractor_a@provider.com",
                "user_type": "CONTRACTOR",
                "company_id": "comp_provider_a",
                "grants": ["project:QueryMetrics", "work_order:QueryMetrics"],
                "query_scope": "ALL",
                "status": "ACTIVE"
            },
            "parents": []
        },
        {
            "uid": {
                "type": "Metri::User",
                "id": "usr_suspended_internal"
            },
            "attrs": {
                "tenant_id": "tnt_01",
                "user_id": "usr_suspended_internal",
                "roles": [],
                "email": "suspended@metri.com",
                "user_type": "INTERNAL",
                "company_id": "",
                "grants": ["project:QueryMetrics", "work_order:QueryMetrics"],
                "query_scope": "ALL",
                "status": "SUSPENDED"
            },
            "parents": []
        },
        {
            "uid": {
                "type": "Metri::MetriResource",
                "id": "res_project_a"
            },
            "attrs": {
                "tenant_id": "tnt_01",
                "entity_type": "project",
                "rpc": "",
                "assigned_company_id": "comp_provider_a",
                "query_scope": "ALL",
                "required_grant": "project:QueryMetrics"
            },
            "parents": []
        },
        {
            "uid": {
                "type": "Metri::MetriResource",
                "id": "res_project_b"
            },
            "attrs": {
                "tenant_id": "tnt_01",
                "entity_type": "project",
                "rpc": "",
                "assigned_company_id": "comp_provider_b",
                "query_scope": "ALL",
                "required_grant": "project:QueryMetrics"
            },
            "parents": []
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
                "id": "ActionGroup::\"analytical\""
            },
            "attrs": {},
            "parents": []
        }
    ]"#;

    let entities = Entities::from_json_str(entities_json, Some(&schema))
        .expect("Failed to parse entities JSON with schema");

    let authorizer = CedarAuthorizer::new();

    // SCENARIO 1: Internal user accesses project A
    {
        let principal = "Metri::User::\"usr_internal\""
            .parse::<EntityUid>()
            .unwrap();
        let action = "Metri::Action::\"QueryMetrics\""
            .parse::<EntityUid>()
            .unwrap();
        let resource = "Metri::MetriResource::\"res_project_a\""
            .parse::<EntityUid>()
            .unwrap();
        let request = Request::new(
            Some(principal),
            Some(action),
            Some(resource),
            Context::empty(),
            Some(&schema),
        )
        .unwrap();

        let result = authorizer
            .is_authorized(&policies, &entities, &request)
            .unwrap();
        assert!(
            result,
            "Internal user should be allowed to access project A"
        );
    }

    // SCENARIO 2: Contractor of Provider A accesses project A (assigned to Provider A) -> ALLOW
    {
        let principal = "Metri::User::\"usr_contractor_a\""
            .parse::<EntityUid>()
            .unwrap();
        let action = "Metri::Action::\"QueryMetrics\""
            .parse::<EntityUid>()
            .unwrap();
        let resource = "Metri::MetriResource::\"res_project_a\""
            .parse::<EntityUid>()
            .unwrap();
        let request = Request::new(
            Some(principal),
            Some(action),
            Some(resource),
            Context::empty(),
            Some(&schema),
        )
        .unwrap();

        let result = authorizer
            .is_authorized(&policies, &entities, &request)
            .unwrap();
        assert!(
            result,
            "Contractor of Provider A should be ALLOWED to access project A"
        );
    }

    // SCENARIO 3: Contractor of Provider A accesses project B (assigned to Provider B) -> DENY
    {
        let principal = "Metri::User::\"usr_contractor_a\""
            .parse::<EntityUid>()
            .unwrap();
        let action = "Metri::Action::\"QueryMetrics\""
            .parse::<EntityUid>()
            .unwrap();
        let resource = "Metri::MetriResource::\"res_project_b\""
            .parse::<EntityUid>()
            .unwrap();
        let request = Request::new(
            Some(principal),
            Some(action),
            Some(resource),
            Context::empty(),
            Some(&schema),
        )
        .unwrap();

        let result = authorizer
            .is_authorized(&policies, &entities, &request)
            .unwrap();
        assert!(
            !result,
            "Contractor of Provider A should be DENIED access to project B"
        );
    }

    // SCENARIO 4: Suspended internal user accesses project A -> DENY
    {
        let principal = "Metri::User::\"usr_suspended_internal\""
            .parse::<EntityUid>()
            .unwrap();
        let action = "Metri::Action::\"QueryMetrics\""
            .parse::<EntityUid>()
            .unwrap();
        let resource = "Metri::MetriResource::\"res_project_a\""
            .parse::<EntityUid>()
            .unwrap();
        let request = Request::new(
            Some(principal),
            Some(action),
            Some(resource),
            Context::empty(),
            Some(&schema),
        )
        .unwrap();

        let result = authorizer
            .is_authorized(&policies, &entities, &request)
            .unwrap();
        assert!(!result, "Suspended user should be DENIED access");
    }
}

#[test]
fn test_cedar_native_action_validation() {
    let policies_src = include_str!("../../../config/policies/metri.cedar");
    let policies = PolicySet::from_str(policies_src).expect("Failed to parse metri.cedar");

    let mut policy_cache = HashMap::new();
    policy_cache.insert("role_admin".to_string(), policies);

    let principal = PrincipalData {
        user_id: "usr_test".to_string(),
        tenant_id: "tnt_01".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: vec!["role_admin".to_string()].into_iter().collect(),
        roles_boundaries: vec![RoleBoundary {
            role_id: "role_admin".to_string(),
            grants: vec![serde_json::json!({
                "domain": "project",
                "actions": ["CREATE"]
            })],
            permitted_locations: vec![],
            permitted_assets: vec![],
        }],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: HashSet::new(),
    };

    // CREATE should be allowed
    let resource_ok = serde_json::json!({
        "entity_id": "res_project_1",
        "entity_type": "project"
    });
    let res_create = step4_mutational(
        &CedarAuthorizer::new(),
        &policy_cache,
        &principal,
        "CREATE",
        &resource_ok,
    );
    assert!(
        res_create.is_ok(),
        "CREATE should be allowed with project:CREATE grant: {:?}",
        res_create
    );

    // DELETE should be denied
    let res_delete = step4_mutational(
        &CedarAuthorizer::new(),
        &policy_cache,
        &principal,
        "DELETE",
        &resource_ok,
    );
    assert!(
        res_delete.is_err(),
        "DELETE should be denied when user only has project:CREATE grant"
    );
    assert_eq!(res_delete.unwrap_err().code, ErrorCode::Auth403);
}

#[test]
fn test_mutational_abac_hydration() {
    let policies_src = include_str!("../../../config/policies/metri.cedar");
    let policies = PolicySet::from_str(policies_src).expect("Failed to parse metri.cedar");

    let mut policy_cache = HashMap::new();
    policy_cache.insert("role_contractor".to_string(), policies);

    let principal = PrincipalData {
        user_id: "usr_contractor".to_string(),
        tenant_id: "tnt_01".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "CONTRACTOR".to_string(),
        company_id: "comp_provider_a".to_string(),
        roles: vec!["role_contractor".to_string()].into_iter().collect(),
        roles_boundaries: vec![RoleBoundary {
            role_id: "role_contractor".to_string(),
            grants: vec![serde_json::json!({
                "domain": "project",
                "actions": ["UPDATE"]
            })],
            permitted_locations: vec![],
            permitted_assets: vec![],
        }],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: HashSet::new(),
    };

    // Case 1: Resource is assigned to the contractor's company -> ALLOW
    let resource_matching = serde_json::json!({
        "entity_id": "res_project_1",
        "entity_type": "project",
        "assigned_company_id": "comp_provider_a"
    });
    let res_matching = step4_mutational(
        &CedarAuthorizer::new(),
        &policy_cache,
        &principal,
        "UPDATE",
        &resource_matching,
    );
    assert!(
        res_matching.is_ok(),
        "Contractor should be allowed to update resource assigned to their company"
    );

    // Case 2: Resource is assigned to a different company -> DENY (P06)
    let resource_different = serde_json::json!({
        "entity_id": "res_project_2",
        "entity_type": "project",
        "assigned_company_id": "comp_provider_b"
    });
    let res_different = step4_mutational(
        &CedarAuthorizer::new(),
        &policy_cache,
        &principal,
        "UPDATE",
        &resource_different,
    );
    assert!(
        res_different.is_err(),
        "Contractor should be denied access to a different company's resource"
    );
    assert_eq!(res_different.unwrap_err().code, ErrorCode::Auth403);

    // Case 3: Resource has no company assigned -> DENY (P06)
    let resource_none = serde_json::json!({
        "entity_id": "res_project_3",
        "entity_type": "project"
    });
    let res_none = step4_mutational(
        &CedarAuthorizer::new(),
        &policy_cache,
        &principal,
        "UPDATE",
        &resource_none,
    );
    assert!(
        res_none.is_err(),
        "Contractor should be denied access if resource has no company assigned"
    );
    assert_eq!(res_none.unwrap_err().code, ErrorCode::Auth403);
}

#[test]
fn test_csv_export_action_authorization() {
    let principal_view_only = PrincipalData {
        user_id: "usr_view".to_string(),
        tenant_id: "tnt_01".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: vec!["role_viewer".to_string()].into_iter().collect(),
        roles_boundaries: vec![RoleBoundary {
            role_id: "role_viewer".to_string(),
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

    let principal_export_only = PrincipalData {
        user_id: "usr_exporter".to_string(),
        tenant_id: "tnt_01".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: vec!["role_exporter".to_string()].into_iter().collect(),
        roles_boundaries: vec![RoleBoundary {
            role_id: "role_exporter".to_string(),
            grants: vec![serde_json::json!({
                "domain": "asset",
                "actions": ["EXPORT"],
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

    let principal_wildcard = PrincipalData {
        user_id: "usr_admin".to_string(),
        tenant_id: "tnt_01".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: vec!["role_admin".to_string()].into_iter().collect(),
        roles_boundaries: vec![RoleBoundary {
            role_id: "role_admin".to_string(),
            grants: vec![serde_json::json!({
                "domain": "asset",
                "actions": ["*"],
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

    let resource = serde_json::json!({
        "entity_type": "asset",
        "entity_id": "",
        "domains": ["asset"]
    });

    // 1. Viewer only -> VIEW should be Ok, EXPORT should be Err
    let res_view_ok = step4_analytical(&principal_view_only, "VIEW", &resource);
    assert!(res_view_ok.is_ok());
    let res_view_err = step4_analytical(&principal_view_only, "EXPORT", &resource);
    assert!(res_view_err.is_err());
    assert_eq!(res_view_err.unwrap_err().code, ErrorCode::Auth403);

    // 2. Exporter only -> EXPORT should be Ok, VIEW should be Err
    let res_exp_ok = step4_analytical(&principal_export_only, "EXPORT", &resource);
    assert!(res_exp_ok.is_ok());
    let res_exp_err = step4_analytical(&principal_export_only, "VIEW", &resource);
    assert!(res_exp_err.is_err());
    assert_eq!(res_exp_err.unwrap_err().code, ErrorCode::Auth403);

    // 3. Wildcard -> both should be Ok
    let res_wild_view = step4_analytical(&principal_wildcard, "VIEW", &resource);
    assert!(res_wild_view.is_ok());
    let res_wild_exp = step4_analytical(&principal_wildcard, "EXPORT", &resource);
    assert!(res_wild_exp.is_ok());
}
