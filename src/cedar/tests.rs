// cedar/tests.rs — Suite del módulo cedar.
//
// Tests de la pipeline de autorización completa sobre FAKES en memoria
// (`FakeEntityReader`, `FailingReader`): sin DynamoClient real, sin sembrar
// candados globales y sin manipular el entorno — paralelizables sin carreras.

use crate::cedar::cache::principal::InMemoryPrincipalCache;
use crate::cedar::cache::session::InMemorySessionStore;
use crate::cedar::engine::CedarAuthorizer;
use crate::cedar::evaluator::{step4_analytical, step4_mutational};
use crate::cedar::ports::EntityReader;
use crate::cedar::principal_graph::{assemble_principal_graph, step2_query_oltp, step3_consolidate};
use crate::cedar::request::AuthRequest;
use crate::cedar::rules::{step3b_validate_time_window, SystemSecurityRules};
use crate::cedar::types::{PrincipalData, RoleBoundary, TimeRestriction};
use crate::cedar::{step1_extract_token, verify_hmac_token_local_in_step, HmacTokenVerifier, PrincipalCache};
use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::Session;
use crate::eav::reader::pull::EntityMap;
use crate::eav::types::datom::DatomValue;
use cedar_policy::{Context, Entities, EntityUid, PolicySet, Request, Schema};
use chrono::{TimeZone, Utc};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

/// Lector EAV en memoria: mapas de atributos por `tenant/entity_id`.
/// Suficiente para el grafo de principal — los tests seedean directamente.
#[derive(Default, Clone)]
struct FakeEntityReader {
    entities: HashMap<String, EntityMap>,
}

#[async_trait::async_trait]
impl EntityReader for FakeEntityReader {
    async fn pull(
        &self,
        tenant_id: &str,
        entity_id: &str,
        _attrs: Option<&[&str]>,
    ) -> Result<EntityMap, DomainError> {
        Ok(self
            .entities
            .get(&format!("{tenant_id}/{entity_id}"))
            .cloned()
            .unwrap_or_default())
    }

    async fn children_with_attr(
        &self,
        _tenant_id: &str,
        _attr_name: &str,
        _value: &DatomValue,
    ) -> Result<Vec<String>, DomainError> {
        Ok(vec![])
    }

    fn clone_reader(&self) -> Box<dyn EntityReader> {
        Box::new(self.clone())
    }
}

/// Lector que FALLA ante cualquier consulta: usado para probar que el camino
/// cacheado no toca el almacenamiento.
struct FailingReader;

#[async_trait::async_trait]
impl EntityReader for FailingReader {
    async fn pull(
        &self,
        _tenant_id: &str,
        _entity_id: &str,
        _attrs: Option<&[&str]>,
    ) -> Result<EntityMap, DomainError> {
        Err(DomainError::new(
            ErrorCode::Infra001,
            "el lector no debe consultarse en este camino",
        ))
    }

    async fn children_with_attr(
        &self,
        _tenant_id: &str,
        _attr_name: &str,
        _value: &DatomValue,
    ) -> Result<Vec<String>, DomainError> {
        Err(DomainError::new(
            ErrorCode::Infra001,
            "el lector no debe consultarse en este camino",
        ))
    }

    fn clone_reader(&self) -> Box<dyn EntityReader> {
        Box::new(FailingReader)
    }
}

#[test]
fn test_cedar_contractor_isolation() {
    // 1. Load policies
    let policies_src = include_str!("../../config/policies/metri.cedar");
    let policies = PolicySet::from_str(policies_src).expect("Failed to parse metri.cedar");

    // 2. Load schema
    let schema_src = include_str!("../../config/policies/cedar-schema.json");
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

#[tokio::test]
async fn test_token_missing() {
    let valkey = InMemorySessionStore::new();
    let req = tonic::Request::new(());
    let res = step1_extract_token(&AuthRequest::from_tonic(&req), &valkey).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code, ErrorCode::Auth401);
}

#[tokio::test]
async fn test_user_suspended() {
    let valkey = InMemorySessionStore::new();
    valkey.insert(
        "token_suspended",
        Session {
            tenant_id: "tnt_01".to_string(),
            user_id: "usr_susp".to_string(),
            jti: "test-jti".to_string(),
            exp: Utc::now().timestamp() + 3600,
        },
    );

    let cache = InMemoryPrincipalCache::new();
    let principal = PrincipalData {
        user_id: "usr_susp".to_string(),
        tenant_id: "tnt_01".to_string(),
        status: "SUSPENDED".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: "".to_string(),
        roles: HashSet::new(),
        roles_boundaries: vec![],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: HashSet::new(),
    };
    cache.store_principal("usr_susp", principal).await.unwrap();

    // Lector fake: el camino caché debe rechazar sin consultarlo.
    let reader = FakeEntityReader::default();
    let res = step2_query_oltp(&reader, "tnt_01", "usr_susp", &cache).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code, ErrorCode::Auth403);
}

#[tokio::test]
async fn test_cache_hit_prevents_db_query() {
    let cache = InMemoryPrincipalCache::new();
    let principal = PrincipalData {
        user_id: "usr_cached".to_string(),
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
    cache
        .store_principal("usr_cached", principal)
        .await
        .unwrap();

    // Un lector que FALLA ante cualquier consulta: el Ok demuestra el cache hit.
    let reader = FailingReader;
    let res = step2_query_oltp(&reader, "tnt_01", "usr_cached", &cache).await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap().user_id, "usr_cached");
}

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

#[tokio::test]
async fn test_user_group_hierarchy_and_refinement() {
    // 1. Prepare EAV records maps
    let mut user_map = HashMap::new();
    user_map.insert(
        "user_type".to_string(),
        DatomValue::Str("INTERNAL".to_string()),
    );
    user_map.insert("status".to_string(), DatomValue::Str("ACTIVE".to_string()));
    user_map.insert(
        "role_ids".to_string(),
        DatomValue::Array(vec!["role_user".to_string()]),
    );
    user_map.insert(
        "group_ids".to_string(),
        DatomValue::Array(vec!["group_sub".to_string()]),
    );

    let mut role_map = HashMap::new();
    role_map.insert(
        "grants".to_string(),
        DatomValue::Str("[\"work_order\"]".to_string()),
    );
    role_map.insert(
        "permitted_locations".to_string(),
        DatomValue::Array(vec![
            "loc_root_role".to_string(),
            "loc_other_role".to_string(),
        ]),
    );
    role_map.insert(
        "permitted_assets".to_string(),
        DatomValue::Array(vec!["asset_root_role".to_string()]),
    );

    let mut group_sub_map = HashMap::new();
    group_sub_map.insert(
        "allowed_locations".to_string(),
        DatomValue::Array(vec!["loc_sub_group".to_string()]),
    );
    group_sub_map.insert(
        "allowed_assets".to_string(),
        DatomValue::Array(vec!["asset_sub_group".to_string()]),
    );
    group_sub_map.insert(
        "parent_user_group_id".to_string(),
        DatomValue::Str("group_parent".to_string()),
    );

    let mut group_parent_map = HashMap::new();
    group_parent_map.insert(
        "allowed_locations".to_string(),
        DatomValue::Array(vec![
            "loc_root_role".to_string(),
            "loc_new_group".to_string(),
        ]),
    );
    group_parent_map.insert(
        "allowed_assets".to_string(),
        DatomValue::Array(vec!["asset_root_role".to_string()]),
    );
    group_parent_map.insert(
        "time_restrictions".to_string(),
        DatomValue::Str(
            "[{\"days_of_week\":[1,2,3,4,5],\"start_minute\":480,\"end_minute\":1080}]"
                .to_string(),
        ),
    );

    let user_map_clone = user_map.clone();
    let mut reader = FakeEntityReader::default();
    reader
        .entities
        .insert("tnt_01/usr_hierarchy_test".to_string(), user_map);
    reader
        .entities
        .insert("tnt_01/role_user".to_string(), role_map);
    reader
        .entities
        .insert("tnt_01/group_sub".to_string(), group_sub_map);
    reader
        .entities
        .insert("tnt_01/group_parent".to_string(), group_parent_map);

    // 2. Load principal graph (resolves roles & user groups recursively)
    let principal =
        assemble_principal_graph(&reader, "tnt_01", "usr_hierarchy_test", user_map_clone)
            .await
            .unwrap();

    assert_eq!(principal.user_id, "usr_hierarchy_test");
    assert!(principal.roles.contains("role_user"));

    // Check user group attributes union:
    assert!(principal
        .group_allowed_locations
        .contains(&"loc_sub_group".to_string()));
    assert!(principal
        .group_allowed_locations
        .contains(&"loc_root_role".to_string()));
    assert!(principal
        .group_allowed_locations
        .contains(&"loc_new_group".to_string()));

    assert!(principal
        .group_allowed_assets
        .contains(&"asset_sub_group".to_string()));
    assert!(principal
        .group_allowed_assets
        .contains(&"asset_root_role".to_string()));

    assert_eq!(principal.time_restrictions.len(), 1);
    assert_eq!(
        principal.time_restrictions[0].days_of_week,
        vec![1, 2, 3, 4, 5]
    );

    // 3. Consolidate (performs expansion and intersection).
    // La jerarquía del test vive en los mapas del fake (punteros al
    // padre); no se necesita la expansión por índice AVET.
    let consolidated = step3_consolidate(&reader, "tnt_01", principal, false)
        .await
        .unwrap();

    // Assert that role boundaries are intersected with the group boundaries
    let boundary = &consolidated.roles_boundaries[0];

    assert_eq!(boundary.permitted_locations.len(), 1);
    assert_eq!(boundary.permitted_locations[0], "loc_root_role");

    assert_eq!(boundary.permitted_assets.len(), 1);
    assert_eq!(boundary.permitted_assets[0], "asset_root_role");
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
        &CedarAuthorizer::new(),
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
        &CedarAuthorizer::new(),
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
        &CedarAuthorizer::new(),
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
fn test_cedar_native_action_validation() {
    let policies_src = include_str!("../../config/policies/metri.cedar");
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
    let policies_src = include_str!("../../config/policies/metri.cedar");
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
fn test_verify_hmac_token() {
    // El verificador se construye con el secreto explícito — sin tocar el
    // entorno global (set_var es una carrera entre tests en paralelo).
    let secret = "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2";
    let verifier = HmacTokenVerifier::new(secret);
    let exp = chrono::Utc::now().timestamp() + 3600;
    let token = crate::cedar::authn::tests::mint_for_tests(
        &verifier,
        exp,
        "e2575d40e197f5180121dcfb9470b3dc",
        "system",
        "usr_system_bff",
    );

    // Verificación estricta del camino Cedar (skew 0).
    let res = verifier.verify(&token, 0);
    assert!(res.is_some());
    let verified = res.unwrap();
    assert_eq!(verified.tenant_id, "system");
    assert_eq!(verified.user_id, "usr_system_bff");

    // Un token firmado con un secreto distinto al de la configuración del
    // proceso jamás verifica en el envoltorio estricto del camino Cedar
    // (aserción determinista: el secreto de test es único en el proceso).
    let wrapper = crate::cedar::authn::tests::mint_for_tests(
        &verifier,
        chrono::Utc::now().timestamp() + 3600,
        "jti_2",
        "tnt_01",
        "usr_2",
    );
    assert!(verify_hmac_token_local_in_step(&wrapper).is_none());
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

    let cedar_auth = CedarAuthorizer::new();

    // 1. Viewer only -> VIEW should be Ok, EXPORT should be Err
    let res_view_ok = step4_analytical(&cedar_auth, &principal_view_only, "VIEW", &resource);
    assert!(res_view_ok.is_ok());
    let res_view_err = step4_analytical(&cedar_auth, &principal_view_only, "EXPORT", &resource);
    assert!(res_view_err.is_err());
    assert_eq!(res_view_err.unwrap_err().code, ErrorCode::Auth403);

    // 2. Exporter only -> EXPORT should be Ok, VIEW should be Err
    let res_exp_ok = step4_analytical(&cedar_auth, &principal_export_only, "EXPORT", &resource);
    assert!(res_exp_ok.is_ok());
    let res_exp_err = step4_analytical(&cedar_auth, &principal_export_only, "VIEW", &resource);
    assert!(res_exp_err.is_err());
    assert_eq!(res_exp_err.unwrap_err().code, ErrorCode::Auth403);

    // 3. Wildcard -> both should be Ok
    let res_wild_view = step4_analytical(&cedar_auth, &principal_wildcard, "VIEW", &resource);
    assert!(res_wild_view.is_ok());
    let res_wild_exp = step4_analytical(&cedar_auth, &principal_wildcard, "EXPORT", &resource);
    assert!(res_wild_exp.is_ok());
}

#[test]
fn test_system_entities_authorization() {
    let system_entities = vec!["domain_plugin", "domain_quota", "tenant_plugin"];

    for entity in &system_entities {
        assert!(SystemSecurityRules::is_master_only_entity(entity));
        assert!(SystemSecurityRules::is_quota_exempt(entity));

        // System tenant user should pass check_crud_authorization
        let sys_res = SystemSecurityRules::check_crud_authorization(
            entity, "system", "usr_sys", "mutate",
        );
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
    let cedar_auth = CedarAuthorizer::new();

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
    let view_res = step4_analytical(
        &cedar_auth,
        &principal_sys_with_grant,
        "VIEW",
        &resource_domain_plugin,
    );
    assert!(
        view_res.is_ok(),
        "System user with grant should view domain_plugin"
    );

    // 2. System tenant user without grant -> unauthorized
    let view_no_grant_res = step4_analytical(
        &cedar_auth,
        &principal_sys_no_grant,
        "VIEW",
        &resource_domain_plugin,
    );
    assert!(
        view_no_grant_res.is_err(),
        "System user without grant should be rejected"
    );
    assert_eq!(view_no_grant_res.unwrap_err().code, ErrorCode::Auth403);
}
