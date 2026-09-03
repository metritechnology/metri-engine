// cedar/tests/principal_graph.rs — Grafo de principal: caché, suspensión y jerarquías.

use super::fakes::{FailingReader, FakeEntityReader};
use crate::cedar::cache::principal::InMemoryPrincipalCache;
use crate::cedar::cache::session::InMemorySessionStore;
use crate::cedar::ports::PrincipalCache;
use crate::cedar::principal_graph::{assemble_principal_graph, step2_query_oltp, step3_consolidate};
use crate::cedar::types::PrincipalData;
use crate::domain::errors::ErrorCode;
use crate::domain::protocols::Session;
use crate::eav::types::datom::DatomValue;
use chrono::Utc;
use std::collections::{HashMap, HashSet};

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
