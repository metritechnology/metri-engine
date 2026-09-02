// authorizer/principal_graph.rs — grafo de principal (fase 2).
// step2 (consulta OLTP), step3 (consolidación), expansión de jerarquías
// y ensamblado del grafo completo de roles/grupos/vecinos.

use std::collections::HashSet;

use crate::cedar::authorizer::{
    PrincipalCache, PrincipalData, RoleBoundary, TimeRestriction, MAX_HIERARCHY_DEPTH,
};
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::reader::pull::{EavReader, EntityMap};
use crate::eav::reader::query::{EavQueryExecutor, NativeQueryPlan};
use crate::eav::types::datom::DatomValue;

pub async fn step2_query_oltp(
    eav_reader: &EavReader,
    tenant_id: &str,
    user_id: &str,
    cache: &dyn PrincipalCache,
) -> Result<PrincipalData, DomainError> {
    if user_id == "usr_system_bff" {
        return Ok(PrincipalData {
            user_id: "usr_system_bff".to_string(),
            tenant_id: tenant_id.to_string(),
            status: "ACTIVE".to_string(),
            user_type: "INTERNAL".to_string(),
            company_id: String::new(),
            roles: ["system-bff".to_string()].into_iter().collect(),
            roles_boundaries: vec![RoleBoundary {
                role_id: "system-bff".to_string(),
                grants: vec![serde_json::json!({
                    "domain": "*",
                    "actions": ["VIEW", "CREATE", "UPDATE", "DELETE", "EXECUTE", "EXPORT"],
                    "scope": "ALL"
                })],
                permitted_locations: vec![],
                permitted_assets: vec![],
            }],
            time_restrictions: vec![],
            group_allowed_locations: vec![],
            group_allowed_assets: vec![],
            groups: std::collections::HashSet::new(),
        });
    }

    if let Some(cached) = cache.lookup_principal(user_id).await {
        if cached.status == "SUSPENDED" {
            return Err(DomainError::new(
                ErrorCode::Auth403,
                format!("User {user_id} is suspended"),
            )
            .with_stage("cedar"));
        }
        return Ok(cached);
    }

    let user_map = eav_reader.pull(tenant_id, user_id, None).await?;
    tracing::info!(
        "[step2_query_oltp] tenant_id={}, user_id={}, user_map={:?}",
        tenant_id,
        user_id,
        user_map
    );
    if user_map.is_empty() {
        return Err(
            DomainError::new(ErrorCode::Auth403, format!("User {user_id} not found"))
                .with_stage("cedar"),
        );
    }

    let status = user_map
        .get("status")
        .or_else(|| user_map.get("user/status"))
        .and_then(|v| match v {
            DatomValue::Str(s) => Some(s.as_str()),
            _ => None,
        })
        .unwrap_or("ACTIVE");

    if status == "SUSPENDED" {
        return Err(
            DomainError::new(ErrorCode::Auth403, format!("User {user_id} is suspended"))
                .with_stage("cedar"),
        );
    }

    let principal = assemble_principal_graph(eav_reader, tenant_id, user_id, user_map).await?;
    cache.store_principal(user_id, principal.clone()).await?;

    Ok(principal)
}

pub async fn step3_consolidate(
    eav_reader: &EavReader,
    tenant_id: &str,
    user_data: PrincipalData,
    expand_children: bool,
) -> Result<PrincipalData, DomainError> {
    if user_data.roles.is_empty() {
        return Err(
            DomainError::new(ErrorCode::Auth403, "User has no roles assigned").with_stage("cedar"),
        );
    }

    // `expand_children = false` es la costura de prueba: las jerarquías de los
    // tests unitarios se expresan con punteros al padre dentro de los mapas en
    // caché, así que la expansión por índice AVET se omite y el conjunto
    // expandido es exactamente las raíces. En producción siempre es `true`.
    // 1. Expand group-level allowed locations and assets (in parallel)
    let eav_reader_g1 = eav_reader.clone();
    let tenant_g1 = tenant_id.to_string();
    let group_locations_roots = user_data.group_allowed_locations.clone();
    let group_locs_handle = tokio::spawn(async move {
        if expand_children {
            expand_hierarchy(
                &eav_reader_g1,
                &tenant_g1,
                "location",
                group_locations_roots,
            )
            .await
        } else {
            Ok(group_locations_roots)
        }
    });

    let eav_reader_g2 = eav_reader.clone();
    let tenant_g2 = tenant_id.to_string();
    let group_assets_roots = user_data.group_allowed_assets.clone();
    let group_assets_handle = tokio::spawn(async move {
        if expand_children {
            expand_hierarchy(&eav_reader_g2, &tenant_g2, "asset", group_assets_roots).await
        } else {
            Ok(group_assets_roots)
        }
    });

    let (g_locs_res, g_assets_res) = tokio::join!(group_locs_handle, group_assets_handle);
    let expanded_group_locations: HashSet<String> = g_locs_res
        .map_err(|e| DomainError::new(ErrorCode::Infra001, e.to_string()))??
        .into_iter()
        .collect();
    let expanded_group_assets: HashSet<String> = g_assets_res
        .map_err(|e| DomainError::new(ErrorCode::Infra001, e.to_string()))??
        .into_iter()
        .collect();

    let mut consolidated_boundaries = Vec::new();

    // 2. Expand role boundaries and intersect with group boundaries
    for boundary in &user_data.roles_boundaries {
        let eav_reader_clone = eav_reader.clone();
        let tenant_clone = tenant_id.to_string();
        let roots_locations = boundary.permitted_locations.clone();

        let locs_handle = tokio::spawn(async move {
            if expand_children {
                expand_hierarchy(
                    &eav_reader_clone,
                    &tenant_clone,
                    "location",
                    roots_locations,
                )
                .await
            } else {
                Ok(roots_locations)
            }
        });

        let eav_reader_clone2 = eav_reader.clone();
        let tenant_clone2 = tenant_id.to_string();
        let roots_assets = boundary.permitted_assets.clone();

        let assets_handle = tokio::spawn(async move {
            if expand_children {
                expand_hierarchy(&eav_reader_clone2, &tenant_clone2, "asset", roots_assets).await
            } else {
                Ok(roots_assets)
            }
        });

        let (locs_res, assets_res) = tokio::join!(locs_handle, assets_handle);

        let expanded_locations: HashSet<String> = locs_res
            .map_err(|e| DomainError::new(ErrorCode::Infra001, e.to_string()))??
            .into_iter()
            .collect();
        let expanded_assets: HashSet<String> = assets_res
            .map_err(|e| DomainError::new(ErrorCode::Infra001, e.to_string()))??
            .into_iter()
            .collect();

        // Intersect location boundaries
        let final_permitted_locations = if !user_data.group_allowed_locations.is_empty() {
            if expanded_locations.is_empty() {
                // Role has no location restrictions, so we inherit group locations
                expanded_group_locations.iter().cloned().collect()
            } else {
                // Both have location restrictions, so we intersect
                expanded_locations
                    .intersection(&expanded_group_locations)
                    .cloned()
                    .collect()
            }
        } else {
            expanded_locations.into_iter().collect()
        };

        // Intersect asset boundaries
        let final_permitted_assets = if !user_data.group_allowed_assets.is_empty() {
            if expanded_assets.is_empty() {
                // Role has no asset restrictions, so we inherit group assets
                expanded_group_assets.iter().cloned().collect()
            } else {
                // Both have asset restrictions, so we intersect
                expanded_assets
                    .intersection(&expanded_group_assets)
                    .cloned()
                    .collect()
            }
        } else {
            expanded_assets.into_iter().collect()
        };

        consolidated_boundaries.push(RoleBoundary {
            role_id: boundary.role_id.clone(),
            grants: boundary.grants.clone(),
            permitted_locations: final_permitted_locations,
            permitted_assets: final_permitted_assets,
        });
    }

    let mut final_principal = user_data;
    final_principal.roles_boundaries = consolidated_boundaries;
    Ok(final_principal)
}

async fn expand_hierarchy(
    eav_reader: &EavReader,
    tenant_id: &str,
    entity_type: &str,
    roots: Vec<String>,
) -> Result<Vec<String>, DomainError> {
    if roots.is_empty() {
        return Ok(vec![]);
    }

    let mut expanded = HashSet::new();
    let mut queue = roots;

    for _depth in 0..MAX_HIERARCHY_DEPTH {
        if queue.is_empty() {
            break;
        }

        let mut next_level = Vec::new();
        for id in queue {
            if expanded.insert(id.clone()) {
                let children = fetch_children_eav(eav_reader, tenant_id, entity_type, &id).await?;
                next_level.extend(children);
            }
        }
        queue = next_level;
    }

    Ok(expanded.into_iter().collect())
}

async fn fetch_children_eav(
    eav_reader: &EavReader,
    tenant_id: &str,
    _entity_type: &str,
    parent_id: &str,
) -> Result<Vec<String>, DomainError> {
    let query_executor = EavQueryExecutor::new(eav_reader.ddb.clone(), eav_reader.table.clone());
    let plan = NativeQueryPlan::AvetSingle {
        tenant_id: tenant_id.to_string(),
        attr_name: "parent_id".to_string(),
        value: DatomValue::Str(parent_id.to_string()),
    };

    query_executor.execute_native_plan(&plan).await
}

pub async fn assemble_principal_graph(
    eav_reader: &EavReader,
    tenant_id: &str,
    user_id: &str,
    user_map: EntityMap,
) -> Result<PrincipalData, DomainError> {
    let user_type = match user_map
        .get("user_type")
        .or_else(|| user_map.get("user/user_type"))
    {
        Some(DatomValue::Str(s)) => s.clone(),
        _ => "INTERNAL".to_string(),
    };

    let company_id = match user_map
        .get("company_id")
        .or_else(|| user_map.get("user/company_id"))
    {
        Some(DatomValue::Str(s)) => s.clone(),
        _ => "".to_string(),
    };

    let status = match user_map
        .get("status")
        .or_else(|| user_map.get("user/status"))
    {
        Some(DatomValue::Str(s)) => s.clone(),
        _ => "ACTIVE".to_string(),
    };

    let mut roles = HashSet::new();
    let mut roles_boundaries = Vec::new();

    if let Some(val) = user_map
        .get("role_ids")
        .or_else(|| user_map.get("user/role_ids"))
    {
        let role_ids = match val {
            DatomValue::Array(arr) => arr.clone(),
            DatomValue::Str(s) => {
                if s.starts_with('[') {
                    serde_json::from_str::<Vec<String>>(s).unwrap_or_else(|_| vec![s.clone()])
                } else {
                    vec![s.clone()]
                }
            }
            _ => vec![],
        };

        for r_id in role_ids {
            roles.insert(r_id.clone());

            let mut role_map = eav_reader.pull(tenant_id, &r_id, None).await?;
            if role_map.is_empty() {
                let master_tenant_id = std::env::var("METRI_MASTER_TENANT_ID")
                    .unwrap_or_else(|_| "system".to_string());
                if tenant_id != master_tenant_id {
                    role_map = eav_reader.pull(&master_tenant_id, &r_id, None).await?;
                }
            }

            let grants = match role_map
                .get("grants")
                .or_else(|| role_map.get("role/grants"))
            {
                Some(DatomValue::Str(s)) => {
                    let parsed: serde_json::Value =
                        serde_json::from_str(s).unwrap_or_else(|_| serde_json::json!([]));
                    if let serde_json::Value::Array(arr) = parsed {
                        let v: Vec<serde_json::Value> = arr
                            .into_iter()
                            .map(|item| match item {
                                serde_json::Value::String(str_val) => {
                                    serde_json::from_str(&str_val)
                                        .unwrap_or_else(|_| serde_json::json!(str_val))
                                }
                                _ => item,
                            })
                            .collect();
                        serde_json::Value::Array(v)
                    } else {
                        parsed
                    }
                }
                Some(DatomValue::Array(arr)) => {
                    let v: Vec<serde_json::Value> = arr
                        .iter()
                        .map(|s| serde_json::from_str(s).unwrap_or_else(|_| serde_json::json!(s)))
                        .collect();
                    serde_json::Value::Array(v)
                }
                _ => serde_json::json!([]),
            };

            let grants_vec = grants.as_array().cloned().unwrap_or_default();

            let permitted_locations = match role_map
                .get("permitted_locations")
                .or_else(|| role_map.get("role/permitted_locations"))
            {
                Some(DatomValue::Array(arr)) => arr.clone(),
                Some(DatomValue::Str(s)) => vec![s.clone()],
                _ => vec![],
            };

            let permitted_assets = match role_map
                .get("permitted_assets")
                .or_else(|| role_map.get("role/permitted_assets"))
            {
                Some(DatomValue::Array(arr)) => arr.clone(),
                Some(DatomValue::Str(s)) => vec![s.clone()],
                _ => vec![],
            };

            roles_boundaries.push(RoleBoundary {
                role_id: r_id.clone(),
                grants: grants_vec,
                permitted_locations,
                permitted_assets,
            });
        }
    }

    let mut group_allowed_locations = HashSet::new();
    let mut group_allowed_assets = HashSet::new();
    let mut group_time_restrictions = Vec::new();
    let mut visited_groups = HashSet::new();

    if let Some(val) = user_map
        .get("group_ids")
        .or_else(|| user_map.get("user/group_ids"))
    {
        let direct_group_ids = match val {
            DatomValue::Array(arr) => arr.clone(),
            DatomValue::Str(s) => {
                if s.starts_with('[') {
                    serde_json::from_str::<Vec<String>>(s).unwrap_or_else(|_| vec![s.clone()])
                } else {
                    vec![s.clone()]
                }
            }
            _ => vec![],
        };

        let mut queue = direct_group_ids;

        while let Some(g_id) = queue.pop() {
            if !visited_groups.insert(g_id.clone()) {
                continue;
            }

            let group_map = eav_reader.pull(tenant_id, &g_id, None).await?;
            if group_map.is_empty() {
                continue;
            }

            if let Some(loc_val) = group_map
                .get("allowed_locations")
                .or_else(|| group_map.get("user_group/allowed_locations"))
            {
                let locs = match loc_val {
                    DatomValue::Array(arr) => arr.clone(),
                    DatomValue::Str(s) => vec![s.clone()],
                    _ => vec![],
                };
                for loc in locs {
                    group_allowed_locations.insert(loc);
                }
            }

            if let Some(asset_val) = group_map
                .get("allowed_assets")
                .or_else(|| group_map.get("user_group/allowed_assets"))
            {
                let assets = match asset_val {
                    DatomValue::Array(arr) => arr.clone(),
                    DatomValue::Str(s) => vec![s.clone()],
                    _ => vec![],
                };
                for asset in assets {
                    group_allowed_assets.insert(asset);
                }
            }

            if let Some(tr_val) = group_map
                .get("time_restrictions")
                .or_else(|| group_map.get("user_group/time_restrictions"))
            {
                if let DatomValue::Str(s) = tr_val {
                    if let Ok(res) = serde_json::from_str::<Vec<TimeRestriction>>(s) {
                        group_time_restrictions.extend(res);
                    }
                }
            }

            if let Some(parent_val) = group_map
                .get("parent_user_group_id")
                .or_else(|| group_map.get("user_group/parent_user_group_id"))
            {
                if let DatomValue::Str(parent_id) = parent_val {
                    if !parent_id.is_empty() {
                        queue.push(parent_id.clone());
                    }
                }
            }
        }
    }

    let mut time_restrictions = Vec::new();
    if let Some(val) = user_map
        .get("time_restrictions")
        .or_else(|| user_map.get("user/time_restrictions"))
    {
        if let DatomValue::Str(s) = val {
            if let Ok(res) = serde_json::from_str::<Vec<TimeRestriction>>(s) {
                time_restrictions = res;
            }
        }
    }
    time_restrictions.extend(group_time_restrictions);

    Ok(PrincipalData {
        user_id: user_id.to_string(),
        tenant_id: tenant_id.to_string(),
        status,
        user_type,
        company_id,
        roles,
        roles_boundaries,
        time_restrictions,
        group_allowed_locations: group_allowed_locations.into_iter().collect(),
        group_allowed_assets: group_allowed_assets.into_iter().collect(),
        groups: visited_groups,
    })
}
