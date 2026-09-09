//! Principal graph — role and group hierarchy expansion.
//!
//! grafo de principal (fase 2).
//! step2 (consulta OLTP), step3 (consolidación), expansión de jerarquías
//! y ensamblado del grafo completo de roles/grupos/vecinos.

use std::collections::HashSet;

use futures::future::join_all;

use crate::cedar::ports::{EntityReader, PrincipalCache};
use crate::cedar::rules::is_master_tenant;
use crate::cedar::types::{PrincipalData, RoleBoundary, TimeRestriction};

/// Profundidad máxima de expansión de jerarquías (locations/assets).
const MAX_HIERARCHY_DEPTH: usize = 10;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::reader::pull::EntityMap;
use crate::eav::types::datom::DatomValue;

/// Atributo de una entidad: primero la clave desnuda, luego la calificada por
/// tipo (`status` → `user/status`). Contrato del pull EAV.
fn attr<'m>(map: &'m EntityMap, prefix: &str, key: &str) -> Option<&'m DatomValue> {
    map.get(key).or_else(|| map.get(&format!("{prefix}/{key}")))
}

/// Lista de ids desde un datom: array, o string JSON, o string único.
fn id_list(val: Option<&DatomValue>) -> Vec<String> {
    match val {
        Some(DatomValue::Array(arr)) => arr.clone(),
        Some(DatomValue::Str(s)) => {
            if s.starts_with('[') {
                serde_json::from_str::<Vec<String>>(s).unwrap_or_else(|_| vec![s.clone()])
            } else {
                vec![s.clone()]
            }
        }
        _ => vec![],
    }
}

/// Lista de strings desde un datom (array o string único).
fn str_list(val: Option<&DatomValue>) -> Vec<String> {
    match val {
        Some(DatomValue::Array(arr)) => arr.clone(),
        Some(DatomValue::Str(s)) => vec![s.clone()],
        _ => vec![],
    }
}

fn ensure_not_suspended(status: &str, user_id: &str) -> Result<(), DomainError> {
    if status == "SUSPENDED" {
        return Err(
            DomainError::new(ErrorCode::Auth403, format!("User {user_id} is suspended"))
                .with_stage("cedar"),
        );
    }
    Ok(())
}

pub async fn step2_query_oltp(
    eav_reader: &dyn EntityReader,
    tenant_id: &str,
    user_id: &str,
    cache: &dyn PrincipalCache,
) -> Result<PrincipalData, DomainError> {
    if user_id == "usr_system_bff" {
        return Ok(system_bff_principal(tenant_id));
    }

    if let Some(cached) = cache.lookup_principal(user_id).await {
        ensure_not_suspended(&cached.status, user_id)?;
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

    let status = match attr(&user_map, "user", "status") {
        Some(DatomValue::Str(s)) => s.as_str(),
        _ => "ACTIVE",
    };
    ensure_not_suspended(status, user_id)?;

    let principal = assemble_principal_graph(eav_reader, tenant_id, user_id, user_map).await?;
    cache.store_principal(user_id, principal.clone()).await?;

    Ok(principal)
}

/// La cuenta BFF de sistema: wildcard de acciones sobre todos los dominios.
fn system_bff_principal(tenant_id: &str) -> PrincipalData {
    PrincipalData {
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
        groups: HashSet::new(),
    }
}

/// Expande (o no) un perímetro en una tarea propia para correr en paralelo
/// con su par locations/assets — el patrón estaba copiado cuatro veces.
fn spawn_expand(
    eav_reader: &dyn EntityReader,
    tenant_id: &str,
    entity_type: &'static str,
    roots: Vec<String>,
    expand_children: bool,
) -> tokio::task::JoinHandle<Result<Vec<String>, DomainError>> {
    let reader = eav_reader.clone_reader();
    let tenant = tenant_id.to_string();
    tokio::spawn(async move {
        if expand_children {
            expand_hierarchy(reader.as_ref(), &tenant, entity_type, roots).await
        } else {
            Ok(roots)
        }
    })
}

/// Interseca el perímetro del rol con el del grupo; un rol sin restricción
/// hereda el del grupo; sin grupo, manda el rol.
fn intersect_or_inherit(role: HashSet<String>, group: HashSet<String>) -> Vec<String> {
    if !group.is_empty() {
        if role.is_empty() {
            group.into_iter().collect()
        } else {
            role.intersection(&group).cloned().collect()
        }
    } else {
        role.into_iter().collect()
    }
}

pub async fn step3_consolidate(
    eav_reader: &dyn EntityReader,
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
    let group_locs_handle = spawn_expand(
        eav_reader,
        tenant_id,
        "location",
        user_data.group_allowed_locations.clone(),
        expand_children,
    );
    let group_assets_handle = spawn_expand(
        eav_reader,
        tenant_id,
        "asset",
        user_data.group_allowed_assets.clone(),
        expand_children,
    );

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

    for boundary in &user_data.roles_boundaries {
        consolidated_boundaries.push(
            consolidate_boundary(
                eav_reader,
                tenant_id,
                boundary,
                &expanded_group_locations,
                &expanded_group_assets,
                expand_children,
            )
            .await?,
        );
    }

    let mut final_principal = user_data;
    final_principal.roles_boundaries = consolidated_boundaries;
    Ok(final_principal)
}

async fn expand_hierarchy(
    eav_reader: &dyn EntityReader,
    tenant_id: &str,
    _entity_type: &str,
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
                let children = eav_reader
                    .children_with_attr(tenant_id, "parent_id", &DatomValue::Str(id.clone()))
                    .await?;
                next_level.extend(children);
            }
        }
        queue = next_level;
    }

    Ok(expanded.into_iter().collect())
}

/// Grants de un rol: string JSON, array de strings JSON o array de objetos —
/// tres formatos legados que conviven en los datos.
fn parse_grants(role_map: &EntityMap) -> Vec<serde_json::Value> {
    let grants: serde_json::Value = match attr(role_map, "role", "grants") {
        Some(DatomValue::Str(s)) => {
            let parsed: serde_json::Value =
                serde_json::from_str(s).unwrap_or_else(|_| serde_json::json!([]));
            if let serde_json::Value::Array(arr) = parsed {
                serde_json::Value::Array(
                    arr.into_iter()
                        .map(|item| match item {
                            serde_json::Value::String(str_val) => serde_json::from_str(&str_val)
                                .unwrap_or_else(|_| serde_json::json!(str_val)),
                            _ => item,
                        })
                        .collect(),
                )
            } else {
                parsed
            }
        }
        Some(DatomValue::Array(arr)) => serde_json::Value::Array(
            arr.iter()
                .map(|s| serde_json::from_str(s).unwrap_or_else(|_| serde_json::json!(s)))
                .collect(),
        ),
        _ => serde_json::json!([]),
    };
    grants.as_array().cloned().unwrap_or_default()
}

/// Pull del mapa de un rol, con fallback al tenant maestro si el rol no vive
/// en el tenant del usuario (roles compartidos).
async fn pull_role_map(
    eav_reader: &dyn EntityReader,
    tenant_id: &str,
    r_id: &str,
) -> Result<EntityMap, DomainError> {
    let role_map = eav_reader.pull(tenant_id, r_id, None).await?;
    if role_map.is_empty() && !is_master_tenant(tenant_id) {
        let master_tenant_id = crate::domain::config::engine_config()
            .master_tenant_id
            .clone();
        return eav_reader.pull(&master_tenant_id, r_id, None).await;
    }
    Ok(role_map)
}

pub async fn assemble_principal_graph(
    eav_reader: &dyn EntityReader,
    tenant_id: &str,
    user_id: &str,
    user_map: EntityMap,
) -> Result<PrincipalData, DomainError> {
    let user_type = match attr(&user_map, "user", "user_type") {
        Some(DatomValue::Str(s)) => s.clone(),
        _ => "INTERNAL".to_string(),
    };

    let company_id = match attr(&user_map, "user", "company_id") {
        Some(DatomValue::Str(s)) => s.clone(),
        _ => "".to_string(),
    };

    let status = match attr(&user_map, "user", "status") {
        Some(DatomValue::Str(s)) => s.clone(),
        _ => "ACTIVE".to_string(),
    };

    let (roles, roles_boundaries) = assemble_roles(eav_reader, tenant_id, &user_map).await?;
    let (group_allowed_locations, group_allowed_assets, mut time_restrictions, groups) =
        assemble_groups(eav_reader, tenant_id, &user_map).await?;

    if let Some(DatomValue::Str(s)) = attr(&user_map, "user", "time_restrictions") {
        if let Ok(res) = serde_json::from_str::<Vec<TimeRestriction>>(s) {
            time_restrictions = res;
        }
    }

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
        groups,
    })
}

/// Roles del usuario: ids, grants parseados y perímetros — pulls concurrentes.
async fn assemble_roles(
    eav_reader: &dyn EntityReader,
    tenant_id: &str,
    user_map: &EntityMap,
) -> Result<(HashSet<String>, Vec<RoleBoundary>), DomainError> {
    let mut roles = HashSet::new();
    let role_ids: Vec<String> = id_list(
        user_map
            .get("role_ids")
            .or_else(|| user_map.get("user/role_ids")),
    );

    let role_maps = join_all(
        role_ids
            .iter()
            .map(|r_id| pull_role_map(eav_reader, tenant_id, r_id)),
    )
    .await;

    let mut roles_boundaries = Vec::new();
    for (r_id, role_map_res) in role_ids.into_iter().zip(role_maps) {
        roles.insert(r_id.clone());

        let role_map = role_map_res?;
        roles_boundaries.push(RoleBoundary {
            role_id: r_id,
            grants: parse_grants(&role_map),
            permitted_locations: str_list(attr(&role_map, "role", "permitted_locations")),
            permitted_assets: str_list(attr(&role_map, "role", "permitted_assets")),
        });
    }

    Ok((roles, roles_boundaries))
}

/// Grupos del usuario en BFS por olas paralelas: unión de perímetros,
/// restricciones temporales heredadas y el conjunto visitado (ciclos fuera).
async fn assemble_groups(
    eav_reader: &dyn EntityReader,
    tenant_id: &str,
    user_map: &EntityMap,
) -> Result<
    (
        HashSet<String>,
        HashSet<String>,
        Vec<TimeRestriction>,
        HashSet<String>,
    ),
    DomainError,
> {
    let mut group_allowed_locations = HashSet::new();
    let mut group_allowed_assets = HashSet::new();
    let mut group_time_restrictions = Vec::new();
    let mut visited_groups = HashSet::new();

    let mut queue: Vec<String> = id_list(
        user_map
            .get("group_ids")
            .or_else(|| user_map.get("user/group_ids")),
    );

    while !queue.is_empty() {
        // Marca visitados al armar la ola: un grupo alcanzable por dos padres
        // se procesa una sola vez (el BFS original usaba pop() LIFO; el orden
        // de visita no afecta: las uniones son conmutativas).
        let level: Vec<String> = queue
            .drain(..)
            .filter(|g| visited_groups.insert(g.clone()))
            .collect();
        if level.is_empty() {
            break;
        }

        let pulls = join_all(
            level
                .iter()
                .map(|g_id| eav_reader.pull(tenant_id, g_id, None)),
        )
        .await;

        let mut next_level = Vec::new();
        for (_g_id, group_map_res) in level.into_iter().zip(pulls) {
            let group_map = group_map_res?;
            absorb_group(
                &group_map,
                &mut group_allowed_locations,
                &mut group_allowed_assets,
                &mut group_time_restrictions,
                &visited_groups,
                &mut next_level,
            );
        }
        queue = next_level;
    }

    Ok((
        group_allowed_locations,
        group_allowed_assets,
        group_time_restrictions,
        visited_groups,
    ))
}

/// Absorbe los atributos de un grupo del BFS: unión de perímetros,
/// restricciones temporales heredadas y encolo del padre si no es visitado.
fn absorb_group(
    group_map: &EntityMap,
    locations: &mut HashSet<String>,
    assets: &mut HashSet<String>,
    time_restrictions: &mut Vec<TimeRestriction>,
    visited: &HashSet<String>,
    next_level: &mut Vec<String>,
) {
    if group_map.is_empty() {
        return;
    }

    for loc in str_list(attr(group_map, "user_group", "allowed_locations")) {
        locations.insert(loc);
    }
    for asset in str_list(attr(group_map, "user_group", "allowed_assets")) {
        assets.insert(asset);
    }

    if let Some(DatomValue::Str(s)) = attr(group_map, "user_group", "time_restrictions") {
        if let Ok(res) = serde_json::from_str::<Vec<TimeRestriction>>(s) {
            time_restrictions.extend(res);
        }
    }

    if let Some(DatomValue::Str(parent_id)) = attr(group_map, "user_group", "parent_user_group_id")
    {
        if !parent_id.is_empty() && !visited.contains(parent_id) {
            next_level.push(parent_id.clone());
        }
    }
}

/// Expande y consolida UN boundary de rol contra los perímetros del grupo.
async fn consolidate_boundary(
    eav_reader: &dyn EntityReader,
    tenant_id: &str,
    boundary: &RoleBoundary,
    group_locations: &HashSet<String>,
    group_assets: &HashSet<String>,
    expand_children: bool,
) -> Result<RoleBoundary, DomainError> {
    let locs_handle = spawn_expand(
        eav_reader,
        tenant_id,
        "location",
        boundary.permitted_locations.clone(),
        expand_children,
    );
    let assets_handle = spawn_expand(
        eav_reader,
        tenant_id,
        "asset",
        boundary.permitted_assets.clone(),
        expand_children,
    );
    let (locs_res, assets_res) = tokio::join!(locs_handle, assets_handle);

    let expanded_locations: HashSet<String> = locs_res
        .map_err(|e| DomainError::new(ErrorCode::Infra001, e.to_string()))??
        .into_iter()
        .collect();
    let expanded_assets: HashSet<String> = assets_res
        .map_err(|e| DomainError::new(ErrorCode::Infra001, e.to_string()))??
        .into_iter()
        .collect();

    Ok(RoleBoundary {
        role_id: boundary.role_id.clone(),
        grants: boundary.grants.clone(),
        permitted_locations: intersect_or_inherit(expanded_locations, group_locations.clone()),
        permitted_assets: intersect_or_inherit(expanded_assets, group_assets.clone()),
    })
}
