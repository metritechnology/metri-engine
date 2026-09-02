// cedar/authorizer.rs — Integración con cedar-policy 3.x para ABAC.
// SRP: Evalúa peticiones gRPC/REST contra políticas de Cedar.
// [Blueprint: docs/architecture/06_FASE_CEDAR_AUTHORIZER.md]
//
// Fase 2: el grafo de principal vive en authorizer/principal_graph.rs y la
// evaluación step4 en authorizer/evaluator.rs; este archivo conserva el
// pipeline (step1/step3b/intercept), las reglas del sistema y los tipos.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use cedar_policy::{Authorizer, Decision, Entities, PolicySet, Request};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::reader::pull::EavReader;
use crate::eav::types::datom::DatomValue;
// --- Constants ---
const MAX_HIERARCHY_DEPTH: usize = 10;

// --- Structs & Traits ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleBoundary {
    pub role_id: String,
    pub grants: Vec<serde_json::Value>,
    pub permitted_locations: Vec<String>,
    pub permitted_assets: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRestriction {
    pub days_of_week: Vec<i32>,
    pub start_minute: u32,
    pub end_minute: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalData {
    pub user_id: String,
    pub tenant_id: String,
    pub status: String,
    pub user_type: String,
    pub company_id: String,
    pub roles: HashSet<String>,
    pub roles_boundaries: Vec<RoleBoundary>,
    pub time_restrictions: Vec<TimeRestriction>,
    pub group_allowed_locations: Vec<String>,
    pub group_allowed_assets: Vec<String>,
    pub groups: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct InvalidationMsg {
    pub tenant_id: String,
    pub entity_type: String,
    pub entity_id: String,
}

pub static INVALIDATION_TX: once_cell::sync::Lazy<tokio::sync::broadcast::Sender<InvalidationMsg>> =
    once_cell::sync::Lazy::new(|| {
        let (tx, _rx) = tokio::sync::broadcast::channel(100);
        tx
    });

use crate::domain::protocols::{ISessionStore, Session};

pub const MAX_SESSION_CACHE_SIZE: usize = 10_000;
pub const MAX_PRINCIPAL_CACHE_SIZE: usize = 5_000;

pub struct InMemorySessionStore {
    sessions: RwLock<HashMap<String, Session>>,
}

impl InMemorySessionStore {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, token: &str, session: Session) {
        if let Ok(mut lock) = self.sessions.write() {
            if lock.len() >= MAX_SESSION_CACHE_SIZE && !lock.contains_key(token) {
                let to_remove: Vec<String> = lock
                    .keys()
                    .take(MAX_SESSION_CACHE_SIZE / 5)
                    .cloned()
                    .collect();
                for k in to_remove {
                    lock.remove(&k);
                }
            }
            lock.insert(token.to_string(), session);
        }
    }
}

#[async_trait]
impl ISessionStore for InMemorySessionStore {
    async fn get_session(&self, token: &str) -> Result<Option<Session>, DomainError> {
        if let Ok(lock) = self.sessions.read() {
            Ok(lock.get(token).cloned())
        } else {
            Ok(None)
        }
    }

    async fn revoke_session(&self, _jti: &str, _ttl_seconds: u64) -> Result<(), DomainError> {
        Ok(())
    }

    async fn unrevoke_session(&self, _jti: &str) -> Result<(), DomainError> {
        Ok(())
    }
}

#[async_trait]
pub trait PrincipalCache: Send + Sync {
    async fn lookup_principal(&self, user_id: &str) -> Option<PrincipalData>;
    async fn store_principal(
        &self,
        user_id: &str,
        principal: PrincipalData,
    ) -> Result<(), DomainError>;
    async fn evict_user(&self, user_id: &str) -> Result<(), DomainError>;
    async fn evict_by_role(&self, role_id: &str) -> Result<(), DomainError>;
}

pub struct InMemoryPrincipalCache {
    cache: Arc<RwLock<HashMap<String, PrincipalData>>>,
}

impl InMemoryPrincipalCache {
    pub fn new() -> Self {
        let cache = Arc::new(RwLock::new(HashMap::<String, PrincipalData>::new()));
        let cache_clone = cache.clone();

        tokio::spawn(async move {
            let mut rx = INVALIDATION_TX.subscribe();
            while let Ok(msg) = rx.recv().await {
                // Evict from EAV_CACHE
                let eav_pk = format!("T#{}#E#{}", msg.tenant_id, msg.entity_id);
                if let Ok(mut eav_lock) = crate::eav::reader::pull::EAV_CACHE.write() {
                    eav_lock.remove(&eav_pk);
                    tracing::info!("[CacheInvalidation] Evicted from EAV_CACHE: {}", eav_pk);
                }

                // Evict from InMemoryPrincipalCache
                if let Ok(mut principal_lock) = cache_clone.write() {
                    match msg.entity_type.as_str() {
                        "user" => {
                            principal_lock.remove(&msg.entity_id);
                            tracing::info!(
                                "[CacheInvalidation] Evicted User {} from Principal Cache",
                                msg.entity_id
                            );
                        }
                        "role" => {
                            let before_len = principal_lock.len();
                            principal_lock.retain(|_, v| !v.roles.contains(&msg.entity_id));
                            let evicted_count = before_len - principal_lock.len();
                            tracing::info!("[CacheInvalidation] Evicted {} users having Role {} from Principal Cache", evicted_count, msg.entity_id);
                        }
                        "user_group" => {
                            let before_len = principal_lock.len();
                            principal_lock.retain(|_, v| !v.groups.contains(&msg.entity_id));
                            let evicted_count = before_len - principal_lock.len();
                            tracing::info!("[CacheInvalidation] Evicted {} users in Group {} from Principal Cache", evicted_count, msg.entity_id);
                        }
                        _ => {}
                    }
                }
            }
        });

        Self { cache }
    }
}

#[async_trait]
impl PrincipalCache for InMemoryPrincipalCache {
    async fn lookup_principal(&self, user_id: &str) -> Option<PrincipalData> {
        if let Ok(lock) = self.cache.read() {
            lock.get(user_id).cloned()
        } else {
            None
        }
    }

    async fn store_principal(
        &self,
        user_id: &str,
        principal: PrincipalData,
    ) -> Result<(), DomainError> {
        if let Ok(mut lock) = self.cache.write() {
            if lock.len() >= MAX_PRINCIPAL_CACHE_SIZE && !lock.contains_key(user_id) {
                let to_remove: Vec<String> = lock
                    .keys()
                    .take(MAX_PRINCIPAL_CACHE_SIZE / 5)
                    .cloned()
                    .collect();
                for k in to_remove {
                    lock.remove(&k);
                }
            }
            lock.insert(user_id.to_string(), principal);
        }
        Ok(())
    }

    async fn evict_user(&self, user_id: &str) -> Result<(), DomainError> {
        if let Ok(mut lock) = self.cache.write() {
            lock.remove(user_id);
        }
        Ok(())
    }

    async fn evict_by_role(&self, role_id: &str) -> Result<(), DomainError> {
        if let Ok(mut lock) = self.cache.write() {
            lock.retain(|_, v| !v.roles.contains(role_id));
        }
        Ok(())
    }
}

// --- CedarAuthorizer ---

pub struct CedarAuthorizer {
    authorizer: Authorizer,
}

impl CedarAuthorizer {
    pub fn new() -> Self {
        Self {
            authorizer: Authorizer::new(),
        }
    }

    pub fn is_authorized(
        &self,
        policies: &PolicySet,
        entities: &Entities,
        request: &Request,
    ) -> Result<bool, DomainError> {
        let response = self.authorizer.is_authorized(request, policies, entities);

        match response.decision() {
            Decision::Allow => {
                info!("[Cedar ABAC] Request ALLOWED");
                Ok(true)
            }
            Decision::Deny => {
                warn!(
                    "[Cedar ABAC] Request DENIED by policies: {:?}",
                    response.diagnostics().reason().collect::<Vec<_>>()
                );
                Ok(false)
            }
        }
    }
}

pub fn verify_hmac_token_local_in_step(raw_token: &str) -> Option<Session> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use constant_time_eq::constant_time_eq;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let token = raw_token.strip_prefix("mk_")?;
    let dot = token.find('.')?;
    let (payload_b64, sig_b64) = token.split_at(dot);
    let sig_b64 = &sig_b64[1..];

    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let hmac_secret = std::env::var("HMAC_SECRET")
        .unwrap_or_else(|_| "secret-key-development-metri-256-bits!!!".to_string());

    let mut mac = HmacSha256::new_from_slice(hmac_secret.as_bytes()).ok()?;
    mac.update(&payload_bytes);
    let expected_sig = mac.finalize().into_bytes();
    let provided_sig = URL_SAFE_NO_PAD.decode(sig_b64).ok()?;

    if !constant_time_eq(&expected_sig, &provided_sig) {
        return None;
    }

    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;
    let now = chrono::Utc::now().timestamp();
    let exp = claims["exp"].as_i64()?;
    if exp <= now {
        return None;
    }

    Some(Session {
        tenant_id: claims["tid"].as_str()?.to_string(),
        user_id: claims["uid"].as_str()?.to_string(),
        jti: claims["jti"].as_str()?.to_string(),
        exp,
    })
}

pub fn is_master_tenant(tenant_id: &str) -> bool {
    let env_master: &str = &crate::domain::config::engine_config().master_tenant_id;
    tenant_id == "system" || tenant_id == "tnt_master" || tenant_id == env_master
}

/// Rules for system and master tenant entities (like `tenant` and `domain_quota`).
pub struct SystemSecurityRules;

impl SystemSecurityRules {
    /// Returns true if the entity type is a master-only system entity.
    pub fn is_master_only_entity(entity_type: &str) -> bool {
        matches!(
            entity_type,
            "tenant" | "domain_quota" | "quota" | "domain_plugin" | "tenant_plugin"
        )
    }

    /// Checks if a user is authorized to perform CRUD operations (Query, Explore, Mutation)
    /// on a given entity type.
    /// Master-only entities can only be accessed by master tenant users or the system BFF account.
    pub fn check_crud_authorization(
        entity_type: &str,
        tenant_id: &str,
        user_id: &str,
        action: &str,
    ) -> Result<(), DomainError> {
        if Self::is_master_only_entity(entity_type) {
            let is_master = is_master_tenant(tenant_id);
            let is_system_bff = user_id == "usr_system_bff";
            if !is_master && !is_system_bff {
                let msg = match action {
                    "mutate" => format!(
                        "Auth403: Only master tenant users can mutate {}",
                        entity_type
                    ),
                    "read" => {
                        "Auth403: Only master tenant users can read tenants or quotas".to_string()
                    }
                    "explore" => format!(
                        "Auth403: Only master tenant users can explore {}",
                        entity_type
                    ),
                    _ => format!(
                        "Auth403: Only master tenant users can access {}",
                        entity_type
                    ),
                };
                return Err(DomainError::new(ErrorCode::Auth403, msg).with_stage("security_rules"));
            }
        }
        Ok(())
    }

    /// Checks tenant isolation for a mutation or query.
    /// Restricts cross-tenant actions unless the caller is master or system BFF.
    pub fn check_tenant_isolation(
        target_tenant_id: &str,
        caller_tenant_id: &str,
        caller_user_id: &str,
    ) -> Result<(), DomainError> {
        let is_master = is_master_tenant(caller_tenant_id);
        let is_system_bff = caller_user_id == "usr_system_bff";
        if target_tenant_id != caller_tenant_id && !is_master && !is_system_bff {
            return Err(
                DomainError::new(ErrorCode::Auth403, "Auth403: Tenant mismatch")
                    .with_stage("security_rules"),
            );
        }
        Ok(())
    }

    /// Returns true if the entity type is exempt from quota validation and usage increment (unlimited).
    pub fn is_quota_exempt(entity_type: &str) -> bool {
        matches!(
            entity_type,
            "tenant" | "domain_quota" | "quota" | "domain_plugin" | "tenant_plugin"
        )
    }
}

// --- Public Helper Interceptor Methods ---

pub async fn step1_extract_token<T>(
    req: &tonic::Request<T>,
    valkey_store: &dyn ISessionStore,
) -> Result<Session, DomainError> {
    let token = if let Some(auth_header) = req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
    {
        auth_header
            .strip_prefix("Bearer ")
            .or_else(|| auth_header.strip_prefix("bearer "))
            .unwrap_or(auth_header)
            .trim()
            .to_string()
    } else if let Some(sid_header) = req.metadata().get("sid").and_then(|v| v.to_str().ok()) {
        sid_header.trim().to_string()
    } else {
        return Err(
            DomainError::new(ErrorCode::Auth401, "Missing authorization or sid header")
                .with_stage("cedar"),
        );
    };

    if token.starts_with("mk_") {
        if let Some(session) = verify_hmac_token_local_in_step(&token) {
            return Ok(session);
        }
    }

    let session = valkey_store.get_session(&token).await?.ok_or_else(|| {
        DomainError::new(ErrorCode::Auth401, "Invalid or expired token").with_stage("cedar")
    })?;

    if session.user_id.is_empty() || session.tenant_id.is_empty() {
        return Err(
            DomainError::new(ErrorCode::Auth401, "Malformed session payload from Valkey")
                .with_stage("cedar"),
        );
    }

    Ok(session)
}

mod evaluator;
mod principal_graph;

pub use evaluator::step4_evaluate_cedar;
#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use evaluator::{step4_analytical, step4_mutational};
pub use principal_graph::{assemble_principal_graph, step2_query_oltp, step3_consolidate};

pub fn step3b_validate_time_window(
    principal: &PrincipalData,
    now: DateTime<Utc>,
) -> Result<(), DomainError> {
    if principal.time_restrictions.is_empty() {
        return Ok(());
    }

    let day = now.weekday().number_from_monday() as i32;
    let minute = now.hour() * 60 + now.minute();

    let matches_any = principal.time_restrictions.iter().any(|r| {
        r.days_of_week.contains(&day) && minute >= r.start_minute && minute <= r.end_minute
    });

    if !matches_any {
        return Err(
            DomainError::new(ErrorCode::Auth403, "Access outside allowed time window")
                .with_stage("cedar"),
        );
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct CedarContext {
    pub tenant_id: String,
    pub user_id: String,
    pub roles: HashSet<String>,
    pub domain_boundaries: serde_json::Value,
}

pub async fn intercept<T>(
    req: &tonic::Request<T>,
    valkey_store: &dyn ISessionStore,
    eav_reader: &EavReader,
    cache: &dyn PrincipalCache,
    cedar_engine: &CedarAuthorizer,
    policy_cache: &HashMap<String, PolicySet>,
) -> Result<CedarContext, DomainError> {
    let session = step1_extract_token(req, valkey_store).await?;
    let raw_principal =
        step2_query_oltp(eav_reader, &session.tenant_id, &session.user_id, cache).await?;
    let principal = step3_consolidate(eav_reader, &session.tenant_id, raw_principal, true).await?;
    step3b_validate_time_window(&principal, Utc::now())?;

    let action = extract_cedar_action(req)?;
    let mut resource = extract_cedar_resource(req)?;
    let body = extract_req_body(req)?;

    let resource_id = resource
        .get("entity_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if !resource_id.is_empty() {
        if let Ok(entity_map) = eav_reader.pull(&session.tenant_id, resource_id, None).await {
            if let Some(obj) = resource.as_object_mut() {
                for key in &[
                    "assigned_company_id",
                    "company_id",
                    "assigned_company",
                    "company",
                ] {
                    let entity_type = obj
                        .get("entity_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if let Some(val) = entity_map
                        .get(*key)
                        .or_else(|| entity_map.get(&format!("{}/{}", entity_type, key)))
                    {
                        match val {
                            DatomValue::Str(s) => {
                                obj.insert("assigned_company_id".to_string(), serde_json::json!(s));
                                break;
                            }
                            DatomValue::Ref(r) => {
                                obj.insert("assigned_company_id".to_string(), serde_json::json!(r));
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    let domain_dict = step4_evaluate_cedar(
        cedar_engine,
        policy_cache,
        &principal,
        &action,
        &resource,
        &body,
    )?;

    Ok(CedarContext {
        tenant_id: session.tenant_id,
        user_id: session.user_id,
        roles: principal.roles,
        domain_boundaries: domain_dict,
    })
}

fn extract_cedar_action<T>(req: &tonic::Request<T>) -> Result<String, DomainError> {
    if let Some(act) = req
        .metadata()
        .get("x-metri-action")
        .and_then(|v| v.to_str().ok())
    {
        Ok(act.to_string())
    } else {
        Ok("QueryMetrics".to_string())
    }
}

fn extract_cedar_resource<T>(req: &tonic::Request<T>) -> Result<serde_json::Value, DomainError> {
    let entity_type = req
        .metadata()
        .get("x-metri-entity-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("project");

    let domains = req
        .metadata()
        .get("x-metri-domains")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            s.split(',')
                .map(|d| d.trim().to_string())
                .collect::<Vec<String>>()
        })
        .unwrap_or_else(|| vec![entity_type.to_string()]);

    let entity_id = req
        .metadata()
        .get("x-metri-entity-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    Ok(serde_json::json!({
        "entity_type": entity_type,
        "entity_id": entity_id,
        "domains": domains,
    }))
}

fn extract_req_body<T>(_req: &tonic::Request<T>) -> Result<serde_json::Value, DomainError> {
    Ok(serde_json::json!({}))
}

pub async fn get_principal_data<T>(
    req: &tonic::Request<T>,
    valkey_store: &dyn ISessionStore,
    eav_reader: &EavReader,
    cache: &dyn PrincipalCache,
    dev_auth_bypass: bool,
) -> Result<PrincipalData, DomainError> {
    // Costura explícita de desarrollo: `dev_auth_bypass` lo decide la raíz de
    // composición vía `resolve_dev_auth_bypass` (fail-closed) — nunca el
    // entorno por petición. En producción es siempre `false`.
    if dev_auth_bypass {
        let test_tenant = req
            .metadata()
            .get("test-tenant")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("system")
            .to_string();
        let test_user = req
            .metadata()
            .get("test-user")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("usr_system_bff")
            .to_string();
        let test_roles = req
            .metadata()
            .get("test-roles")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(',').map(|r| r.to_string()).collect())
            .unwrap_or_else(|| ["system-admin".to_string()].into_iter().collect());

        return Ok(PrincipalData {
            user_id: test_user,
            tenant_id: test_tenant,
            status: "ACTIVE".to_string(),
            user_type: "SYSTEM".to_string(),
            company_id: String::new(),
            roles: test_roles,
            roles_boundaries: vec![],
            time_restrictions: vec![],
            group_allowed_locations: vec![],
            group_allowed_assets: vec![],
            groups: std::collections::HashSet::new(),
        });
    }
    let session = step1_extract_token(req, valkey_store).await?;
    let raw_principal =
        step2_query_oltp(eav_reader, &session.tenant_id, &session.user_id, cache).await?;
    let principal = step3_consolidate(eav_reader, &session.tenant_id, raw_principal, true).await?;
    step3b_validate_time_window(&principal, Utc::now())?;
    Ok(principal)
}

// --- Temporal Stub Legacy Method ---
pub fn always_allow_stub(tenant_id: &str) -> Result<bool, DomainError> {
    if tenant_id == "evil-tenant" {
        warn!("[Cedar STUB] Access Denied para evil-tenant");
        Err(DomainError::new(ErrorCode::Janus403, "access-denied").with_stage("cedar"))
    } else {
        info!("[Cedar STUB] AlwaysAllow activo");
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eav::reader::pull::{CacheEntry, EAV_CACHE};
    use cedar_policy::EntityUid;
    use cedar_policy::{Context, Schema};
    use chrono::TimeZone;
    use std::str::FromStr;

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
        let res = step1_extract_token(&req, &valkey).await;
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

        let ddb =
            Arc::new(crate::infrastructure::dynamodb::DynamoClient::new("metri-dynamo").await);
        let eav_reader = EavReader::new(ddb, "metri-dynamo");
        let res = step2_query_oltp(&eav_reader, "tnt_01", "usr_susp", &cache).await;
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

        let ddb =
            Arc::new(crate::infrastructure::dynamodb::DynamoClient::new("invalid-table").await);
        let eav_reader = EavReader::new(ddb, "invalid-table");
        let res = step2_query_oltp(&eav_reader, "tnt_01", "usr_cached", &cache).await;
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
        // Populate global EAV Cache
        {
            let mut cache = EAV_CACHE.write().unwrap();
            cache.insert(
                "T#tnt_01#E#usr_hierarchy_test".to_string(),
                CacheEntry {
                    is_complete: true,
                    map: user_map,
                },
            );
            cache.insert(
                "T#tnt_01#E#role_user".to_string(),
                CacheEntry {
                    is_complete: true,
                    map: role_map,
                },
            );
            cache.insert(
                "T#tnt_01#E#group_sub".to_string(),
                CacheEntry {
                    is_complete: true,
                    map: group_sub_map,
                },
            );
            cache.insert(
                "T#tnt_01#E#group_parent".to_string(),
                CacheEntry {
                    is_complete: true,
                    map: group_parent_map,
                },
            );
        }

        let ddb =
            Arc::new(crate::infrastructure::dynamodb::DynamoClient::new("invalid-table").await);
        let eav_reader = EavReader::new(ddb, "invalid-table");

        // 2. Load principal graph (resolves roles & user groups recursively)
        let principal =
            assemble_principal_graph(&eav_reader, "tnt_01", "usr_hierarchy_test", user_map_clone)
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
        // La jerarquía del test vive en los mapas en caché (punteros al
        // padre); no se necesita la expansión por índice AVET.
        let consolidated = step3_consolidate(&eav_reader, "tnt_01", principal, false)
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
        std::env::set_var("METRI_MASTER_TENANT_ID", "system");

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
            &serde_json::json!({}),
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
            &serde_json::json!({}),
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
            &serde_json::json!({}),
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
            &serde_json::json!({}),
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
            &serde_json::json!({}),
        );
        assert!(
            res_none.is_err(),
            "Contractor should be denied access if resource has no company assigned"
        );
        assert_eq!(res_none.unwrap_err().code, ErrorCode::Auth403);
    }

    #[test]
    fn test_verify_hmac_token_debug() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;

        let secret = "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2";
        std::env::set_var("HMAC_SECRET", secret);

        let exp = chrono::Utc::now().timestamp() + 3600;
        let claims = serde_json::json!({
            "exp": exp,
            "iat": exp - 3600,
            "jti": "e2575d40e197f5180121dcfb9470b3dc",
            "tid": "system",
            "uid": "usr_system_bff"
        });
        let payload_bytes = serde_json::to_vec(&claims).unwrap();
        let payload_b64 = URL_SAFE_NO_PAD.encode(&payload_bytes);

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(&payload_bytes);
        let sig = mac.finalize().into_bytes();
        let sig_b64 = URL_SAFE_NO_PAD.encode(sig);

        let token = format!("mk_{}.{}", payload_b64, sig_b64);

        let res = verify_hmac_token_local_in_step(&token);
        println!("DEBUG TOKEN RESULT: {:?}", res);
        assert!(res.is_some());
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
}
