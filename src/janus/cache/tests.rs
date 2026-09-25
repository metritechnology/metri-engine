//! Unit tests del frontend de caché — sin infraestructura (§11.1).
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::json;

use super::*;
use crate::domain::protocols::{CacheEntry, CachePut, DomainResult, IQueryCache};

// ── Reloj controlable ─────────────────────────────────────────────────────────

static TEST_NOW: AtomicI64 = AtomicI64::new(1_700_000_000);

fn test_now() -> i64 {
    TEST_NOW.load(Ordering::SeqCst)
}

fn set_now(ts: i64) {
    TEST_NOW.store(ts, Ordering::SeqCst);
}

// ── Fakes ────────────────────────────────────────────────────────────────────

/// Backend fake con entradas precargadas y registro de puts (spy).
struct FakeCache {
    entries: Mutex<std::collections::HashMap<String, CacheEntry>>,
    puts: Mutex<Vec<String>>,
}

impl FakeCache {
    fn new() -> Self {
        Self {
            entries: Mutex::new(std::collections::HashMap::new()),
            puts: Mutex::new(Vec::new()),
        }
    }

    fn seed(&self, key: &str, entry: CacheEntry) {
        self.entries.lock().unwrap().insert(key.to_string(), entry);
    }

    fn put_count(&self, key: &str) -> usize {
        self.puts
            .lock()
            .unwrap()
            .iter()
            .filter(|k| *k == key)
            .count()
    }
}

#[async_trait]
impl IQueryCache for FakeCache {
    async fn get(&self, key: &str, _tenant_id: &str) -> DomainResult<Option<CacheEntry>> {
        Ok(self.entries.lock().unwrap().get(key).cloned())
    }
    async fn put(&self, entry: &CachePut) -> DomainResult<()> {
        self.puts.lock().unwrap().push(entry.key.clone());
        let mut map = self.entries.lock().unwrap();
        map.insert(
            entry.key.clone(),
            CacheEntry {
                payload: entry.payload.clone(),
                tenant_id: entry.tenant_id.clone(),
                expires_at: test_now() + entry.ttl_secs as i64,
                generation: None,
                stored_at_ms: test_now() * 1000,
            },
        );
        Ok(())
    }
}

/// Backend que siempre falla — prueba el fail-open.
struct FailingCache;

#[async_trait]
impl IQueryCache for FailingCache {
    async fn get(&self, _k: &str, _t: &str) -> DomainResult<Option<CacheEntry>> {
        Err(crate::domain::errors::DomainError::infra(
            crate::domain::errors::ErrorCode::Infra001,
            "failing cache",
        ))
    }
    async fn put(&self, _e: &CachePut) -> DomainResult<()> {
        Err(crate::domain::errors::DomainError::infra(
            crate::domain::errors::ErrorCode::Infra001,
            "failing cache",
        ))
    }
}

fn entry(tenant: &str, expires_at: i64) -> CacheEntry {
    CacheEntry {
        payload: json!({"data": [1, 2], "total": 2}),
        tenant_id: tenant.to_string(),
        expires_at,
        generation: None,
        stored_at_ms: 0,
    }
}

fn ddb_frontend(backend: Arc<dyn IQueryCache>) -> QueryCacheFrontend {
    let mut policy = QueryCachePolicy::default();
    policy.mode = CacheMode::Ddb;
    QueryCacheFrontend::ddb(backend, policy).with_clock(test_now)
}

fn cand<'a>(tenant: &'a str, entity: &'a str) -> CacheCandidate<'a> {
    CacheCandidate {
        channel: CacheChannel::Oltp,
        tenant_id: tenant,
        entity,
        explain_plan: false,
        overlay_entity: false,
    }
}

// ── evaluate: gates G0-G5 ─────────────────────────────────────────────────────

#[test]
fn gate_mode_off_bypasses_everything() {
    let fe = QueryCacheFrontend::disabled();
    assert!(matches!(
        fe.evaluate(&cand("t1", "asset")),
        LookupOutcome::Bypass {
            reason: BypassReason::ModeOff
        }
    ));
}

#[test]
fn gate_explain_plan_never_caches() {
    let fe = ddb_frontend(Arc::new(FakeCache::new()));
    let mut c = cand("t1", "asset");
    c.explain_plan = true;
    assert!(matches!(
        fe.evaluate(&c),
        LookupOutcome::Bypass {
            reason: BypassReason::ExplainPlan
        }
    ));
}

#[test]
fn gate_overlay_entity_never_caches() {
    let fe = ddb_frontend(Arc::new(FakeCache::new()));
    let mut c = cand("t1", "domain_quota");
    c.overlay_entity = true;
    assert!(matches!(
        fe.evaluate(&c),
        LookupOutcome::Bypass {
            reason: BypassReason::OverlayEntity
        }
    ));
}

#[test]
fn gate_history_path_bypasses() {
    let fe = ddb_frontend(Arc::new(FakeCache::new()));
    assert!(matches!(
        fe.evaluate(&cand("t1", "asset_history")),
        LookupOutcome::Bypass {
            reason: BypassReason::HistoryPath
        }
    ));
}

#[test]
fn gate_channel_disabled_bypasses() {
    let mut policy = QueryCachePolicy::default();
    policy.mode = CacheMode::Ddb;
    policy.channels.olap = false;
    let fe = QueryCacheFrontend::ddb(Arc::new(FakeCache::new()), policy).with_clock(test_now);
    let c = CacheCandidate {
        channel: CacheChannel::Olap,
        ..cand("t1", "work_order")
    };
    assert!(matches!(
        fe.evaluate(&c),
        LookupOutcome::Bypass {
            reason: BypassReason::ChannelDisabled
        }
    ));
}

#[test]
fn gate_tenant_allowlist_bypasses_non_canary() {
    let mut policy = QueryCachePolicy::default();
    policy.mode = CacheMode::Ddb;
    policy.tenant_allowlist = Some(vec!["canario".to_string()]);
    let fe = QueryCacheFrontend::ddb(Arc::new(FakeCache::new()), policy).with_clock(test_now);
    assert!(matches!(
        fe.evaluate(&cand("otro", "asset")),
        LookupOutcome::Bypass {
            reason: BypassReason::TenantNotAllowlisted
        }
    ));
    assert!(matches!(
        fe.evaluate(&cand("canario", "asset")),
        LookupOutcome::Miss
    ));
}

// ── lookup: corrección ────────────────────────────────────────────────────────

#[tokio::test]
async fn lookup_serves_fresh_entry_of_same_tenant() {
    let fake = Arc::new(FakeCache::new());
    fake.seed("QC#qc1#abc", entry("t1", test_now() + 50));
    let fe = ddb_frontend(fake);
    match fe.lookup("QC#qc1#abc", "t1", CacheChannel::Oltp).await {
        LookupOutcome::Hit { entry } => assert_eq!(entry.tenant_id, "t1"),
        other => panic!("esperaba Hit, obtuve {other:?}"),
    }
}

#[tokio::test]
async fn lookup_expired_entry_is_miss() {
    let fake = Arc::new(FakeCache::new());
    fake.seed("QC#qc1#abc", entry("t1", test_now() - 1));
    let fe = ddb_frontend(fake);
    assert!(matches!(
        fe.lookup("QC#qc1#abc", "t1", CacheChannel::Oltp).await,
        LookupOutcome::Miss
    ));
}

#[tokio::test]
async fn lookup_tenant_mismatch_is_miss() {
    let fake = Arc::new(FakeCache::new());
    fake.seed("QC#qc1#abc", entry("otro-tenant", test_now() + 50));
    let fe = ddb_frontend(fake);
    assert!(matches!(
        fe.lookup("QC#qc1#abc", "t1", CacheChannel::Oltp).await,
        LookupOutcome::Miss
    ));
}

#[tokio::test]
async fn lookup_backend_error_degrades_open() {
    let fe = ddb_frontend(Arc::new(FailingCache));
    assert!(matches!(
        fe.lookup("QC#qc1#abc", "t1", CacheChannel::Oltp).await,
        LookupOutcome::Miss
    ));
}

// ── store: cap y spawn ────────────────────────────────────────────────────────

#[tokio::test]
async fn store_respects_size_cap_and_marks_oversize() {
    let fake = Arc::new(FakeCache::new());
    let mut policy = QueryCachePolicy::default();
    policy.mode = CacheMode::Ddb;
    policy.max_item_bytes = 64;
    let fe = QueryCacheFrontend::ddb(Arc::clone(&fake) as Arc<dyn IQueryCache>, policy)
        .with_clock(test_now);

    let big = json!({"data": vec!["x".repeat(200)]});
    fe.store("QC#qc1#big", "t1", CacheChannel::Oltp, &big).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        fake.put_count("QC#qc1#big"),
        0,
        "payload oversize no debe guardarse"
    );

    // Segundo store de la misma clave: ni siquiera intenta serializar.
    fe.store("QC#qc1#big", "t1", CacheChannel::Oltp, &big).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(fake.put_count("QC#qc1#big"), 0);
}

#[tokio::test]
async fn store_happy_path_puts_in_background() {
    let fake = Arc::new(FakeCache::new());
    let fe = ddb_frontend(Arc::clone(&fake) as Arc<dyn IQueryCache>);
    fe.store(
        "QC#qc1#ok",
        "t1",
        CacheChannel::Oltp,
        &json!({"data": [1], "total": 1}),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(fake.put_count("QC#qc1#ok"), 1);
}

#[tokio::test]
async fn store_is_noop_in_off_and_shadow() {
    let fake = Arc::new(FakeCache::new());
    let fe_off = QueryCacheFrontend::disabled().with_clock(test_now);
    fe_off
        .store("QC#qc1#x", "t1", CacheChannel::Oltp, &json!({}))
        .await;

    let mut policy = QueryCachePolicy::default();
    policy.mode = CacheMode::Shadow;
    let fe_shadow = QueryCacheFrontend::shadow(policy).with_clock(test_now);
    fe_shadow
        .store("QC#qc1#x", "t1", CacheChannel::Oltp, &json!({}))
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(fake.put_count("QC#qc1#x"), 0);
}

// ── shadow: la respuesta nunca cambia ─────────────────────────────────────────

#[tokio::test]
async fn shadow_mode_never_changes_outcome() {
    let mut policy = QueryCachePolicy::default();
    policy.mode = CacheMode::Shadow;
    let fe = QueryCacheFrontend::shadow(policy).with_clock(test_now);

    // Primera observación: miss; segunda (dentro de TTL): would-be-hit.
    assert!(matches!(
        fe.lookup("QC#qc1#k", "t1", CacheChannel::Oltp).await,
        LookupOutcome::Miss
    ));
    assert!(matches!(
        fe.lookup("QC#qc1#k", "t1", CacheChannel::Oltp).await,
        LookupOutcome::Miss // SIEMPRE Miss aunque el ring tenga la clave
    ));

    // Tras expirar el TTL del ring, vuelve a medir miss.
    set_now(test_now() + 61);
    assert!(matches!(
        fe.lookup("QC#qc1#k", "t1", CacheChannel::Oltp).await,
        LookupOutcome::Miss
    ));
}

// ── metadata: body.cache → metadata del chunk (§8.1) ─────────────────────────

#[test]
fn metadata_reflects_cache_status_on_hit() {
    let body = serde_json::json!({
        "channel": "oltp",
        "total": 1,
        "execution_time_ms": 5,
        "cache": {"hit": true, "remaining_secs": 42, "channel": "oltp"},
    });
    let normalized = crate::janus::normalizer::normalize_chunk(&body);
    let meta = normalized.get("metadata").unwrap();
    assert_eq!(meta.get("cache_hits").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(
        meta.get("cache_ttl_seconds").and_then(|v| v.as_i64()),
        Some(42)
    );
    // El campo de transporte se consume: no viaja en el body final.
    assert!(normalized.get("cache").is_none());
}

#[test]
fn metadata_defaults_to_zero_without_cache_block() {
    let body = serde_json::json!({"channel": "oltp", "total": 1, "execution_time_ms": 5});
    let normalized = crate::janus::normalizer::normalize_chunk(&body);
    let meta = normalized.get("metadata").unwrap();
    assert_eq!(meta.get("cache_hits").and_then(|v| v.as_i64()), Some(0));
    assert_eq!(
        meta.get("cache_ttl_seconds").and_then(|v| v.as_i64()),
        Some(0)
    );
}
