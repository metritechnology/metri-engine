//! Frontend de caché read-through para la capa de consultas de Janus (`janus::cache`).
//!
//! El frontend posee la **política** (modos `Off` | `Shadow` | `Ddb`, compuertas de bypass y
//! anillos de medición); el backend posee el **almacenamiento** (puerto `IQueryCache`).
//! El modo `Shadow` registra el *hit rate* candidato y devuelve siempre `Miss`, manteniendo
//! la respuesta byte-idéntica al modo `Off`.
//!
//! Doble política de fallo:
//! - *Fail-open* en disponibilidad: cualquier error de backend degrada a bypass con `warn!`.
//! - *Fail-closed* en corrección: una entrada vencida, de otro tenant o con generación
//!   invalidada nunca se sirve.

#[cfg(test)]
mod cache_tests;
pub mod keys;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use rand::Rng;
use serde_json::Value;
use tracing::{info, warn};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::{CacheEntry, CachePut, DomainResult, IQueryCache};

// ── Modos y política ──────────────────────────────────────────────────────────

/// Modo de operación de la caché (D5/D7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMode {
    /// Null Object: sin caché. El motor arranca sin tabla (invariante
    /// "el engine levanta sin AWS").
    Off,
    /// Medición: calcula claves y registra hit rate candidato SIN tocar
    /// el almacenamiento (cero I/O). Límite inferior del hit rate distribuido.
    Shadow,
    /// Caché activa contra el adaptador de almacenamiento configurado.
    Ddb,
}

/// Canal del read path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheChannel {
    /// Canal transaccional e indexado OLTP.
    Oltp,
    /// Canal analítico y de agregación OLAP.
    Olap,
}

/// Encendido por canal (`QUERY_CACHE_CHANNELS`).
#[derive(Debug, Clone, Copy)]
pub struct ChannelFlags {
    /// Indica si la caché está habilitada para el canal OLTP.
    pub oltp: bool,
    /// Indica si la caché está habilitada para el canal OLAP.
    pub olap: bool,
}

/// Política resuelta UNA vez en la raíz de composición.
#[derive(Debug, Clone)]
pub struct QueryCachePolicy {
    /// Modo operativo activo de la caché (`Off`, `Shadow` o `Ddb`).
    pub mode: CacheMode,
    /// Banderas de activación por canal (`oltp`, `olap`).
    pub channels: ChannelFlags,
    /// Tiempo de vida base en segundos para entradas del canal OLTP.
    pub ttl_oltp_secs: u64,
    /// Tiempo de vida base en segundos para entradas del canal OLAP.
    pub ttl_olap_secs: u64,
    /// Amplitud máxima de jitter aleatorio en segundos sumado al TTL base.
    pub ttl_jitter_secs: u64,
    /// Tamaño máximo en bytes permitido para almacenar una entrada en caché.
    pub max_item_bytes: usize,
    /// `None` = sin restricción; `Some(lista)` = canary de producción (G0).
    pub tenant_allowlist: Option<Vec<String>>,
}

impl Default for QueryCachePolicy {
    fn default() -> Self {
        Self {
            mode: CacheMode::Off,
            channels: ChannelFlags {
                oltp: true,
                olap: true,
            },
            ttl_oltp_secs: 60,
            ttl_olap_secs: 60,
            ttl_jitter_secs: 10,
            max_item_bytes: 256 * 1024,
            tenant_allowlist: None,
        }
    }
}

/// Límite duro del item DynamoDB (400 KB) menos margen de atributos de control.
const MAX_ITEM_BYTES_CEILING: usize = 380 * 1024;

impl QueryCachePolicy {
    /// Resuelve la política desde env vars con validación fail-fast
    /// (`InfraConfig001`, mismo patrón que `resolve_hmac_secret`).
    pub fn from_env() -> DomainResult<Self> {
        let mode = match std::env::var("QUERY_CACHE_MODE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "" | "off" => CacheMode::Off,
            "shadow" => CacheMode::Shadow,
            "ddb" => CacheMode::Ddb,
            other => {
                return Err(DomainError::infra(
                    ErrorCode::InfraConfig001,
                    format!("QUERY_CACHE_MODE inválido: '{other}' (off|shadow|ddb)"),
                ));
            }
        };

        let channels_raw =
            std::env::var("QUERY_CACHE_CHANNELS").unwrap_or_else(|_| "oltp,olap".to_string());
        let mut channels = ChannelFlags {
            oltp: false,
            olap: false,
        };
        for token in channels_raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            match token.to_ascii_lowercase().as_str() {
                "oltp" => channels.oltp = true,
                "olap" => channels.olap = true,
                other => {
                    return Err(DomainError::infra(
                        ErrorCode::InfraConfig001,
                        format!("QUERY_CACHE_CHANNELS inválido: '{other}' (olap,oltp)"),
                    ));
                }
            }
        }

        let ttl_oltp_secs = parse_env_u64("QUERY_CACHE_TTL_OLTP_SECS", 60)?;
        let ttl_olap_secs = parse_env_u64("QUERY_CACHE_TTL_OLAP_SECS", 60)?;
        let ttl_jitter_secs = parse_env_u64("QUERY_CACHE_TTL_JITTER_SECS", 10)?;
        let max_item_kb = parse_env_u64("QUERY_CACHE_MAX_ITEM_KB", 256)?;
        let max_item_bytes = (max_item_kb as usize) * 1024;

        if ttl_oltp_secs == 0 || ttl_olap_secs == 0 {
            return Err(DomainError::infra(
                ErrorCode::InfraConfig001,
                "QUERY_CACHE_TTL_*_SECS debe ser > 0",
            ));
        }
        if max_item_bytes > MAX_ITEM_BYTES_CEILING {
            return Err(DomainError::infra(
                ErrorCode::InfraConfig001,
                format!("QUERY_CACHE_MAX_ITEM_KB excede el límite de item DynamoDB ({max_item_kb} KB > 380 KB)"),
            ));
        }

        let tenant_allowlist = std::env::var("QUERY_CACHE_TENANT_ALLOWLIST")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<String>>()
            })
            .filter(|v| !v.is_empty());

        Ok(Self {
            mode,
            channels,
            ttl_oltp_secs,
            ttl_olap_secs,
            ttl_jitter_secs,
            max_item_bytes,
            tenant_allowlist,
        })
    }
}

fn parse_env_u64(name: &str, default: u64) -> DomainResult<u64> {
    match std::env::var(name) {
        Ok(raw) => raw.trim().parse::<u64>().map_err(|e| {
            DomainError::infra(
                ErrorCode::InfraConfig001,
                format!("{name} inválido ('{raw}'): {e}"),
            )
        }),
        Err(_) => Ok(default),
    }
}

// ── Gates y outcome ──────────────────────────────────────────────────────────

/// Motivo de bypass — nunca viaja en la respuesta, solo en el span (§7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BypassReason {
    /// La caché se encuentra en modo `CacheMode::Off`.
    ModeOff,
    /// El tenant no está incluido en la lista blanca (`tenant_allowlist`).
    TenantNotAllowlisted,
    /// La solicitud pide `explain_plan = true` (diagnóstico sin ejecución).
    ExplainPlan,
    /// La entidad tiene un `RowOverlay` dinámico registrado (como `domain_quota`).
    OverlayEntity,
    /// La consulta recorre el camino histórico o de auditoría (`as_of`).
    HistoryPath,
    /// El canal (`Oltp` u `Olap`) está deshabilitado en `ChannelFlags`.
    ChannelDisabled,
    /// El payload de la clave superó el límite `max_item_bytes`.
    Oversize,
}

/// Resultado de la evaluación/lookup para una query candidata.
#[derive(Debug)]
pub enum LookupOutcome {
    /// Entrada fresca, del tenant correcto y con generación válida.
    Hit {
        /// Entrada recuperada y validada desde la caché.
        entry: CacheEntry,
    },
    /// Ejecutar y poblar (en Shadow: siempre, tras registrar).
    Miss,
    /// No elegible: ejecutar sin poblar.
    Bypass {
        /// Causa específica por la cual se omitió la caché.
        reason: BypassReason,
    },
}

/// Datos de la query necesarios para los gates (§7). Puros y prestados.
pub struct CacheCandidate<'a> {
    /// Canal de lectura evaluado (`Oltp` u `Olap`).
    pub channel: CacheChannel,
    /// Identificador del tenant propietario de la consulta.
    pub tenant_id: &'a str,
    /// Nombre canónico de la entidad consultada.
    pub entity: &'a str,
    /// Indica si la petición solicita únicamente el plan de ejecución (`explain_plan`).
    pub explain_plan: bool,
    /// La entidad tiene un `RowOverlay` registrado — su valor de verdad NO
    /// vive en el log de datoms y se muta en lectura (G2).
    pub overlay_entity: bool,
}

// ── Rings in-process ─────────────────────────────────────────────────────────

/// Registro de claves recientes para el modo shadow (D7). Mismo patrón que
/// `EAV_CACHE`: RwLock + HashMap, expulsión de ~20% al llegar al tope.
struct ShadowKeyRing {
    inner: RwLock<HashMap<String, i64>>,
}

const SHADOW_RING_CAPACITY: usize = 10_000;

impl ShadowKeyRing {
    fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Registra la clave; retorna `true` si ya estaba VIVA (would-be-hit).
    /// Ring envenenado ⇒ `false` (mide miss y sigue): la medición nunca
    /// justifica tumbar la query — mismo patrón que `cache_policy.rs`.
    fn observe(&self, key: &str, ttl_secs: i64, now: i64) -> bool {
        let Ok(mut map) = self.inner.write() else {
            return false;
        };
        let was_fresh = map.get(key).map(|exp| now < *exp).unwrap_or(false);
        if map.len() >= SHADOW_RING_CAPACITY && !map.contains_key(key) {
            let evict: Vec<String> = map.keys().take(SHADOW_RING_CAPACITY / 5).cloned().collect();
            for k in evict {
                map.remove(&k);
            }
        }
        map.insert(key.to_string(), now + ttl_secs);
        was_fresh
    }
}

/// Claves cuyo payload medido excede el cap: evita re-serializar en cada
/// request (G4-negativo). La marca expira con el TTL de la entrada.
struct OversizeKeyRing {
    inner: RwLock<HashMap<String, i64>>,
}

const OVERSIZE_RING_CAPACITY: usize = 1_000;

impl OversizeKeyRing {
    fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    fn contains_fresh(&self, key: &str, now: i64) -> bool {
        if let Ok(map) = self.inner.read() {
            map.get(key).map(|exp| now < *exp).unwrap_or(false)
        } else {
            false
        }
    }

    fn mark(&self, key: &str, exp: i64) {
        if let Ok(mut map) = self.inner.write() {
            if map.len() >= OVERSIZE_RING_CAPACITY && !map.contains_key(key) {
                let evict: Vec<String> = map
                    .keys()
                    .take(OVERSIZE_RING_CAPACITY / 5)
                    .cloned()
                    .collect();
                for k in evict {
                    map.remove(&k);
                }
            }
            map.insert(key.to_string(), exp);
        }
    }
}

// ── Reloj inyectable ─────────────────────────────────────────────────────────

type Clock = fn() -> i64;

fn default_now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

// ── Frontend ─────────────────────────────────────────────────────────────────

/// Orquestador de la caché del read path. Se inyecta SIEMPRE (Null Object en
/// modo `off`): los routers no bifurcan sobre `Option`.
pub struct QueryCacheFrontend {
    policy: QueryCachePolicy,
    backend: Arc<dyn IQueryCache>,
    shadow: ShadowKeyRing,
    oversize: OversizeKeyRing,
    clock: Clock,
}

impl QueryCacheFrontend {
    /// Modo `off` — Null Object (motor sin tabla).
    pub fn disabled() -> Self {
        Self {
            policy: QueryCachePolicy::default(),
            backend: Arc::new(NoopQueryCache),
            shadow: ShadowKeyRing::new(),
            oversize: OversizeKeyRing::new(),
            clock: default_now_secs,
        }
    }

    /// Modo `shadow` — medición sin costo de almacenamiento.
    pub fn shadow(policy: QueryCachePolicy) -> Self {
        Self {
            policy,
            backend: Arc::new(NoopQueryCache),
            shadow: ShadowKeyRing::new(),
            oversize: OversizeKeyRing::new(),
            clock: default_now_secs,
        }
    }

    /// Modo `ddb` — caché activa contra el adaptador DynamoDB.
    pub fn ddb(backend: Arc<dyn IQueryCache>, policy: QueryCachePolicy) -> Self {
        Self {
            policy,
            backend,
            shadow: ShadowKeyRing::new(),
            oversize: OversizeKeyRing::new(),
            clock: default_now_secs,
        }
    }

    /// Constructor con reloj inyectable (tests de expiración).
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Devuelve el modo de operación configurado (`Off`, `Shadow` o `Ddb`).
    pub fn mode(&self) -> CacheMode {
        self.policy.mode
    }

    /// ¿El router debe escribir el bloque `cache` en el body del chunk?
    /// Solo en `ddb`: en shadow/off la respuesta es byte-idéntica a pre-caché.
    pub fn reports_metadata(&self) -> bool {
        self.policy.mode == CacheMode::Ddb
    }

    fn ttl_base_secs(&self, channel: CacheChannel) -> u64 {
        match channel {
            CacheChannel::Oltp => self.policy.ttl_oltp_secs,
            CacheChannel::Olap => self.policy.ttl_olap_secs,
        }
    }

    fn now_secs(&self) -> i64 {
        (self.clock)()
    }

    /// Segundos restantes de frescura de una entrada (para metadata §8.1).
    pub fn remaining_secs(&self, expires_at: i64) -> i64 {
        (expires_at - self.now_secs()).max(0)
    }

    /// Gates de elegibilidad (§7) — función pura, testeable.
    pub fn evaluate(&self, cand: &CacheCandidate<'_>) -> LookupOutcome {
        let bypass = |reason: BypassReason| LookupOutcome::Bypass { reason };

        if self.policy.mode == CacheMode::Off {
            return bypass(BypassReason::ModeOff);
        }
        // G0 — canary por tenant
        if let Some(allow) = &self.policy.tenant_allowlist {
            if !allow.iter().any(|t| t == cand.tenant_id) {
                return bypass(BypassReason::TenantNotAllowlisted);
            }
        }
        // G1 — dry-run: nunca
        if cand.explain_plan {
            return bypass(BypassReason::ExplainPlan);
        }
        // G2 — entidades con overlay: el valor se muta en lectura
        if cand.overlay_entity {
            return bypass(BypassReason::OverlayEntity);
        }
        // G3 — time-travel explícito: bypass v1 (el camino AsOfSnapshot es
        // determinista —lee el pasado inmutable— y fluye; `_history` se
        // excluye por conservadurismo del plan).
        if cand.entity.ends_with("_history") {
            return bypass(BypassReason::HistoryPath);
        }
        // G5 — canal deshabilitado
        let enabled = match cand.channel {
            CacheChannel::Oltp => self.policy.channels.oltp,
            CacheChannel::Olap => self.policy.channels.olap,
        };
        if !enabled {
            return bypass(BypassReason::ChannelDisabled);
        }
        LookupOutcome::Miss
    }

    /// Lookup con verificaciones de corrección. Shadow registra y devuelve
    /// siempre `Miss` (la respuesta nunca cambia). Ddb verifica expiración
    /// lógica y tenant; la generación la resuelve el adaptador (BatchGet).
    pub async fn lookup(&self, key: &str, tenant_id: &str, channel: CacheChannel) -> LookupOutcome {
        let now = self.now_secs();

        if self.policy.mode == CacheMode::Shadow {
            let was_fresh = self
                .shadow
                .observe(key, self.ttl_base_secs(channel) as i64, now);
            let status = if was_fresh {
                "SHADOW_HIT"
            } else {
                "SHADOW_MISS"
            };
            info!(
                target: "metri_engine::janus::cache",
                cache_status = status, cache_key = %key_short(key), tenant_id, cache_channel = ?channel,
                "janus.cache.lookup"
            );
            return LookupOutcome::Miss;
        }

        match self.backend.get(key, tenant_id).await {
            Ok(Some(entry)) => {
                if now >= entry.expires_at {
                    emit_lookup("STALE", key, tenant_id, channel);
                    return LookupOutcome::Miss;
                }
                // Defensa Zero-Trust: el tenant ya va dentro del payload
                // hasheado; esta verificación adicional hace que ni siquiera
                // un bug de canonicalización pueda cruzar tenants.
                if entry.tenant_id != tenant_id {
                    warn!(
                        target: "metri_engine::janus::cache",
                        cache_key = %key_short(key), expected_tenant = tenant_id,
                        entry_tenant = %entry.tenant_id,
                        "janus.cache.lookup: tenant mismatch — entrada descartada"
                    );
                    return LookupOutcome::Miss;
                }
                emit_lookup("HIT", key, tenant_id, channel);
                LookupOutcome::Hit { entry }
            }
            Ok(None) => {
                emit_lookup("MISS", key, tenant_id, channel);
                LookupOutcome::Miss
            }
            Err(e) => {
                // Fail-open (D-disponibilidad): degradar, jamás tumbar la query.
                // cache_status=ERROR alimenta el metric filter y la alarma §8.3.
                warn!(
                    target: "metri_engine::janus::cache",
                    cache_status = "ERROR", cache_key = %key_short(key), tenant_id, error = %e,
                    "janus.cache.lookup: backend degradado — bypass"
                );
                LookupOutcome::Miss
            }
        }
    }

    /// Almacena el resultado del executor (D1). Solo modo `ddb`; el `put`
    /// se despacha en background para no sumar latencia al camino caliente.
    /// G4: payload > cap ⇒ no se almacena y la clave se marca en el ring
    /// `oversize` para no re-serializar en los próximos requests.
    pub async fn store(&self, key: &str, tenant_id: &str, channel: CacheChannel, payload: &Value) {
        if self.policy.mode != CacheMode::Ddb {
            return;
        }
        let now = self.now_secs();
        let ttl_base = self.ttl_base_secs(channel);

        if self.oversize.contains_fresh(key, now) {
            info!(
                target: "metri_engine::janus::cache",
                cache_status = "BYPASS", bypass_reason = "oversize",
                cache_key = %key_short(key), tenant_id,
                "janus.cache.store"
            );
            return;
        }

        let serialized = match serde_json::to_string(payload) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    target: "metri_engine::janus::cache",
                    cache_key = %key_short(key), error = %e,
                    "janus.cache.store: serialización falló — no se cachea"
                );
                return;
            }
        };
        if serialized.len() > self.policy.max_item_bytes {
            self.oversize.mark(key, now + ttl_base as i64);
            info!(
                target: "metri_engine::janus::cache",
                cache_status = "BYPASS", bypass_reason = "oversize",
                payload_bytes = serialized.len(), max_item_bytes = self.policy.max_item_bytes,
                cache_key = %key_short(key), tenant_id,
                "janus.cache.store"
            );
            return;
        }

        let ttl_secs = ttl_base + rand::thread_rng().gen_range(0..=self.policy.ttl_jitter_secs);
        let put = CachePut {
            tenant_id: tenant_id.to_string(),
            key: key.to_string(),
            payload: payload.clone(),
            ttl_secs,
        };

        let backend = Arc::clone(&self.backend);
        let key_log = key_short(key).to_string();
        tokio::spawn(async move {
            if let Err(e) = backend.put(&put).await {
                warn!(
                    target: "metri_engine::janus::cache",
                    cache_key = %key_log, error = %e,
                    "janus.cache.store: put falló (fail-open)"
                );
            }
        });
    }
}

fn emit_lookup(status: &str, key: &str, tenant_id: &str, channel: CacheChannel) {
    info!(
        target: "metri_engine::janus::cache",
        cache_status = status, cache_key = %key_short(key), tenant_id, cache_channel = ?channel,
        "janus.cache.lookup"
    );
}

/// Clave acortada para logs (la completa son 70+ chars de hex).
fn key_short(key: &str) -> &str {
    &key[key.len().saturating_sub(12)..]
}

// ── Null Object ──────────────────────────────────────────────────────────────

/// Null Object del puerto `IQueryCache` — modo `off`/`shadow`. Sin I/O.
pub struct NoopQueryCache;

#[async_trait]
impl IQueryCache for NoopQueryCache {
    async fn get(&self, _key: &str, _tenant_id: &str) -> DomainResult<Option<CacheEntry>> {
        Ok(None)
    }
    async fn put(&self, _entry: &CachePut) -> DomainResult<()> {
        Ok(())
    }
}
