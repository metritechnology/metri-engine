//! Sherlog — the EDA fault pipeline.
//!
//! Módulo Sherlog — Pipeline EDA de Errores (Módulo V).
//!
//! # Origin
//! En el stack anterior: IFaultNotifier protocol + EventBridgeNotifier + process-fault!
//! En Rust:    IFaultNotifier trait + EventBridgeNotifier + process_fault()
//!
//! Dispatch de severidad:
//! :info    → solo log
//! :warning → log + notify
//! :error   → log + notify
//! :fatal   → log + notify (máxima prioridad)
//!
//! Nunca interrumpe el flujo principal — fire-and-forget seguro.

use serde_json::Value;
use tracing::{debug, error, info, warn};

use crate::domain::error_catalog;
use crate::domain::errors::DomainError;
use crate::iop::core::IopContext;
use crate::janus_router::router::IWriteChannel;
use aws_sdk_eventbridge::types::PutEventsRequestEntry;

// ── Severidad de fallo ────────────────────────────────────────────────────────

/// Nivel de severidad del fallo de dominio.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultSeverity {
    Info,
    Warning,
    Error,
    Fatal,
}

impl FaultSeverity {
    /// Parsea desde el string del catálogo ("info" | "warning" | "error" | "fatal").
    pub fn from_catalog_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "info" => FaultSeverity::Info,
            "error" => FaultSeverity::Error,
            "fatal" => FaultSeverity::Fatal,
            _ => FaultSeverity::Warning, // default seguro
        }
    }

    /// Determina si la severidad requiere notificación al Fault Bus.
    pub fn requires_notification(&self) -> bool {
        matches!(
            self,
            FaultSeverity::Warning | FaultSeverity::Error | FaultSeverity::Fatal
        )
    }
}

// ── Trait IFaultNotifier ──────────────────────────────────────────────────────

/// Puerto de notificación de fallos al Fault Bus externo.
#[async_trait::async_trait]
pub trait IFaultNotifier: Send + Sync {
    /// Envía el DTO al destino externo de forma fire-and-forget.
    /// Nunca propaga error — retorna Result para logging interno.
    async fn notify(&self, error_dto: &Value, severity: &FaultSeverity) -> Result<(), DomainError>;
}

// ── EventBridgeNotifier ───────────────────────────────────────────────────────

/// Implementación de IFaultNotifier que emite eventos a AWS EventBridge.
pub struct EventBridgeNotifier {
    bus_name: String,
    client: aws_sdk_eventbridge::Client,
}

impl EventBridgeNotifier {
    pub async fn new(bus_name: impl Into<String>) -> Self {
        let bus_name = bus_name.into();
        let region_provider =
            aws_config::meta::region::RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::from_env().region(region_provider).load().await;

        let client = if let Ok(endpoint_url) = std::env::var("AWS_ENDPOINT_URL") {
            info!(
                "[Sherlog] Usando endpoint override de AWS_ENDPOINT_URL: {}",
                endpoint_url
            );
            let eb_config = aws_sdk_eventbridge::config::Builder::from(&config)
                .endpoint_url(endpoint_url)
                .build();
            aws_sdk_eventbridge::Client::from_conf(eb_config)
        } else if let Ok(endpoint_url) = std::env::var("EVENTBRIDGE_ENDPOINT") {
            info!(
                "[Sherlog] Usando endpoint override de EVENTBRIDGE_ENDPOINT: {}",
                endpoint_url
            );
            let eb_config = aws_sdk_eventbridge::config::Builder::from(&config)
                .endpoint_url(endpoint_url)
                .build();
            aws_sdk_eventbridge::Client::from_conf(eb_config)
        } else {
            aws_sdk_eventbridge::Client::new(&config)
        };

        info!("[Sherlog] EventBridgeNotifier inicializado | Bus: {bus_name}");
        Self { bus_name, client }
    }
}

#[async_trait::async_trait]
impl IFaultNotifier for EventBridgeNotifier {
    /// Emite un evento `DOMAIN_FAULT_<SEVERITY>` al EventBridge bus.
    async fn notify(&self, error_dto: &Value, severity: &FaultSeverity) -> Result<(), DomainError> {
        let detail_type = format!("DOMAIN_FAULT_{:?}", severity).to_uppercase();
        let error_code = error_dto
            .get("error")
            .and_then(|e| e.get("code"))
            .and_then(|c| c.as_str())
            .unwrap_or("UNKNOWN");

        let entry = PutEventsRequestEntry::builder()
            .event_bus_name(&self.bus_name)
            .source("metri.engine")
            .detail_type(&detail_type)
            .detail(error_dto.to_string())
            .build();

        let res = self
            .client
            .put_events()
            .entries(entry)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("{e:?}");
                DomainError::infra(
                    crate::domain::errors::ErrorCode::Infra004,
                    format!(
                        "EventBridge PutEvents call failed on bus '{}': {msg}",
                        self.bus_name
                    ),
                )
            })?;

        if res.failed_entry_count() > 0 {
            let error_entries = res
                .entries()
                .iter()
                .filter(|entry| entry.error_code().is_some())
                .collect::<Vec<_>>();
            warn!(
                "[Sherlog] EventBridge dispatch has failed entries: {:?}",
                error_entries
            );
            return Err(DomainError::infra(
                crate::domain::errors::ErrorCode::Infra004,
                format!("EventBridge dispatch failed: {:?}", error_entries),
            ));
        }

        info!(
            bus      = %self.bus_name,
            detail   = %detail_type,
            code     = %error_code,
            "[Sherlog] EventBridge dispatch exitoso"
        );

        Ok(())
    }
}

// ── Stub de no-op para tests / local dev ──────────────────────────────────────

/// Notifier nulo — solo loguea, nunca emite. Útil para tests y local dev.
pub struct NoopFaultNotifier;

#[async_trait::async_trait]
impl IFaultNotifier for NoopFaultNotifier {
    async fn notify(
        &self,
        _error_dto: &Value,
        severity: &FaultSeverity,
    ) -> Result<(), DomainError> {
        debug!("[Sherlog::Noop] Fault descartado (no-op) | severity={severity:?}");
        Ok(())
    }
}

// ── process_fault — Dispatch Central ─────────────────────────────────────────

/// Evalúa el DomainError contra el catálogo, determina severidad y despacha.
/// Fire-and-forget: nunca propaga error al caller.
///
///
/// Flujo:
///   1. Lookup del código en el ErrorCatalog
///   2. Parsear severidad (string → FaultSeverity)
///   3. Si :info → solo log
///   4. Si :warning | :error | :fatal → log + notify al bus + persistencia OLAP
pub async fn process_fault(
    notifier: &dyn IFaultNotifier,
    olap_channel: &dyn IWriteChannel,
    error: &DomainError,
    error_dto: &Value,
    entity_type: Option<String>,
) {
    let code_str = error.code.canonical_code();

    // Lookup en el catálogo global
    let severity = match error_catalog::try_global() {
        Some(catalog) => {
            catalog
                .get(code_str)
                .map(|entry| FaultSeverity::from_catalog_str(&entry.severity))
                .unwrap_or(FaultSeverity::Warning) // default conservador
        }
        None => {
            // Catálogo no inicializado — asumir warning, no bloquear
            warn!("[Sherlog] ErrorCatalog no disponible — asumiendo Warning para {code_str}");
            FaultSeverity::Warning
        }
    };

    if !severity.requires_notification() {
        info!("[Sherlog] Fault(info): {code_str}");
        return;
    }

    warn!(
        code     = %code_str,
        severity = ?severity,
        "[Sherlog] Escalando fallo al Fault Bus y registrando en persistencia OLAP"
    );

    // Fire-and-forget: el error del notifier nunca interrumpe el flujo principal.
    if let Err(e) = notifier.notify(error_dto, &severity).await {
        error!("[Sherlog] Notifier falló — ignorando para no interrumpir flujo: {e:?}");
    }

    // Persistencia en canal OLAP (domain_fault)
    let severity_str = match severity {
        FaultSeverity::Warning => "WARNING",
        FaultSeverity::Error => "ERROR",
        FaultSeverity::Fatal => "FATAL",
        FaultSeverity::Info => "WARNING", // Fallback seguro
    };

    let error_inner = error_dto.get("error");
    let trace_id = error_inner
        .and_then(|e| e.get("trace_id"))
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    let tenant_id = error_inner
        .and_then(|e| e.get("tenant_id"))
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    let user_id = error_inner
        .and_then(|e| e.get("user_id"))
        .and_then(|u| u.as_str())
        .map(|s| s.to_string());
    let error_code = error_inner
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str())
        .unwrap_or(code_str)
        .to_string();
    let stage = error_inner
        .and_then(|e| e.get("stage"))
        .and_then(|s| s.as_str())
        .unwrap_or(&error.stage)
        .to_string();
    let component = error_inner
        .and_then(|e| e.get("component"))
        .and_then(|c| c.as_str())
        .unwrap_or("metri-engine")
        .to_string();
    let retryable = error_inner
        .and_then(|e| e.get("retryable"))
        .and_then(|r| r.as_bool())
        .unwrap_or(error.retryable);
    let occurred_at = error_inner
        .and_then(|e| e.get("timestamp"))
        .and_then(|t| t.as_i64())
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
    let context = error_inner
        .and_then(|e| e.get("context"))
        .cloned()
        .unwrap_or(Value::Null);

    let mut fault_record = serde_json::Map::new();
    fault_record.insert("trace_id".to_string(), Value::String(trace_id));
    fault_record.insert("tenant_id".to_string(), Value::String(tenant_id.clone()));
    if let Some(uid) = user_id {
        fault_record.insert("user_id".to_string(), Value::String(uid));
    }
    fault_record.insert("error_code".to_string(), Value::String(error_code));
    fault_record.insert(
        "severity".to_string(),
        Value::String(severity_str.to_string()),
    );
    fault_record.insert("stage".to_string(), Value::String(stage));
    fault_record.insert("component".to_string(), Value::String(component));
    if let Some(et) = entity_type {
        fault_record.insert("entity_type".to_string(), Value::String(et));
    }
    fault_record.insert("retryable".to_string(), Value::Bool(retryable));
    fault_record.insert("occurred_at".to_string(), Value::Number(occurred_at.into()));
    fault_record.insert("context".to_string(), context);

    let mut req_map = serde_json::Map::new();
    req_map.insert(
        "data".to_string(),
        Value::Array(vec![Value::Object(fault_record)]),
    );

    let fault_ctx = IopContext::new(
        tenant_id,
        error_inner
            .and_then(|e| e.get("user_id"))
            .and_then(|u| u.as_str())
            .unwrap_or("system"),
        "domain_fault",
        "BULK_CREATE",
        req_map,
    );

    if let Err(e) = olap_channel.route(fault_ctx).await {
        error!("[Sherlog] Fallo al encolar domain_fault en canal OLAP: {e:?}");
    }
}

#[cfg(test)]
#[path = "tests/sherlog_tests.rs"]
mod tests;
