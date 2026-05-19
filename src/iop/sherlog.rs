// [PORTED_FROM: src/metri/iop/sherlog.clj]
// iop/sherlog.rs — Módulo Sherlog — Pipeline EDA de Errores (Módulo V).
// En Clojure: IFaultNotifier protocol + EventBridgeNotifier + process-fault!
// En Rust:    IFaultNotifier trait + EventBridgeNotifier (stub) + process_fault()
//
// Dispatch de severidad:
//   :info    → solo log
//   :warning → log + notify
//   :error   → log + notify
//   :fatal   → log + notify (máxima prioridad)
//
// Nunca interrumpe el flujo principal — fire-and-forget seguro.

use serde_json::Value;
use tracing::{debug, error, info, warn};

use crate::domain::error_catalog;
use crate::domain::errors::DomainError;

// ── Severidad de fallo ────────────────────────────────────────────────────────

/// Nivel de severidad del fallo de dominio.
/// [PORTED_FROM: (keyword (:severity cat-entry :warning)) en sherlog.clj]
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
            "info"    => FaultSeverity::Info,
            "error"   => FaultSeverity::Error,
            "fatal"   => FaultSeverity::Fatal,
            _         => FaultSeverity::Warning, // default seguro
        }
    }

    /// Determina si la severidad requiere notificación al Fault Bus.
    /// [PORTED_FROM: (contains? #{:warning :error :fatal} severity)]
    pub fn requires_notification(&self) -> bool {
        matches!(self, FaultSeverity::Warning | FaultSeverity::Error | FaultSeverity::Fatal)
    }
}

// ── Trait IFaultNotifier ──────────────────────────────────────────────────────

/// Puerto de notificación de fallos al Fault Bus externo.
/// [PORTED_FROM: (defprotocol IFaultNotifier (notify! [this error-dto severity] ...))]
#[async_trait::async_trait]
pub trait IFaultNotifier: Send + Sync {
    /// Envía el DTO al destino externo de forma fire-and-forget.
    /// Nunca propaga error — retorna Result para logging interno.
    async fn notify(&self, error_dto: &Value, severity: &FaultSeverity) -> Result<(), DomainError>;
}

// ── EventBridgeNotifier ───────────────────────────────────────────────────────

/// Implementación de IFaultNotifier que emite eventos a AWS EventBridge.
/// [PORTED_FROM: (defrecord EventBridgeNotifier [eb-client bus-name] IFaultNotifier ...)]
pub struct EventBridgeNotifier {
    bus_name: String,
    // FASE 4: aws_sdk_eventbridge::Client
}

impl EventBridgeNotifier {
    pub fn new(bus_name: impl Into<String>) -> Self {
        let bus_name = bus_name.into();
        info!("[Sherlog] EventBridgeNotifier inicializado | Bus: {bus_name}");
        Self { bus_name }
    }
}

#[async_trait::async_trait]
impl IFaultNotifier for EventBridgeNotifier {
    /// Emite un evento `DOMAIN_FAULT_<SEVERITY>` al EventBridge bus.
    /// [PORTED_FROM: (proto/put-event! eb-client bus-name "metri.engine" detail-type error-dto)]
    async fn notify(&self, error_dto: &Value, severity: &FaultSeverity) -> Result<(), DomainError> {
        let detail_type = format!("DOMAIN_FAULT_{:?}", severity).to_uppercase();
        let error_code = error_dto
            .get("error")
            .and_then(|e| e.get("code"))
            .and_then(|c| c.as_str())
            .unwrap_or("UNKNOWN");

        // FASE 4: reemplazar con llamada real a EventBridge
        // aws_sdk_eventbridge put_events(PutEventsRequestEntry { ... })
        info!(
            bus      = %self.bus_name,
            detail   = %detail_type,
            code     = %error_code,
            "[Sherlog] → EventBridge STUB — evento encolado (FASE 4: real EB client)"
        );

        Ok(())
    }
}

// ── Stub de no-op para tests / local dev ──────────────────────────────────────

/// Notifier nulo — solo loguea, nunca emite. Útil para tests y local dev.
pub struct NoopFaultNotifier;

#[async_trait::async_trait]
impl IFaultNotifier for NoopFaultNotifier {
    async fn notify(&self, _error_dto: &Value, severity: &FaultSeverity) -> Result<(), DomainError> {
        debug!("[Sherlog::Noop] Fault descartado (no-op) | severity={severity:?}");
        Ok(())
    }
}

// ── process_fault — Dispatch Central ─────────────────────────────────────────

/// Evalúa el DomainError contra el catálogo, determina severidad y despacha.
/// Fire-and-forget: nunca propaga error al caller.
///
/// [PORTED_FROM: (defn process-fault! [notifier error-map error-dto] ...)]
///
/// Flujo:
///   1. Lookup del código en el ErrorCatalog
///   2. Parsear severidad (string → FaultSeverity)
///   3. Si :info → solo log
///   4. Si :warning | :error | :fatal → log + notify al bus
pub async fn process_fault(
    notifier:  &dyn IFaultNotifier,
    error:     &DomainError,
    error_dto: &Value,
) {
    let code_str = format!("{:?}", error.code);

    // Lookup en el catálogo global
    // [PORTED_FROM: (errors/lookup code)]
    let severity = match error_catalog::try_global() {
        Some(catalog) => {
            catalog
                .get(&code_str)
                .map(|entry| FaultSeverity::from_catalog_str(&entry.severity))
                .unwrap_or(FaultSeverity::Warning) // default conservador
        }
        None => {
            // Catálogo no inicializado — asumir warning, no bloquear
            warn!("[Sherlog] ErrorCatalog no disponible — asumiendo Warning para {code_str}");
            FaultSeverity::Warning
        }
    };

    // [PORTED_FROM: (cond (= severity :info) ... (contains? #{:warning :error :fatal}) ...)]
    if !severity.requires_notification() {
        info!("[Sherlog] Fault(info): {code_str}");
        return;
    }

    warn!(
        code     = %code_str,
        severity = ?severity,
        "[Sherlog] Escalando fallo al Fault Bus"
    );

    // Fire-and-forget: el error del notifier nunca interrumpe el flujo principal.
    // [PORTED_FROM: (try (notify! notifier ...) (catch Exception e (log/error ...)))]
    if let Err(e) = notifier.notify(error_dto, &severity).await {
        error!("[Sherlog] Notifier falló — ignorando para no interrumpir flujo: {e:?}");
    }
}
