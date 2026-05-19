// [PORTED_FROM: src/metri/domain/errors.clj]
// Equivalencia: Railway-oriented error monad constructor.
// En Clojure: (error :JANUS_400 {:reason "..."}) → [:error {...}]
// En Rust:    DomainError::janus(JANUS_400, ctx) → Result::Err(DomainError)
//
// Zero-Drop Policy: todos los códigos de error del catálogo original
// están representados como variantes del enum `ErrorCode`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Catálogo completo de códigos de error.
/// Mapea 1:1 con errors/error_catalog.edn del Clojure.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    // ── Janus Pipeline ───────────────────────────────────────────────────────
    Janus400,
    Janus401,
    Janus403,
    Janus404,
    Janus422,
    Janus500,
    JanusAstCompileError,
    JanusFilterCompileError,
    JanusSchemaNotFound,
    JanusTenantMismatch,
    JanusVal001,    // Defensa Inquebrantable — payload inválido
    Jns001,         // Sin canal de escritura para engine
    JnsLock001,     // write_path_locked = true
    JnsSeed001,     // is_system_seeded = true
    JnsOlap001,     // rpc Transact prohibido en engine:olap (solo BulkIngest)
    JnsTx001,       // TX ACID falló en el canal OLTP

    // ── Aegis SQL Compiler ───────────────────────────────────────────────────
    Aeg001,  // SQL compilation failed
    Aeg002,  // Athena execution failed
    Aeg003,  // Athena timeout
    Aeg004,  // Athena output parse error
    Aeg005,  // Unsupported aggregation function

    // ── EAV Engine ──────────────────────────────────────────────────────────
    Eav001,  // TransactWriteItems failed
    Eav002,  // Entity not found
    Eav003,  // Optimistic lock conflict
    Eav004,  // Attribute not in registry
    Eav005,  // Sort key overflow (> 1024 bytes)
    EavFts001, // FTS index write failed

    // ── Códice / Schema Registry ─────────────────────────────────────────────
    Cod001,  // Model file parse error
    Cod002,  // Duplicate entity in registry
    Cod003,  // SHA-256 fingerprint collision
    CodScope001, // Sequence scope provider not found

    // ── IOP Pipeline ─────────────────────────────────────────────────────────
    Iop001,  // Integrity validation failed
    Iop002,  // Enrichment failed
    Iop003,  // ACID transaction failed
    Iop004,  // Outbox publish failed

    // ── Quota ────────────────────────────────────────────────────────────────
    Quota001, // Tenant quota exhausted

    // ── Infraestructura ──────────────────────────────────────────────────────
    Infra001, // DynamoDB client error
    Infra002, // S3 client error
    Infra003, // SQS client error
    Infra004, // EventBridge client error
    Infra005, // Kinesis client error

    // ── Auth / Tenant ────────────────────────────────────────────────────────
    Auth401, // Token inválido o expirado
    Auth403, // Tenant mismatch — acceso denegado
    AuthRevoked, // Token revocado (blacklist)
}

impl ErrorCode {
    /// Retorna true si el error admite reintento por el cliente.
    /// Mapea :retryable? del error_catalog.edn
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ErrorCode::Aeg003
                | ErrorCode::Eav001
                | ErrorCode::Infra001
                | ErrorCode::Infra002
                | ErrorCode::Infra003
                | ErrorCode::Infra004
                | ErrorCode::Infra005
        )
    }

    /// Stage del pipeline donde ocurrió el error.
    /// Mapea :stage del error_catalog.edn
    pub fn stage(&self) -> &'static str {
        match self {
            ErrorCode::Janus400
            | ErrorCode::Janus401
            | ErrorCode::Janus403
            | ErrorCode::Janus404
            | ErrorCode::Janus422
            | ErrorCode::Janus500
            | ErrorCode::JanusAstCompileError
            | ErrorCode::JanusFilterCompileError
            | ErrorCode::JanusSchemaNotFound
            | ErrorCode::JanusTenantMismatch
            | ErrorCode::JanusVal001
            | ErrorCode::Jns001
            | ErrorCode::JnsLock001
            | ErrorCode::JnsSeed001
            | ErrorCode::JnsOlap001
            | ErrorCode::JnsTx001 => "janus",
            ErrorCode::Aeg001
            | ErrorCode::Aeg002
            | ErrorCode::Aeg003
            | ErrorCode::Aeg004
            | ErrorCode::Aeg005 => "aegis",
            ErrorCode::Eav001
            | ErrorCode::Eav002
            | ErrorCode::Eav003
            | ErrorCode::Eav004
            | ErrorCode::Eav005
            | ErrorCode::EavFts001 => "eav",
            ErrorCode::Cod001
            | ErrorCode::Cod002
            | ErrorCode::Cod003
            | ErrorCode::CodScope001 => "codice",
            ErrorCode::Iop001
            | ErrorCode::Iop002
            | ErrorCode::Iop003
            | ErrorCode::Iop004 => "iop",
            ErrorCode::Quota001 => "quota",
            ErrorCode::Infra001
            | ErrorCode::Infra002
            | ErrorCode::Infra003
            | ErrorCode::Infra004
            | ErrorCode::Infra005 => "infrastructure",
            ErrorCode::Auth401 | ErrorCode::Auth403 | ErrorCode::AuthRevoked => "auth",
        }
    }
}

/// Error de dominio Railway-Oriented.
/// Reemplaza el vector Clojure [:error {:code :JANUS_400 :detail "..." :retryable? false}]
#[derive(Debug, Clone, Error, PartialEq, Serialize, Deserialize)]
#[error("{code:?}: {detail}")]
pub struct DomainError {
    pub code: ErrorCode,
    pub stage: String,
    pub detail: String,
    pub retryable: bool,
    /// Contexto adicional (equivale al ctx-map del error Clojure)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
}

impl DomainError {
    /// Constructor principal — equivale a (error code ctx-map) en Clojure.
    pub fn new(code: ErrorCode, detail: impl Into<String>) -> Self {
        let retryable = code.is_retryable();
        let stage = code.stage().to_string();
        DomainError {
            code,
            stage,
            detail: detail.into(),
            retryable,
            context: None,
        }
    }

    /// Sobreescribe la etapa (stage) inferida.
    pub fn with_stage(mut self, stage: impl Into<String>) -> Self {
        self.stage = stage.into();
        self
    }

    /// Añade contexto estructurado al error.
    pub fn with_context(mut self, context: serde_json::Value) -> Self {
        self.context = Some(context);
        self
    }

    // ── Constructores rápidos por dominio ────────────────────────────────────

    pub fn janus(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    pub fn eav(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    pub fn aegis(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    pub fn auth(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    pub fn infra(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    pub fn codice(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }
}
