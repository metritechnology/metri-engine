//! Railway error monad — DomainError and the canonical ErrorCode catalog.
//!
//! # Origin
//! Equivalencia: Railway-oriented error monad constructor.
//! En el stack anterior: (error :JANUS_400 {:reason "..."}) → [:error {...}]
//! En Rust:    DomainError::janus(JANUS_400, ctx) → Result::Err(DomainError)
//!
//! Zero-Drop Policy: todos los códigos de error del catálogo original
//! están representados como variantes del enum `ErrorCode`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Catálogo completo de códigos de error.
/// Mapea 1:1 con errors/error_catalog.edn del el stack anterior.
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
    JanusVal001, // Defensa Inquebrantable — payload inválido
    Jns001,      // Sin canal de escritura para engine
    JnsLock001,  // write_path_locked = true
    JnsSeed001,  // is_system_seeded = true
    JnsOlap001,  // rpc Transact prohibido en engine:olap (solo BulkIngest)
    JnsTx001,    // TX ACID falló en el canal OLTP

    // ── Aegis SQL Compiler ───────────────────────────────────────────────────
    Aeg001, // SQL compilation failed
    Aeg002, // Athena execution failed
    Aeg003, // Athena timeout
    Aeg004, // Athena output parse error
    Aeg005, // Unsupported aggregation function

    // ── EAV Engine ──────────────────────────────────────────────────────────
    Eav001, // TransactWriteItems failed
    Eav002, // Entity not found
    // Cursor obsoleto: el AST cambió entre páginas (eav/cursor/composite.rs).
    // El código canónico dice `EAV_TX_003` y el catálogo lo describe como un
    // conflicto de bloqueo optimista, que es lo que se pensaba usar cuando se
    // reservó. Nada implementa ese bloqueo; cambiar la cadena tocaría el
    // contrato con los clientes, así que se deja y se documenta.
    Eav003,
    Eav004,    // Attribute not in registry
    Eav005,    // Sort key overflow (> 1024 bytes)
    EavFts001, // FTS index write failed

    // ── Códice / Schema Registry ─────────────────────────────────────────────
    Cod001,      // Model file parse error
    Cod002,      // Duplicate entity in registry
    Cod003,      // SHA-256 fingerprint collision
    CodScope001, // Sequence scope provider not found
    CodMat001,   // Materialization spec references unknown entity/attribute/hook

    // ── IOP Pipeline ─────────────────────────────────────────────────────────
    Iop001, // Integrity validation failed
    Iop002, // Enrichment failed
    Iop003, // ACID transaction failed
    Iop004, // Outbox publish failed

    // ── Quota ────────────────────────────────────────────────────────────────
    Quota001, // Tenant quota exhausted

    // ── Infraestructura ──────────────────────────────────────────────────────
    Infra001,         // DynamoDB client error
    Infra002,         // S3 client error
    Infra003,         // SQS client error
    Infra004,         // EventBridge client error
    Infra005,         // Kinesis client error
    InfraCatalog001,  // Error catalog TOML failed to load/validate at bootstrap
    InfraConfig001,   // Engine configuration failed to resolve/validate at bootstrap
    InfraGlue001,     // Glue SDK error — Iceberg table sync failed
    InfraFirehose001, // Firehose SDK error — delivery stream operation failed

    // ── Auth / Tenant ────────────────────────────────────────────────────────
    Auth401,     // Token inválido o expirado
    Auth403,     // Tenant mismatch — acceso denegado
    AuthRevoked, // Token revocado (blacklist)

    // ── Auditoria ────────────────────────────────────────────────────────────
    Aud001,
    Aud002,

    // ── Missing error catalog codes ──────────────────────────────────────────
    JnsRef002,
    JnsConflict001,
    EavTx001,
    EavTx004,
    GrpcTenant001,
    InfraAthena005,
    InfraCedar002,
    Mcp503,

    // ── Formula Engine ───────────────────────────────────────────────────────
    Fml001,
    Fml002,
    Fml003,
    Fml004,
    Fml005,
    Fml006,
    Fml007,
    Fml008,
    Fml009,
    Fml010,
    Fml011,
    Fml012,
    Fml013,
}

impl ErrorCode {
    pub const ALL: &'static [ErrorCode] = &[
        ErrorCode::Janus400,
        ErrorCode::Janus401,
        ErrorCode::Janus403,
        ErrorCode::Janus404,
        ErrorCode::Janus422,
        ErrorCode::Janus500,
        ErrorCode::JanusAstCompileError,
        ErrorCode::JanusFilterCompileError,
        ErrorCode::JanusSchemaNotFound,
        ErrorCode::JanusTenantMismatch,
        ErrorCode::JanusVal001,
        ErrorCode::Jns001,
        ErrorCode::JnsLock001,
        ErrorCode::JnsSeed001,
        ErrorCode::JnsOlap001,
        ErrorCode::JnsTx001,
        ErrorCode::Aeg001,
        ErrorCode::Aeg002,
        ErrorCode::Aeg003,
        ErrorCode::Aeg004,
        ErrorCode::Aeg005,
        ErrorCode::Eav001,
        ErrorCode::Eav002,
        ErrorCode::Eav003,
        ErrorCode::Eav004,
        ErrorCode::Eav005,
        ErrorCode::EavFts001,
        ErrorCode::Cod001,
        ErrorCode::Cod002,
        ErrorCode::Cod003,
        ErrorCode::CodScope001,
        ErrorCode::CodMat001,
        ErrorCode::Iop001,
        ErrorCode::Iop002,
        ErrorCode::Iop003,
        ErrorCode::Iop004,
        ErrorCode::Quota001,
        ErrorCode::Infra001,
        ErrorCode::Infra002,
        ErrorCode::Infra003,
        ErrorCode::Infra004,
        ErrorCode::Infra005,
        ErrorCode::InfraCatalog001,
        ErrorCode::InfraConfig001,
        ErrorCode::InfraGlue001,
        ErrorCode::InfraFirehose001,
        ErrorCode::Auth401,
        ErrorCode::Auth403,
        ErrorCode::AuthRevoked,
        ErrorCode::Aud001,
        ErrorCode::Aud002,
        ErrorCode::JnsRef002,
        ErrorCode::JnsConflict001,
        ErrorCode::EavTx001,
        ErrorCode::EavTx004,
        ErrorCode::GrpcTenant001,
        ErrorCode::InfraAthena005,
        ErrorCode::InfraCedar002,
        ErrorCode::Mcp503,
        ErrorCode::Fml001,
        ErrorCode::Fml002,
        ErrorCode::Fml003,
        ErrorCode::Fml004,
        ErrorCode::Fml005,
        ErrorCode::Fml006,
        ErrorCode::Fml007,
        ErrorCode::Fml008,
        ErrorCode::Fml009,
        ErrorCode::Fml010,
        ErrorCode::Fml011,
        ErrorCode::Fml012,
        ErrorCode::Fml013,
    ];

    /// Obtiene el código canónico correspondiente en el catálogo de errores (TOML).
    ///
    /// Mapeo 1:1 (PLAN_PATRON_RESULT.md §3.2, catálogo v3.0.0): cada variante
    /// tiene un código propio y semánticamente correcto. El guard
    /// `codigos_canonicos_son_unicos` impide compartir códigos; el guard
    /// `todo_codigo_canonico_esta_en_el_catalogo` impide códigos fuera del TOML.
    pub fn canonical_code(&self) -> &'static str {
        match self {
            ErrorCode::Janus400 => "JANUS_400",
            ErrorCode::Janus401 => "JANUS_401",
            ErrorCode::Janus403 => "JANUS_403",
            ErrorCode::Janus404 => "JANUS_404",
            ErrorCode::Janus422 => "JANUS_422",
            ErrorCode::Janus500 => "GRPC_500",
            ErrorCode::JanusAstCompileError => "AEG_COMPILE_001",
            ErrorCode::JanusFilterCompileError => "AEG_COMPILE_002",
            ErrorCode::JanusSchemaNotFound => "CDX_003",
            ErrorCode::JanusTenantMismatch => "AEG_TENANT_MISSING",
            ErrorCode::JanusVal001 => "JANUS_VAL_001",
            ErrorCode::Jns001 => "JNS_001",
            ErrorCode::JnsLock001 => "JNS_LOCK_001",
            ErrorCode::JnsSeed001 => "JNS_SEED_001",
            ErrorCode::JnsOlap001 => "JNS_OLAP_003",
            ErrorCode::JnsTx001 => "JNS_TX_001",

            ErrorCode::Aeg001 => "AEG_COMPILE_003",
            ErrorCode::Aeg002 => "AEG_002",
            ErrorCode::Aeg003 => "AEG_006",
            ErrorCode::Aeg004 => "AEG_007",
            ErrorCode::Aeg005 => "AEG_008",

            ErrorCode::Eav001 => "EAV_001",
            ErrorCode::Eav002 => "EAV_005",
            // Contrato histórico: los clientes conocen EAV_TX_003 para cursor
            // obsoleto aunque la entrada EAV_003 (deprecada) lo describiera. ADR-005.
            ErrorCode::Eav003 => "EAV_TX_003",
            ErrorCode::Eav004 => "EAV_004",
            ErrorCode::Eav005 => "EAV_KEY_001",
            ErrorCode::EavFts001 => "EAV_FTS_001",

            ErrorCode::Cod001 => "CDX_001",
            ErrorCode::Cod002 => "CDX_002",
            ErrorCode::Cod003 => "CDX_004",
            ErrorCode::CodScope001 => "JNS_SCOPE_001",
            ErrorCode::CodMat001 => "COD_MAT_001",

            ErrorCode::Iop001 => "IOP_001",
            ErrorCode::Iop002 => "IOP_002",
            ErrorCode::Iop003 => "IOP_003",
            ErrorCode::Iop004 => "IOP_004",

            ErrorCode::Quota001 => "QUOTA_001",

            ErrorCode::Infra001 => "INFRA_DDB_001",
            ErrorCode::Infra002 => "INFRA_S3_001",
            ErrorCode::Infra003 => "INFRA_SQS_001",
            ErrorCode::Infra004 => "INFRA_EB_001",
            ErrorCode::Infra005 => "INFRA_KIN_001",
            ErrorCode::InfraCatalog001 => "INFRA_CATALOG_001",
            ErrorCode::InfraConfig001 => "INFRA_CONFIG_001",
            ErrorCode::InfraGlue001 => "INFRA_GLUE_001",
            ErrorCode::InfraFirehose001 => "INFRA_FIREHOSE_001",

            ErrorCode::Auth401 => "GRPC_AUTH_001",
            ErrorCode::Auth403 => "GRPC_AUTH_004",
            ErrorCode::AuthRevoked => "GRPC_AUTH_003",

            ErrorCode::Aud001 => "AUD_001",
            ErrorCode::Aud002 => "AUD_002",

            ErrorCode::JnsRef002 => "JNS_REF_002",
            ErrorCode::JnsConflict001 => "JNS_CONFLICT_001",
            ErrorCode::EavTx001 => "EAV_TX_001",
            ErrorCode::EavTx004 => "EAV_TX_004",
            ErrorCode::GrpcTenant001 => "GRPC_TENANT_001",
            ErrorCode::InfraAthena005 => "INFRA_ATHENA_005",
            ErrorCode::InfraCedar002 => "INFRA_CEDAR_002",
            ErrorCode::Mcp503 => "MCP_503",

            ErrorCode::Fml001 => "FML_001",
            ErrorCode::Fml002 => "FML_002",
            ErrorCode::Fml003 => "FML_003",
            ErrorCode::Fml004 => "FML_004",
            ErrorCode::Fml005 => "FML_005",
            ErrorCode::Fml006 => "FML_006",
            ErrorCode::Fml007 => "FML_007",
            ErrorCode::Fml008 => "FML_008",
            ErrorCode::Fml009 => "FML_009",
            ErrorCode::Fml010 => "FML_010",
            ErrorCode::Fml011 => "FML_011",
            ErrorCode::Fml012 => "FML_012",
            ErrorCode::Fml013 => "FML_013",
        }
    }

    /// Retorna true si el error admite reintento por el cliente.
    ///
    /// Consulta el catálogo global (TOML, fuente canónica); antes del bootstrap
    /// usa [`ErrorCode::fallback_is_retryable`].
    pub fn is_retryable(&self) -> bool {
        match crate::domain::error_catalog::try_global() {
            Some(catalog) => catalog.is_retryable(self.canonical_code()),
            None => self.fallback_is_retryable(),
        }
    }

    /// Fallback estático previo al bootstrap.
    ///
    /// DEBE coincidir con el campo `retryable` de `error_catalog.toml`: el
    /// guard `fallback_retryable_coincide_con_el_catalogo` rompe el build si
    /// diverge (PLAN_PATRON_RESULT.md, regla C4).
    pub fn fallback_is_retryable(&self) -> bool {
        matches!(
            self,
            ErrorCode::JnsTx001
                | ErrorCode::CodScope001
                | ErrorCode::Eav003
                | ErrorCode::EavTx001
                | ErrorCode::EavTx004
                | ErrorCode::Aeg002
                | ErrorCode::Iop003
                | ErrorCode::Quota001
                | ErrorCode::Infra001
                | ErrorCode::Infra002
                | ErrorCode::Infra003
                | ErrorCode::InfraGlue001
                | ErrorCode::InfraFirehose001
                | ErrorCode::Mcp503
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
            | ErrorCode::JnsTx001
            | ErrorCode::JnsRef002
            | ErrorCode::JnsConflict001 => "janus",
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
            | ErrorCode::EavFts001
            | ErrorCode::EavTx001
            | ErrorCode::EavTx004 => "eav",
            ErrorCode::Cod001
            | ErrorCode::Cod002
            | ErrorCode::Cod003
            | ErrorCode::CodScope001
            | ErrorCode::CodMat001 => "codice",
            ErrorCode::Iop001 | ErrorCode::Iop002 | ErrorCode::Iop003 | ErrorCode::Iop004 => "iop",
            ErrorCode::Quota001 => "quota",
            ErrorCode::Infra001
            | ErrorCode::Infra002
            | ErrorCode::Infra003
            | ErrorCode::Infra004
            | ErrorCode::Infra005
            | ErrorCode::InfraCatalog001
            | ErrorCode::InfraConfig001
            | ErrorCode::InfraGlue001
            | ErrorCode::InfraFirehose001
            | ErrorCode::InfraAthena005
            | ErrorCode::InfraCedar002 => "infrastructure",
            ErrorCode::Auth401 | ErrorCode::Auth403 | ErrorCode::AuthRevoked => "auth",
            ErrorCode::Aud001 | ErrorCode::Aud002 => "audit-interceptor",
            ErrorCode::GrpcTenant001 => "grpc-interceptor",
            ErrorCode::Mcp503 => "mcp",
            ErrorCode::Fml001
            | ErrorCode::Fml002
            | ErrorCode::Fml003
            | ErrorCode::Fml004
            | ErrorCode::Fml005
            | ErrorCode::Fml006
            | ErrorCode::Fml007
            | ErrorCode::Fml008
            | ErrorCode::Fml009
            | ErrorCode::Fml010
            | ErrorCode::Fml011
            | ErrorCode::Fml012
            | ErrorCode::Fml013 => "aegis::formula",
        }
    }
}

/// Error de dominio Railway-Oriented.
/// Reemplaza el vector el stack anterior [:error {:code :JANUS_400 :detail "..." :retryable? false}]
#[derive(Debug, Clone, Error, PartialEq, Serialize, Deserialize)]
#[error("{code:?}: {detail}")]
pub struct DomainError {
    pub code: ErrorCode,
    pub stage: String,
    pub detail: String,
    pub retryable: bool,
    /// /// Contexto adicional (equivale al ctx-map del error el stack anterior)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Box<serde_json::Value>>,
}

impl DomainError {
    /// /// Constructor principal — equivale a (error code ctx-map) en el stack anterior.
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
        self.context = Some(Box::new(context));
        self
    }

    /// Contexto estructurado, si lo hay.
    ///
    /// El campo vive encajonado para mantener `DomainError` por debajo del
    /// umbral de clippy `result_large_err`: es el tipo de retorno de casi todo
    /// el dominio y su peso se pagaría en cada llamada.
    pub fn context(&self) -> Option<&serde_json::Value> {
        self.context.as_deref()
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

#[cfg(test)]
#[path = "tests/errors_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/catalog_parity_tests.rs"]
mod catalog_parity_tests;
