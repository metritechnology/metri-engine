//! Mónada de error Railway (`DomainError`) y catálogo canónico de códigos de error (`ErrorCode`).
//!
//! Mantiene una correspondencia biyectiva 1:1 entre cada variante de `ErrorCode`
//! y su entrada canónica en `config/errors/error_catalog.toml` (ADR-006).

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Catálogo exhaustivo de códigos de error tipados del motor.
#[rustfmt::skip]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    #[doc = "Petición rechazada por validación inicial en Janus (`JANUS_400`)."] Janus400,
    #[doc = "Token de autenticación inválido o expirado en Janus (`JANUS_401`)."] Janus401,
    #[doc = "Acceso denegado por evaluación de política Cedar en Janus (`JANUS_403`)."] Janus403,
    #[doc = "Recurso o entidad objetivo no encontrado en Janus (`JANUS_404`)."] Janus404,
    #[doc = "Petición semánticamente inválida o referencia fallida (`JANUS_422`)."] Janus422,
    #[doc = "Error interno no controlado en el manejador gRPC (`GRPC_500`)."] Janus500,
    #[doc = "Fallo al compilar el AST IR hacia un plan de consulta EAV (`AEG_COMPILE_001`)."] JanusAstCompileError,
    #[doc = "Fallo al compilar el árbol de filtros del AST IR (`AEG_COMPILE_002`)."] JanusFilterCompileError,
    #[doc = "Tipo de entidad no encontrado en el registro del Códice (`CDX_003`)."] JanusSchemaNotFound,
    #[doc = "Aislamiento de tenant ausente en la cláusula `where` del AST IR (`AEG_TENANT_MISSING`)."] JanusTenantMismatch,
    #[doc = "Validación de esquema o contrato fallida en Janus Validator (`JANUS_VAL_001`)."] JanusVal001,
    #[doc = "No existe canal de escritura registrado para el motor solicitado (`JNS_001`)."] Jns001,
    #[doc = "Entidad bloqueada para escritura directa (`write_path_locked = true`, `JNS_LOCK_001`)."] JnsLock001,
    #[doc = "Entidad sembrada por el sistema e inmutable por clientes (`is_system_seeded = true`, `JNS_SEED_001`)."] JnsSeed001,
    #[doc = "Transacción ACID abortada en el canal de escritura (`JNS_TX_001`)."] JnsTx001,
    #[doc = "Fallo general de compilación de consulta analítica en Aegis (`AEG_COMPILE_003`)."] Aeg001,
    #[doc = "Fallo de ejecución de consulta en el motor Aegis (`AEG_002`)."] Aeg002,
    #[doc = "Tiempo máximo de ejecución excedido en consulta Aegis (`AEG_006`)."] Aeg003,
    #[doc = "Error al interpretar o dar forma a la salida de consulta Aegis (`AEG_007`)."] Aeg004,
    #[doc = "Función de agregación no soportada por el motor Aegis (`AEG_008`)."] Aeg005,
    #[doc = "Error al construir los ítems de escritura del almacén EAV (`EAV_001`)."] Eav001,
    #[doc = "Entidad no encontrada para el `entity_id` y `tenant` indicados (`EAV_005`)."] Eav002,
    #[doc = "Conflicto de concurrencia optimista o cursor de paginación obsoleto (`EAV_TX_003`)."] Eav003,
    #[doc = "Entidad o atributo no registrado en el esquema EAV (`EAV_004`)."] Eav004,
    #[doc = "Desbordamiento de clave de ordenamiento (> 1024 bytes, `EAV_KEY_001`)."] Eav005,
    #[doc = "Fallo al escribir el lote de trigramas en el índice FTS (`EAV_FTS_001`)."] EavFts001,
    #[doc = "Fallo al leer o parsear un archivo de modelo del Códice (`CDX_001`)."] Cod001,
    #[doc = "Entidad duplicada o esquema inválido en el Códice (`CDX_002`)."] Cod002,
    #[doc = "Colisión de huella SHA-256 entre modelos del Códice (`CDX_004`)."] Cod003,
    #[doc = "Proveedor de ámbito de secuencia inválido o inexistente (`JNS_SCOPE_001`)."] CodScope001,
    #[doc = "Especificación de materialización referencia entidad, atributo o hook desconocido (`COD_MAT_001`)."] CodMat001,
    #[doc = "Fallo de validación de integridad en el pipeline IOP (`IOP_001`)."] Iop001,
    #[doc = "Fallo durante el enriquecimiento de contexto en el pipeline IOP (`IOP_002`)."] Iop002,
    #[doc = "Fallo de transacción ACID en el pipeline IOP (`IOP_003`)."] Iop003,
    #[doc = "Fallo al publicar en el outbox transaccional del pipeline IOP (`IOP_004`)."] Iop004,
    #[doc = "Cuota operativa del tenant agotada (`QUOTA_001`)."] Quota001,
    #[doc = "Error inesperado en el cliente de almacenamiento clave-valor (`INFRA_DDB_001`)."] Infra001,
    #[doc = "Error inesperado en operación sobre almacenamiento de objetos S3 (`INFRA_S3_001`)."] Infra002,
    #[doc = "Error inesperado en operación sobre cola de mensajes SQS (`INFRA_SQS_001`)."] Infra003,
    #[doc = "Error al publicar evento en el bus EventBridge (`INFRA_EB_001`)."] Infra004,
    #[doc = "Fallo al cargar o validar el catálogo de errores TOML en el arranque (`INFRA_CATALOG_001`)."] InfraCatalog001,
    #[doc = "Fallo al resolver o validar la configuración del motor en el arranque (`INFRA_CONFIG_001`)."] InfraConfig001,
    #[doc = "Fallo de sincronización de catálogo en el cliente Glue (`INFRA_GLUE_001`)."] InfraGlue001,
    #[doc = "Fallo en flujo de entrega de telemetría Firehose (`INFRA_FIREHOSE_001`)."] InfraFirehose001,
    #[doc = "Verificación de firma HMAC-SHA256 fallida o token expirado (`GRPC_AUTH_001`)."] Auth401,
    #[doc = "Discrepancia de tenant: principal sin acceso al tenant solicitado (`GRPC_AUTH_004`)."] Auth403,
    #[doc = "Token presente en la lista de sesiones revocadas (`GRPC_AUTH_003`)."] AuthRevoked,
    #[doc = "Fallo al emitir registro en el canal de auditoría (`AUD_001`)."] Aud001,
    #[doc = "Derivación de `ActionType` desconocida en el interceptor de auditoría (`AUD_002`)."] Aud002,
    #[doc = "Entidad referenciada existe pero pertenece a un tipo de entidad distinto (`JNS_REF_002`)."] JnsRef002,
    #[doc = "Violación de restricción de unicidad en el índice AVET (`JNS_CONFLICT_001`)."] JnsConflict001,
    #[doc = "Transacción de escritura cancelada por condición atómica (`EAV_TX_001`)."] EavTx001,
    #[doc = "Colisión de escritura condicional sobre datom histórico inmutable (`EAV_TX_004`)."] EavTx004,
    #[doc = "Metadato `tenant_id` ausente o vacío en la cabecera gRPC (`GRPC_TENANT_001`)."] GrpcTenant001,
    #[doc = "Fallo crítico al cargar el almacén de políticas Cedar en el arranque (`INFRA_CEDAR_002`)."] InfraCedar002,
    #[doc = "Servicio de modelo de lenguaje (LLM) no disponible o con timeout (`MCP_503`)."] Mcp503,
    #[doc = "Token inesperado durante el análisis léxico de una fórmula (`FML_001`)."] Fml001,
    #[doc = "Paréntesis desbalanceados en la expresión de fórmula (`FML_002`)."] Fml002,
    #[doc = "Función desconocida invocada dentro de la fórmula (`FML_003`)."] Fml003,
    #[doc = "Número de argumentos inválido para la función en la fórmula (`FML_004`)."] Fml004,
    #[doc = "División por cero detectada al evaluar la fórmula (`FML_005`)."] Fml005,
    #[doc = "Error de dominio matemático al evaluar la fórmula (`FML_006`)."] Fml006,
    #[doc = "Patrón de inyección o token prohibido detectado en la fórmula (`FML_007`)."] Fml007,
    #[doc = "La fórmula excede la longitud máxima permitida (`FML_008`)."] Fml008,
    #[doc = "La profundidad de anidamiento de paréntesis excede el límite (`FML_009`)."] Fml009,
    #[doc = "La cantidad de tokens de la fórmula excede el límite (`FML_010`)."] Fml010,
    #[doc = "La profundidad de anidamiento de funciones excede el límite (`FML_011`)."] Fml011,
    #[doc = "Variable referenciada ausente en el contexto de la fila (`FML_012`)."] Fml012,
    #[doc = "Expresión de fórmula vacía recibida (`FML_013`)."] Fml013,
    #[doc = "Corrupción de integridad binaria en bloque `.mbf` (`MBF_CORRUPT_001`)."] MbfCorrupt001,
    #[doc = "Conflicto de escritura condicional CAS por solapamiento real (`MBF_CAS_CONFLICT_001`)."] MbfCasConflict001,
    #[doc = "Asiento contable desbalanceado o lote de inventario insuficiente (`MBF_LEDGER_UNBALANCED_001`)."] MbfLedgerUnbalanced001,
    #[doc = "Intento de mutación sobre un período fiscal cerrado (`MBF_FISCAL_CLOSED_001`)."] MbfFiscalClosed001,
    #[doc = "Discrepancia en el sello criptográfico de aislamiento por tenant (`MBF_TENANT_SEAL_001`)."] MbfTenantSeal001,
}

impl ErrorCode {
    #[doc = "Arreglo estático con la totalidad de las variantes de `ErrorCode` para validación exhaustiva."]
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
        ErrorCode::MbfCorrupt001,
        ErrorCode::MbfCasConflict001,
        ErrorCode::MbfLedgerUnbalanced001,
        ErrorCode::MbfFiscalClosed001,
        ErrorCode::MbfTenantSeal001,
    ];

    /// Obtiene el código canónico correspondiente en el catálogo de errores (TOML).
    ///
    /// Mapeo 1:1 (catálogo v3.0.0): cada variante tiene un código propio y
    /// semánticamente correcto. El guard `codigos_canonicos_son_unicos` impide
    /// compartir códigos; `todo_codigo_canonico_esta_en_el_catalogo` verifica el TOML.
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
            ErrorCode::MbfCorrupt001 => "MBF_CORRUPT_001",
            ErrorCode::MbfCasConflict001 => "MBF_CAS_CONFLICT_001",
            ErrorCode::MbfLedgerUnbalanced001 => "MBF_LEDGER_UNBALANCED_001",
            ErrorCode::MbfFiscalClosed001 => "MBF_FISCAL_CLOSED_001",
            ErrorCode::MbfTenantSeal001 => "MBF_TENANT_SEAL_001",
        }
    }

    /// Retorna true si el error admite reintento por el cliente.
    ///
    /// Consulta el catálogo global (TOML, fuente canónica); antes del bootstrap
    /// usa `ErrorCode::fallback_is_retryable`.
    pub fn is_retryable(&self) -> bool {
        match crate::domain::error_catalog::try_global() {
            Some(catalog) => catalog.is_retryable(self.canonical_code()),
            None => self.fallback_is_retryable(),
        }
    }

    /// Fallback estático previo al bootstrap.
    ///
    /// DEBE coincidir con el campo `retryable` de `error_catalog.toml`: el
    /// guard `fallback_retryable_coincide_con_el_catalogo` rompe el build si diverge.
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
                | ErrorCode::MbfCasConflict001
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
            | ErrorCode::InfraCatalog001
            | ErrorCode::InfraConfig001
            | ErrorCode::InfraGlue001
            | ErrorCode::InfraFirehose001
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
            ErrorCode::MbfCorrupt001 => "mbf::format",
            ErrorCode::MbfCasConflict001 => "mbf::tx",
            ErrorCode::MbfLedgerUnbalanced001 | ErrorCode::MbfFiscalClosed001 => "mbf::ledger",
            ErrorCode::MbfTenantSeal001 => "mbf::crypto",
        }
    }
}

/// Error de dominio estructurado bajo el patrón Railway-Oriented Programming.
#[derive(Debug, Clone, Error, PartialEq, Serialize, Deserialize)]
#[error("{code:?}: {detail}")]
pub struct DomainError {
    #[doc = "Código tipado del error dentro del catálogo canónico."]
    pub code: ErrorCode,
    #[doc = "Etapa o subsistema del pipeline donde se produjo el error."]
    pub stage: String,
    #[doc = "Mensaje descriptivo detallado de la causa del fallo."]
    pub detail: String,
    #[doc = "Indica si la operación puede ser reintentada por el llamador."]
    pub retryable: bool,
    /// Contexto adicional estructurado asociado al fallo.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Box<serde_json::Value>>,
}

impl DomainError {
    /// Constructor principal de `DomainError` que infiere `stage` y `retryable` desde `ErrorCode`.
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

    #[doc = "Construye un `DomainError` originado en el subsistema Janus."]
    pub fn janus(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    #[doc = "Construye un `DomainError` originado en el motor EAV."]
    pub fn eav(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    #[doc = "Construye un `DomainError` originado en el motor analítico Aegis."]
    pub fn aegis(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    #[doc = "Construye un `DomainError` originado en la capa de autenticación/autorización."]
    pub fn auth(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    #[doc = "Construye un `DomainError` originado en adaptadores de infraestructura."]
    pub fn infra(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    #[doc = "Construye un `DomainError` originado en el registro o validador del Códice."]
    pub fn codice(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }

    #[doc = "Construye un `DomainError` originado en el motor binario `.mbf`."]
    pub fn mbf(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, detail)
    }
}

#[cfg(test)]
#[path = "tests/errors_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/catalog_parity_tests.rs"]
mod catalog_parity_tests;
