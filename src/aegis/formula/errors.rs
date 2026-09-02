// aegis/formula/errors.rs — Errores específicos del motor de fórmulas.
// SRP: Solo define tipos de error y el bridge a DomainError.

use crate::domain::errors::{DomainError, ErrorCode};

/// Errores específicos del motor de fórmulas.
/// Cada variante corresponde a un código `FML_0XX` del catálogo global.
#[derive(Debug, Clone, PartialEq)]
pub enum FormulaError {
    // ─── Errores de Lexer/Parser (validación estática) ───
    /// FML_001: Token no reconocido en la posición indicada.
    UnexpectedToken { position: usize, found: String },
    /// FML_002: Paréntesis abierto sin cerrar o viceversa.
    UnbalancedParentheses { position: usize },
    /// FML_003: Función no registrada en el catálogo.
    UnknownFunction { name: String },
    /// FML_004: Aridad incorrecta (esperada vs recibida).
    ArityMismatch {
        function: String,
        expected: usize,
        got: usize,
    },
    /// FML_007: Inyección SQL detectada (solo OLAP path).
    SqlInjectionDetected { fragment: String },
    /// FML_013: Fórmula vacía.
    EmptyFormula,

    // ─── Errores de Evaluación (runtime OLTP) ───
    /// FML_005: División por cero.
    DivisionByZero,
    /// FML_006: Dominio matemático inválido (ej: SQRT(-1), LOG(0)).
    MathDomainError { function: String, detail: String },
    /// FML_012: Variable no encontrada en el row/contexto.
    UnresolvedVariable { name: String },

    // ─── Errores de Límites ───
    /// FML_008: Fórmula excede longitud máxima.
    FormulaTooLong { length: usize, max: usize },
    /// FML_009: Profundidad de paréntesis excede límite.
    NestingTooDeep { depth: usize, max: usize },
    /// FML_010: Número de tokens excede límite.
    TooManyTokens { count: usize, max: usize },
    /// FML_011: Funciones anidadas exceden límite.
    FunctionNestingTooDeep { depth: usize, max: usize },
}

impl FormulaError {
    /// Código de error del catálogo global (TOML).
    /// Usado por Sherlog para lookup de severidad y dispatch EDA.
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::UnexpectedToken { .. } => "FML_001",
            Self::UnbalancedParentheses { .. } => "FML_002",
            Self::UnknownFunction { .. } => "FML_003",
            Self::ArityMismatch { .. } => "FML_004",
            Self::DivisionByZero => "FML_005",
            Self::MathDomainError { .. } => "FML_006",
            Self::SqlInjectionDetected { .. } => "FML_007",
            Self::FormulaTooLong { .. } => "FML_008",
            Self::NestingTooDeep { .. } => "FML_009",
            Self::TooManyTokens { .. } => "FML_010",
            Self::FunctionNestingTooDeep { .. } => "FML_011",
            Self::UnresolvedVariable { .. } => "FML_012",
            Self::EmptyFormula => "FML_013",
        }
    }

    /// Mensaje humano para el error.
    pub fn detail(&self) -> String {
        match self {
            Self::UnexpectedToken { position, found } => {
                format!("Unexpected token '{}' at position {}", found, position)
            }
            Self::UnbalancedParentheses { position } => {
                format!("Unbalanced parentheses at position {}", position)
            }
            Self::UnknownFunction { name } => format!("Unknown function: {}", name),
            Self::ArityMismatch {
                function,
                expected,
                got,
            } => format!("{}() expects {} args, got {}", function, expected, got),
            Self::DivisionByZero => "Division by zero".to_string(),
            Self::MathDomainError { function, detail } => {
                format!("Math domain error in {}(): {}", function, detail)
            }
            Self::SqlInjectionDetected { fragment } => {
                format!("SQL injection detected: '{}'", fragment)
            }
            Self::FormulaTooLong { length, max } => {
                format!("Formula length {} exceeds max {}", length, max)
            }
            Self::NestingTooDeep { depth, max } => {
                format!("Nesting depth {} exceeds max {}", depth, max)
            }
            Self::TooManyTokens { count, max } => {
                format!("Token count {} exceeds max {}", count, max)
            }
            Self::FunctionNestingTooDeep { depth, max } => {
                format!("Function nesting depth {} exceeds max {}", depth, max)
            }
            Self::UnresolvedVariable { name } => format!("Unresolved variable: {}", name),
            Self::EmptyFormula => "Empty formula".to_string(),
        }
    }

    /// Convierte a DomainError del catálogo de errores del engine.
    /// El código se usa para lookup en error_catalog.toml y Sherlog.
    pub fn to_domain_error(&self) -> DomainError {
        let code = match self {
            Self::UnexpectedToken { .. } => ErrorCode::Fml001,
            Self::UnbalancedParentheses { .. } => ErrorCode::Fml002,
            Self::UnknownFunction { .. } => ErrorCode::Fml003,
            Self::ArityMismatch { .. } => ErrorCode::Fml004,
            Self::DivisionByZero => ErrorCode::Fml005,
            Self::MathDomainError { .. } => ErrorCode::Fml006,
            Self::SqlInjectionDetected { .. } => ErrorCode::Fml007,
            Self::FormulaTooLong { .. } => ErrorCode::Fml008,
            Self::NestingTooDeep { .. } => ErrorCode::Fml009,
            Self::TooManyTokens { .. } => ErrorCode::Fml010,
            Self::FunctionNestingTooDeep { .. } => ErrorCode::Fml011,
            Self::UnresolvedVariable { .. } => ErrorCode::Fml012,
            Self::EmptyFormula => ErrorCode::Fml013,
        };
        let detail = self.detail();
        let retryable = false; // Todas las FormulaError son NOT retryable

        DomainError {
            code,
            stage: "aegis::formula".to_string(),
            detail,
            retryable,
            context: Some(serde_json::json!({
                "error_type": format!("{:?}", self),
                "formula_module": "aegis::formula",
            })),
        }
    }

    /// Emite el error al pipeline Sherlog (si la severidad lo requiere).
    pub async fn emit_to_sherlog(
        &self,
        notifier: &dyn crate::iop::sherlog::IFaultNotifier,
        olap_channel: &dyn crate::janus_router::router::IWriteChannel,
        ctx: &crate::iop::core::IopContext,
    ) {
        let domain_error = self.to_domain_error();
        let model =
            crate::codice::registry::global_opt().and_then(|r| r.get_model(&ctx.entity_type));

        let error_dto = crate::iop::error_response::build_error_dto(
            &domain_error,
            &ctx.tenant_id,
            &ctx.user_id,
            domain_error.context.clone(),
            model,
        );

        crate::iop::sherlog::process_fault(
            notifier,
            olap_channel,
            &domain_error,
            &error_dto,
            Some("formula".to_string()),
        )
        .await;
    }
}
