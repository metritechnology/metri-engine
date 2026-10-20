//! Resolución de variables para contextos de evaluación de fórmulas.
//!
//! Define el contrato `VariableResolver` (LSP / DIP) que permite a `FormulaEvaluator`
//! resolver identificadores numéricos sin acoplarse a la estructura física de la fila.

/// Contrato para resolver el valor numérico de una variable en el contexto de una fórmula.
pub trait VariableResolver: Send + Sync {
    /// Resuelve una variable a su valor numérico.
    /// Retorna None si la variable no existe o no es numérica.
    fn resolve(&self, var_name: &str) -> Option<f64>;
}

/// Resolvedor de variables sobre filas JSON en memoria con búsqueda dual (con y sin namespace).
pub struct OltpVariableResolver<'a> {
    row: &'a serde_json::Map<String, serde_json::Value>,
}

impl<'a> OltpVariableResolver<'a> {
    /// Construye un resolvedor vinculado al mapa de atributos de una fila JSON.
    pub fn new(row: &'a serde_json::Map<String, serde_json::Value>) -> Self {
        Self { row }
    }
}

impl<'a> VariableResolver for OltpVariableResolver<'a> {
    fn resolve(&self, var_name: &str) -> Option<f64> {
        let bare_name = var_name.split('/').next_back().unwrap_or(var_name);
        self.row
            .get(var_name)
            .or_else(|| self.row.get(bare_name))
            .and_then(|v| v.as_f64())
    }
}

/// Normalizador de nombres de atributos con namespace hacia identificadores planos de columna.
pub struct OlapVariableResolver;

impl OlapVariableResolver {
    /// Transforma "asset/health_score" → "health_score"
    /// Transforma "asset/health-score" → "health_score"
    pub fn resolve_to_sql_column(var_name: &str) -> String {
        if var_name == crate::eav::writer::system_attrs::TENANT_NAME {
            return "_tenant".to_string();
        }
        let base = var_name.split('/').next_back().unwrap_or(var_name);
        base.replace('-', "_")
    }
}

impl VariableResolver for OlapVariableResolver {
    fn resolve(&self, _var_name: &str) -> Option<f64> {
        None
    }
}
