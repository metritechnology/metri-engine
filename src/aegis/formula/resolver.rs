// aegis/formula/resolver.rs — Trait para resolución de variables.
// LSP: OltpVariableResolver y OlapVariableResolver son intercambiables.
// DIP: evaluator.rs depende de este trait, NO de implementaciones concretas.

/// Contrato para resolver el valor de una variable en el contexto de una fórmula.
/// Abstrae la diferencia entre OLAP (columna SQL) y OLTP (row JSON).
pub trait VariableResolver: Send + Sync {
    /// Resuelve una variable a su valor numérico.
    /// Retorna None si la variable no existe o no es numérica.
    fn resolve(&self, var_name: &str) -> Option<f64>;
}

/// Resolución OLTP: busca en un row JSON con dual-lookup namespace-aware.
pub struct OltpVariableResolver<'a> {
    row: &'a serde_json::Map<String, serde_json::Value>,
}

impl<'a> OltpVariableResolver<'a> {
    pub fn new(row: &'a serde_json::Map<String, serde_json::Value>) -> Self {
        Self { row }
    }
}

impl<'a> VariableResolver for OltpVariableResolver<'a> {
    fn resolve(&self, var_name: &str) -> Option<f64> {
        let bare_name = var_name.split('/').last().unwrap_or(var_name);
        self.row
            .get(var_name)
            .or_else(|| self.row.get(bare_name))
            .and_then(|v| v.as_f64())
    }
}

/// Resolución OLAP: transforma variable namespace → nombre de columna SQL.
pub struct OlapVariableResolver;

impl OlapVariableResolver {
    /// Transforma "asset/health_score" → "health_score"
    /// Transforma "asset/health-score" → "health_score"
    pub fn resolve_to_sql_column(var_name: &str) -> String {
        if var_name == "tenant/id" || var_name == "entity/tenant-id" {
            return "_tenant".to_string();
        }
        let base = var_name.split('/').last().unwrap_or(var_name);
        base.replace('-', "_")
    }
}

impl VariableResolver for OlapVariableResolver {
    fn resolve(&self, _var_name: &str) -> Option<f64> {
        None
    }
}
