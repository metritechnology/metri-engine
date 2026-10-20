//! Contrato de estrategia de normalización (`NormalizerStrategy`) — una implementación por forma de RPC.
use serde_json::Value;

/// Estrategia que normaliza in-place el cuerpo JSON de una respuesta según su categoría de operación.
pub trait NormalizerStrategy {
    /// Aplica las garantías de estructura y valores por defecto sobre `body`.
    fn normalize(&self, body: &mut Value, success: bool);
}
