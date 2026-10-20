//! Cargador nativo para el catálogo canónico de errores en formato TOML.
//!
//! Lee `config/errors/error_catalog.toml` durante el arranque del motor y expone
//! una tabla hash inmutable con consulta $O(1)$ por código canónico.
//! Verifica la paridad 1:1 entre `ErrorCode::ALL` y las definiciones del catálogo.

use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

use serde::Deserialize;
use tracing::info;

use crate::domain::errors::{DomainError, ErrorCode};

/// Una entrada del catálogo de errores.
#[derive(Debug, Clone, Deserialize)]
pub struct ErrorEntry {
    /// Código canónico único del error (ej. `"JANUS_400"`, `"EAV_005"`).
    pub code: String,
    /// Familia funcional a la que pertenece el error (ej. `"jns"`, `"eav"`, `"aeg"`, `"mbf"`).
    pub family: String,
    /// Etapa o subsistema del pipeline donde se origina el fallo.
    pub stage: String,
    /// Nivel de severidad operativa (`"info"`, `"warning"`, `"error"`, `"fatal"`).
    pub severity: String,
    /// Código de estado HTTP equivalente para pasarelas REST/BFF.
    pub http_status: u16,
    /// Nombre canónico del código de estado gRPC (ej. `"INVALID_ARGUMENT"`, `"ABORTED"`).
    pub grpc_status: String,
    /// Descripción técnica detallada de la condición de error.
    pub description: String,
    /// Lista de claves de contexto obligatorias al construir el error.
    pub context_required: Vec<String>,
    /// Indica si la operación fallida puede ser reintentada de forma segura por el cliente.
    pub retryable: bool,
    /// Entrada reservada para uso futuro, sin variante activa asociada todavía.
    #[serde(default)]
    pub reserved: bool,
    /// Ya publicada a clientes pero retirada del mapeo actual.
    #[serde(default)]
    pub deprecated: bool,
}

/// Catálogo completo — cargado una sola vez en bootstrap.
#[derive(Debug, Deserialize)]
struct ErrorCatalogRaw {
    pub version: String,
    pub errors: Vec<ErrorEntry>,
}

/// Catálogo canónico de errores indexado en memoria para resolución $O(1)$ tras el arranque.
pub struct ErrorCatalog {
    /// Versión semántica declarada en el archivo `error_catalog.toml`.
    pub version: String,
    by_code: HashMap<String, ErrorEntry>,
}

impl std::fmt::Debug for ErrorCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorCatalog")
            .field("version", &self.version)
            .field("entry_count", &self.by_code.len())
            .finish()
    }
}

impl ErrorCatalog {
    /// Carga y parsea el catálogo desde el archivo TOML.
    ///
    /// # Errors
    ///
    /// Retorna `ErrorCode::InfraCatalog001` si el archivo no existe o el TOML
    /// está malformado — en bootstrap el llamador hace fail-fast (`exit(1)`).
    pub fn load(path: impl AsRef<Path>) -> Result<Self, DomainError> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            DomainError::new(
                ErrorCode::InfraCatalog001,
                format!("cannot read {:?}: {e}", path.as_ref()),
            )
        })?;

        let raw: ErrorCatalogRaw = toml::from_str(&content).map_err(|e| {
            DomainError::new(
                ErrorCode::InfraCatalog001,
                format!("TOML parse failed: {e}"),
            )
        })?;

        let entry_count = raw.errors.len();
        let by_code: HashMap<String, ErrorEntry> = raw
            .errors
            .into_iter()
            .map(|e| (e.code.clone(), e))
            .collect();

        info!(
            version = %raw.version,
            entries = entry_count,
            "[ErrorCatalog] Catálogo cargado"
        );

        Ok(ErrorCatalog {
            version: raw.version,
            by_code,
        })
    }

    /// Lookup O(1) por código de error.
    pub fn get(&self, code: &str) -> Option<&ErrorEntry> {
        self.by_code.get(code)
    }

    /// Devuelve el http_status para un código (útil en el gRPC handler).
    pub fn http_status(&self, code: &str) -> u16 {
        self.get(code).map(|e| e.http_status).unwrap_or(500)
    }

    /// Devuelve si el error es reintentable.
    pub fn is_retryable(&self, code: &str) -> bool {
        self.get(code).map(|e| e.retryable).unwrap_or(false)
    }

    /// Retorna la cantidad total de entradas registradas en el catálogo.
    pub fn entry_count(&self) -> usize {
        self.by_code.len()
    }
}

// ── Singleton global (OnceLock — zero-cost after init) ───────────────────────

static GLOBAL_CATALOG: OnceLock<ErrorCatalog> = OnceLock::new();

/// Inicializa el catálogo global. Llamar UNA VEZ en bootstrap.
#[allow(clippy::panic)] // invariante allowlisted (PLAN_PATRON_RESULT.md R7)
#[allow(clippy::expect_used)] // invariante allowlisted (PLAN_PATRON_RESULT.md R7)
pub fn init_global(catalog: ErrorCatalog) {
    // Validar que cada código de error de Rust tiene su definición en el catálogo TOML
    for code in crate::domain::errors::ErrorCode::ALL {
        let canon = code.canonical_code();
        if catalog.get(canon).is_none() {
            panic!("ErrorCatalog: missing definition for canonical code '{canon}' corresponding to ErrorCode::{code:?}");
        }
    }
    GLOBAL_CATALOG
        .set(catalog)
        .expect("ErrorCatalog already initialized");
}

/// Accede al catálogo global (panic si no fue inicializado).
#[allow(clippy::expect_used)] // invariante allowlisted (PLAN_PATRON_RESULT.md R7)
pub fn global() -> &'static ErrorCatalog {
    GLOBAL_CATALOG
        .get()
        .expect("ErrorCatalog not initialized — call init_global() in bootstrap")
}

/// Versión safe para contextos donde el catálogo puede no estar disponible.
pub fn try_global() -> Option<&'static ErrorCatalog> {
    GLOBAL_CATALOG.get()
}

#[cfg(test)]
#[path = "tests/error_catalog_tests.rs"]
mod tests;
