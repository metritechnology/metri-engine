// domain/error_catalog.rs — Loader nativo del catálogo de errores
// Lee config/errors/error_catalog.toml en bootstrap y expone un HashMap O(1).
// Reemplaza la referencia a errors/error_catalog.edn de Clojure.
//
// Uso:
//   let catalog = ErrorCatalog::load("config/errors/error_catalog.toml").unwrap();
//   let entry = catalog.get("EAV_002");

use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

use serde::Deserialize;
use tracing::info;

/// Una entrada del catálogo de errores.
#[derive(Debug, Clone, Deserialize)]
pub struct ErrorEntry {
    pub code:             String,
    pub family:           String,
    pub stage:            String,
    pub severity:         String,
    pub http_status:      u16,
    pub grpc_status:      String,
    pub description:      String,
    pub context_required: Vec<String>,
    pub retryable:        bool,
}

/// Catálogo completo — cargado una sola vez en bootstrap.
#[derive(Debug, Deserialize)]
struct ErrorCatalogRaw {
    pub version:   String,
    pub errors:    Vec<ErrorEntry>,
}

pub struct ErrorCatalog {
    pub version: String,
    by_code:     HashMap<String, ErrorEntry>,
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
    /// Falla en bootstrap si el archivo no existe o está malformado.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| format!("ErrorCatalog: cannot read {:?}: {e}", path.as_ref()))?;

        let raw: ErrorCatalogRaw = toml::from_str(&content)
            .map_err(|e| format!("ErrorCatalog: TOML parse failed: {e}"))?;

        let entry_count = raw.errors.len();
        let by_code: HashMap<String, ErrorEntry> = raw.errors
            .into_iter()
            .map(|e| (e.code.clone(), e))
            .collect();

        info!(
            version = %raw.version,
            entries = entry_count,
            "[ErrorCatalog] Catálogo cargado"
        );

        Ok(ErrorCatalog { version: raw.version, by_code })
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

    pub fn entry_count(&self) -> usize {
        self.by_code.len()
    }
}

// ── Singleton global (OnceLock — zero-cost after init) ───────────────────────

static GLOBAL_CATALOG: OnceLock<ErrorCatalog> = OnceLock::new();

/// Inicializa el catálogo global. Llamar UNA VEZ en bootstrap.
pub fn init_global(catalog: ErrorCatalog) {
    GLOBAL_CATALOG.set(catalog).expect("ErrorCatalog already initialized");
}

/// Accede al catálogo global (panic si no fue inicializado).
pub fn global() -> &'static ErrorCatalog {
    GLOBAL_CATALOG.get().expect("ErrorCatalog not initialized — call init_global() in bootstrap")
}

/// Versión safe para contextos donde el catálogo puede no estar disponible.
pub fn try_global() -> Option<&'static ErrorCatalog> {
    GLOBAL_CATALOG.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_catalog_from_file() {
        let catalog = ErrorCatalog::load("config/errors/error_catalog.toml").unwrap();
        assert!(catalog.entry_count() > 10);
        assert!(catalog.get("EAV_002").is_some());
        assert!(catalog.get("JANUS_400").is_some());
        assert!(catalog.get("AEG_001").is_some());
    }

    #[test]
    fn http_status_lookup() {
        let catalog = ErrorCatalog::load("config/errors/error_catalog.toml").unwrap();
        assert_eq!(catalog.http_status("JANUS_403"), 403);
        assert_eq!(catalog.http_status("EAV_TX_003"), 409);
        assert_eq!(catalog.http_status("INFRA_DDB_002"), 429);
    }

    #[test]
    fn retryable_flag() {
        let catalog = ErrorCatalog::load("config/errors/error_catalog.toml").unwrap();
        assert!(catalog.is_retryable("EAV_TX_003"));   // ConcurrentModification → retry
        assert!(!catalog.is_retryable("JANUS_400"));   // Bad request → no retry
    }

    #[test]
    fn unknown_code_returns_none() {
        let catalog = ErrorCatalog::load("config/errors/error_catalog.toml").unwrap();
        assert!(catalog.get("NONEXISTENT_CODE").is_none());
        assert_eq!(catalog.http_status("NONEXISTENT_CODE"), 500); // fallback
    }
}
