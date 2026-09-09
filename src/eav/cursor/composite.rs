//! Composite cursor for multi-index pagination.
//!
//! Cursor compuesto para paginación multi-índice
//! Blueprint: Metri EAV §XIII.1

use crate::domain::errors::{DomainError, ErrorCode};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};

/// Estado de paginación para UN índice participante en la query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexCursorState {
    pub index_name: String,
    pub last_pk: String,
    pub last_sk: Vec<u8>,
    pub exhausted: bool,
}

/// Cursor compuesto opaco — codifica el estado completo de la paginación.
/// Soporta intersecciones de múltiples índices GSI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeCursor {
    /// Versión del protocolo — permite cambios futuros sin romper clientes.
    pub version: u8,
    /// Hash del AST IR original — invalida el cursor si el query cambia entre páginas.
    pub query_fingerprint: u64,
    pub page_size: u32,
    pub index_states: Vec<IndexCursorState>,
    /// Entity IDs que quedaron a mitad de una intersección parcial.
    pub intersection_resume: Vec<u64>,
}

impl CompositeCursor {
    /// Crea un cursor vacío para la primera página.
    pub fn first_page(query_fingerprint: u64, page_size: u32) -> Self {
        CompositeCursor {
            version: 1,
            query_fingerprint,
            page_size,
            index_states: vec![],
            intersection_resume: vec![],
        }
    }

    /// Serializa a Base64(MessagePack) — URL-safe, < 512 bytes en casos normales.
    pub fn encode(&self) -> Result<String, DomainError> {
        let bytes = rmp_serde::to_vec(self).map_err(|e| {
            DomainError::eav(ErrorCode::Eav003, format!("cursor encode error: {e}"))
        })?;
        Ok(URL_SAFE_NO_PAD.encode(&bytes))
    }

    /// Deserializa y valida el fingerprint del query actual.
    /// Si el fingerprint no coincide → el cliente cambió los filtros → reiniciar.
    pub fn decode(token: &str, current_fingerprint: u64) -> Result<Self, DomainError> {
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| {
            DomainError::eav(ErrorCode::Eav003, "cursor base64 inválido".to_string())
        })?;

        let cursor: CompositeCursor = rmp_serde::from_slice(&bytes).map_err(|e| {
            DomainError::eav(ErrorCode::Eav003, format!("cursor decode error: {e}"))
        })?;

        if cursor.query_fingerprint != current_fingerprint {
            return Err(DomainError::eav(
                ErrorCode::Eav003,
                "cursor stale: el query cambió entre páginas".to_string(),
            ));
        }

        Ok(cursor)
    }
}

/// Genera un fingerprint determinístico para el AST IR de un query.
/// Usa xxh64 — extremadamente rápido, suficiente para validación de cursors.
pub fn fingerprint_query(query_json: &serde_json::Value) -> u64 {
    use xxhash_rust::xxh64::xxh64;
    let serialized = serde_json::to_vec(query_json).unwrap_or_default();
    xxh64(&serialized, 0xdeadbeef)
}
