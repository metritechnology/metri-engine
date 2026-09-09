//! Cursor pagination — Base64(offset:limit).
//!
//! Paginación por cursor Base64(offset:limit).
//!
//! Estrategia: cursor = Base64("offset:limit")
//! cursor = "" / None → página 1, offset = 0
//! "MTA6MTA=" → Base64("10:10") → offset=10, limit=10 (página 2)
//! "MjA6MTA=" → Base64("20:10") → offset=20, limit=10 (página 3)
//!
//! SRP: módulo exclusivo de paginación — sin dependencias de negocio.
//! Puro: todas las funciones son puras (sin side-effects).

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde_json::{json, Value};

/// Decodifica un cursor Base64(offset:limit) en (offset, limit).
/// Retorna (0, fallback_limit) cuando el cursor es None/vacío o inválido.
pub fn decode_cursor(cursor: Option<&str>, fallback_limit: usize) -> (usize, usize) {
    let Some(c) = cursor.filter(|s| !s.is_empty()) else {
        return (0, fallback_limit);
    };

    let decoded = match BASE64.decode(c) {
        Ok(b) => String::from_utf8(b).unwrap_or_default(),
        Err(_) => return (0, fallback_limit),
    };

    let mut parts = decoded.splitn(2, ':');
    let offset = parts
        .next()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let limit = parts
        .next()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(fallback_limit);
    (offset, limit)
}

/// Codifica offset + limit en un cursor opaco Base64(offset:limit).
pub fn encode_cursor(offset: usize, limit: usize) -> String {
    BASE64.encode(format!("{offset}:{limit}"))
}

/// Construye el mapa de paginación con cursores reales.
///
/// Retorna un JSON compatible con el contrato `QueryMetadata.Pagination`:
///   { page_size, has_next, has_previous, next_cursor?, previous_cursor? }
pub fn build_pagination(offset: usize, limit: usize, total: usize) -> Value {
    let has_next = (offset + limit) < total;
    let has_previous = offset > 0;

    let next_offset = offset + limit;
    let prev_offset = offset.saturating_sub(limit);

    let mut pagination = json!({
        "page_size":    limit,
        "has_next":     has_next,
        "has_previous": has_previous,
    });

    if has_next {
        pagination["next_cursor"] = json!(encode_cursor(next_offset, limit));
    }
    if has_previous {
        pagination["previous_cursor"] = json!(encode_cursor(prev_offset, limit));
    }

    pagination
}

/// Aplica offset + limit a un vector de rows ya ordenados.
/// Equivalente a SQL: OFFSET offset LIMIT limit.
pub fn paginate_rows(rows: Vec<Value>, offset: usize, limit: usize) -> Vec<Value> {
    rows.into_iter().skip(offset).take(limit).collect()
}
