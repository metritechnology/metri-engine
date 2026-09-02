// [NUEVO — no tiene equivalente en Clojure]
// src/eav/types/value_type.rs
// Define los 12 tipos de valor del sistema EAV y sus tag bytes para SK binario.
// Diseñado según Metri EAV - OLPT.md §II.3

use serde::{Deserialize, Serialize};

/// Los 12 tipos de valor del sistema EAV.
/// Cada variante tiene un tag byte para el Sort Key binario.
/// El tag determina el orden lexicográfico entre tipos distintos.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueType {
    Null,      // 0x00 — valor nulo / retract marker
    Boolean,   // 0x01
    Long,      // 0x02 — number / epoch (i64)
    Double,    // 0x03 — decimal (f64, IEEE 754 bit-flip para AVET ordering)
    String,    // 0x04
    Uuid,      // 0x05
    Reference, // 0x06 — entity_id (u64) apuntando a otra entidad
    Array,     // 0x07 — Vec<String> serializado
    Bytes,     // 0x08 — datos binarios opacos
    BigInt,    // 0x09 — números grandes (sequencias)
    Instant,   // 0x0A — timestamp epoch ms (i64, mismo encoding que Long)
    Geo,       // 0x0B — [lat, lon] codificados como dos f64
}

impl ValueType {
    /// Tag byte para el Sort Key binario (prefijo del SK en AVET/VAET).
    pub const fn tag(&self) -> u8 {
        match self {
            ValueType::Null => 0x00,
            ValueType::Boolean => 0x01,
            ValueType::Long => 0x02,
            ValueType::Double => 0x03,
            ValueType::String => 0x04,
            ValueType::Uuid => 0x05,
            ValueType::Reference => 0x06,
            ValueType::Array => 0x07,
            ValueType::Bytes => 0x08,
            ValueType::BigInt => 0x09,
            ValueType::Instant => 0x0A,
            ValueType::Geo => 0x0B,
        }
    }

    /// Indica si este tipo puede generar un item AVET (indexable por valor).
    /// Bytes y Array no son indexables directamente por valor.
    pub const fn is_avet_indexable(&self) -> bool {
        !matches!(self, ValueType::Bytes | ValueType::Array | ValueType::Null)
    }
}
