//! The twelve EAV value types and their SK tag bytes.
//!
//! # Origin
//! [NUEVO — no tiene equivalente en el stack anterior]
//! src/eav/types/value_type.rs
//! Define los 12 tipos de valor del sistema EAV y sus tag bytes para SK binario.
//! Diseñado según Metri EAV - OLPT.md §II.3

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

/// Regla canónica de indexabilidad AVET a nivel de ATRIBUTO (códice).
///
/// Único lugar donde se decide qué tipos de atributo puede filtrar el índice
/// AVET. `validate_list_filters` (grpc) consulta aquí en vez de reimplementar
/// la regla; el escritor aplica el mismo criterio a nivel de datom vía
/// [`ValueType::is_avet_indexable`]. La divergencia histórica en `Null` muere
/// por construcción: `Null` no existe como `AttrType` y a nivel de datom ya lo
/// excluye `is_avet_indexable`.
pub fn attr_type_is_avet_indexable(attr_type: &crate::codice::registry::AttrType) -> bool {
    use crate::codice::registry::AttrType;
    !matches!(attr_type, AttrType::Bytes | AttrType::Array)
}

#[cfg(test)]
mod attr_rule_tests {
    use super::attr_type_is_avet_indexable;
    use crate::codice::registry::AttrType;

    #[test]
    fn los_tipos_que_el_avet_no_indexa_estan_prohibidos_como_filtro() {
        assert!(!attr_type_is_avet_indexable(&AttrType::Bytes));
        assert!(!attr_type_is_avet_indexable(&AttrType::Array));
    }

    #[test]
    fn los_tipos_indexables_se_aceptan_como_filtro() {
        assert!(attr_type_is_avet_indexable(&AttrType::String));
        assert!(attr_type_is_avet_indexable(&AttrType::Number));
        assert!(attr_type_is_avet_indexable(&AttrType::Epoch));
        assert!(attr_type_is_avet_indexable(&AttrType::Reference));
        assert!(attr_type_is_avet_indexable(&AttrType::Uuid));
        assert!(attr_type_is_avet_indexable(&AttrType::Enum));
        assert!(attr_type_is_avet_indexable(&AttrType::Json));
        assert!(attr_type_is_avet_indexable(&AttrType::Boolean));
    }
}
