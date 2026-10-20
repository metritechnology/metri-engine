//! Los doce tipos de valor (`ValueType`) del sistema EAV y sus etiquetas binarias (`tag` bytes) para claves de ordenamiento (`SK`).

use serde::{Deserialize, Serialize};

/// Los 12 tipos de valor del sistema EAV.
/// Cada variante posee un byte de etiqueta (`tag`) para el Sort Key binario que define el orden lexicográfico entre tipos distintos.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueType {
    /// Valor nulo o marcador de retracción (`0x00`).
    Null,
    /// Valor booleano (`0x01`).
    Boolean,
    /// Entero con signo de 64 bits (`0x02`).
    Long,
    /// Punto flotante IEEE 754 de 64 bits con inversión de bits de signo para ordenamiento `AVET` (`0x03`).
    Double,
    /// Cadena de texto UTF-8 (`0x04`).
    String,
    /// Identificador único UUID o ULID (`0x05`).
    Uuid,
    /// Referencia a otra entidad (`0x06`).
    Reference,
    /// Arreglo de cadenas serializado (`0x07`).
    Array,
    /// Bloque opaco de bytes binarios (`0x08`).
    Bytes,
    /// Entero con signo de 128 bits para secuencias y contadores (`0x09`).
    BigInt,
    /// Marca de tiempo en milisegundos desde la época Unix (`0x0A`).
    Instant,
    /// Par de coordenadas geográficas `[lat, lon]` codificadas como dos `f64` (`0x0B`).
    Geo,
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

/// Regla canónica de indexabilidad `AVET` a nivel de atributo del Códice (`AttrType`).
///
/// Único punto donde se decide qué tipos de atributo puede filtrar el índice
/// `AVET`. El validador de filtros de listado y el escritor aplican este mismo
/// criterio junto con `ValueType::is_avet_indexable`.
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
