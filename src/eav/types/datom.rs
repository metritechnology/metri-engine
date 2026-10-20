//! Definición de `Datom` y `DatomValue` — la unidad atómica inmutable del modelo EAV sobre `.mbf`.

use crate::eav::types::value_type::ValueType;
use serde::{Deserialize, Serialize};

/// Valor tipado de un `Datom`.
/// Mapea de forma biunívoca con los 12 tipos de valor del sistema EAV (`ValueType`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DatomValue {
    /// Marcador de valor nulo o retracción lógica.
    Null,
    /// Valor lógico booleano (`true` o `false`).
    Bool(bool),
    /// Entero con signo de 64 bits (`i64`).
    Long(i64),
    /// Número en punto flotante de doble precisión IEEE 754 (`f64`).
    Double(f64),
    /// Cadena de texto UTF-8.
    Str(String),
    /// Identificador universal único (UUID o ULID en representación textual).
    Uuid(String),
    /// Referencia numérica (`u64`) hacia otra entidad del grafo.
    Ref(u64),
    /// Colección ordenada de cadenas de texto (`Vec<String>`).
    Array(Vec<String>),
    /// Secuencia opaca de bytes binarios (`Vec<u8>`).
    Bytes(Vec<u8>),
    /// Entero de alta precisión con signo de 128 bits (`i128`), usado en contadores y secuencias.
    BigInt(i128),
    /// Marca de tiempo en milisegundos desde la época Unix (`i64`).
    Instant(i64),
    /// Coordenada geográfica WGS-84 compuesta por latitud y longitud.
    Geo {
        /// Latitud en grados decimales WGS-84 (`[-90.0, 90.0]`).
        lat: f64,
        /// Longitud en grados decimales WGS-84 (`[-180.0, 180.0]`).
        lon: f64,
    },
}

impl DatomValue {
    /// Retorna el ValueType correspondiente a este valor.
    pub fn value_type(&self) -> ValueType {
        match self {
            DatomValue::Null => ValueType::Null,
            DatomValue::Bool(_) => ValueType::Boolean,
            DatomValue::Long(_) => ValueType::Long,
            DatomValue::Double(_) => ValueType::Double,
            DatomValue::Str(_) => ValueType::String,
            DatomValue::Uuid(_) => ValueType::Uuid,
            DatomValue::Ref(_) => ValueType::Reference,
            DatomValue::Array(_) => ValueType::Array,
            DatomValue::Bytes(_) => ValueType::Bytes,
            DatomValue::BigInt(_) => ValueType::BigInt,
            DatomValue::Instant(_) => ValueType::Instant,
            DatomValue::Geo { .. } => ValueType::Geo,
        }
    }

    /// Extrae como f64 para agregaciones numéricas.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            DatomValue::Long(n) => Some(*n as f64),
            DatomValue::Double(d) => Some(*d),
            DatomValue::Instant(t) => Some(*t as f64),
            DatomValue::BigInt(b) => Some(*b as f64),
            _ => None,
        }
    }
}

/// Datom — unidad atómica inmutable del sistema EAV sobre `.mbf`.
/// Mapea a una entrada indexada en el índice primario (EAVT) de la partición `.mbf`.
///
/// Un Datom representa el hecho: "La entidad E tiene el atributo A con valor V
/// a partir de la transacción T, y el hecho es op (true=assert, false=retract)."
///
/// En `.mbf`: `PK=T#<tenant>#E#<eid>`, SK=`[attr_id_u16][tx_u64][op_u8]`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Datom {
    /// Tenant ID (aislamiento multitenant — parte del PK de la partición `.mbf`)
    pub tenant_id: String,
    /// Entity ID (ULID string o UUID)
    pub entity_id: String,
    /// Nombre del atributo (ej. "entity/title")
    pub attr_name: String,
    /// ID numérico del atributo (u16) — índice en el AttributeRegistry
    pub attr_id: u16,
    /// Valor del atributo
    pub value: DatomValue,
    /// Transaction ID (ULID epoch ms — monotónico creciente)
    pub tx_id: u64,
    /// true = assert (nuevo valor), false = retract (valor eliminado)
    pub op: bool,
}

impl Datom {
    /// Genera el hash DJB2 a 16 bits para un atributo (utilizado como attr_id inmutable).
    pub fn hash_attr_name(name: &str) -> u16 {
        let mut h = 5381u32;
        for b in name.as_bytes() {
            h = h.wrapping_mul(33).wrapping_add(*b as u32);
        }
        (h ^ (h >> 16)) as u16
    }

    /// Constructor para un assert (nuevo valor).
    pub fn assert(
        tenant_id: impl Into<String>,
        entity_id: impl Into<String>,
        attr_name: impl Into<String>,
        attr_id: u16,
        value: DatomValue,
        tx_id: u64,
    ) -> Self {
        Datom {
            tenant_id: tenant_id.into(),
            entity_id: entity_id.into(),
            attr_name: attr_name.into(),
            attr_id,
            value,
            tx_id,
            op: true,
        }
    }

    /// Constructor para un retract (eliminar valor anterior).
    pub fn retract(
        tenant_id: impl Into<String>,
        entity_id: impl Into<String>,
        attr_name: impl Into<String>,
        attr_id: u16,
        value: DatomValue,
        tx_id: u64,
    ) -> Self {
        Datom {
            tenant_id: tenant_id.into(),
            entity_id: entity_id.into(),
            attr_name: attr_name.into(),
            attr_id,
            value,
            tx_id,
            op: false,
        }
    }

    /// Genera el Partition Key para el índice primario (EAVT).
    /// Formato: "T#<tenant_id>#E#<entity_id>"
    pub fn eavt_pk(&self) -> String {
        format!("T#{}#E#{}", self.tenant_id, self.entity_id)
    }

    /// Genera el Partition Key para el índice secundario AEVT (por tipo de entidad/atributo).
    /// Formato: "T#<tenant_id>#A#<attr_name>" (o segmentado por tipo si es entity/type)
    pub fn aevt_pk(&self) -> String {
        if self.attr_name == crate::eav::writer::system_attrs::TYPE_NAME {
            if let DatomValue::Str(val) = &self.value {
                return format!("T#{}#A#{}#{}", self.tenant_id, self.attr_name, val);
            }
        }
        format!("T#{}#A#{}", self.tenant_id, self.attr_name)
    }

    /// Genera el Partition Key para el índice secundario AVET (por atributo + valor).
    /// Formato: "T#<tenant_id>#AV#<attr_name>" (100% aislado por tenant y agnóstico al dominio).
    pub fn avet_pk(&self) -> String {
        format!("T#{}#AV#{}", self.tenant_id, self.attr_name)
    }

    /// Genera el Partition Key para el índice secundario VAET (grafo inverso, solo para Ref).
    /// Formato: "T#<tenant_id>#V#<ref_entity_id>"
    pub fn vaet_pk(&self) -> Option<String> {
        if let DatomValue::Ref(ref_eid) = &self.value {
            Some(format!("T#{}#V#{}", self.tenant_id, ref_eid))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datom_index_partition_keys_are_tenant_isolated_and_domain_agnostic() {
        let datom = Datom::assert(
            "tenant-1",
            "entity-123",
            "custom_attr",
            100,
            DatomValue::Str("val".to_string()),
            1000,
        );
        assert_eq!(datom.eavt_pk(), "T#tenant-1#E#entity-123");
        assert_eq!(datom.aevt_pk(), "T#tenant-1#A#custom_attr");
        assert_eq!(datom.avet_pk(), "T#tenant-1#AV#custom_attr");
        assert_eq!(datom.vaet_pk(), None);

        let ref_datom = Datom::assert(
            "tenant-1",
            "entity-123",
            "parent_ref",
            101,
            DatomValue::Ref(42),
            1000,
        );
        assert_eq!(ref_datom.vaet_pk(), Some("T#tenant-1#V#42".to_string()));

        let type_datom = Datom::assert(
            "tenant-1",
            "entity-123",
            crate::eav::writer::system_attrs::TYPE_NAME,
            crate::eav::writer::system_attrs::TYPE_ID,
            DatomValue::Str("asset".to_string()),
            1000,
        );
        assert_eq!(type_datom.aevt_pk(), "T#tenant-1#A#entity/type#asset");
    }
}
