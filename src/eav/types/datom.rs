// [NUEVO — reemplaza: src/metri/infrastructure/datahike.clj]
// src/eav/types/datom.rs
// Struct Datom + enum DatomValue — unidad atómica del sistema EAV.
// Diseñado según Metri EAV - OLPT.md §I

use serde::{Deserialize, Serialize};
use crate::eav::types::value_type::ValueType;

/// Valor tipado de un Datom.
/// Mapea 1:1 con los 12 tipos del sistema EAV (ValueType).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DatomValue {
    Null,
    Bool(bool),
    Long(i64),
    Double(f64),
    Str(String),
    Uuid(String),
    Ref(u64),           // entity_id de la entidad referenciada
    Array(Vec<String>),
    Bytes(Vec<u8>),
    BigInt(i128),
    Instant(i64),       // epoch ms
    Geo { lat: f64, lon: f64 },
}

impl DatomValue {
    /// Retorna el ValueType correspondiente a este valor.
    pub fn value_type(&self) -> ValueType {
        match self {
            DatomValue::Null       => ValueType::Null,
            DatomValue::Bool(_)    => ValueType::Boolean,
            DatomValue::Long(_)    => ValueType::Long,
            DatomValue::Double(_)  => ValueType::Double,
            DatomValue::Str(_)     => ValueType::String,
            DatomValue::Uuid(_)    => ValueType::Uuid,
            DatomValue::Ref(_)     => ValueType::Reference,
            DatomValue::Array(_)   => ValueType::Array,
            DatomValue::Bytes(_)   => ValueType::Bytes,
            DatomValue::BigInt(_)  => ValueType::BigInt,
            DatomValue::Instant(_) => ValueType::Instant,
            DatomValue::Geo { .. } => ValueType::Geo,
        }
    }

    /// Convierte el valor a su representación para almacenamiento en DynamoDB
    /// como atributo de tipo String (columna `v`).
    pub fn to_dynamo_string(&self) -> Option<String> {
        match self {
            DatomValue::Str(s)     => Some(s.clone()),
            DatomValue::Uuid(u)    => Some(u.clone()),
            DatomValue::Array(arr) => Some(serde_json::to_string(arr).unwrap_or_default()),
            _                      => None,
        }
    }

    /// Convierte el valor a su representación para almacenamiento en DynamoDB
    /// como atributo de tipo Number (columna `v`).
    pub fn to_dynamo_number(&self) -> Option<String> {
        match self {
            DatomValue::Long(n)    => Some(n.to_string()),
            DatomValue::Double(d)  => Some(d.to_string()),
            DatomValue::BigInt(b)  => Some(b.to_string()),
            DatomValue::Instant(t) => Some(t.to_string()),
            DatomValue::Ref(r)     => Some(r.to_string()),
            _                      => None,
        }
    }

    /// Extrae como f64 para agregaciones numéricas.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            DatomValue::Long(n)    => Some(*n as f64),
            DatomValue::Double(d)  => Some(*d),
            DatomValue::Instant(t) => Some(*t as f64),
            DatomValue::BigInt(b)  => Some(*b as f64),
            _                      => None,
        }
    }
}

/// Datom — unidad atómica inmutable del sistema EAV.
/// Mapea a una fila en la tabla principal (EAVT) de DynamoDB.
///
/// Un Datom representa el hecho: "La entidad E tiene el atributo A con valor V
/// a partir de la transacción T, y el hecho es op (true=assert, false=retract)."
///
/// En Datomic/Datahike: [E A V T op]
/// En DynamoDB: PK=T#<tenant>#E#<eid>, SK=[attr_id_u16][tx_u64][op_u8]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Datom {
    /// Tenant ID (aislamiento multitenant — parte del PK de DynamoDB)
    pub tenant_id: String,
    /// Entity ID (ULID string o UUID)
    pub entity_id: String,
    /// Nombre del atributo (ej. "asset/name")
    pub attr_name: String,
    /// ID numérico del atributo (u16) — índice en el AttributeRegistry
    pub attr_id:   u16,
    /// Valor del atributo
    pub value:     DatomValue,
    /// Transaction ID (ULID epoch ms — monotónico creciente)
    pub tx_id:     u64,
    /// true = assert (nuevo valor), false = retract (valor eliminado)
    pub op:        bool,
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
        attr_id:   u16,
        value:     DatomValue,
        tx_id:     u64,
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
        attr_id:   u16,
        value:     DatomValue,
        tx_id:     u64,
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

    /// Genera el Partition Key para la tabla principal (EAVT).
    /// Formato: "T#<tenant_id>#E#<entity_id>"
    pub fn eavt_pk(&self) -> String {
        format!("T#{}#E#{}", self.tenant_id, self.entity_id)
    }

    /// Genera el Partition Key para el GSI-AEVT (por tipo de entidad/atributo).
    /// Formato: "T#<tenant_id>#A#<attr_name>" (o segmentado por tipo si es entity_type)
    pub fn aevt_pk(&self) -> String {
        if self.attr_name == "entity_type" {
            if let DatomValue::Str(val) = &self.value {
                return format!("T#{}#A#entity_type#{}", self.tenant_id, val);
            }
        }
        format!("T#{}#A#{}", self.tenant_id, self.attr_name)
    }

    /// Genera el Partition Key para el GSI-AVET (por atributo + valor).
    /// Formato: "T#<tenant_id>#AV#<attr_name>"
    pub fn avet_pk(&self) -> String {
        let is_global = self.attr_name == "username" || self.attr_name == "email" || self.attr_name == "primary_phone";
        let tenant = if is_global { "GLOBAL" } else { &self.tenant_id };
        format!("T#{}#AV#{}", tenant, self.attr_name)
    }

    /// Genera el Partition Key para el GSI-VAET (grafo inverso, solo para Ref).
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
    fn test_global_attributes_avet_pk() {
        // Test standard local attribute
        let local_datom = Datom::assert(
            "tenant-1",
            "entity-123",
            "first_name",
            100,
            DatomValue::Str("John".to_string()),
            1000,
        );
        assert_eq!(local_datom.avet_pk(), "T#tenant-1#AV#first_name");

        // Test global attributes
        for global_attr in &["username", "email", "primary_phone"] {
            let global_datom = Datom::assert(
                "tenant-1",
                "entity-123",
                *global_attr,
                200,
                DatomValue::Str("value".to_string()),
                1000,
            );
            assert_eq!(
                global_datom.avet_pk(),
                format!("T#GLOBAL#AV#{}", global_attr)
            );
        }
    }
}
