// eav/registry/descriptor.rs
// AttributeDescriptor — catálogo de atributos del sistema EAV.
// Blueprint: Metri EAV §MÓDULO 2

use crate::eav::types::datom::DatomValue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueType {
    Str,
    U64,
    I64,
    F64,
    Bool,
    Ulid,
    Bytes,
    Epoch,
    Json,
    Nil,
    Array,
    Ref,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Cardinality {
    One,
    Many,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UniqueStrategy {
    Identity,
    Value,
    Tenant,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AttrStatus {
    Active,
    BackfillPending,
    Deprecated,
}

/// Descriptor de un atributo — compilado desde el modelo JSON del Códice.
/// Permite lookup O(1) por attr_id (u16) durante el ciclo de vida de la Lambda.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeDescriptor {
    pub id: u16,
    pub name: String, // "work_order/status"
    pub entity_type: String,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
    pub indexed: bool, // genera item AVET
    pub unique: Option<UniqueStrategy>,
    pub fts: bool,    // genera trigrams FTS
    pub is_ref: bool, // genera item VAET
    pub deprecated: bool,
    pub status: AttrStatus,
    pub default_value: Option<DatomValue>,
}

impl AttributeDescriptor {
    pub fn is_avet_indexable(&self) -> bool {
        self.indexed || self.unique.is_some() || self.fts
    }
}
