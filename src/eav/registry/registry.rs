// eav/registry/registry.rs
// AttributeRegistry — O(1) lookup por id o nombre.
// Blueprint: Metri EAV §MÓDULO 2

use super::descriptor::AttributeDescriptor;
use std::collections::HashMap;

/// Registry compilado en bootstrap desde los modelos JSON del Códice.
/// Inmutable durante el ciclo de vida de la Lambda.
#[derive(Debug, Clone)]
pub struct AttributeRegistry {
    /// by_id[attr_id] = descriptor (O(1) lookup)
    by_id: Vec<Option<AttributeDescriptor>>,
    /// "entity/attr" → attr_id
    by_name: HashMap<String, u16>,
    /// "asset" → [attr_id1, attr_id2, ...]
    by_entity: HashMap<String, Vec<u16>>,
}

impl AttributeRegistry {
    pub fn new() -> Self {
        AttributeRegistry {
            by_id: vec![None; 65536],
            by_name: HashMap::new(),
            by_entity: HashMap::new(),
        }
    }

    /// Registra un atributo en el registry.
    pub fn register(&mut self, desc: AttributeDescriptor) {
        let id = desc.id as usize;
        self.by_name.insert(desc.name.clone(), desc.id);
        self.by_entity
            .entry(desc.entity_type.clone())
            .or_default()
            .push(desc.id);
        if id < self.by_id.len() {
            self.by_id[id] = Some(desc);
        }
    }

    /// Lookup por attr_id — O(1).
    pub fn get_by_id(&self, attr_id: u16) -> Option<&AttributeDescriptor> {
        self.by_id.get(attr_id as usize)?.as_ref()
    }

    /// Lookup por nombre completo (ej. "asset/status").
    pub fn get_by_name(&self, name: &str) -> Option<&AttributeDescriptor> {
        let id = *self.by_name.get(name)?;
        self.get_by_id(id)
    }

    /// Devuelve todos los atributos de un entity_type.
    pub fn attrs_for_entity(&self, entity_type: &str) -> Vec<&AttributeDescriptor> {
        self.by_entity
            .get(entity_type)
            .map(|ids| ids.iter().filter_map(|id| self.get_by_id(*id)).collect())
            .unwrap_or_default()
    }
}

impl Default for AttributeRegistry {
    fn default() -> Self {
        Self::new()
    }
}
