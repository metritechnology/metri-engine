// cedar/tests/fakes.rs — Lectores EAV en memoria para la suite.
//
// Sin DynamoClient real, sin sembrar candados globales: los tests del grafo
// y de pipeline seedean estos fakes directamente — paralelizables sin carreras.

use crate::cedar::ports::EntityReader;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::reader::pull::EntityMap;
use crate::eav::types::datom::DatomValue;
use std::collections::HashMap;

/// Lector EAV en memoria: mapas de atributos por `tenant/entity_id`.
/// Suficiente para el grafo de principal — los tests seedean directamente.
#[derive(Default, Clone)]
pub(crate) struct FakeEntityReader {
    pub(crate) entities: HashMap<String, EntityMap>,
}

#[async_trait::async_trait]
impl EntityReader for FakeEntityReader {
    async fn pull(
        &self,
        tenant_id: &str,
        entity_id: &str,
        _attrs: Option<&[&str]>,
    ) -> Result<EntityMap, DomainError> {
        Ok(self
            .entities
            .get(&format!("{tenant_id}/{entity_id}"))
            .cloned()
            .unwrap_or_default())
    }

    async fn children_with_attr(
        &self,
        _tenant_id: &str,
        _attr_name: &str,
        _value: &DatomValue,
    ) -> Result<Vec<String>, DomainError> {
        Ok(vec![])
    }

    fn clone_reader(&self) -> Box<dyn EntityReader> {
        Box::new(self.clone())
    }
}

/// Lector que FALLA ante cualquier consulta: usado para probar que el camino
/// cacheado no toca el almacenamiento.
pub(crate) struct FailingReader;

#[async_trait::async_trait]
impl EntityReader for FailingReader {
    async fn pull(
        &self,
        _tenant_id: &str,
        _entity_id: &str,
        _attrs: Option<&[&str]>,
    ) -> Result<EntityMap, DomainError> {
        Err(DomainError::new(
            ErrorCode::Infra001,
            "el lector no debe consultarse en este camino",
        ))
    }

    async fn children_with_attr(
        &self,
        _tenant_id: &str,
        _attr_name: &str,
        _value: &DatomValue,
    ) -> Result<Vec<String>, DomainError> {
        Err(DomainError::new(
            ErrorCode::Infra001,
            "el lector no debe consultarse en este camino",
        ))
    }

    fn clone_reader(&self) -> Box<dyn EntityReader> {
        Box::new(FailingReader)
    }
}
