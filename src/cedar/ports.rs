// cedar/ports.rs — Puertos del autorizador Cedar (DIP).
// El core de autorización no conoce DynamoClient ni el detalle del executor
// EAV: consume los puertos de este archivo. Los adaptadores concretos
// (`EavReader`, el `HashMap` de PolicySets) los provee la raíz de composición,
// y los tests pueden sustituirlos por fakes en memoria.

use std::collections::HashMap;

use async_trait::async_trait;
use cedar_policy::PolicySet;

use crate::domain::errors::DomainError;
use crate::eav::reader::pull::{EavReader, EntityMap};
use crate::eav::reader::query::{EavQueryExecutor, NativeQueryPlan};
use crate::eav::types::datom::DatomValue;

/// Lectura de entidades del modelo EAV para decisiones de autorización.
///
/// Un puerto, dos consumos: `pull` trae el estado vigente de una entidad y
/// `children_with_attr` expande jerarquías (hijos por atributo, p. ej.
/// `parent_id`). `clone_reader` permite clonar el lector para consultas
/// concurrentes (`tokio::spawn`) sin acoplar al tipo concreto.
#[async_trait]
pub trait EntityReader: Send + Sync {
    async fn pull(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attrs: Option<&[&str]>,
    ) -> Result<EntityMap, DomainError>;

    async fn children_with_attr(
        &self,
        tenant_id: &str,
        attr_name: &str,
        value: &DatomValue,
    ) -> Result<Vec<String>, DomainError>;

    fn clone_reader(&self) -> Box<dyn EntityReader>;
}

#[async_trait]
impl EntityReader for EavReader {
    async fn pull(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attrs: Option<&[&str]>,
    ) -> Result<EntityMap, DomainError> {
        EavReader::pull(self, tenant_id, entity_id, attrs).await
    }

    async fn children_with_attr(
        &self,
        tenant_id: &str,
        attr_name: &str,
        value: &DatomValue,
    ) -> Result<Vec<String>, DomainError> {
        let query_executor = EavQueryExecutor::new(self.ddb.clone(), self.table.clone());
        let plan = NativeQueryPlan::AvetSingle {
            tenant_id: tenant_id.to_string(),
            attr_name: attr_name.to_string(),
            value: value.clone(),
        };
        query_executor.execute_native_plan(&plan).await
    }

    fn clone_reader(&self) -> Box<dyn EntityReader> {
        Box::new(self.clone())
    }
}

/// Políticas Cedar compiladas por rol.
///
/// Sustituye al `&HashMap<String, PolicySet>` crudo en las firmas: el
/// evaluador solo necesita resolver la política de un boundary y, como último
/// recurso, cualquiera. Los factores de fallback ("admin", "tenant-admin")
/// son decisión del evaluador, no del almacenamiento.
pub trait PolicyStore: Send + Sync {
    fn policy_for(&self, role_id: &str) -> Option<&PolicySet>;
    fn any_policy(&self) -> Option<&PolicySet>;
}

impl PolicyStore for HashMap<String, PolicySet> {
    fn policy_for(&self, role_id: &str) -> Option<&PolicySet> {
        self.get(role_id)
    }

    fn any_policy(&self) -> Option<&PolicySet> {
        self.values().next()
    }
}
