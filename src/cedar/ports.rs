//! Puertos del autorizador Cedar (Inversión de Dependencias — DIP).
//!
//! El núcleo de autorización no conoce `DynamoClient` ni los detalles internos del ejecutor
//! EAV: consume los traits definidos en este archivo. Los adaptadores concretos
//! (`EavReader`, `InMemoryPrincipalCache`, `BroadcastBus` y `HashMap<String, PolicySet>`)
//! los inyecta la raíz de composición y los tests los sustituyen por dobles en memoria.

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
    /// Recupera el mapa de atributos vigentes de una entidad en el tenant indicado.
    async fn pull(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attrs: Option<&[&str]>,
    ) -> Result<EntityMap, DomainError>;

    /// Obtiene los identificadores de las entidades hijas cuyo atributo `attr_name` coincide con `value`.
    async fn children_with_attr(
        &self,
        tenant_id: &str,
        attr_name: &str,
        value: &DatomValue,
    ) -> Result<Vec<String>, DomainError>;

    /// Crea un clon en caja (`Box<dyn EntityReader>`) apto para tareas asíncronas concurrentes.
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

/// Caché de principals consolidados por user_id.
///
/// Puerto del desmonte fase 2: la pipeline resuelve el principal a través de
/// este contrato; las implementaciones (hoy `InMemoryPrincipalCache`, mañana
/// Redis/Valkey) deciden expiración y expulsión.
#[async_trait]
pub trait PrincipalCache: Send + Sync {
    /// Consulta el principal consolidado en caché para el `user_id` dado, si existe y sigue vigente.
    async fn lookup_principal(&self, user_id: &str) -> Option<crate::cedar::types::PrincipalData>;
    /// Almacena o actualiza el principal consolidado asociado al `user_id`.
    async fn store_principal(
        &self,
        user_id: &str,
        principal: crate::cedar::types::PrincipalData,
    ) -> Result<(), DomainError>;
    /// Expulsa de la caché la entrada correspondiente al `user_id` indicado.
    async fn evict_user(&self, user_id: &str) -> Result<(), DomainError>;
    /// Expulsa de la caché a todos los usuarios que posean el rol `role_id`.
    async fn evict_by_role(&self, role_id: &str) -> Result<(), DomainError>;
}

/// Bus de invalidación de cachés: los mutadores publican y los suscriptores
/// expulsan. Sin canal global — la instancia la crea la raíz de composición y
/// se comparte entre publicadores y caché.
pub trait InvalidationBus: Send + Sync {
    /// Publica un mensaje; no falla si no hay suscriptores.
    fn publish(&self, msg: crate::cedar::types::InvalidationMsg);
    /// Crea un receptor suscrito al flujo de mensajes de invalidación.
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<crate::cedar::types::InvalidationMsg>;
}

/// Políticas Cedar compiladas por rol.
///
/// Sustituye al `&HashMap<String, PolicySet>` crudo en las firmas: el
/// evaluador solo necesita resolver la política de un boundary y, como último
/// recurso, cualquiera. Los factores de fallback ("admin", "tenant-admin")
/// son decisión del evaluador, no del almacenamiento.
pub trait PolicyStore: Send + Sync {
    /// Devuelve el `PolicySet` compilado asociado al identificador de rol indicado.
    fn policy_for(&self, role_id: &str) -> Option<&PolicySet>;
    /// Devuelve cualquier `PolicySet` compilado disponible como respaldo.
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
