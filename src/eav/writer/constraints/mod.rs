//! Restricciones de integridad aplicadas EN la transacción.
//!
//! ## Por qué existe este módulo
//!
//! La unicidad se comprobaba leyendo el índice AVET y escribiendo después
//! (`oltp_channel.rs`). Entre la lectura y la escritura cabe otra
//! transacción, así que dos `create` concurrentes con el mismo valor leen
//! ambos «no existe» y ambos escriben. La restricción era *advisoria*, no un
//! invariante — y afectaba a todos los modelos con `unique`, no solo a
//! `tenant_plugin`.
//!
//! Aquí la unicidad se enforcea con un **item de reclamación** escrito en la
//! MISMA `TransactWriteItems`, con `attribute_not_exists`. DynamoDB aborta uno
//! de los dos escritores. Un ganador, siempre, y sin lectura previa: es a la
//! vez más correcto y más rápido.
//!
//! El patrón ya estaba probado en este repositorio, en `quota/ledger.rs` y
//! `quota/dynamo_store.rs`.
//!
//! ## Por qué no se cambia el ULID de la entidad
//!
//! La alternativa era derivar el id de entidad de la clave natural (UUIDv5).
//! Habría roto `track_history`, las referencias `entityRef`, la ordenación
//! temporal de los ULID y los datos ya escritos. El item de reclamación
//! consigue lo mismo sin tocar nada de eso: la identidad sigue siendo un
//! ULID, y la unicidad vive en un item aparte.

mod unique_claim;

pub use unique_claim::UniqueClaimPlanner;

use crate::codice::registry::EntityModel;
use crate::domain::errors::DomainError;
use crate::eav::types::datom::DatomValue;
use crate::eav::writer::transact::TransactOp;
use aws_sdk_dynamodb::types::TransactWriteItem;
use std::collections::HashMap;

/// Todo lo que un planificador necesita saber de una escritura.
pub struct ConstraintContext<'a> {
    pub tenant_id: &'a str,
    pub entity_id: &'a str,
    pub model: &'a EntityModel,
    pub attrs: &'a HashMap<String, DatomValue>,
    pub op: TransactOp,
    pub table: &'a str,
}

/// Traduce una restricción declarada en items de la MISMA transacción.
///
/// **No lee.** Es la regla del módulo: si una restricción necesita consultar
/// para decidir, no es un invariante — es una comprobación, y una
/// comprobación tiene una ventana de carrera por construcción.
pub trait ConstraintPlanner: Send + Sync {
    /// Nombre, para trazas y mensajes de error.
    fn name(&self) -> &'static str;

    /// Si este planificador tiene algo que decir sobre este modelo.
    ///
    /// Separado de `plan` para poder descartar el modelo entero sin recorrer
    /// sus atributos, que es el caso común: hoy ningún modelo declara
    /// restricciones.
    fn applies_to(&self, model: &EntityModel) -> bool;

    fn plan(&self, ctx: &ConstraintContext) -> Result<Vec<TransactWriteItem>, DomainError>;
}

/// Los planificadores activos del motor.
///
/// Se compone una vez y se inyecta; el escritor no construye planificadores ni
/// sabe cuáles hay. Añadir una restricción nueva —`check`, `foreign_key`,
/// `exclusion`— es añadir un tipo aquí, sin tocar el camino de escritura.
pub fn default_planners() -> Vec<Box<dyn ConstraintPlanner>> {
    vec![Box::new(UniqueClaimPlanner::new())]
}

/// Planifica los items de todos los planificadores que apliquen.
pub fn plan_all(
    planners: &[Box<dyn ConstraintPlanner>],
    ctx: &ConstraintContext,
) -> Result<Vec<TransactWriteItem>, DomainError> {
    let mut items = Vec::new();
    for planner in planners {
        if planner.applies_to(ctx.model) {
            items.extend(planner.plan(ctx)?);
        }
    }
    Ok(items)
}
