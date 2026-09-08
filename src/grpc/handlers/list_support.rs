use crate::domain::errors::DomainError;
// Handlers del MetriGrpcService — fase 2: service.rs delega, aquí vive el cuerpo.

/// Valida los filtros de `ListEntities` contra el Codice.
///
/// Rechaza atributos inexistentes y los de tipo no indexable en AVET: un filtro que
/// el indice no puede resolver degeneraria en un escaneo encubierto.
///
/// **Se valida por TIPO, no por el flag `indexed` del descriptor.** Ese flag es
/// declarativo y el camino de escritura lo ignora: `build_write_items` decide
/// escribir las claves AVET segun `ValueType::is_avet_indexable()`, que solo excluye
/// `Bytes`, `Array` y `Null`. Validar por el flag rechazaria filtros que funcionan
/// —`scheduled_job.status` no declara `index: true` y sin embargo esta en el indice—.
pub(crate) fn validate_list_filters(
    model: &crate::codice::registry::EntityModel,
    filter_names: &[&str],
) -> Result<(), DomainError> {
    for name in filter_names {
        match model.attributes.iter().find(|a| a.name == *name) {
            None => {
                return Err(DomainError::janus(
                    crate::domain::errors::ErrorCode::Janus400,
                    format!("'{}' no es un atributo de '{}'", name, model.entity),
                ));
            }
            Some(a) => {
                // La regla vive en el escritor: la validación consulta, no reimplementa.
                if !crate::eav::types::value_type::attr_type_is_avet_indexable(&a.attr_type) {
                    return Err(DomainError::janus(
                        crate::domain::errors::ErrorCode::Janus400,
                        "'{}' es de tipo no indexable en AVET: no se puede filtrar por el"
                            .replace("{}", name),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Ordena y recorta. Devuelve true si quedaron resultados fuera.
///
/// El orden importa por correccion, no por estetica: el ejecutor devuelve el
/// resultado de un HashSet, cuyo orden no es determinista. Sin ordenar, dos llamadas
/// identicas con el mismo limite pueden devolver conjuntos DISTINTOS, y un consumidor
/// que reconcilie estado actuaria sobre una foto arbitraria.
pub(crate) fn sort_and_truncate(ids: &mut Vec<String>, limit: usize) -> bool {
    ids.sort();
    let truncated = ids.len() > limit;
    if truncated {
        ids.truncate(limit);
    }
    truncated
}
