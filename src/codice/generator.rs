// [PORTED_FROM: src/metri/codice/generator.clj]
// codice/generator.rs — Inyector de atributos auto-generados.
// En Clojure: (inject! db-conn tenant-guard schema tenant-id payload)
//             con reducción Railway sobre atributos :auto_generate.
// En Rust:    inject() — función pura excepto por los efectos de I/O
//             de sequence/next (DynamoDB) y base36/generate (CSPRNG).


use serde_json::{Map, Value};

use crate::codice::base36;
use crate::codice::registry::EntityModel;
use crate::codice::sequence::{self, ScopeResolution, SeqAttrConfig};
use crate::domain::errors::DomainError;
use crate::infrastructure::dynamodb::DynamoClient;

/// Estrategia de auto-generación de un atributo.
/// [PORTED_FROM: (keyword (:strategy attr-config)) → :stochastic_base36 | :sequential]
#[derive(Debug, Clone)]
pub enum AutoGenStrategy {
    StochasticBase36 { prefix: String, length: usize },
    Sequential(SeqAttrConfig),
}

/// Extrae los atributos con :auto_generate del modelo.
/// [PORTED_FROM: (auto-generate-attrs schema)]
fn auto_generate_attrs(model: &EntityModel) -> Vec<(String, AutoGenStrategy)> {
    model
        .attributes
        .iter()
        .filter_map(|attr| {
            if let Some(auto_gen) = &attr.auto_generate {
                let strategy_str = auto_gen
                    .get("strategy")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if strategy_str == "stochastic_base36" {
                    let prefix = auto_gen
                        .get("prefix")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let length =
                        auto_gen.get("length").and_then(|v| v.as_u64()).unwrap_or(7) as usize;
                    return Some((
                        attr.name.clone(),
                        AutoGenStrategy::StochasticBase36 { prefix, length },
                    ));
                } else if strategy_str == "sequential" {
                    // Si en un futuro agregamos validación secuencial explícita en JSON
                }
            }

            // Fallback para secuencias:
            if attr.is_sequence_scope_via {
                Some((
                    attr.name.clone(),
                    AutoGenStrategy::Sequential(SeqAttrConfig {
                        name: attr.name.clone(),
                        prefix: String::new(),
                        padding: 4,
                        scope_resolution: ScopeResolution::Exact,
                    }),
                ))
            } else {
                None
            }
        })
        .collect()
}

/// Busca el campo scope (is_sequence_scope o is_sequence_scope_via).
/// [PORTED_FROM: (find-scope-field schema)]
fn find_scope_field(model: &EntityModel) -> Option<&str> {
    model
        .attributes
        .iter()
        .find(|a| a.is_sequence_scope || a.is_sequence_scope_via)
        .map(|a| a.name.as_str())
}

/// Inyecta los valores auto-generados en el payload.
///
/// Flujo Railway (mismo que el Clojure):
///   - Si no hay atributos auto_generate → retorna el payload sin cambios.
///   - Para cada atributo, ejecuta la estrategia y reduce el payload.
///   - Un solo error en la cadena → retorna [:error] inmediatamente.
///
/// [PORTED_FROM: (inject! db-conn tenant-guard schema tenant-id payload)]
pub async fn inject(
    ddb: &DynamoClient,
    model: &EntityModel,
    tenant_id: &str,
    payload: Map<String, Value>,
) -> Result<Map<String, Value>, DomainError> {
    let attrs_to_gen = auto_generate_attrs(model);

    if attrs_to_gen.is_empty() {
        // [PORTED_FROM: (if (empty? attrs) (do (otel/set-status! ...) [:ok payload]))]
        return Ok(payload);
    }

    let scope_field = find_scope_field(model);

    let mut enriched = payload;

    for (attr_name, strategy) in attrs_to_gen {
        // Si el atributo ya existe en el payload, no sobreescribir
        // (semántica idéntica al Clojure: (assoc enriched attr-kw (second result)))
        if enriched.contains_key(&attr_name) {
            continue;
        }

        let generated_value: String = match &strategy {
            AutoGenStrategy::StochasticBase36 { prefix, length } => {
                // [PORTED_FROM: [:ok (base36/generate (:prefix attr-config "") (long (:length attr-config 7)))]]
                base36::generate(prefix, *length)
            }
            AutoGenStrategy::Sequential(config) => {
                // Resolver scope_tag del payload si hay campo de scope
                let scope_tag: Option<String> = scope_field
                    .and_then(|field_name| enriched.get(field_name))
                    .and_then(|v| v.as_str())
                    .map(str::to_string);

                // [PORTED_FROM: (sequence/next! db-conn tenant-guard attr-config scope-field tenant-id enriched)]
                sequence::next(ddb, config, scope_tag.as_deref(), tenant_id).await?
            }
        };

        enriched.insert(attr_name, Value::String(generated_value));
    }

    Ok(enriched)
}

#[cfg(test)]
#[path = "tests/generator_tests.rs"]
mod tests;
