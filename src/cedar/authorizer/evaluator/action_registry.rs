// authorizer/evaluator/action_registry.rs — Registro de acciones Cedar.
//
// La acción → grupo y la base de entidades de acciones se DERIVAN de
// cedar-schema.json (una sola fuente de datos): añadir una acción al schema
// ya no exige tocar Rust. La base se compila UNA vez por proceso (OnceLock);
// por petición solo se ensamblan User y Resource encima (entities_builder).
//
// Divergencia conservada a propósito: el schema pone `GET` en el grupo
// mutacional, pero el enrutador (`is_mutational_action`) la trata como
// analítica desde siempre — cambiar el enrutamiento de GET es una decisión
// de producto, no de este refactor.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::OnceLock;

use cedar_policy::{Entities, Schema};

pub const CEDAR_SCHEMA_SRC: &str = include_str!("../../../../config/policies/cedar-schema.json");

/// Esquema Cedar del proceso — compilado una sola vez.
pub fn cedar_schema() -> &'static Schema {
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Schema::from_str(CEDAR_SCHEMA_SRC).expect("Failed to parse cedar-schema.json")
    })
}

/// Acciones que el enrutador dirige al camino mutacional (semántica
/// histórica — ver la nota de divergencia en el encabezado).
pub fn is_mutational_action(action: &str) -> bool {
    matches!(action, "CREATE" | "UPDATE" | "DELETE" | "UPSERT")
}

struct Registry {
    /// nombre de acción → grupo ("mutational" | "analytical" | "system")
    groups: HashMap<String, String>,
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let schema: serde_json::Value =
            serde_json::from_str(CEDAR_SCHEMA_SRC).expect("cedar-schema.json válido");
        let actions = schema["Metri"]["actions"]
            .as_object()
            .expect("el schema declara acciones");

        let mut groups = HashMap::new();
        for (name, spec) in actions {
            if name.starts_with("ActionGroup::") {
                continue;
            }
            if let Some(group) = spec
                .get("memberOf")
                .and_then(|m| m.as_array())
                .and_then(|m| m.first())
                .and_then(|g| g.get("id"))
                .and_then(|id| id.as_str())
                .map(|id| id.trim_start_matches("ActionGroup::\"").trim_end_matches('"').to_string())
            {
                groups.insert(name.clone(), group);
            }
        }
        Registry { groups }
    })
}

/// Grupo del schema al que pertenece una acción (None: no declarada).
#[allow(dead_code)]
pub fn group_of(action: &str) -> Option<&'static str> {
    registry().groups.get(action).map(|s| s.as_str())
}

/// Base de entidades precompilada: TODAS las acciones y grupos declarados en
/// el schema — el loader de Cedar los añade desde el schema mismo, con sus
/// jerarquías `memberOf`. Solo varían User y Resource por petición; esta base
/// se construye una vez por proceso y se clona.
pub fn base_action_entities() -> &'static Entities {
    static BASE: OnceLock<Entities> = OnceLock::new();
    BASE.get_or_init(|| {
        Entities::from_json_str("[]", Some(cedar_schema()))
            .expect("las acciones del schema cargan como entidades")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_mutacional_conserva_la_semantica_historica() {
        assert!(is_mutational_action("CREATE"));
        assert!(is_mutational_action("UPDATE"));
        assert!(is_mutational_action("DELETE"));
        assert!(is_mutational_action("UPSERT"));
        assert!(!is_mutational_action("QueryMetrics"));
        assert!(!is_mutational_action("DiscoverSchema"));
        assert!(!is_mutational_action("BulkIngestData"));
    }

    #[test]
    fn el_registry_deduce_grupos_del_schema() {
        assert_eq!(group_of("CREATE"), Some("mutational"));
        assert_eq!(group_of("QueryMetrics"), Some("analytical"));
        assert_eq!(group_of("BulkIngestData"), Some("mutational"));
        assert_eq!(group_of("no_existe"), None);
    }

    #[test]
    fn la_base_de_entidades_deriva_del_schema() {
        let base = base_action_entities();
        // El schema declara 16 acciones + 3 grupos; la base los contiene todos.
        assert!(base.iter().count() >= 19);
        // Las jerarquías memberOf del schema llegan como parents en la base.
        let query_metrics = "Metri::Action::\"QueryMetrics\""
            .parse::<cedar_policy::EntityUid>()
            .unwrap();
        assert!(base.get(&query_metrics).is_some(), "QueryMetrics en la base");
        let group = "Metri::Action::\"ActionGroup::\\\"analytical\\\"\""
            .parse::<cedar_policy::EntityUid>()
            .unwrap();
        assert!(base.get(&group).is_some(), "grupo analytical en la base");
    }
}
