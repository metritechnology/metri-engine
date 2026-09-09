//! Delta — the before/after of a mutation in the event contract.
//!
//! delta — el antes/después de una mutación, como parte del contrato de eventos.
//!
//! Dos consumidores dependen de su semántica:
//! - el ledger del Hub (§10.3): sin ulid no ordena, pero sin delta correcto
//! reprovisiona de más;
//! - el guard de bitácora (§10.4): una mutación que SÓLO tocó campos de
//! bitácora debe descartarse, o cada disparo del ejecutor crearía una
//! trampa duplicada.
//!
//! La semántica de `is_bookkeeping_only` es un espejo deliberado de
//! `internal/event/delta.go` en metri-schedulers. Cambiarla aquí sin cambiarla
//! allá rompe el candado de golden_test.rs — a propósito.

use serde_json::Value;
use std::collections::BTreeMap;

/// BTreeMap, no HashMap: el orden de las llaves en el JSON publicado debe ser
/// determinista para que los fixtures dorados comparen por igualdad.
pub type Delta = BTreeMap<String, DeltaEntry>;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeltaEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<Value>,
}

/// Campos que muta el EJECUTOR, no el usuario. Espejo de `bookkeepingFields`
/// en metri-schedulers/internal/event/delta.go.
const BOOKKEEPING_FIELDS: [&str; 3] = ["last_run_at", "run_count", "last_error"];

/// Delta de un UPDATE: sólo atributos que cambiaron de verdad.
///
/// `before` son los valores activos previos; `after` los nuevos. Un atributo
/// con el mismo valor no entra al delta — si entrara, un UPDATE idempotente
/// del ejecutor reprovisionaría trampas sin motivo.
pub fn compute_delta(
    before: &serde_json::Map<String, Value>,
    after: &serde_json::Map<String, Value>,
) -> Delta {
    let mut delta = Delta::new();
    for (key, new_value) in after {
        let old_value = before.get(key);
        if old_value != Some(new_value) {
            delta.insert(
                key.clone(),
                DeltaEntry {
                    before: old_value.cloned(),
                    after: Some(new_value.clone()),
                },
            );
        }
    }
    delta
}

/// true si la mutación sólo tocó campos de bitácora → el Hub debe DESCARTAR
/// el evento (reprovisionar sería crear trampas duplicadas).
///
/// Un delta vacío devuelve false a propósito — igual que en Go: si no sabemos
/// qué cambió, reprovisionar es la opción segura, y el ledger hace la
/// reprovisión idempotente. Perder un cambio de umbral es peor que
/// reprovisionar de más.
pub fn is_bookkeeping_only(delta: &Delta) -> bool {
    if delta.is_empty() {
        return false;
    }
    delta
        .keys()
        .all(|k| BOOKKEEPING_FIELDS.contains(&k.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(v: Value) -> serde_json::Map<String, Value> {
        v.as_object().cloned().unwrap()
    }

    #[test]
    fn solo_los_cambiados_entran_al_delta() {
        let before = map(json!({"status": "ACTIVE", "priority": "HIGH"}));
        let after = map(json!({"status": "ACTIVE", "priority": "CRITICAL"}));
        let delta = compute_delta(&before, &after);
        assert_eq!(delta.len(), 1);
        assert_eq!(delta["priority"].before, Some(json!("HIGH")));
        assert_eq!(delta["priority"].after, Some(json!("CRITICAL")));
    }

    #[test]
    fn update_idempotente_produce_delta_vacio() {
        let before = map(json!({"status": "ACTIVE"}));
        let after = map(json!({"status": "ACTIVE"}));
        assert!(compute_delta(&before, &after).is_empty());
    }

    #[test]
    fn bitacora_pura_se_descarta() {
        let delta = compute_delta(
            &map(json!({"run_count": 41, "last_run_at": 1})),
            &map(json!({"run_count": 42, "last_run_at": 2})),
        );
        assert!(
            is_bookkeeping_only(&delta),
            "42 escrituras de bitácora no deben reprovisionar trampas"
        );
    }

    #[test]
    fn bitacora_mas_cambio_real_no_se_descarta() {
        let delta = compute_delta(
            &map(json!({"run_count": 41, "status": "ACTIVE"})),
            &map(json!({"run_count": 42, "status": "SUSPENDED"})),
        );
        assert!(!is_bookkeeping_only(&delta));
    }

    #[test]
    fn delta_vacio_no_se_descarta_opcion_segura() {
        // Espejo del comentario en delta.go: un delta vacío devuelve false a
        // propósito — reprovisionar de más es preferible a perder un umbral.
        assert!(!is_bookkeeping_only(&Delta::new()));
    }

    #[test]
    fn atributo_nuevo_entra_con_before_ausente() {
        let before = map(json!({}));
        let after = map(json!({"last_error": "timeout"}));
        let delta = compute_delta(&before, &after);
        assert_eq!(delta["last_error"].before, None);
        assert_eq!(delta["last_error"].after, Some(json!("timeout")));
    }
}
