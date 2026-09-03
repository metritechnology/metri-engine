// cedar/resource_hydrator.rs — Hidratación ABAC del recurso.
//
// Antes del evaluador, el recurso sintetizado desde cabeceras se enriquece
// con la empresa asignada según el EAV: las políticas mutacionales comparan
// `assigned_company_id` del recurso con el `company_id` del principal. Este
// paso vive aparte del intercept para que la pipeline sea legible y el
// acceso a EAV no esté incrustado en el orquestador.

use crate::cedar::ports::EntityReader;
use crate::eav::types::datom::DatomValue;

/// Claves de atributo, en orden de precedencia, con las que una entidad
/// declara su empresa asignada.
const COMPANY_ATTR_KEYS: &[&str] = &[
    "assigned_company_id",
    "company_id",
    "assigned_company",
    "company",
];

/// Rellena `resource.assigned_company_id` con el valor vigente en el EAV.
/// Best-effort: si la entidad no existe o no declara empresa, el recurso
/// queda tal cual (la política denegará por falta de empresa).
pub(crate) async fn hydrate_company_assignment(
    eav_reader: &dyn EntityReader,
    tenant_id: &str,
    resource: &mut serde_json::Value,
) {
    let resource_id = resource
        .get("entity_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if resource_id.is_empty() {
        return;
    }

    let Ok(entity_map) = eav_reader.pull(tenant_id, resource_id, None).await else {
        return;
    };
    let Some(obj) = resource.as_object_mut() else {
        return;
    };

    let entity_type = obj
        .get("entity_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    for key in COMPANY_ATTR_KEYS {
        if let Some(val) = entity_map
            .get(*key)
            .or_else(|| entity_map.get(&format!("{}/{}", entity_type, key)))
        {
            match val {
                DatomValue::Str(s) => {
                    obj.insert("assigned_company_id".to_string(), serde_json::json!(s));
                    return;
                }
                DatomValue::Ref(r) => {
                    obj.insert("assigned_company_id".to_string(), serde_json::json!(r));
                    return;
                }
                _ => {}
            }
        }
    }
}
