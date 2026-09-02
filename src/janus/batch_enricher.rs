// janus/batch_enricher.rs — Enriquecedor de queries pre-compilación.
// SRP: aplica BatchContext.common-filters y DashboardCrossFilterContext
//      al mapa de sub-queries antes de que el AST compiler los procese.

use serde_json::Value;

/// Mergea el BatchContext en cada sub-query del mapa.
/// common-filters -> prepended a los :filters de cada sub-query.
/// common-entity  -> heredado si la sub-query no tiene :entity propia.
pub fn apply_batch_context(mut queries: Value, batch_ctx: Option<&Value>) -> Value {
    let Some(ctx) = batch_ctx else {
        return queries;
    };
    let common_filters = ctx.get("common_filters").and_then(|v| v.as_array());
    let common_entity = ctx.get("common_entity").and_then(|v| v.as_str());

    if let Some(obj) = queries.as_object_mut() {
        for (_, qm) in obj.iter_mut() {
            if let Some(qm_obj) = qm.as_object_mut() {
                // Apply filters
                if let Some(c_filters) = common_filters {
                    if !c_filters.is_empty() {
                        let mut new_filters = c_filters.clone();
                        if let Some(existing) = qm_obj.get("filters").and_then(|v| v.as_array()) {
                            new_filters.extend(existing.clone());
                        }
                        qm_obj.insert("filters".to_string(), Value::Array(new_filters));
                    }
                }

                // Apply common entity
                if let Some(c_ent) = common_entity {
                    let has_entity = qm_obj
                        .get("entity")
                        .and_then(|v| v.as_str())
                        .map(|s| !s.trim().is_empty())
                        .unwrap_or(false);

                    if !has_entity {
                        qm_obj.insert("entity".to_string(), Value::String(c_ent.to_string()));
                    }
                }
            }
        }
    }
    queries
}

/// Inyecta los cross-filters del DashboardCrossFilterContext en cada sub-query.
pub fn apply_cross_filter(mut queries: Value, cross_filter: Option<&Value>) -> Value {
    let Some(cf) = cross_filter else {
        return queries;
    };
    let cross_filters = cf.get("cross_filters").and_then(|v| v.as_array());

    if let Some(c_filters) = cross_filters {
        if !c_filters.is_empty() {
            if let Some(obj) = queries.as_object_mut() {
                for (_, qm) in obj.iter_mut() {
                    if let Some(qm_obj) = qm.as_object_mut() {
                        let mut new_filters = qm_obj
                            .get("filters")
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default();
                        new_filters.extend(c_filters.clone());
                        qm_obj.insert("filters".to_string(), Value::Array(new_filters));
                    }
                }
            }
        }
    }
    queries
}
