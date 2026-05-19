// [PORTED_FROM: src/metri/janus/batch_enricher.clj]
// janus/batch_enricher.rs — Enriquecedor de queries pre-compilación.
// SRP: aplica BatchContext.common-filters y DashboardCrossFilterContext
//      al mapa de sub-queries antes de que el AST compiler los procese.

use serde_json::Value;

/// Mergea el BatchContext en cada sub-query del mapa.
/// common-filters -> prepended a los :filters de cada sub-query.
/// common-entity  -> heredado si la sub-query no tiene :entity propia.
/// [PORTED_FROM: (apply-batch-context queries batch-ctx)]
pub fn apply_batch_context(mut queries: Value, batch_ctx: Option<&Value>) -> Value {
    let Some(ctx) = batch_ctx else { return queries; };
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
                    let has_entity = qm_obj.get("entity")
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
/// [PORTED_FROM: (apply-cross-filter queries cross-filter)]
pub fn apply_cross_filter(mut queries: Value, cross_filter: Option<&Value>) -> Value {
    let Some(cf) = cross_filter else { return queries; };
    let cross_filters = cf.get("cross_filters").and_then(|v| v.as_array());

    if let Some(c_filters) = cross_filters {
        if !c_filters.is_empty() {
            if let Some(obj) = queries.as_object_mut() {
                for (_, qm) in obj.iter_mut() {
                    if let Some(qm_obj) = qm.as_object_mut() {
                        let mut new_filters = qm_obj.get("filters").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                        new_filters.extend(c_filters.clone());
                        qm_obj.insert("filters".to_string(), Value::Array(new_filters));
                    }
                }
            }
        }
    }
    queries
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_apply_batch_context() {
        let queries = json!({
            "q1": {"filters": [{"foo": "bar"}]},
            "q2": {"entity": "assets"}
        });
        let ctx = json!({
            "common_filters": [{"global": "true"}],
            "common_entity": "locations"
        });

        let enriched = apply_batch_context(queries, Some(&ctx));

        assert_eq!(enriched["q1"]["entity"], "locations");
        assert_eq!(enriched["q1"]["filters"].as_array().unwrap().len(), 2);
        assert_eq!(enriched["q1"]["filters"][0]["global"], "true");

        assert_eq!(enriched["q2"]["entity"], "assets");
    }

    #[test]
    fn test_apply_cross_filter() {
        let queries = json!({
            "q1": {"filters": [{"foo": "bar"}]}
        });
        let cf = json!({
            "cross_filters": [{"cross": "true"}]
        });

        let enriched = apply_cross_filter(queries, Some(&cf));

        assert_eq!(enriched["q1"]["filters"].as_array().unwrap().len(), 2);
        assert_eq!(enriched["q1"]["filters"][1]["cross"], "true"); // appended
    }
}
