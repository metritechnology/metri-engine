use serde_json::{json, Value};

/// Aplica las plantillas de etiquetas sobre las filas resultantes de forma segura.
pub fn apply_label_templates(
    rows: &mut [Value],
    dimensions: &[crate::janus::fbs::DimensionDefinitionT],
    output_cast_i: i32,
    viz: &Option<String>,
) {
    let is_cartesian = if let Some(ref viz_str) = viz {
        let viz_type = if viz_str.starts_with('{') {
            serde_json::from_str::<serde_json::Value>(viz_str)
                .ok()
                .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(str::to_string))
                .unwrap_or_else(|| viz_str.clone())
        } else {
            viz_str.clone()
        };
        matches!(viz_type.as_str(), "bar" | "line" | "area" | "scatter" | "timeseries")
    } else {
        false
    };

    if (output_cast_i == 3 || output_cast_i == 4 || output_cast_i == 6) && !is_cartesian {
        let mut dims_with_template = Vec::new();
        for d in dimensions {
            if let (Some(attr), Some(lt)) = (&d.attribute, &d.label_template) {
                if !lt.trim().is_empty() {
                    dims_with_template.push((attr.clone(), lt.clone()));
                }
            }
        }

        if !dims_with_template.is_empty() {
            for row in rows.iter_mut() {
                let mut updates = Vec::new();
                for (attr, lt) in &dims_with_template {
                    if let Some(interpolated) = crate::aegis::label_template::interpolate(lt, row) {
                        updates.push((attr.clone(), Value::String(interpolated)));
                    }
                }
                if !updates.is_empty() {
                    if let Some(obj) = row.as_object_mut() {
                        for (attr, val) in updates {
                            obj.insert(attr, val);
                        }
                    }
                }
            }
        }
    }
}

/// Inyecta has_children de jerarquía si se solicita.
pub fn inject_hierarchy_children(
    rows: &mut [Value],
    hierarchy: Option<&crate::janus::fbs::HierarchyContextT>,
) {
    if let Some(h) = hierarchy {
        if h.inject_has_children {
            if let Some(ref _parent_field) = h.parent_field {
                for row in rows.iter_mut() {
                    if let Some(obj) = row.as_object_mut() {
                        obj.insert("has_children".to_string(), json!(true));
                    }
                }
            } else {
                for row in rows.iter_mut() {
                    if let Some(obj) = row.as_object_mut() {
                        obj.entry("has_children").or_insert(json!(false));
                    }
                }
            }
        }
    }
}

/// Deriva las columnas resultantes a partir de las dimensiones y métricas consultadas.
pub fn derive_columns(
    rows: &[Value],
    dimensions: &[crate::janus::fbs::DimensionDefinitionT],
    metrics: &[crate::janus::fbs::MetricDefinitionT],
) -> Vec<Value> {
    let dim_attrs: Vec<String> = dimensions.iter()
        .filter_map(|d| d.attribute.clone()).collect();
    let metric_aliases: Vec<String> = metrics.iter()
        .map(|m| {
            m.name.clone().unwrap_or_else(|| {
                let agg = match m.aggregation.0 {
                    1 => "count", 2 => "sum", 3 => "avg", 4 => "min", 5 => "max", _ => "agg",
                };
                format!("{}_{}", agg, m.attribute.as_deref().unwrap_or("total"))
            })
        }).collect();
    crate::aegis::oltp::aggregation::derive_columns(rows, &dim_attrs, &metric_aliases)
}

/// Construye la especificación del query spec JSON.
pub fn build_query_spec(
    entity_type: &str,
    dimensions: &[crate::janus::fbs::DimensionDefinitionT],
    metrics: &[crate::janus::fbs::MetricDefinitionT],
) -> Value {
    let mut dims_json = Vec::new();
    for d in dimensions {
        dims_json.push(json!({
            "entity": d.entity,
            "attribute": d.attribute,
            "interval": d.interval,
            "label_template": d.label_template,
        }));
    }
    let mut metrics_json = Vec::new();
    for m in metrics {
        let fn_str = match m.aggregation.0 {
            1 => "count",
            2 => "sum",
            3 => "avg",
            4 => "min",
            5 => "max",
            _ => "agg",
        };
        metrics_json.push(json!({
            "entity": m.entity,
            "attribute": m.attribute,
            "fn": fn_str,
            "name": m.name,
            "secondary_attribute": m.secondary_attribute,
            "interval": m.interval,
        }));
    }
    json!({
        "entity": entity_type,
        "dimensions": dims_json,
        "metrics": metrics_json,
    })
}

/// Redacta atributos marcados con "sensitive": true en el Códice si el solicitante NO es el servicio de autenticación autorizado (metri-auth).
pub fn redact_sensitive_attributes(
    rows: &mut [Value],
    entity_type: &str,
    cedar_ctx: &crate::janus::router::CedarCtx,
) {
    let is_auth_service = is_auth_caller(&cedar_ctx.user_id, &cedar_ctx.roles);
    if is_auth_service {
        return; // metri-auth TIENE PERMISO de lectura de campos sensibles (password_hash, mfa_secret, etc.)
    }

    let registry = crate::codice::global();
    let model = match registry.get_model(entity_type) {
        Some(m) => m,
        None => return,
    };

    let sensitive_attrs: Vec<String> = model
        .attributes
        .iter()
        .filter(|a| a.sensitive)
        .map(|a| a.name.clone())
        .collect();

    if sensitive_attrs.is_empty() {
        return;
    }

    for row in rows.iter_mut() {
        if let Some(obj) = row.as_object_mut() {
            for attr_name in &sensitive_attrs {
                // Censurar / remover atributo directo (ej: "password_hash")
                obj.remove(attr_name);
                // Censurar / remover atributo namespaced (ej: "user/password_hash")
                let namespaced = format!("{}/{}", entity_type, attr_name);
                obj.remove(&namespaced);
            }
        }
    }
}

/// Identifica si el solicitante de la consulta es el servicio autorizado metri-auth o un rol de sistema equivalente.
pub fn is_auth_caller(user_id: &str, roles: &[String]) -> bool {
    if user_id == "metri-auth"
        || user_id == "usr_system_bff"
        || user_id == "bootstrap-system"
        || user_id == "system-auth"
        || user_id == "auth_service"
    {
        return true;
    }

    roles.iter().any(|role| {
        role == "role_system_bff"
            || role == "system-bff"
            || role == "system_auth"
            || role == "metri-auth"
            || role == "role_super_master"
    })
}
