// [PORTED_FROM: src/metri/janus/ast_compiler.clj]
// janus/ast_compiler.rs — Ensamblador del AST IR.
// SRP: combina los sub-pasos 4a-4f en un AST IR inmutable completo.

use serde_json::{json, Value};
use tracing::{info, warn};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::janus::router::CedarCtx;
use crate::janus::abac_clauses;
use crate::janus::filter_compiler;

/// Construye la proyección :select
/// [PORTED_FROM: (build-select entity stree schema)]
fn build_select(entity: &str, stree: Option<&Value>) -> Vec<Value> {
    if let Some(stree) = stree {
        if let Some(arr) = stree.as_array() {
            if arr.is_empty() {
                return vec![json!("*")];
            }
            return arr.iter().map(|v| {
                let fname = v.as_str().unwrap_or("");
                if fname == "id" {
                    json!("entity/ulid")
                } else if fname == "created_at" {
                    json!("meta/created_at")
                } else {
                    json!(format!("{}/{}", entity, fname))
                }
            }).collect();
        } else if let Some(obj) = stree.as_object() {
            let mut acc = Vec::new();
            for (k, v) in obj {
                let is_truthy = match v {
                    Value::Bool(b) => *b,
                    Value::Number(n) => n.as_i64() != Some(0),
                    Value::String(s) => s != "false",
                    _ => false,
                };
                if is_truthy {
                    let fname = k.as_str();
                    if fname == "id" {
                        acc.push(json!("entity/ulid"));
                    } else if fname == "created_at" {
                        acc.push(json!("meta/created_at"));
                    } else {
                        acc.push(json!(format!("{}/{}", entity, fname)));
                    }
                }
            }
            if acc.is_empty() {
                return vec![json!("*")];
            }
            return acc;
        }
    }
    vec![json!("*")]
}

/// Ensambla el AST IR completo desde los pasos 4a-4f.
/// [PORTED_FROM: (compile-ast-internal query-descriptor cedar-ctx)]
pub fn compile_ast_internal(
    query_descriptor: &Value,
    cedar_ctx: &CedarCtx,
    schema: &Value,
) -> Result<Value, DomainError> {
    let entity = query_descriptor.get("entity").and_then(|v| v.as_str()).unwrap_or("");
    let tenant_id = &cedar_ctx.tenant_id;
    let user_id = &cedar_ctx.user_id;

    // TODO: En Fase 3 integrar Códice para extraer fronteras y owner/assignee reales
    // FASE 3 stub
    let boundaries = vec![];
    let owner_fields = abac_clauses::ownership_fields(entity, schema);

    // 4a: Tenant isolation
    let tenant_node = abac_clauses::tenant_node(tenant_id);

    // 4b+4c: ABAC
    let abac_node = abac_clauses::build_abac_node(
        entity,
        &boundaries,
        owner_fields.owner_field.as_deref(),
        owner_fields.assignee_field.as_deref(),
        user_id
    )?;

    // 4d: Filters
    let filters = query_descriptor.get("filters").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let filter_nodes = filter_compiler::compile_filters(&filters, entity)?;

    // 4d-SEARCH: Omnisearch -> fuzzy mapping
    let search_term = query_descriptor.get("search").and_then(|v| v.as_str());
    let mut search_node = None;
    if let Some(term) = search_term {
        if let Some(attrs) = schema.get("attributes").and_then(|v| v.as_array()) {
            let fts_fields: Vec<String> = attrs.iter()
                .filter(|a| a.get("fts").and_then(|v| v.as_bool()).unwrap_or(false))
                .filter_map(|a| a.get("name").and_then(|v| v.as_str()))
                .map(|name| format!("{}/{}", entity, name))
                .collect();
            
            if !fts_fields.is_empty() {
                if fts_fields.len() == 1 {
                    search_node = Some(json!(["fuzzy", fts_fields[0], term]));
                } else {
                    let mut or_node = vec![json!("or")];
                    for f in fts_fields {
                        or_node.push(json!(["fuzzy", f, term]));
                    }
                    search_node = Some(Value::Array(or_node));
                }
            }
        }
    }

    // Ensamblaje WHERE (tenant_node SIEMPRE primero)
    let mut where_clauses = vec![tenant_node];
    if let Some(abac) = abac_node {
        where_clauses.push(abac);
    }
    if let Some(search) = search_node {
        where_clauses.push(search);
    }
    where_clauses.extend(filter_nodes);

    let final_where = if where_clauses.len() == 1 {
        where_clauses.remove(0)
    } else {
        let mut and_node = vec![json!("and")];
        and_node.extend(where_clauses);
        Value::Array(and_node)
    };

    // 4e: Select
    let stree = query_descriptor.get("select_tree").or_else(|| query_descriptor.get("select-tree"));
    let select = build_select(entity, stree);

    // Limit and Output Cast
    let limit = query_descriptor.get("limit").and_then(|v| v.as_u64()).unwrap_or(100);
    let output_cast_from_viz = match query_descriptor.get("viz").and_then(|v| v.as_str()) {
        Some("pie") | Some("donut")                              => "PIE",
        Some("bar") | Some("line") | Some("area") | Some("scatter") | Some("timeseries") => "TIMESERIES",
        Some("tree") | Some("table")                             => "TABLE",
        Some("kpi") | Some("indicator") | Some("gauge")          => "KPI",
        _                                                        => "TABLE",
    };
    let output_cast = query_descriptor.get("output_cast")
        .or_else(|| query_descriptor.get("output-cast"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty() && *s != "0" && *s != "OUTPUT_CAST_UNSPECIFIED")
        .unwrap_or(output_cast_from_viz);

    let mut ast = json!({
        "entity": entity,
        "schema": schema,
        "select": select,
        "where": final_where,
        "limit": limit,
        "output_cast": output_cast,
    });

    // Pass-through elements
    let pass_through = [
        "metrics", "dimensions", "group_by", "time_frame", 
        "order_by", "hierarchy", "comparisons", "semantic_measures",
        "search", "cursor", "viz"
    ];

    if let Some(obj) = ast.as_object_mut() {
        for key in pass_through.iter() {
            if let Some(val) = query_descriptor.get(key) {
                obj.insert(key.to_string(), val.clone());
            } else if let Some(val) = query_descriptor.get(key.replace('_', "-")) {
                obj.insert(key.to_string(), val.clone());
            }
        }
    }

    Ok(ast)
}

use crate::janus::fbs;

/// Compilador AST usando la estructura nativa FlatBuffers (AnalyticsRequestT).
/// Combina los sub-pasos 4a-4f en un AST IR fuertemente tipado.
pub fn compile_ast_fbs(
    req: &fbs::AnalyticsRequestT,
    cedar_ctx: &CedarCtx,
    schema: &Value,
) -> Result<fbs::AnalyticsRequestT, DomainError> {
    let mut new_req = req.clone();
    let entity = req.entity.as_deref().unwrap_or("");
    let tenant_id = &cedar_ctx.tenant_id;
    
    let mut base_filters = new_req.filters.take().unwrap_or_default();

    // 4a: Tenant isolation & Entity type
    let tenant_filter = fbs::FilterNodeT {
        criteria: Some(Box::new(fbs::FilterCriteriaT {
            field: Some("tenant_id".to_string()),
            op_ref: fbs::FilterOperator::EQ,
            value: Some(Box::new(fbs::FilterValueT {
                string_val: Some(tenant_id.to_string()),
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    };
    base_filters.insert(0, tenant_filter);

    let entity_type_filter = fbs::FilterNodeT {
        criteria: Some(Box::new(fbs::FilterCriteriaT {
            field: Some("entity_type".to_string()),
            op_ref: fbs::FilterOperator::EQ,
            value: Some(Box::new(fbs::FilterValueT {
                string_val: Some(entity.to_string()),
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    };
    base_filters.insert(1, entity_type_filter);

    // 4b+4c: ABAC stub
    // En Fase 3 se extraen boundaries y ownership de `schema`.
    
    // 4d-SEARCH: Omnisearch -> fuzzy mapping
    if let Some(term) = &req.search {
        if let Some(attrs) = schema.get("attributes").and_then(|v| v.as_array()) {
            let fts_fields: Vec<String> = attrs.iter()
                .filter(|a| a.get("fts").and_then(|v| v.as_bool()).unwrap_or(false))
                .filter_map(|a| a.get("name").and_then(|v| v.as_str()))
                .map(|name| format!("{}/{}", entity, name))
                .collect();
            
            if !fts_fields.is_empty() {
                // Si es un campo, EQUALITY/FUZZY node. Si son varios, un FilterGroup OR.
                // Para simplificar, añadimos el primer campo como filtro FTS.
                base_filters.push(fbs::FilterNodeT {
                    criteria: Some(Box::new(fbs::FilterCriteriaT {
                        field: Some(fts_fields[0].clone()),
                        op_ref: fbs::FilterOperator::CONTAINS,
                        value: Some(Box::new(fbs::FilterValueT {
                            string_val: Some(term.clone()),
                            ..Default::default()
                        })),
                        ..Default::default()
                    })),
                    ..Default::default()
                });
            }
        }
    }

    new_req.filters = Some(base_filters);

    // Select resolution
    // Convert select_tree JSON string into measures/dimensions if needed, 
    // but the FlatBuffers already separates them in metric/dimension definitions!
    // We just keep the typed objects.

    // Infer output_cast from viz if not explicitly set
    if new_req.output_cast.0 == 0 /* OUTPUT_CAST_UNSPECIFIED */ {
        if let Some(viz) = &new_req.viz {
            let cast = match viz.as_str() {
                "pie" | "donut" => 4, // PIE
                "bar" | "line" | "area" | "scatter" | "timeseries" => 2, // TIMESERIES
                "tree" | "table" => 3, // TABLE
                "kpi" | "indicator" | "gauge" => 1, // KPI
                _ => 3, // TABLE
            };
            new_req.output_cast = fbs::OutputCastType(cast);
        } else {
            new_req.output_cast = fbs::OutputCastType(3); // TABLE
        }
    }

    Ok(new_req)
}
