//! DTO translators — protobuf messages to domain structures.
//!
//! Traductores DTO ↔ Domain
//! Convierte mensajes protobuf gRPC a estructuras internas JSON/Domain.

use crate::domain::errors::DomainError;
use crate::grpc::pb::QueryRequest;
use crate::janus::fbs;
use serde_json::{json, Value};
use std::collections::HashMap;

/// Traduce el Request a un map de Sub-Queries fuertemente tipado (FlatBuffers Object API).
pub fn query_request_to_queries_map(
    req: &QueryRequest,
) -> Result<HashMap<String, fbs::AnalyticsRequestT>, DomainError> {
    let mut queries = HashMap::new();

    for (key, query) in &req.queries {
        let mut fbs_req = translate_analytics_request(query)?;
        if fbs_req.tenant_id.is_none() || fbs_req.tenant_id.as_deref().unwrap_or("").is_empty() {
            fbs_req.tenant_id = Some(req.tenant_id.clone());
        }
        queries.insert(key.clone(), fbs_req);
    }

    Ok(queries)
}

fn translate_analytics_request(
    query: &crate::grpc::pb::AnalyticsRequest,
) -> Result<fbs::AnalyticsRequestT, DomainError> {
    let metrics = query
        .metrics
        .iter()
        .map(|m| fbs::MetricDefinitionT {
            entity: if m.entity.is_empty() {
                None
            } else {
                Some(m.entity.clone())
            },
            attribute: if m.attribute.is_empty() {
                None
            } else {
                Some(m.attribute.clone())
            },
            aggregation: fbs::AggregationFunction(m.aggregation),
            name: if m.name.is_empty() {
                None
            } else {
                Some(m.name.clone())
            },
            secondary_attribute: if m.secondary_attribute.is_empty() {
                None
            } else {
                Some(m.secondary_attribute.clone())
            },
            filter: m
                .filter
                .as_ref()
                .and_then(translate_filter_node)
                .map(Box::new),
            interval: if m.interval.is_empty() {
                None
            } else {
                Some(m.interval.clone())
            },
        })
        .collect::<Vec<_>>();

    let dimensions = query
        .dimensions
        .iter()
        .map(|d| fbs::DimensionDefinitionT {
            entity: if d.entity.is_empty() {
                None
            } else {
                Some(d.entity.clone())
            },
            attribute: if d.attribute.is_empty() {
                None
            } else {
                Some(d.attribute.clone())
            },
            interval: if d.interval.is_empty() {
                None
            } else {
                Some(d.interval.clone())
            },
            label_template: if d.label_template.is_empty() {
                None
            } else {
                Some(d.label_template.clone())
            },
        })
        .collect::<Vec<_>>();

    let sort = query
        .sort
        .iter()
        .map(|s| fbs::SortDefinitionT {
            field: if s.field.is_empty() {
                None
            } else {
                Some(s.field.clone())
            },
            descending: s.descending,
        })
        .collect::<Vec<_>>();

    // Traducir FilterNodes del AnalyticsRequest.filters
    let filters: Vec<fbs::FilterNodeT> = query
        .filters
        .iter()
        .filter_map(translate_filter_node)
        .collect();

    // Traducir FormulaEntry measures (fórmulas ad-hoc, e.g. "income - tax")
    // BUG FIX: anteriormente hardcodeado como None, perdiendo el campo completo del contrato.
    let measures: Vec<fbs::FormulaEntryT> = query
        .measures
        .iter()
        .map(|m| fbs::FormulaEntryT {
            name: if m.name.is_empty() {
                None
            } else {
                Some(m.name.clone())
            },
            formula: if m.formula.is_empty() {
                None
            } else {
                Some(m.formula.clone())
            },
        })
        .collect();

    // Traducir AnalyticalComparison comparisons (WoW, YoY, vs Benchmark, SMART anomaly)
    // BUG FIX: anteriormente hardcodeado como None, imposibilitando cualquier ventana temporal comparativa.
    let comparisons: Vec<fbs::AnalyticalComparisonT> = query
        .comparisons
        .iter()
        .map(|c| fbs::AnalyticalComparisonT {
            type_: fbs::AnalyticalComparison_ComparisonType(c.r#type),
            label: if c.label.is_empty() {
                None
            } else {
                Some(c.label.clone())
            },
            relative_granularity: if c.relative_granularity.is_empty() {
                None
            } else {
                Some(c.relative_granularity.clone())
            },
            relative_amount: c.relative_amount,
            shortcut: fbs::AnalyticalComparison_ShiftShortcut(c.shortcut),
            absolute_start_ts: c.absolute_start_ts,
            absolute_end_ts: c.absolute_end_ts,
            benchmark_value: c.benchmark_value,
        })
        .collect();

    // Traducir SemanticMetricRef semantic_measures (override Capa Semántica LookML desde el Schema Registry)
    // BUG FIX: anteriormente hardcodeado como None, impidiendo la resolución de métricas semánticas globales.
    let semantic_measures: Vec<fbs::SemanticMetricRefT> = query
        .semantic_measures
        .iter()
        .map(|s| fbs::SemanticMetricRefT {
            metric_key: if s.metric_key.is_empty() {
                None
            } else {
                Some(s.metric_key.clone())
            },
        })
        .collect();

    Ok(fbs::AnalyticsRequestT {
        tenant_id: if query.tenant_id.is_empty() {
            None
        } else {
            Some(query.tenant_id.clone())
        },
        entity: if query.entity.is_empty() {
            None
        } else {
            Some(query.entity.clone())
        },
        metrics: if metrics.is_empty() {
            None
        } else {
            Some(metrics)
        },
        dimensions: if dimensions.is_empty() {
            None
        } else {
            Some(dimensions)
        },
        time_frame: query.time_frame.as_ref().map(|tf| {
            Box::new(fbs::TimeFrameContextT {
                type_: fbs::TimeFrameContext_TimeFilterType(tf.r#type),
                n_value: tf.n_value,
                start_ts: tf.start_ts,
                end_ts: tf.end_ts,
                timezone: if tf.timezone.is_empty() {
                    None
                } else {
                    Some(tf.timezone.clone())
                },
            })
        }),
        filters: if filters.is_empty() {
            None
        } else {
            Some(filters)
        },
        limit: query.limit,
        cursor: if query.cursor.is_empty() {
            None
        } else {
            Some(query.cursor.clone())
        },
        sort: if sort.is_empty() { None } else { Some(sort) },
        viz: if query.viz.is_empty() {
            None
        } else {
            Some(query.viz.clone())
        },
        output_cast: fbs::OutputCastType(query.output_cast),
        measures: if measures.is_empty() {
            None
        } else {
            Some(measures)
        },
        comparisons: if comparisons.is_empty() {
            None
        } else {
            Some(comparisons)
        },
        search: if query.search.is_empty() {
            None
        } else {
            Some(query.search.clone())
        },
        hierarchy: query.hierarchy.as_ref().map(|h| {
            Box::new(fbs::HierarchyContextT {
                parent_field: if h.parent_field.is_empty() {
                    None
                } else {
                    Some(h.parent_field.clone())
                },
                current_node_id: if h.current_node_id.is_empty() {
                    None
                } else {
                    Some(h.current_node_id.clone())
                },
                inject_has_children: h.inject_has_children,
            })
        }),
        semantic_measures: if semantic_measures.is_empty() {
            None
        } else {
            Some(semantic_measures)
        },
        select_tree: query
            .select_tree
            .as_ref()
            .map(|s| serde_json::to_string(&struct_to_value(s.clone())).unwrap_or_default()),
    })
}

fn translate_filter_value(val: &crate::grpc::pb::FilterValue) -> fbs::FilterValueT {
    use crate::grpc::pb::filter_value::Kind;
    match &val.kind {
        Some(Kind::StringVal(s)) => fbs::FilterValueT {
            string_val: Some(s.clone()),
            ..Default::default()
        },
        Some(Kind::NumberVal(n)) => fbs::FilterValueT {
            number_val: *n,
            ..Default::default()
        },
        Some(Kind::BoolVal(b)) => fbs::FilterValueT {
            bool_val: *b,
            ..Default::default()
        },
        Some(Kind::TimestampVal(t)) => fbs::FilterValueT {
            timestamp_val: *t,
            ..Default::default()
        },
        Some(Kind::ListVal(lst)) => fbs::FilterValueT {
            list_val: Some(Box::new(fbs::StringListT {
                values: Some(lst.values.clone()),
            })),
            ..Default::default()
        },
        Some(Kind::RangeValues(range)) => fbs::FilterValueT {
            range_values: Some(Box::new(fbs::FilterValueListT {
                values: Some(range.values.iter().map(translate_filter_value).collect()),
            })),
            ..Default::default()
        },
        None => fbs::FilterValueT::default(),
    }
}

/// Traduce un proto FilterNode a FBS FilterNodeT recursivamente.
fn translate_filter_node(node: &crate::grpc::pb::FilterNode) -> Option<fbs::FilterNodeT> {
    use crate::grpc::pb::filter_node::Node;

    match &node.node {
        Some(Node::Criteria(crit)) => {
            let fbs_value = crit
                .value
                .as_ref()
                .map(|v| Box::new(translate_filter_value(v)));

            Some(fbs::FilterNodeT {
                criteria: Some(Box::new(fbs::FilterCriteriaT {
                    field: Some(crit.field.clone()),
                    op_ref: fbs::FilterOperator(crit.op_ref),
                    value: fbs_value,
                    ..Default::default()
                })),
                ..Default::default()
            })
        }
        Some(Node::Group(grp)) => {
            let child_nodes: Vec<fbs::FilterNodeT> =
                grp.nodes.iter().filter_map(translate_filter_node).collect();
            if child_nodes.is_empty() {
                return None;
            }
            // FilterGroupT usa FilterGroup_Conjunction
            Some(fbs::FilterNodeT {
                group: Some(Box::new(fbs::FilterGroupT {
                    conjunction: fbs::FilterGroup_Conjunction(grp.conjunction),
                    nodes: Some(child_nodes),
                })),
                ..Default::default()
            })
        }
        None => None,
    }
}
/// Convierte un google.protobuf.Struct a serde_json::Value
pub fn struct_to_value(s: prost_types::Struct) -> Value {
    let mut map = serde_json::Map::new();
    for (k, v) in s.fields {
        map.insert(k, value_to_json(v));
    }
    Value::Object(map)
}

pub fn value_to_json(v: prost_types::Value) -> Value {
    use prost_types::value::Kind;
    match v.kind {
        Some(Kind::NullValue(_)) => Value::Null,
        Some(Kind::NumberValue(n)) => {
            if n.fract() == 0.0 {
                json!(n as i64)
            } else {
                json!(n)
            }
        }
        Some(Kind::StringValue(s)) => Value::String(s),
        Some(Kind::BoolValue(b)) => Value::Bool(b),
        Some(Kind::StructValue(s)) => struct_to_value(s),
        Some(Kind::ListValue(l)) => Value::Array(l.values.into_iter().map(value_to_json).collect()),
        None => Value::Null,
    }
}

pub fn value_to_struct(v: &Value) -> prost_types::Struct {
    let mut fields = std::collections::BTreeMap::new();
    if let Some(obj) = v.as_object() {
        for (k, val) in obj {
            fields.insert(k.clone(), json_to_value(val));
        }
    }
    prost_types::Struct { fields }
}

pub fn json_to_value(v: &Value) -> prost_types::Value {
    use prost_types::value::Kind;
    match v {
        Value::Null => prost_types::Value {
            kind: Some(Kind::NullValue(0)),
        },
        Value::Bool(b) => prost_types::Value {
            kind: Some(Kind::BoolValue(*b)),
        },
        Value::Number(n) => prost_types::Value {
            kind: Some(Kind::NumberValue(n.as_f64().unwrap_or(0.0))),
        },
        Value::String(s) => prost_types::Value {
            kind: Some(Kind::StringValue(s.clone())),
        },
        Value::Array(a) => prost_types::Value {
            kind: Some(Kind::ListValue(prost_types::ListValue {
                values: a.iter().map(json_to_value).collect(),
            })),
        },
        Value::Object(_) => prost_types::Value {
            kind: Some(Kind::StructValue(value_to_struct(v))),
        },
    }
}

#[cfg(test)]
#[path = "tests/translator_tests.rs"]
mod tests;
