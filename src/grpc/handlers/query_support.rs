//! Query support — proto/FBS filter translation and shared helpers.
// Handlers del MetriGrpcService — fase 2: service.rs delega, aquí vive el cuerpo.

use crate::grpc::translator;

pub(crate) fn fc_to_fbs_filter_node(
    filter_conditions: &serde_json::Value,
) -> Option<crate::janus::fbs::FilterNodeT> {
    use crate::janus::fbs;

    let parsed_value;
    let fc = if let Some(s) = filter_conditions.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_value = v;
            &parsed_value
        } else {
            filter_conditions
        }
    } else {
        filter_conditions
    };

    let arr = fc.as_array()?;
    if arr.is_empty() {
        return None;
    }

    if arr.len() == 1 {
        return fc_item_to_fbs_node(&arr[0]);
    }

    let first = &arr[0];
    let parsed_first;
    let first_obj = if let Some(s) = first.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_first = v;
            &parsed_first
        } else {
            first
        }
    } else {
        first
    };

    let logical_op = first_obj
        .get("logical_operator")
        .and_then(|v| v.as_str())
        .unwrap_or("AND");
    let conjunction = match logical_op {
        "OR" => 2,
        _ => 1,
    };

    let mut child_nodes = Vec::new();
    for item in arr {
        if let Some(node) = fc_item_to_fbs_node(item) {
            child_nodes.push(node);
        }
    }

    if child_nodes.is_empty() {
        return None;
    }

    Some(fbs::FilterNodeT {
        group: Some(Box::new(fbs::FilterGroupT {
            conjunction: fbs::FilterGroup_Conjunction(conjunction),
            nodes: Some(child_nodes),
        })),
        ..Default::default()
    })
}

pub(crate) fn fc_item_to_fbs_node(
    item: &serde_json::Value,
) -> Option<crate::janus::fbs::FilterNodeT> {
    use crate::janus::fbs;

    let parsed_value;
    let obj = if let Some(s) = item.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_value = v;
            &parsed_value
        } else {
            item
        }
    } else {
        item
    };

    let field = obj.get("field_name").and_then(|v| v.as_str())?.to_string();
    let op_str = obj.get("operator").and_then(|v| v.as_str())?.to_lowercase();
    let target_val_str = obj.get("target_value").and_then(|v| v.as_str())?;
    let val_type = obj
        .get("target_value_type")
        .and_then(|v| v.as_str())
        .unwrap_or("string");

    let op = match op_str.as_str() {
        "eq" => fbs::FilterOperator::EQ,
        "neq" => fbs::FilterOperator::NEQ,
        "gt" => fbs::FilterOperator::GT,
        "lt" => fbs::FilterOperator::LT,
        "gte" => fbs::FilterOperator::GTE,
        "lte" => fbs::FilterOperator::LTE,
        "contains" => fbs::FilterOperator::CONTAINS,
        "in" => fbs::FilterOperator::IN,
        _ => fbs::FilterOperator::EQ,
    };

    let fval = match val_type {
        "long" | "double" => fbs::FilterValueT {
            number_val: target_val_str.parse::<f64>().unwrap_or(0.0),
            ..Default::default()
        },
        "boolean" => fbs::FilterValueT {
            bool_val: target_val_str.parse::<bool>().unwrap_or(false),
            ..Default::default()
        },
        _ => fbs::FilterValueT {
            string_val: Some(target_val_str.to_string()),
            ..Default::default()
        },
    };

    Some(fbs::FilterNodeT {
        criteria: Some(Box::new(fbs::FilterCriteriaT {
            field: Some(field),
            op_ref: op,
            value: Some(Box::new(fval)),
            ..Default::default()
        })),
        ..Default::default()
    })
}

pub(crate) fn fc_to_proto_filter_node(
    filter_conditions: &serde_json::Value,
) -> Option<crate::grpc::pb::FilterNode> {
    let parsed_value;
    let fc = if let Some(s) = filter_conditions.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_value = v;
            &parsed_value
        } else {
            filter_conditions
        }
    } else {
        filter_conditions
    };

    let arr = fc.as_array()?;
    if arr.is_empty() {
        return None;
    }

    if arr.len() == 1 {
        return fc_item_to_proto_node(&arr[0]);
    }

    let first = &arr[0];
    let parsed_first;
    let first_obj = if let Some(s) = first.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_first = v;
            &parsed_first
        } else {
            first
        }
    } else {
        first
    };

    let logical_op = first_obj
        .get("logical_operator")
        .and_then(|v| v.as_str())
        .unwrap_or("AND");
    let conjunction = match logical_op {
        "OR" => 2,
        _ => 1,
    };

    let mut child_nodes = Vec::new();
    for item in arr {
        if let Some(node) = fc_item_to_proto_node(item) {
            child_nodes.push(node);
        }
    }

    if child_nodes.is_empty() {
        return None;
    }

    use crate::grpc::pb::{filter_node::Node, FilterGroup, FilterNode};
    Some(FilterNode {
        node: Some(Node::Group(FilterGroup {
            conjunction,
            nodes: child_nodes,
        })),
    })
}

pub(crate) fn fc_item_to_proto_node(
    item: &serde_json::Value,
) -> Option<crate::grpc::pb::FilterNode> {
    let parsed_value;
    let obj = if let Some(s) = item.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_value = v;
            &parsed_value
        } else {
            item
        }
    } else {
        item
    };

    let field = obj.get("field_name").and_then(|v| v.as_str())?.to_string();
    let op_str = obj.get("operator").and_then(|v| v.as_str())?.to_lowercase();
    let target_val_str = obj.get("target_value").and_then(|v| v.as_str())?;
    let val_type = obj
        .get("target_value_type")
        .and_then(|v| v.as_str())
        .unwrap_or("string");

    let op = match op_str.as_str() {
        "eq" => 1,
        "neq" => 2,
        "gt" => 3,
        "gte" => 4,
        "lt" => 5,
        "lte" => 6,
        "in" => 7,
        "not_in" => 8,
        "between" => 9,
        "like" => 10,
        "is_null" => 11,
        "is_not_null" => 12,
        "matches" => 13,
        "contains" => 14,
        _ => 1,
    };

    use crate::grpc::pb::{
        filter_node::Node, filter_value::Kind, FilterCriteria, FilterNode, FilterValue,
    };

    let kind = match val_type {
        "long" | "double" => Some(Kind::NumberVal(
            target_val_str.parse::<f64>().unwrap_or(0.0),
        )),
        "boolean" => Some(Kind::BoolVal(
            target_val_str.parse::<bool>().unwrap_or(false),
        )),
        _ => Some(Kind::StringVal(target_val_str.to_string())),
    };

    Some(FilterNode {
        node: Some(Node::Criteria(FilterCriteria {
            field,
            op_ref: op,
            value: Some(FilterValue { kind }),
        })),
    })
}

pub(crate) fn map_columns(
    columns_val: Option<&serde_json::Value>,
    is_system_bff: bool,
) -> (Vec<crate::grpc::pb::ColumnSchema>, Vec<String>) {
    let mut pb_columns = Vec::new();
    let mut col_keys = Vec::new();
    if let Some(cols) = columns_val.and_then(|c| c.as_array()) {
        for col in cols {
            let key = col
                .get("key")
                .and_then(|k| k.as_str())
                .unwrap_or("")
                .to_string();

            // FLS: Si el atributo es password_hash y el cliente NO tiene el rol system-bff, omitirlo
            if key == "password_hash" && !is_system_bff {
                continue;
            }

            col_keys.push(key.clone());
            pb_columns.push(crate::grpc::pb::ColumnSchema {
                key,
                label: col
                    .get("label")
                    .and_then(|l| l.as_str())
                    .unwrap_or("")
                    .to_string(),
                r#type: col
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string(),
                format: col
                    .get("format")
                    .and_then(|f| f.as_str())
                    .unwrap_or("")
                    .to_string(),
                is_dimension: col
                    .get("is_dimension")
                    .and_then(|b| b.as_bool())
                    .unwrap_or(false),
                is_measure: col
                    .get("is_measure")
                    .and_then(|b| b.as_bool())
                    .unwrap_or(false),
            });
        }
    }
    (pb_columns, col_keys)
}

pub(crate) fn map_data_rows(
    data_val: Option<&serde_json::Value>,
    col_keys: &[String],
) -> Vec<crate::grpc::pb::DataRow> {
    let mut pb_rows = Vec::new();
    if let Some(data) = data_val.and_then(|d| d.as_array()) {
        for r in data {
            if let Some(obj) = r.as_object() {
                let mut values = Vec::new();
                for k in col_keys {
                    let v = obj.get(k).unwrap_or(&serde_json::Value::Null);
                    let pb_val = translator::json_to_value(v);
                    values.push(pb_val);
                }
                pb_rows.push(crate::grpc::pb::DataRow { values });
            }
        }
    }
    pb_rows
}

pub(crate) fn map_metadata(
    meta_val: Option<&serde_json::Value>,
) -> Option<crate::grpc::pb::QueryMetadata> {
    meta_val.map(|m| crate::grpc::pb::QueryMetadata {
        execution_time_ms: m
            .get("execution_time_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        total_count: m.get("total_count").and_then(|v| v.as_i64()).unwrap_or(0),
        engine: m
            .get("engine")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        is_semantic: m
            .get("is_semantic")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        total_queries: m.get("total_queries").and_then(|v| v.as_i64()).unwrap_or(1) as i32,
        parallelism_factor: m
            .get("parallelism_factor")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0),
        cache_hits: m.get("cache_hits").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
        query_id: m
            .get("query_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        cache_ttl_seconds: m
            .get("cache_ttl_seconds")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
    })
}

pub(crate) fn map_pagination(
    pag_val: Option<&serde_json::Value>,
) -> Option<crate::grpc::pb::Pagination> {
    pag_val.and_then(|p| {
        if p.is_null() {
            return None;
        }
        let mut links = Vec::new();
        if let Some(arr) = p.get("links").and_then(|l| l.as_array()) {
            for lnk in arr {
                links.push(crate::grpc::pb::Link {
                    rel: lnk
                        .get("rel")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    href: lnk
                        .get("href")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    method: lnk
                        .get("method")
                        .and_then(|v| v.as_str())
                        .unwrap_or("POST")
                        .to_string(),
                });
            }
        }
        Some(crate::grpc::pb::Pagination {
            next_cursor: p
                .get("next_cursor")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            previous_cursor: p
                .get("previous_cursor")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            page_size: p.get("page_size").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
            has_next: p.get("has_next").and_then(|v| v.as_bool()).unwrap_or(false),
            has_previous: p
                .get("has_previous")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            links,
        })
    })
}

pub(crate) fn map_viz_meta(
    viz_val: Option<&serde_json::Value>,
) -> Option<crate::grpc::pb::VizMeta> {
    viz_val.map(|vz| {
        let viz_type = vz
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let payload_json = vz.get("payload");

        let payload = if let Some(pj) = payload_json {
            if let Some(signal) = pj.get("signal") {
                Some(crate::grpc::pb::viz_meta::Payload::Signal(
                    crate::grpc::pb::AnalyticalSignal {
                        value: signal.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        previous_value: signal.get("previous_value").and_then(|v| v.as_f64()),
                        unit: signal
                            .get("unit")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        status_label: signal
                            .get("status_label")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        entity_ref: signal
                            .get("entity_ref")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        intelligence: signal.get("intelligence").map(|intel| {
                            crate::grpc::pb::IntelligenceSignal {
                                direction: intel
                                    .get("direction")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("neutral")
                                    .to_string(),
                                percentage: intel
                                    .get("percentage")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0),
                                delta_abs: intel
                                    .get("delta_abs")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0),
                                previous_value: intel
                                    .get("previous_value")
                                    .and_then(|v| v.as_f64()),
                                label: intel
                                    .get("label")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                is_anomaly: intel
                                    .get("is_anomaly")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false),
                                z_score: intel
                                    .get("z_score")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0),
                                represents_initial: intel
                                    .get("represents_initial")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false),
                            }
                        }),
                        ..Default::default()
                    },
                ))
            } else if let Some(chart) = pj.get("chart") {
                Some(crate::grpc::pb::viz_meta::Payload::Chart(
                    crate::grpc::pb::ChartDecoration {
                        x_dimension: chart
                            .get("x_dimension")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        y_dimensions: chart
                            .get("y_dimensions")
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default(),
                        color_scheme: chart
                            .get("color_scheme")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        show_legend: chart
                            .get("show_legend")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true),
                        show_tooltip: chart
                            .get("show_tooltip")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true),
                        title: chart
                            .get("title")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        stacked: chart
                            .get("stacked")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                        smooth: chart
                            .get("smooth")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                        label_template: chart
                            .get("label_template")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        fill_gaps: chart
                            .get("fill_gaps")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                        x_axis_label_template: chart
                            .get("x_axis_label_template")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        y_axis_label_template: chart
                            .get("y_axis_label_template")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        horizontal: chart
                            .get("horizontal")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                    },
                ))
            } else if let Some(breakdown) = pj.get("breakdown") {
                let mut pb_signals = std::collections::HashMap::new();
                if let Some(signals) = breakdown.get("signals").and_then(|s| s.as_object()) {
                    for (k, v) in signals {
                        pb_signals.insert(
                            k.clone(),
                            crate::grpc::pb::AnalyticalSignal {
                                value: v.get("value").and_then(|val| val.as_f64()).unwrap_or(0.0),
                                ..Default::default()
                            },
                        );
                    }
                }
                Some(crate::grpc::pb::viz_meta::Payload::Breakdown(
                    crate::grpc::pb::BreakdownSignal {
                        signals: pb_signals,
                    },
                ))
            } else if let Some(table) = pj.get("table") {
                let table_columns = table
                    .get("columns")
                    .and_then(|c| c.as_array())
                    .map(|cols| {
                        cols.iter()
                            .map(|col| crate::grpc::pb::TableColumn {
                                key: col
                                    .get("key")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                label: col
                                    .get("label")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                r#type: col
                                    .get("type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("string")
                                    .to_string(),
                                sortable: col
                                    .get("sortable")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(true),
                                format: col
                                    .get("format")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                ..Default::default()
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Some(crate::grpc::pb::viz_meta::Payload::Table(
                    crate::grpc::pb::TableMeta {
                        columns: table_columns,
                        row_actions: vec![],
                        global_links: vec![],
                    },
                ))
            } else if let Some(tree) = pj.get("tree") {
                Some(crate::grpc::pb::viz_meta::Payload::Tree(
                    crate::grpc::pb::TreeMeta {
                        id_key: tree
                            .get("id_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        parent_id_key: tree
                            .get("parent_id_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        label_key: tree
                            .get("label_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        has_children_key: tree
                            .get("has_children_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        icon_key: tree
                            .get("icon_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                    },
                ))
            } else {
                None
            }
        } else {
            None
        };

        crate::grpc::pb::VizMeta {
            r#type: viz_type,
            payload,
        }
    })
}

pub(crate) fn map_status(
    status_val: Option<&serde_json::Value>,
    chunk_success: bool,
) -> crate::grpc::pb::Status {
    status_val
        .map(|s| crate::grpc::pb::Status {
            success: s
                .get("success")
                .and_then(|v| v.as_bool())
                .unwrap_or(chunk_success),
            error_code: s
                .get("error_code")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            error_message: s
                .get("error_message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            error_context: None,
        })
        .unwrap_or(crate::grpc::pb::Status {
            success: chunk_success,
            error_code: if !chunk_success {
                "JANUS_ERROR".to_string()
            } else {
                "".to_string()
            },
            error_message: String::new(),
            error_context: None,
        })
}

pub(crate) async fn map_chunk_to_response(
    chunk: crate::janus::router::QueryChunk,
    is_system_bff: bool,
    export_storage: Option<&std::sync::Arc<dyn crate::domain::protocols::IExportStorage>>,
    tenant_id: &str,
) -> crate::grpc::pb::QueryResponse {
    tracing::debug!("CHUNK BODY: {}", chunk.body);

    let mut batch_results = std::collections::HashMap::new();

    // ── 1. Columns & 2. DataRows ────────────────────────────────
    let columns_val = chunk.body.get("columns");
    let (pb_columns, col_keys) = map_columns(columns_val, is_system_bff);

    let data_val = chunk.body.get("data");
    let pb_rows = map_data_rows(data_val, &col_keys);

    let output_cast = chunk.body.get("output_cast").and_then(|v| v.as_str());

    let payload_strategy = match export_storage
        .filter(|_| output_cast == Some("CSV_EXPORT") && pb_rows.len() > 5000)
    {
        Some(storage) => {
            tracing::info!(
                "[Service] Output is CSV_EXPORT with {} rows (> 5000), uploading to S3...",
                pb_rows.len()
            );
            match storage
                .generate_presigned_url(tenant_id, &chunk.query_key, &pb_columns, &pb_rows)
                .await
            {
                Ok(url) => {
                    tracing::info!(
                        "[Service] S3 export success. Presigned URL generated: {}",
                        url
                    );
                    Some(crate::grpc::pb::row_set::PayloadStrategy::PresignedCsvUrl(
                        url,
                    ))
                }
                Err(e) => {
                    tracing::error!(
                        "[Service] S3 export failed: {}. Falling back to inline JSON.",
                        e.detail
                    );
                    Some(crate::grpc::pb::row_set::PayloadStrategy::RowsJson(
                        crate::grpc::pb::DataRowList { iter: pb_rows },
                    ))
                }
            }
        }
        // Sin storage de export (o cast distinto de CSV_EXPORT): inline JSON.
        None => Some(crate::grpc::pb::row_set::PayloadStrategy::RowsJson(
            crate::grpc::pb::DataRowList { iter: pb_rows },
        )),
    };

    // ── 3. QueryMetadata ────────────────────────────────────────
    let pb_metadata = map_metadata(chunk.body.get("metadata"));

    // ── 4. Pagination ───────────────────────────────────────────
    let pb_pagination = map_pagination(chunk.body.get("pagination"));

    // ── 5. VizMeta ──────────────────────────────────────────────
    let pb_viz_ext = map_viz_meta(chunk.body.get("viz_ext"));

    // ── 6. Inner batch_result status ────────────────────────────
    let inner_status = map_status(chunk.body.get("status"), chunk.success);

    // ── 7. Assemble inner QueryResponse ─────────────────────────
    let query_key = chunk.query_key.clone();
    batch_results.insert(
        query_key,
        crate::grpc::pb::QueryResponse {
            status: Some(inner_status),
            data: Some(crate::grpc::pb::RowSet {
                columns: pb_columns,
                payload_strategy,
            }),
            viz_ext: pb_viz_ext.clone(),
            metadata: pb_metadata.clone(),
            pagination: pb_pagination.clone(),
            ..Default::default()
        },
    );

    // ── 8. Assemble outer (envelope) QueryResponse ──────────────
    crate::grpc::pb::QueryResponse {
        status: Some(crate::grpc::pb::Status {
            success: chunk.success,
            error_code: String::new(),
            error_message: String::new(),
            error_context: None,
        }),
        batch_results,
        metadata: pb_metadata,
        pagination: pb_pagination,
        ..Default::default()
    }
}
