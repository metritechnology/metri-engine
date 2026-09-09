//! FBS to domain translators for the read path.
use crate::codice::global as codice_global;
use crate::janus::fbs::AnalyticsRequestT;
use serde_json::{json, Value};

pub fn fbs_filter_value_to_json(
    value: &crate::janus::fbs::FilterValueT,
    entity: &str,
    field_name: &str,
) -> Value {
    let mut val_map = serde_json::Map::new();

    // Check optional/reference fields first
    if let Some(ref s) = value.string_val {
        val_map.insert("string_val".to_string(), json!(s));
        return Value::Object(val_map);
    }

    if let Some(ref l) = value.list_val {
        if let Some(ref vals) = l.values {
            val_map.insert("list_val".to_string(), json!({ "values": vals }));
            return Value::Object(val_map);
        }
    }

    if let Some(ref r) = value.range_values {
        if let Some(ref vals) = r.values {
            let json_vals: Vec<Value> = vals
                .iter()
                .map(|item| fbs_filter_value_to_json(item, entity, field_name))
                .collect();
            val_map.insert("range_values".to_string(), json!({ "values": json_vals }));
            return Value::Object(val_map);
        }
    }

    // Schema-directed type mapping for scalars
    let attr_type = codice_global()
        .get_attribute(entity, field_name)
        .map(|a| &a.attr_type);

    match attr_type {
        Some(crate::codice::registry::AttrType::Boolean) => {
            val_map.insert("bool_val".to_string(), json!(value.bool_val));
        }
        Some(crate::codice::registry::AttrType::Epoch) => {
            val_map.insert("timestamp_val".to_string(), json!(value.timestamp_val));
        }
        _ => {
            if field_name == "created_at"
                || field_name == "updated_at"
                || field_name.contains("timestamp")
            {
                val_map.insert("timestamp_val".to_string(), json!(value.timestamp_val));
            } else {
                val_map.insert("number_val".to_string(), json!(value.number_val));
            }
        }
    }

    Value::Object(val_map)
}

pub fn fbs_filter_node_to_json(node: &crate::janus::fbs::FilterNodeT, entity: &str) -> Value {
    let mut map = serde_json::Map::new();
    if let Some(ref criteria) = node.criteria {
        let mut crit_map = serde_json::Map::new();
        let mut field_name = "unknown";
        if let Some(ref field) = criteria.field {
            crit_map.insert("field".to_string(), json!(field));
            field_name = field;
        }
        let op_str = match criteria.op_ref {
            crate::janus::fbs::FilterOperator::EQ => "EQ",
            crate::janus::fbs::FilterOperator::NEQ => "NEQ",
            crate::janus::fbs::FilterOperator::GT => "GT",
            crate::janus::fbs::FilterOperator::GTE => "GTE",
            crate::janus::fbs::FilterOperator::LT => "LT",
            crate::janus::fbs::FilterOperator::LTE => "LTE",
            crate::janus::fbs::FilterOperator::IN => "IN",
            crate::janus::fbs::FilterOperator::NOT_IN => "NOT_IN",
            crate::janus::fbs::FilterOperator::BETWEEN => "BETWEEN",
            crate::janus::fbs::FilterOperator::LIKE => "LIKE",
            crate::janus::fbs::FilterOperator::IS_NULL => "IS_NULL",
            crate::janus::fbs::FilterOperator::IS_NOT_NULL => "IS_NOT_NULL",
            crate::janus::fbs::FilterOperator::MATCHES => "MATCHES",
            crate::janus::fbs::FilterOperator::CONTAINS => "CONTAINS",
            _ => "EQ",
        };
        crit_map.insert("op_ref".to_string(), json!(op_str));
        if let Some(ref value) = criteria.value {
            let val_json = fbs_filter_value_to_json(value, entity, field_name);
            crit_map.insert("value".to_string(), val_json);
        }
        map.insert("criteria".to_string(), Value::Object(crit_map));
    } else if let Some(ref group) = node.group {
        let mut grp_map = serde_json::Map::new();
        let conj_str = match group.conjunction {
            crate::janus::fbs::FilterGroup_Conjunction::AND => "AND",
            crate::janus::fbs::FilterGroup_Conjunction::OR => "OR",
            crate::janus::fbs::FilterGroup_Conjunction::NOT => "NOT",
            _ => "AND",
        };
        grp_map.insert("conjunction".to_string(), json!(conj_str));
        if let Some(ref nodes) = group.nodes {
            let json_nodes: Vec<Value> = nodes
                .iter()
                .map(|n| fbs_filter_node_to_json(n, entity))
                .collect();
            grp_map.insert("nodes".to_string(), Value::Array(json_nodes));
        }
        map.insert("group".to_string(), Value::Object(grp_map));
    }
    Value::Object(map)
}

pub fn analytics_request_to_json(req: &AnalyticsRequestT) -> Value {
    let mut map = serde_json::Map::new();
    if let Some(ref tenant_id) = req.tenant_id {
        map.insert("tenant_id".to_string(), json!(tenant_id));
    }
    let entity = req.entity.as_deref().unwrap_or("unknown");
    if let Some(ref entity_val) = req.entity {
        map.insert("entity".to_string(), json!(entity_val));
    }
    if let Some(ref metrics) = req.metrics {
        let mut m_vals = Vec::new();
        for m in metrics {
            let mut m_map = serde_json::Map::new();
            if let Some(ref ent) = m.entity {
                m_map.insert("entity".to_string(), json!(ent));
            }
            if let Some(ref attr) = m.attribute {
                m_map.insert("attribute".to_string(), json!(attr));
            }
            let agg_str = match m.aggregation.0 {
                1 => "count",
                2 => "sum",
                3 => "avg",
                4 => "min",
                5 => "max",
                _ => "count",
            };
            m_map.insert("aggregation".to_string(), json!(agg_str));
            if let Some(ref name) = m.name {
                m_map.insert("name".to_string(), json!(name));
            }
            if let Some(ref sec_attr) = m.secondary_attribute {
                m_map.insert("secondary_attribute".to_string(), json!(sec_attr));
            }
            if let Some(ref interval) = m.interval {
                m_map.insert("interval".to_string(), json!(interval));
            }
            m_vals.push(Value::Object(m_map));
        }
        map.insert("metrics".to_string(), Value::Array(m_vals));
    }
    if let Some(ref dimensions) = req.dimensions {
        let mut d_vals = Vec::new();
        for d in dimensions {
            let mut d_map = serde_json::Map::new();
            if let Some(ref ent) = d.entity {
                d_map.insert("entity".to_string(), json!(ent));
            }
            if let Some(ref attr) = d.attribute {
                d_map.insert("attribute".to_string(), json!(attr));
            }
            if let Some(ref interval) = d.interval {
                d_map.insert("interval".to_string(), json!(interval));
            }
            if let Some(ref label_template) = d.label_template {
                d_map.insert("label_template".to_string(), json!(label_template));
            }
            d_vals.push(Value::Object(d_map));
        }
        map.insert("dimensions".to_string(), Value::Array(d_vals));
    }
    if let Some(ref tf) = req.time_frame {
        let type_str = match tf.type_ {
            crate::janus::fbs::TimeFrameContext_TimeFilterType::CUSTOM_RANGE => "CUSTOM_RANGE",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::TODAY => "TODAY",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::YESTERDAY => "YESTERDAY",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::TOMORROW => "TOMORROW",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_N_MINUTES => "LAST_N_MINUTES",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_N_HOURS => "LAST_N_HOURS",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_N_DAYS => "LAST_N_DAYS",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::NEXT_N_DAYS => "NEXT_N_DAYS",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::THIS_WEEK => "THIS_WEEK",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_WEEK => "LAST_WEEK",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::NEXT_WEEK => "NEXT_WEEK",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_N_WEEKS => "LAST_N_WEEKS",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::NEXT_N_WEEKS => "NEXT_N_WEEKS",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::WEEK_TO_DATE => "WEEK_TO_DATE",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::THIS_MONTH => "THIS_MONTH",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_MONTH => "LAST_MONTH",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::NEXT_MONTH => "NEXT_MONTH",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_N_MONTHS => "LAST_N_MONTHS",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::NEXT_N_MONTHS => "NEXT_N_MONTHS",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::MONTH_TO_DATE => "MONTH_TO_DATE",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::THIS_QUARTER => "THIS_QUARTER",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_QUARTER => "LAST_QUARTER",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_N_QUARTERS => {
                "LAST_N_QUARTERS"
            }
            crate::janus::fbs::TimeFrameContext_TimeFilterType::QUARTER_TO_DATE => {
                "QUARTER_TO_DATE"
            }
            crate::janus::fbs::TimeFrameContext_TimeFilterType::THIS_YEAR => "THIS_YEAR",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_YEAR => "LAST_YEAR",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::LAST_N_YEARS => "LAST_N_YEARS",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::YEAR_TO_DATE => "YEAR_TO_DATE",
            crate::janus::fbs::TimeFrameContext_TimeFilterType::ALL_TIME => "ALL_TIME",
            _ => "ALL_TIME",
        };
        let mut tf_map = serde_json::Map::new();
        tf_map.insert("type".to_string(), json!(type_str));
        tf_map.insert("n_value".to_string(), json!(tf.n_value));
        tf_map.insert("start_ts".to_string(), json!(tf.start_ts));
        tf_map.insert("end_ts".to_string(), json!(tf.end_ts));
        if let Some(ref tz) = tf.timezone {
            tf_map.insert("timezone".to_string(), json!(tz));
        }
        map.insert("time_frame".to_string(), Value::Object(tf_map));
    }
    if let Some(ref filters) = req.filters {
        let json_filters: Vec<Value> = filters
            .iter()
            .map(|f| fbs_filter_node_to_json(f, entity))
            .collect();
        map.insert("filters".to_string(), Value::Array(json_filters));
    }
    map.insert("limit".to_string(), json!(req.limit));
    if let Some(ref cursor) = req.cursor {
        map.insert("cursor".to_string(), json!(cursor));
    }
    if let Some(ref viz) = req.viz {
        map.insert("viz".to_string(), json!(viz));
    }
    let output_cast_str = match req.output_cast.0 {
        1 => "KPI",
        2 => "TIMESERIES",
        3 => "TABLE",
        4 => "PIE",
        5 => "BUBBLE",
        6 => "CSV_EXPORT",
        _ => "TABLE",
    };
    map.insert("output_cast".to_string(), json!(output_cast_str));
    if let Some(ref search) = req.search {
        map.insert("search".to_string(), json!(search));
    }
    if let Some(ref comps) = req.comparisons {
        let mut comp_vals = Vec::new();
        for c in comps {
            let mut c_map = serde_json::Map::new();
            let type_str = match c.type_.0 {
                1 => "TIME_SHIFT_RELATIVE",
                2 => "TIME_SHIFT_SHORTCUT",
                3 => "TIME_SHIFT_ABSOLUTE",
                4 => "BENCHMARK",
                5 => "SMART",
                _ => "TIME_SHIFT_RELATIVE",
            };
            c_map.insert("type".to_string(), json!(type_str));
            if let Some(ref label) = c.label {
                c_map.insert("label".to_string(), json!(label));
            }
            if let Some(ref rg) = c.relative_granularity {
                c_map.insert("relative_granularity".to_string(), json!(rg));
            }
            c_map.insert("relative_amount".to_string(), json!(c.relative_amount));
            let shortcut_str = match c.shortcut.0 {
                1 => "PREVIOUS_PERIOD",
                2 => "SAME_PERIOD_LAST_YEAR",
                3 => "SAME_PERIOD_LAST_QUARTER",
                4 => "SAME_PERIOD_LAST_MONTH",
                5 => "SAME_DAY_LAST_WEEK",
                6 => "SAME_DAY_LAST_MONTH",
                7 => "SAME_DAY_LAST_YEAR",
                8 => "YESTERDAY_LAST_YEAR",
                9 => "YESTERDAY_LAST_MONTH",
                10 => "YESTERDAY_LAST_WEEK",
                11 => "TODAY_LAST_YEAR",
                12 => "TODAY_LAST_MONTH",
                _ => "PREVIOUS_PERIOD",
            };
            c_map.insert("shortcut".to_string(), json!(shortcut_str));
            c_map.insert("absolute_start_ts".to_string(), json!(c.absolute_start_ts));
            c_map.insert("absolute_end_ts".to_string(), json!(c.absolute_end_ts));
            c_map.insert("benchmark_value".to_string(), json!(c.benchmark_value));
            comp_vals.push(Value::Object(c_map));
        }
        map.insert("comparisons".to_string(), Value::Array(comp_vals));
    }
    if let Some(ref hierarchy) = req.hierarchy {
        let mut h_map = serde_json::Map::new();
        if let Some(ref pf) = hierarchy.parent_field {
            h_map.insert("parent_field".to_string(), json!(pf));
        }
        if let Some(ref nid) = hierarchy.current_node_id {
            h_map.insert("current_node_id".to_string(), json!(nid));
        }
        h_map.insert(
            "inject_has_children".to_string(),
            json!(hierarchy.inject_has_children),
        );
        map.insert("hierarchy".to_string(), Value::Object(h_map));
    }
    if let Some(ref st) = req.select_tree {
        if let Ok(st_val) = serde_json::from_str::<Value>(st) {
            map.insert("select_tree".to_string(), st_val);
        }
    }
    Value::Object(map)
}
