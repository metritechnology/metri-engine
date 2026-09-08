// aegis/sql/metric_compiler.rs
// Compiles MetricDef analytics metric specifications into sea-query expressions.

use crate::aegis::ast_ir::{MetricDef, WhereNode};
use crate::aegis::sql::dialect::SqlDialect;
use crate::aegis::sql::where_compiler::{col_id_str, compile_where_node};
use crate::domain::errors::DomainError;
use sea_query::{Expr, SimpleExpr};

pub fn format_condition(cond: &sea_query::Condition) -> String {
    let mut select = sea_query::Query::select();
    select.cond_where(cond.clone());
    let sql = select.to_string(sea_query::PostgresQueryBuilder);
    if let Some(pos) = sql.find("WHERE ") {
        sql[pos + 6..].to_string()
    } else {
        "1=1".to_string()
    }
}

pub fn compile_metric(
    m: &MetricDef,
    dialect: &dyn SqlDialect,
    entity: &str,
) -> Result<(SimpleExpr, String), DomainError> {
    let fn_raw = m.aggregation.as_deref().unwrap_or("COUNT");
    let mut fn_str = fn_raw.to_uppercase();

    let is_hybrid = crate::aegis::sql::registry::get_hybrid_config(entity).is_some();
    let is_count = fn_str == "COUNT";

    let attr = m.attribute.as_deref().or(m.field.as_deref());
    let sec_attr = m
        .secondary_attribute
        .as_deref()
        .or(m.secondary_field.as_deref());

    let alias_kw = if let Some(name) = m.name.as_deref() {
        name.to_string()
    } else {
        let attr_suffix = attr.map(col_id_str).unwrap_or_else(|| "total".to_string());
        format!("{}_{}", fn_raw.to_lowercase(), attr_suffix)
    };

    let field = if is_hybrid && is_count {
        "\"reading_count\"".to_string()
    } else {
        attr.map(|a| format!("\"{}\"", col_id_str(a)))
            .unwrap_or_else(|| "*".to_string())
    };

    if is_hybrid && is_count {
        fn_str = "SUM".to_string();
    }

    let eff_field = if let Some(filter_val) = &m.filter {
        if !filter_val.is_null() {
            let where_node = WhereNode::from_value(filter_val)?;
            let cond = compile_where_node(&where_node, dialect);
            let cond_expr = format_condition(&cond);
            let then_val = if fn_str == "COUNT" {
                "1".to_string()
            } else {
                field.clone()
            };
            format!("CASE WHEN {} THEN {} ELSE NULL END", cond_expr, then_val)
        } else {
            field
        }
    } else {
        field
    };

    let mut agg_call = match fn_str.as_str() {
        "COUNT" => format!("COUNT({})", eff_field),
        "SUM" => format!("SUM({})", eff_field),
        "AVG" => format!("AVG({})", eff_field),
        "MIN" => format!("MIN({})", eff_field),
        "MAX" => format!("MAX({})", eff_field),
        "MEDIAN" => dialect.format_percentile(&eff_field, 0.5),
        "STD_DEV" => dialect.format_stddev_samp(&eff_field),
        "VARIANCE" => dialect.format_var_samp(&eff_field),
        "PERCENTILE_90" => dialect.format_percentile(&eff_field, 0.9),
        "PERCENTILE_95" => dialect.format_percentile(&eff_field, 0.95),
        "PERCENTILE_99" => dialect.format_percentile(&eff_field, 0.99),
        "CORRELATION" => {
            let sec = sec_attr
                .map(|a| format!("\"{}\"", col_id_str(a)))
                .unwrap_or_else(|| eff_field.clone());
            dialect.format_corr(&sec, &eff_field)
        }
        "LINEAR_REGRESSION" => {
            let sec = sec_attr
                .map(|a| format!("\"{}\"", col_id_str(a)))
                .unwrap_or_else(|| eff_field.clone());
            dialect.format_regr_slope(&sec, &eff_field)
        }
        "LOGISTIC_REGRESSION" => {
            format!("COUNT({})", eff_field)
        }
        _ => format!("COUNT({})", eff_field),
    };

    let null_safe_fns = vec![
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "MEDIAN",
        "STD_DEV",
        "VARIANCE",
        "PERCENTILE_90",
        "PERCENTILE_95",
        "PERCENTILE_99",
    ];
    if null_safe_fns.contains(&fn_str.as_str()) {
        agg_call = dialect.format_coalesce(&agg_call, "0");
    }

    Ok((Expr::cust(&agg_call), alias_kw))
}
