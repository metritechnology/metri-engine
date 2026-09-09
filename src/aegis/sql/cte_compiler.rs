//! Comparison CTE compiler (TIME_SHIFT, SMART, BENCHMARK).
//!
//! aegis/sql/cte_compiler.rs
//! Compiles comparison CTE queries (TIME_SHIFT, SMART, BENCHMARK) using sea-query.

use crate::aegis::ast_ir::{AstIr, ComparisonDef, OutputCast};
use crate::aegis::sql::dialect::SqlDialect;
use crate::aegis::sql::metric_compiler::compile_metric;
use crate::aegis::sql::select_compiler::build_select_exprs;
use crate::aegis::sql::where_compiler::{col_id_str, compile_where_node};
use crate::domain::errors::DomainError;
use crate::temporal::comparison::{
    resolve_comparison_period, smart_history_window, AnalyticalComparison, ComparisonType,
    ShiftShortcut,
};
use crate::temporal::core::TimeRange;
use sea_query::{Alias, CommonTableExpression, Cond, Expr, Query, SelectStatement};

#[derive(Clone, Debug)]
pub enum TableExpression {
    Simple(String),
    SubQuery(SelectStatement, String),
}

pub fn resolve_ts_col(ast: &AstIr) -> String {
    if let Some(config) = crate::aegis::sql::registry::get_hybrid_config(&ast.entity) {
        return config.timestamp_column.clone();
    }
    if let Some(schema) = &ast.schema {
        for attr in &schema.attributes {
            let is_epoch = attr.attr_type.as_deref() == Some("epoch")
                || attr.field_type.as_deref() == Some("epoch");
            if is_epoch {
                return attr.name.clone();
            }
        }
    }
    "created_at".to_string()
}

pub fn time_frame_to_cond(
    start_ts: Option<i64>,
    end_ts: Option<i64>,
    ts_col: &str,
    dialect: &dyn SqlDialect,
) -> Option<sea_query::Condition> {
    let col = col_id_str(ts_col);
    let dt_col = dialect.format_epoch_to_timestamp(&format!("\"{}\"", col));
    match (start_ts, end_ts) {
        (Some(start), Some(end)) => {
            let start_expr = dialect.format_from_unixtime(&start.to_string());
            let end_expr = dialect.format_from_unixtime(&end.to_string());
            Some(
                sea_query::Cond::all()
                    .add(Expr::cust(format!("{} >= {}", dt_col, start_expr)))
                    .add(Expr::cust(format!("{} <= {}", dt_col, end_expr))),
            )
        }
        (Some(start), None) => {
            let start_expr = dialect.format_from_unixtime(&start.to_string());
            Some(sea_query::Cond::all().add(Expr::cust(format!("{} >= {}", dt_col, start_expr))))
        }
        (None, Some(end)) => {
            let end_expr = dialect.format_from_unixtime(&end.to_string());
            Some(sea_query::Cond::all().add(Expr::cust(format!("{} <= {}", dt_col, end_expr))))
        }
        _ => None,
    }
}

pub fn to_analytical_comparison(c: &ComparisonDef) -> Option<AnalyticalComparison> {
    let comp_type = match c.comp_type.as_str() {
        "TIME_SHIFT_RELATIVE" => ComparisonType::TimeShiftRelative,
        "TIME_SHIFT_SHORTCUT" => ComparisonType::TimeShiftShortcut,
        "TIME_SHIFT_ABSOLUTE" => ComparisonType::TimeShiftAbsolute,
        "BENCHMARK" => ComparisonType::Benchmark,
        "SMART" => ComparisonType::Smart,
        _ => return None,
    };

    let shortcut = match c.shortcut.as_deref().unwrap_or("") {
        "PREVIOUS_PERIOD" => ShiftShortcut::PreviousPeriod,
        "SAME_PERIOD_LAST_YEAR" => ShiftShortcut::SamePeriodLastYear,
        "SAME_PERIOD_LAST_QUARTER" => ShiftShortcut::SamePeriodLastQuarter,
        "SAME_PERIOD_LAST_MONTH" => ShiftShortcut::SamePeriodLastMonth,
        "SAME_DAY_LAST_WEEK" => ShiftShortcut::SameDayLastWeek,
        "SAME_DAY_LAST_MONTH" => ShiftShortcut::SameDayLastMonth,
        "SAME_DAY_LAST_YEAR" => ShiftShortcut::SameDayLastYear,
        "YESTERDAY_LAST_YEAR" => ShiftShortcut::YesterdayLastYear,
        "YESTERDAY_LAST_MONTH" => ShiftShortcut::YesterdayLastMonth,
        "YESTERDAY_LAST_WEEK" => ShiftShortcut::YesterdayLastWeek,
        "TODAY_LAST_YEAR" => ShiftShortcut::TodayLastYear,
        "TODAY_LAST_MONTH" => ShiftShortcut::TodayLastMonth,
        _ => ShiftShortcut::Unspecified,
    };

    Some(AnalyticalComparison {
        comp_type,
        relative_granularity: c
            .relative_granularity
            .clone()
            .unwrap_or_else(|| "day".to_string()),
        relative_amount: c.relative_amount.unwrap_or(0),
        shortcut,
        absolute_start_ts: c.absolute_start_ts,
        absolute_end_ts: c.absolute_end_ts,
    })
}

pub fn build_comparison_cte_query(
    ast: &AstIr,
    table_expr: &TableExpression,
    time_range: &TimeRange,
    dialect: &dyn SqlDialect,
    limit: Option<u64>,
) -> Result<String, DomainError> {
    let ts_col = resolve_ts_col(ast);

    let mut metric_exprs = Vec::new();
    if let Some(metrics) = &ast.metrics {
        for m in metrics {
            let (expr, alias) = compile_metric(m, dialect, &ast.entity)?;
            metric_exprs.push((expr, alias));
        }
    }

    let mut cte_selects = Vec::new();
    let mut cte_group_by = Vec::new();
    let mut bucket_alias = "bucket".to_string();

    if ast.output_cast == OutputCast::TIMESERIES {
        if let Some(dims) = &ast.group_by {
            for d in dims {
                let bucket_expr =
                    crate::aegis::sql::select_compiler::dim_to_bucket_expr(d, dialect);
                let alias = d
                    .attribute
                    .as_deref()
                    .or(d.field.as_deref())
                    .unwrap_or("bucket")
                    .to_string();
                bucket_alias = alias.clone();
                cte_selects.push((Expr::cust(&bucket_expr), Some(alias)));
                cte_group_by.push(Expr::cust(&bucket_expr));
            }
        }
    }

    for (expr, alias) in &metric_exprs {
        cte_selects.push((expr.clone(), Some(alias.clone())));
    }

    if cte_selects.is_empty() {
        let fallbacks = build_select_exprs(ast, dialect)?;
        for (expr, opt_alias) in fallbacks {
            cte_selects.push((expr, opt_alias));
        }
    }

    // Base WHERE condition
    let base_where = if let Some(w) = &ast.where_clause {
        compile_where_node(w, dialect)
    } else {
        Cond::all()
    };

    // ── CTE: current_data
    let curr_clause = time_frame_to_cond(time_range.start_ts, time_range.end_ts, &ts_col, dialect);
    let mut current_wheres = Cond::all().add(base_where.clone());
    if let Some(cc) = curr_clause {
        current_wheres = current_wheres.add(cc);
    }

    let mut current_cte_select = Query::select();
    for (expr, alias) in &cte_selects {
        if let Some(a) = alias {
            current_cte_select.expr_as(expr.clone(), Alias::new(a));
        } else {
            current_cte_select.expr(expr.clone());
        }
    }

    match table_expr {
        TableExpression::Simple(t) => {
            current_cte_select.from(Alias::new(t));
        }
        TableExpression::SubQuery(q, alias) => {
            current_cte_select.from_subquery(q.clone(), Alias::new(alias));
        }
    }

    current_cte_select.cond_where(current_wheres);
    for group_col in &cte_group_by {
        current_cte_select.add_group_by([group_col.clone()]);
    }

    let mut time_shift_ctes = Vec::new();
    let mut smart_ctes = Vec::new();
    let mut benchmark_cols = Vec::new();

    if let Some(comps) = &ast.comparisons {
        for (i, comp_val) in comps.iter().enumerate() {
            if let Some(comp) = to_analytical_comparison(comp_val) {
                match comp.comp_type {
                    ComparisonType::TimeShiftRelative
                    | ComparisonType::TimeShiftShortcut
                    | ComparisonType::TimeShiftAbsolute => {
                        let tz = "UTC"; // Can be dynamic if needed
                        if let Some(period) = resolve_comparison_period(&comp, time_range, tz) {
                            let prev_clause = time_frame_to_cond(
                                Some(period.prev_start),
                                Some(period.prev_end),
                                &ts_col,
                                dialect,
                            );
                            let mut prev_wheres = Cond::all().add(base_where.clone());
                            if let Some(pc) = prev_clause {
                                prev_wheres = prev_wheres.add(pc);
                            }

                            let mut prev_cte_select = Query::select();
                            for (expr, alias) in &cte_selects {
                                if let Some(a) = alias {
                                    prev_cte_select.expr_as(expr.clone(), Alias::new(a));
                                } else {
                                    prev_cte_select.expr(expr.clone());
                                }
                            }

                            match table_expr {
                                TableExpression::Simple(t) => {
                                    prev_cte_select.from(Alias::new(t));
                                }
                                TableExpression::SubQuery(q, alias) => {
                                    prev_cte_select.from_subquery(q.clone(), Alias::new(alias));
                                }
                            }

                            prev_cte_select.cond_where(prev_wheres);
                            for group_col in &cte_group_by {
                                prev_cte_select.add_group_by([group_col.clone()]);
                            }

                            time_shift_ctes.push((format!("prev_{}", i), prev_cte_select));
                        }
                    }
                    ComparisonType::Smart => {
                        let hist_range = smart_history_window(time_range, None);
                        let hist_clause = time_frame_to_cond(
                            hist_range.start_ts,
                            hist_range.end_ts,
                            &ts_col,
                            dialect,
                        );
                        let mut hist_wheres = Cond::all().add(base_where.clone());
                        if let Some(hc) = hist_clause {
                            hist_wheres = hist_wheres.add(hc);
                        }

                        let mut stat_exprs = Vec::new();
                        for (_, alias) in &metric_exprs {
                            let mut raw_field = "*".to_string();
                            if let Some(metrics) = &ast.metrics {
                                if let Some(m) = metrics.iter().find(|m| {
                                    let name = m.name.as_deref();
                                    let agg = m.aggregation.as_deref().unwrap_or("COUNT");
                                    let attr = m.attribute.as_deref().or(m.field.as_deref());
                                    let computed_alias = if let Some(n) = name {
                                        n.to_string()
                                    } else {
                                        let suffix = attr
                                            .map(col_id_str)
                                            .unwrap_or_else(|| "total".to_string());
                                        format!("{}_{}", agg.to_lowercase(), suffix)
                                    };
                                    computed_alias == *alias
                                }) {
                                    raw_field = m
                                        .attribute
                                        .as_deref()
                                        .or(m.field.as_deref())
                                        .map(col_id_str)
                                        .unwrap_or_else(|| "*".to_string());
                                }
                            }

                            let field_for_stats = if raw_field == "*" {
                                "1.0".to_string()
                            } else {
                                format!("\"{}\"", raw_field)
                            };
                            stat_exprs.push((
                                Expr::cust(format!("AVG({})", field_for_stats)),
                                Some(format!("mean_{}", alias)),
                            ));
                            stat_exprs.push((
                                Expr::cust(dialect.format_stddev_samp(&field_for_stats)),
                                Some(format!("std_{}", alias)),
                            ));
                        }

                        let mut smart_cte_select = Query::select();
                        for (expr, alias) in stat_exprs {
                            if let Some(a) = alias {
                                smart_cte_select.expr_as(expr, Alias::new(&a));
                            } else {
                                smart_cte_select.expr(expr);
                            }
                        }

                        match table_expr {
                            TableExpression::Simple(t) => {
                                smart_cte_select.from(Alias::new(t));
                            }
                            TableExpression::SubQuery(q, alias) => {
                                smart_cte_select.from_subquery(q.clone(), Alias::new(alias));
                            }
                        }

                        smart_cte_select.cond_where(hist_wheres);

                        smart_ctes.push((format!("smart_{}", i), smart_cte_select));
                    }
                    ComparisonType::Benchmark => {
                        let b_val = comp_val.benchmark_value.unwrap_or(0.0);
                        let label = comp_val.label.as_deref().unwrap_or("value");
                        benchmark_cols.push((b_val.to_string(), format!("benchmark_{}", label)));
                    }
                    _ => {}
                }
            }
        }
    }

    // Final projections: SELECT c.bucket AS bucket ...
    let mut final_select = Query::select();

    if ast.output_cast == OutputCast::TIMESERIES {
        if metric_exprs.is_empty() {
            final_select.expr(Expr::cust("c.*"));
        } else {
            final_select.expr_as(
                Expr::cust(format!("c.\"{}\"", bucket_alias)),
                Alias::new(&bucket_alias),
            );
        }

        for (_, alias) in &metric_exprs {
            final_select.expr_as(Expr::cust(format!("c.\"{}\"", alias)), Alias::new(alias));
        }

        for (cte_name, _) in &time_shift_ctes {
            for (_, alias) in &metric_exprs {
                final_select.expr_as(
                    Expr::cust(format!("{}.\"{}\"", cte_name, alias)),
                    Alias::new(format!("{}_{}", cte_name, alias)),
                );
            }
        }

        for (b_val, b_alias) in &benchmark_cols {
            final_select.expr_as(Expr::cust(b_val), Alias::new(b_alias));
        }
    } else {
        if metric_exprs.is_empty() {
            final_select.expr(Expr::cust("c.*"));
        }

        for (_, alias) in &metric_exprs {
            final_select.expr_as(
                Expr::cust(format!("c.\"{}\"", alias)),
                Alias::new(format!("current_{}", alias)),
            );
        }

        for (cte_name, _) in &time_shift_ctes {
            for (_, alias) in &metric_exprs {
                final_select.expr_as(
                    Expr::cust(format!("{}.\"{}\"", cte_name, alias)),
                    Alias::new(format!("{}_{}", cte_name, alias)),
                );
            }
        }

        for (b_val, b_alias) in &benchmark_cols {
            final_select.expr_as(Expr::cust(b_val), Alias::new(b_alias));
        }

        for (smart_name, _) in &smart_ctes {
            for (_, alias) in &metric_exprs {
                let z_expr = format!(
                    "(c.\"{}\" - {}.\"{}\") / NULLIF({}.\"{}\", 0)",
                    alias,
                    smart_name,
                    format!("mean_{}", alias),
                    smart_name,
                    format!("std_{}", alias)
                );
                final_select.expr_as(
                    Expr::cust(&z_expr),
                    Alias::new(format!("z_score_{}", alias)),
                );
            }
        }
    }

    final_select.from_as(Alias::new("current_data"), Alias::new("c"));

    if ast.output_cast == OutputCast::TIMESERIES {
        for (cte_name, _) in &time_shift_ctes {
            final_select.left_join(
                Alias::new(cte_name),
                Expr::cust(&format!(
                    "c.\"{}\" = {}.\"{}\"",
                    bucket_alias, cte_name, bucket_alias
                )),
            );
        }
    } else {
        for (cte_name, _) in &time_shift_ctes {
            final_select.left_join(Alias::new(cte_name), Expr::cust("1=1"));
        }
        for (smart_name, _) in &smart_ctes {
            final_select.left_join(Alias::new(smart_name), Expr::cust("1=1"));
        }
    }

    if let Some(lim) = limit {
        final_select.limit(lim);
    }

    // Assemble the CTE WITH query
    let mut with_query = Query::with();

    with_query.cte(
        CommonTableExpression::new()
            .query(current_cte_select)
            .table_name(Alias::new("current_data"))
            .to_owned(),
    );

    for (cte_name, cte_query) in time_shift_ctes {
        with_query.cte(
            CommonTableExpression::new()
                .query(cte_query)
                .table_name(Alias::new(&cte_name))
                .to_owned(),
        );
    }

    for (smart_name, cte_query) in smart_ctes {
        with_query.cte(
            CommonTableExpression::new()
                .query(cte_query)
                .table_name(Alias::new(&smart_name))
                .to_owned(),
        );
    }

    let sql = with_query
        .query(final_select)
        .to_string(sea_query::PostgresQueryBuilder);

    Ok(sql)
}
