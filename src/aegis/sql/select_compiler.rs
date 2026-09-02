// aegis/sql/select_compiler.rs
// Compiles AstIr dimensions, metrics, measures and semantic measures into select expressions.

use crate::aegis::ast_ir::{AstIr, Dimension, OutputCast};
use crate::aegis::formula::compiler_olap::OlapFormulaCompiler;
use crate::aegis::formula::functions_registry::FunctionRegistry;
use crate::aegis::sql::dialect::SqlDialect;
use crate::aegis::sql::metric_compiler::compile_metric;
use crate::aegis::sql::where_compiler::col_id_str;
use sea_query::{Alias, Expr, SimpleExpr};

pub fn dim_to_bucket_expr(d: &Dimension, dialect: &dyn SqlDialect) -> String {
    let attr_name = d
        .attribute
        .as_deref()
        .or(d.field.as_deref())
        .unwrap_or("event_ts");
    let cleaned_attr = col_id_str(attr_name);
    let quoted_attr = format!("\"{}\"", cleaned_attr);

    if let Some(interval) = d.interval.as_deref().filter(|s| !s.is_empty()) {
        dialect.format_date_trunc(interval, &quoted_attr)
    } else {
        quoted_attr
    }
}

pub fn build_select_exprs(
    ast: &AstIr,
    dialect: &dyn SqlDialect,
) -> Result<Vec<(SimpleExpr, Option<String>)>, String> {
    let mut select_exprs = Vec::new();
    match ast.output_cast {
        OutputCast::TIMESERIES => {
            if let Some(dims) = &ast.group_by {
                for d in dims {
                    let expr = dim_to_bucket_expr(d, dialect);
                    let alias = d
                        .attribute
                        .as_deref()
                        .or(d.field.as_deref())
                        .unwrap_or("bucket")
                        .to_string();
                    select_exprs.push((Expr::cust(&expr), Some(alias)));
                }
            }
            if let Some(metrics) = &ast.metrics {
                for m in metrics {
                    let (expr, alias) = compile_metric(m, dialect, &ast.entity)?;
                    select_exprs.push((expr, Some(alias)));
                }
            }
        }
        OutputCast::KPI | OutputCast::PIE | OutputCast::BUBBLE => {
            if let Some(dims) = &ast.group_by {
                for d in dims {
                    let expr = dim_to_bucket_expr(d, dialect);
                    select_exprs.push((Expr::cust(&expr), None));
                }
            }
            if let Some(metrics) = &ast.metrics {
                for m in metrics {
                    let (expr, alias) = compile_metric(m, dialect, &ast.entity)?;
                    select_exprs.push((expr, Some(alias)));
                }
            }
            if let Some(measures) = &ast.measures {
                let registry = FunctionRegistry::standard();
                for fe in measures {
                    let sql_expr = OlapFormulaCompiler::compile(&fe.formula, &registry)
                        .map_err(|e| e.detail())?;
                    select_exprs.push((Expr::cust(&sql_expr), Some(fe.name.clone())));
                }
            }
            if let Some(sem_ms) = &ast.semantic_measures {
                for sm in sem_ms {
                    select_exprs.push((
                        Expr::cust(&format!("NULL /* semantic:{} */", sm.metric_key)),
                        Some(sm.metric_key.clone()),
                    ));
                }
            }
        }
        OutputCast::TABLE => {
            let mut has_sel = false;
            let has_dims = ast
                .group_by
                .as_ref()
                .map(|a| !a.is_empty())
                .unwrap_or(false);
            let has_metrics = ast.metrics.as_ref().map(|a| !a.is_empty()).unwrap_or(false);

            if !has_dims && !has_metrics {
                if let Some(sel_arr) = &ast.select {
                    let is_wildcard = sel_arr.len() == 1 && sel_arr[0] == "*";
                    if !is_wildcard {
                        has_sel = true;
                        for s in sel_arr {
                            select_exprs.push((Expr::col(Alias::new(col_id_str(s))).into(), None));
                        }
                    }
                }
            }

            if !has_sel {
                if has_dims || has_metrics {
                    if let Some(dims) = &ast.group_by {
                        for d in dims {
                            let expr = dim_to_bucket_expr(d, dialect);
                            select_exprs.push((Expr::cust(&expr), None));
                        }
                    }
                    if let Some(metrics) = &ast.metrics {
                        for m in metrics {
                            let (expr, alias) = compile_metric(m, dialect, &ast.entity)?;
                            select_exprs.push((expr, Some(alias)));
                        }
                    }
                } else {
                    select_exprs.push((Expr::asterisk().into(), None));
                }
            }
        }
        OutputCast::CsvExport => {
            select_exprs.push((Expr::asterisk().into(), None));
        }
    }
    Ok(select_exprs)
}
