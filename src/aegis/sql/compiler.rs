// aegis/sql/compiler.rs
// Principal OLAP AST IR to SQL compiler. Orchestrates AST parsing, validation, and sea-query rendering.

use crate::aegis::ast_ir::{AstIr, OutputCast};
use crate::aegis::sql::where_compiler::{col_id_str, compile_where_node};
use crate::aegis::sql::select_compiler::build_select_exprs;
use crate::aegis::sql::cte_compiler::{build_comparison_cte_query, resolve_ts_col, TableExpression, time_frame_to_cond};
use crate::aegis::sql::virtual_table::get_strategy;
use crate::aegis::sql::dialect::{SqlDialect, AthenaDialect};
use crate::temporal::core::TimeRange;
use crate::domain::errors::{DomainError, ErrorCode};
use serde_json::Value;
use tracing::{debug, error};
use sea_query::{Alias, Cond, Expr};

pub use crate::aegis::sql::security::ast_contains_tenant;

pub struct HierarchyParts {
    pub extra_where: Option<String>,
    pub extra_select: Option<(String, String)>,
}

pub struct CompilerResult {
    pub sql: String,
    pub database: String,
    pub entity: String,
    pub output_cast: String,
}

pub fn compile_athena_sql(
    ast_ir: &Value,
    database: &str,
    tenant_id: &str,
) -> Result<CompilerResult, DomainError> {
    let time_range = TimeRange { start_ts: None, end_ts: None };
    compile_athena_sql_with_time_frame(ast_ir, database, tenant_id, &time_range, None)
}

use crate::aegis::sql::registry::{get_hybrid_config, HybridEntityConfig};

pub fn compile_athena_sql_with_time_frame(
    ast_ir: &Value,
    database: &str,
    tenant_id: &str,
    time_range: &TimeRange,
    ts_col: Option<&str>,
) -> Result<CompilerResult, DomainError> {
    let entity = ast_ir.get("entity").and_then(|v| v.as_str()).unwrap_or("");
    if let Some(config) = get_hybrid_config(entity) {
        return compile_hybrid_sql(ast_ir, database, tenant_id, time_range, ts_col, &config);
    }

    let dialect = AthenaDialect;
    compile_sql_with_dialect(ast_ir, database, tenant_id, time_range, &dialect)
}

fn is_structural_field(f: &str, config: &HybridEntityConfig) -> bool {
    match f {
        "id" | "tenant_id" | "tenant/id" | "_tenant" | "created_at" | "updated_at" | "createdAt" | "updatedAt" |
        "timestamp" | "ingested_at" | "metadata" => return true,
        _ => {}
    }
    config.structural_fields.iter().any(|field| field == f)
}

pub fn compile_hybrid_meter_reading_sql(
    ast_ir: &Value,
    database: &str,
    tenant_id: &str,
    time_range: &TimeRange,
    ts_col: Option<&str>,
) -> Result<CompilerResult, DomainError> {
    let config = get_hybrid_config("meter_reading").ok_or_else(|| {
        DomainError::aegis(ErrorCode::Aeg001, "Missing meter_reading hybrid config".to_string())
    })?;
    compile_hybrid_sql(ast_ir, database, tenant_id, time_range, ts_col, &config)
}

pub fn compile_hybrid_sql(
    ast_ir: &Value,
    database: &str,
    tenant_id: &str,
    time_range: &TimeRange,
    _ts_col: Option<&str>,
    config: &HybridEntityConfig,
) -> Result<CompilerResult, DomainError> {
    let mut val = ast_ir.clone();
    let mut metric_name_to_filter = None;
    
    if let Some(metrics) = val.get_mut("metrics").and_then(|m| m.as_array_mut()) {
        for m in metrics {
            let mut original_field = None;
            if let Some(f) = m.get("field").and_then(|v| v.as_str()) {
                if !is_structural_field(f, config) {
                    original_field = Some(f.to_string());
                }
            } else if let Some(a) = m.get("attribute").and_then(|v| v.as_str()) {
                if !is_structural_field(a, config) {
                    original_field = Some(a.to_string());
                }
            }
            
            if let Some(name) = original_field {
                metric_name_to_filter = Some(name.clone());
                if m.get("field").is_some() {
                    m["field"] = serde_json::json!(config.value_column);
                }
                if m.get("attribute").is_some() {
                    m["attribute"] = serde_json::json!(config.value_column);
                }
                if m.get("name").is_none() {
                    m["name"] = serde_json::json!(name);
                }
            }
        }
    }
    
    if let Some(metric_name) = metric_name_to_filter {
        let filter_node = serde_json::json!(["=", config.attribute_column, metric_name]);
        
        if let Some(w) = val.get_mut("where") {
            if let Some(arr) = w.as_array_mut() {
                if arr.first().and_then(|v| v.as_str()) == Some("and") {
                    arr.push(filter_node);
                } else {
                    let old_where = w.clone();
                    *w = serde_json::json!(["and", old_where, filter_node]);
                }
            } else {
                let old_where = w.clone();
                *w = serde_json::json!(["and", old_where, filter_node]);
            }
        } else {
            val["where"] = filter_node;
        }
    }

    let dialect = AthenaDialect;
    compile_sql_with_dialect(&val, database, tenant_id, time_range, &dialect)
}

pub fn compile_sql_with_dialect(
    ast_ir: &Value,
    database: &str,
    tenant_id: &str,
    time_range: &TimeRange,
    dialect: &dyn SqlDialect,
) -> Result<CompilerResult, DomainError> {
    if !ast_contains_tenant(ast_ir) {
        error!("[Aegis Compiler] AST IR sin tenant isolation — bloqueado!");
        return Err(DomainError::aegis(
            ErrorCode::Aeg001,
            format!("Missing tenant isolation for tenant: {tenant_id}")
        ));
    }

    let ast = AstIr::from_value(ast_ir).map_err(|e| DomainError::aegis(ErrorCode::Aeg001, e))?;

    let entity = &ast.entity;
    let output_cast_str = match ast.output_cast {
        OutputCast::KPI => "KPI",
        OutputCast::TIMESERIES => "TIMESERIES",
        OutputCast::TABLE => "TABLE",
        OutputCast::PIE => "PIE",
        OutputCast::BUBBLE => "BUBBLE",
        OutputCast::CsvExport => "CSV_EXPORT",
    };

    let tbl_str = entity.replace('-', "_");
    
    let has_cte_comps = if let Some(comps) = &ast.comparisons {
        comps.iter().any(|c| {
            matches!(c.comp_type.as_str(), "TIME_SHIFT_RELATIVE" | "TIME_SHIFT_SHORTCUT" | "TIME_SHIFT_ABSOLUTE" | "SMART")
        })
    } else {
        false
    };
    
    let limit = match ast.output_cast {
        OutputCast::CsvExport => None,
        OutputCast::TABLE => Some(10000),
        _ => {
            let lim = ast.limit.unwrap_or(1000);
            Some(if lim == 0 { 1000 } else { lim })
        }
    };

    let table_expr = if let Some(strategy) = get_strategy(entity) {
        let q = strategy.build_table_query(database, tenant_id, time_range, dialect);
        TableExpression::SubQuery(q, entity.to_string())
    } else {
        TableExpression::Simple(tbl_str.clone())
    };

    let sql = if has_cte_comps {
        build_comparison_cte_query(
            &ast,
            &table_expr,
            time_range,
            dialect,
            limit,
        ).map_err(|e| DomainError::aegis(ErrorCode::Aeg001, e))?
    } else {
        build_base_query(
            &ast,
            &table_expr,
            tenant_id,
            time_range,
            dialect,
            limit,
        ).map_err(|e| DomainError::aegis(ErrorCode::Aeg001, e))?
    };

    debug!("[Aegis Compiler] SQL compilado con dialecto | entity: {entity} | output_cast: {output_cast_str}");

    Ok(CompilerResult {
        sql,
        database: database.to_string(),
        entity: entity.to_string(),
        output_cast: output_cast_str.to_string(),
    })
}

pub fn build_base_query(
    ast: &AstIr,
    table_expr: &TableExpression,
    tenant_id: &str,
    time_range: &TimeRange,
    dialect: &dyn SqlDialect,
    limit: Option<u64>,
) -> Result<String, String> {
    let mut select_stmt = sea_query::Query::select();
    
    let mut sel_exprs = build_select_exprs(ast, dialect)?;
    
    let base_where = if let Some(w) = &ast.where_clause {
        compile_where_node(w, dialect)
    } else {
        Cond::all()
    };
    
    let mut wheres = Cond::all().add(base_where);
    
    let ts_col = resolve_ts_col(ast);
    let time_clause = time_frame_to_cond(time_range.start_ts, time_range.end_ts, &ts_col, dialect);
    if let Some(tc) = time_clause {
        wheres = wheres.add(tc);
    }
    
    let mut tbl_alias_option = None;
    if let Some(hierarchy) = &ast.hierarchy {
        let parent_field_raw = hierarchy.parent_field.as_deref().unwrap_or("parent_id");
        let pf = col_id_str(parent_field_raw);
        
        if let Some(cid) = &hierarchy.current_node_id {
            if !cid.is_null() {
                let val_expr = crate::aegis::sql::where_compiler::json_to_sea_expr(cid);
                wheres = wheres.add(Expr::col(Alias::new(pf.clone())).eq(val_expr));
            }
        }
        
        let hc = hierarchy.inject_has_children.unwrap_or(false);
        if hc {
            let tbl_str = match table_expr {
                TableExpression::Simple(t) => t.clone(),
                TableExpression::SubQuery(_, alias) => alias.clone(),
            };
            let exists_sql = dialect.format_exists(&tbl_str, tenant_id, &pf);
            sel_exprs.push((Expr::cust(&exists_sql), Some("has_children".to_string())));
            tbl_alias_option = Some("t");
        }
    }
    
    for (expr, alias) in sel_exprs {
        if let Some(a) = alias {
            select_stmt.expr_as(expr, Alias::new(&a));
        } else {
            select_stmt.expr(expr);
        }
    }
    
    match table_expr {
        TableExpression::Simple(t) => {
            if let Some(alias) = tbl_alias_option {
                select_stmt.from_as(Alias::new(t), Alias::new(alias));
            } else {
                select_stmt.from(Alias::new(t));
            }
        }
        TableExpression::SubQuery(q, alias) => {
            let actual_alias = tbl_alias_option.unwrap_or(alias);
            select_stmt.from_subquery(q.clone(), Alias::new(actual_alias));
        }
    }
    
    select_stmt.cond_where(wheres);
    
    if let Some(dims) = &ast.group_by {
        if !dims.is_empty() {
            match ast.output_cast {
                OutputCast::TIMESERIES => {
                    for d in dims {
                        let bucket_expr = crate::aegis::sql::select_compiler::dim_to_bucket_expr(d, dialect);
                        select_stmt.add_group_by([Expr::cust(&bucket_expr)]);
                    }
                }
                OutputCast::PIE | OutputCast::BUBBLE | OutputCast::KPI => {
                    for d in dims {
                        let attr = d.attribute.as_deref()
                            .or(d.field.as_deref())
                            .unwrap_or("event_ts");
                        select_stmt.group_by_col(Alias::new(col_id_str(attr)));
                    }
                }
                OutputCast::TABLE => {
                    let has_metrics = ast.metrics.as_ref().map(|a| !a.is_empty()).unwrap_or(false);
                    if has_metrics {
                        for d in dims {
                            let attr = d.attribute.as_deref()
                                .or(d.field.as_deref())
                                .unwrap_or("event_ts");
                            select_stmt.group_by_col(Alias::new(col_id_str(attr)));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    
    if ast.output_cast == OutputCast::TIMESERIES {
        select_stmt.order_by_expr(Expr::cust("1"), sea_query::Order::Asc);
    }
    if let Some(order_by_arr) = &ast.order_by {
        for item in order_by_arr {
            if let Some(field) = item.field.as_deref().or(item.attribute.as_deref()) {
                let col_name = col_id_str(field);
                let dir = if item.descending.unwrap_or(false) {
                    sea_query::Order::Desc
                } else {
                    sea_query::Order::Asc
                };
                select_stmt.order_by(Alias::new(&col_name), dir);
            }
        }
    }
    
    if let Some(lim) = limit {
        select_stmt.limit(lim);
    }
    
    let sql = select_stmt.to_string(sea_query::PostgresQueryBuilder);
    
    Ok(sql)
}
