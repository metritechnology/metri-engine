// aegis/sql/compiler.rs — Compilador principal: AST IR → SQL string para Athena via sea-query.
// SRP: orquestar sub-compiladores y aplicar el security gate ZT.

use sea_query::{Condition, Expr, Query, SelectStatement, PostgresQueryBuilder, Func, IntoIden, DynIden};
use serde_json::Value;
use tracing::{debug, error, warn};
use std::str::FromStr;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::aegis::sql::fuzzy::expand_term;
use crate::temporal::core::TimeRange;
use crate::temporal::adapters::to_honey_clause;

// ── Security Gate — ZT Invariant §7 ─────────────────────────────────────────

/// Verifica que el AST IR contiene `tenant_id` en el árbol `:where`.
/// Gate de seguridad §7 — si retorna false, el SQL NO se genera nunca.
/// [PORTED_FROM: (ast-contains-tenant? ast-ir)]
pub fn ast_contains_tenant(ast_ir: &Value) -> bool {
    fn scan(node: &Value) -> bool {
        if let Some(arr) = node.as_array() {
            if arr.is_empty() { return false; }
            if let Some(op_str) = arr[0].as_str() {
                match op_str {
                    "=" => {
                        if let Some(field_str) = arr.get(1).and_then(|v| v.as_str()) {
                            return field_str == "entity/tenant-id" 
                                || field_str == "tenant/id" 
                                || field_str.contains("tenant");
                        }
                    }
                    "and" | "or" => {
                        for child in arr.iter().skip(1) {
                            if scan(child) { return true; }
                        }
                    }
                    "not" => {
                        if let Some(child) = arr.get(1) {
                            return scan(child);
                        }
                    }
                    "ref-filter" => return false, // Apunta a otra entidad
                    _ => {}
                }
            }
        }
        false
    }

    if let Some(where_node) = ast_ir.get("where") {
        scan(where_node)
    } else {
        false
    }
}

// ── Helpers de Columnas y Where ──────────────────────────────────────────────

fn col_id(name: &str) -> DynIden {
    // Convierte "entity/asset_name" -> "asset_name"
    let base = name.split('/').last().unwrap_or(name);
    sea_query::Alias::new(base.to_string()).into_iden()
}

/// Transpila un nodo `where` JSON -> `sea_query::Condition`
/// [PORTED_FROM: (where-node->honey node ctx)]
fn compile_where_node(node: &Value) -> Result<Condition, DomainError> {
    if node.is_null() {
        return Err(DomainError::aegis(ErrorCode::Aeg001, "Null WHERE node"));
    }

    let arr = node.as_array().ok_or_else(|| {
        DomainError::aegis(ErrorCode::Aeg001, "WHERE node must be an array")
    })?;

    if arr.is_empty() {
        return Ok(Condition::all()); // true
    }

    let op = arr[0].as_str().unwrap_or("");
    let mut cond = Condition::all();

    match op {
        "=" => {
            let col = arr[1].as_str().unwrap_or("");
            let val = &arr[2];
            if val.is_null() {
                cond = cond.add(Expr::col(col_id(col)).is_null());
            } else if let Some(s) = val.as_str() {
                cond = cond.add(Expr::col(col_id(col)).eq(s));
            } else if let Some(n) = val.as_f64() {
                cond = cond.add(Expr::col(col_id(col)).eq(n));
            } else if let Some(b) = val.as_bool() {
                cond = cond.add(Expr::col(col_id(col)).eq(b));
            }
        }
        "not=" => {
            let col = arr[1].as_str().unwrap_or("");
            let val = &arr[2];
            if val.is_null() {
                cond = cond.add(Expr::col(col_id(col)).is_not_null());
            } else if let Some(s) = val.as_str() {
                cond = cond.add(Expr::col(col_id(col)).ne(s));
            } else if let Some(n) = val.as_f64() {
                cond = cond.add(Expr::col(col_id(col)).ne(n));
            }
        }
        ">" => {
            let col = arr[1].as_str().unwrap_or("");
            if let Some(n) = arr[2].as_f64() {
                cond = cond.add(Expr::col(col_id(col)).gt(n));
            } else if let Some(s) = arr[2].as_str() {
                cond = cond.add(Expr::col(col_id(col)).gt(s));
            }
        }
        "<" => {
            let col = arr[1].as_str().unwrap_or("");
            if let Some(n) = arr[2].as_f64() {
                cond = cond.add(Expr::col(col_id(col)).lt(n));
            } else if let Some(s) = arr[2].as_str() {
                cond = cond.add(Expr::col(col_id(col)).lt(s));
            }
        }
        ">=" => {
            let col = arr[1].as_str().unwrap_or("");
            if let Some(n) = arr[2].as_f64() {
                cond = cond.add(Expr::col(col_id(col)).gte(n));
            } else if let Some(s) = arr[2].as_str() {
                cond = cond.add(Expr::col(col_id(col)).gte(s));
            }
        }
        "<=" => {
            let col = arr[1].as_str().unwrap_or("");
            if let Some(n) = arr[2].as_f64() {
                cond = cond.add(Expr::col(col_id(col)).lte(n));
            } else if let Some(s) = arr[2].as_str() {
                cond = cond.add(Expr::col(col_id(col)).lte(s));
            }
        }
        "in" => {
            let col = arr[1].as_str().unwrap_or("");
            if let Some(vals) = arr[2].as_array() {
                let mut str_vals = Vec::new();
                for v in vals {
                    if let Some(s) = v.as_str() {
                        str_vals.push(s);
                    }
                }
                cond = cond.add(Expr::col(col_id(col)).is_in(str_vals));
            }
        }
        "fuzzy" => {
            let col = arr[1].as_str().unwrap_or("");
            let term = arr[2].as_str().unwrap_or("");
            if let Some(expanded) = expand_term(term) {
                if let Some(regex_pat) = expanded.regex_pat {
                    // LIKE OR REGEXP_LIKE
                    let mut or_cond = Condition::any();
                    or_cond = or_cond.add(
                        Expr::expr(Func::lower(Expr::col(col_id(col))))
                            .like(expanded.like_pat)
                    );
                    // sea_query doesn't natively support REGEXP_LIKE in standard way across all DBs, 
                    // but we can inject a custom function or use Postgres' ~ operator as a placeholder
                    // For Athena, we really need custom SQL.
                    let raw_expr = format!("REGEXP_LIKE(LOWER({}), '{}')", col, regex_pat.replace("'", "''"));
                    or_cond = or_cond.add(Expr::cust(raw_expr));
                    cond = cond.add(or_cond);
                } else {
                    // solo LIKE
                    cond = cond.add(
                        Expr::expr(Func::lower(Expr::col(col_id(col))))
                            .like(expanded.like_pat)
                    );
                }
            } else {
                cond = cond.add(Expr::cust("1=1"));
            }
        }
        "and" => {
            let mut and_cond = Condition::all();
            for child in arr.iter().skip(1) {
                and_cond = and_cond.add(compile_where_node(child)?);
            }
            cond = cond.add(and_cond);
        }
        "or" => {
            let mut or_cond = Condition::any();
            for child in arr.iter().skip(1) {
                or_cond = or_cond.add(compile_where_node(child)?);
            }
            cond = cond.add(or_cond);
        }
        _ => {
            warn!("[Aegis SQL] Operador no soportado parcial: {}", op);
            cond = cond.add(Expr::cust("1=1"));
        }
    }

    Ok(cond)
}

// ── Entry point público ───────────────────────────────────────────────────────

pub struct CompilerResult {
    pub sql: String,
    pub database: String,
    pub entity: String,
    pub output_cast: String,
}

/// Transpila AST IR OLAP -> SQL para Athena.
/// INVARIANTE: Nunca genera SQL sin WHERE tenant_id.
/// [PORTED_FROM: (compile-athena-sql ast-ir database tenant-id)]
pub fn compile_athena_sql(
    ast_ir: &Value,
    database: &str,
    tenant_id: &str,
) -> Result<CompilerResult, DomainError> {
    if !ast_contains_tenant(ast_ir) {
        error!("[Aegis Compiler] AST IR sin tenant isolation — bloqueado!");
        return Err(DomainError::aegis(
            ErrorCode::Aeg001,
            format!("Missing tenant isolation for tenant: {tenant_id}")
        ));
    }

    let entity = ast_ir.get("entity").and_then(|v| v.as_str()).unwrap_or("events");
    let output_cast = ast_ir.get("output_cast").and_then(|v| v.as_str()).unwrap_or("KPI");
    
    let table = sea_query::Alias::new(format!("{database}.{entity}"));

    let mut query = Query::select();
    query.from(table);

    // Limit
    match output_cast {
        "CSV_EXPORT" => {}
        "TABLE" => { query.limit(10000); }
        _ => { 
            let lim = ast_ir.get("limit").and_then(|v| v.as_u64()).unwrap_or(1000);
            query.limit(lim);
        }
    }

    // Select
    if let Some(select_arr) = ast_ir.get("select").and_then(|v| v.as_array()) {
        for s in select_arr {
            if let Some(col_name) = s.as_str() {
                if col_name == "*" {
                    query.expr(Expr::asterisk());
                } else {
                    query.column(col_id(col_name));
                }
            }
        }
    } else {
        query.expr(Expr::asterisk());
    }

    // Where
    if let Some(where_node) = ast_ir.get("where") {
        let condition = compile_where_node(where_node)?;
        query.cond_where(condition);
    }

    // Convert to String using PostgresBuilder as a base (it's close to ANSI/Presto)
    let sql = query.to_string(PostgresQueryBuilder);

    debug!("[Aegis Compiler] SQL compilado | entity: {entity} | output_cast: {output_cast}");

    Ok(CompilerResult {
        sql,
        database: database.to_string(),
        entity: entity.to_string(),
        output_cast: output_cast.to_string(),
    })
}

/// Variante con filtro temporal — inyecta el WHERE de TimeRange en el SQL de Athena.
///
/// La cláusula se genera via `temporal::adapters::to_honey_clause`
/// y se convierte a una `Condition` sea_query raw.
///
/// El campo de timestamp en Athena es `created_at` por defecto;
/// puede sobreescribirse via `ts_col`.
pub fn compile_athena_sql_with_time_frame(
    ast_ir: &Value,
    database: &str,
    tenant_id: &str,
    time_range: &TimeRange,
    ts_col: Option<&str>,
) -> Result<CompilerResult, DomainError> {
    let mut result = compile_athena_sql(ast_ir, database, tenant_id)?;

    // Si hay restricción temporal, inyectar la cláusula BETWEEN en el SQL.
    let col = ts_col.unwrap_or("created_at");
    let honey_val = to_honey_clause(time_range, col);

    if honey_val.is_null() {
        // ALL_TIME — sin cláusula adicional
        return Ok(result);
    }

    // Construir el fragmento SQL temporal raw añadido con AND al final del WHERE.
    // Formato honey_val: ["and", [">=", col, start], ["<=", col, end]]
    //                 o  [">=", col, val]  / ["<=", col, val]
    let ts_clause = build_ts_where_fragment(&honey_val, col);
    if !ts_clause.is_empty() {
        // Inyectar en el SQL generado: si ya tiene WHERE → AND, si no → WHERE
        if result.sql.to_uppercase().contains(" WHERE ") {
            result.sql = format!("{} AND {}", result.sql, ts_clause);
        } else {
            result.sql = format!("{} WHERE {}", result.sql, ts_clause);
        }
        debug!("[Aegis Compiler] Cláusula temporal inyectada: {}", ts_clause);
    }

    Ok(result)
}

/// Genera un fragmento SQL WHERE a partir de un nodo honey_val temporal.
fn build_ts_where_fragment(honey_val: &Value, col: &str) -> String {
    if honey_val.is_null() { return String::new(); }
    let arr = match honey_val.as_array() {
        Some(a) => a,
        None    => return String::new(),
    };
    if arr.is_empty() { return String::new(); }

    match arr[0].as_str().unwrap_or("") {
        "and" => {
            // ["and", [">=", col, start], ["<=", col, end]]
            let left  = build_ts_where_fragment(arr.get(1).unwrap_or(&Value::Null), col);
            let right = build_ts_where_fragment(arr.get(2).unwrap_or(&Value::Null), col);
            if left.is_empty() || right.is_empty() {
                format!("{}{}", left, right)
            } else {
                format!("{} AND {}", left, right)
            }
        }
        ">=" => {
            let val = arr.get(2).and_then(|v| v.as_i64()).unwrap_or(0);
            format!("from_unixtime({}) >= from_unixtime({})", col, val)
        }
        "<=" => {
            let val = arr.get(2).and_then(|v| v.as_i64()).unwrap_or(0);
            format!("from_unixtime({}) <= from_unixtime({})", col, val)
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_ast_contains_tenant() {
        let ast1 = json!({
            "where": ["=", "tenant_id", "t-123"]
        });
        assert!(ast_contains_tenant(&ast1));

        let ast2 = json!({
            "where": ["and", [">", "value", 10], ["=", "tenant/id", "t-123"]]
        });
        assert!(ast_contains_tenant(&ast2));

        let ast_fail = json!({
            "where": ["=", "status", "active"]
        });
        assert!(!ast_contains_tenant(&ast_fail));
    }

    #[test]
    fn test_compile_athena_sql() {
        let ast = json!({
            "entity": "assets",
            "output_cast": "TABLE",
            "select": ["id", "name", "status"],
            "where": ["and", 
                ["=", "tenant_id", "t-123"],
                ["fuzzy", "name", "Pump"]
            ]
        });

        let res = compile_athena_sql(&ast, "metrics_db", "t-123").unwrap();
        // Check that it limits to 10000 for TABLE
        assert!(res.sql.contains("LIMIT 10000"), "SQL debe tener LIMIT 10000, got: {}", res.sql);
        // Check that tenant_id column is in the WHERE (sea_query puede usar 't-123' o E't-123')
        assert!(res.sql.contains("tenant_id"), "SQL debe filtrar por tenant_id, got: {}", res.sql);
        assert!(res.sql.contains("t-123"), "SQL debe incluir el valor del tenant, got: {}", res.sql);
        // Check fuzzy regex like
        assert!(res.sql.contains("REGEXP_LIKE"), "SQL debe tener REGEXP_LIKE, got: {}", res.sql);
    }

    #[test]
    fn test_security_gate_blocks() {
        let ast = json!({
            "entity": "assets",
            "where": ["=", "status", "active"]
        });
        let res = compile_athena_sql(&ast, "metrics_db", "t-123");
        assert!(res.is_err());
    }
}
