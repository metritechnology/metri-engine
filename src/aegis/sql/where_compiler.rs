//! Where compiler — typed WhereNode AST to sea-query Condition.
//!
//! aegis/sql/where_compiler.rs
//! Compiles a typed WhereNode AST into a sea-query Condition.

use crate::aegis::ast_ir::WhereNode;
use crate::aegis::sql::dialect::SqlDialect;
use crate::aegis::sql::fuzzy::expand_term;
use sea_query::{Alias, Cond, Condition, Expr, SimpleExpr};
use serde_json::Value as JsonValue;

pub fn col_id_str(name: &str) -> String {
    if name == "tenant/id" || name == "entity/tenant-id" {
        return "_tenant".to_string();
    }
    let base = name.split('/').next_back().unwrap_or(name);
    base.replace('-', "_")
}

pub fn json_to_sea_expr(val: &JsonValue) -> SimpleExpr {
    match val {
        JsonValue::Null => Expr::cust("NULL"),
        JsonValue::Bool(b) => Expr::value(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Expr::value(i)
            } else if let Some(f) = n.as_f64() {
                Expr::value(f)
            } else {
                Expr::cust("NULL")
            }
        }
        JsonValue::String(s) => Expr::value(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Expr::value(val.to_string()),
    }
}

pub fn json_to_sea_exprs(val: &JsonValue) -> Vec<SimpleExpr> {
    if let Some(arr) = val.as_array() {
        arr.iter().map(json_to_sea_expr).collect()
    } else {
        vec![json_to_sea_expr(val)]
    }
}

pub fn ref_entity_from_inner(inner_node: &WhereNode) -> Option<String> {
    match inner_node {
        WhereNode::And(children) | WhereNode::Or(children) => {
            for child in children {
                if let Some(ns) = ref_entity_from_inner(child) {
                    return Some(ns);
                }
            }
            None
        }
        WhereNode::Not(child) => ref_entity_from_inner(child),
        WhereNode::Eq(col, _)
        | WhereNode::NotEq(col, _)
        | WhereNode::Gt(col, _)
        | WhereNode::Lt(col, _)
        | WhereNode::Gte(col, _)
        | WhereNode::Lte(col, _)
        | WhereNode::In(col, _)
        | WhereNode::NotIn(col, _)
        | WhereNode::Between(col, _)
        | WhereNode::Matches(col, _)
        | WhereNode::Like(col, _)
        | WhereNode::Contains(col, _)
        | WhereNode::IsNull(col)
        | WhereNode::IsNotNull(col)
        | WhereNode::Fuzzy(col, _) => {
            if let Some(idx) = col.find('/') {
                Some(col[..idx].to_string())
            } else {
                None
            }
        }
        _ => None,
    }
}

pub fn compile_where_node(node: &WhereNode, dialect: &dyn SqlDialect) -> Condition {
    match node {
        WhereNode::Empty => Cond::all(),
        WhereNode::Eq(col, val) => {
            let col_name = col_id_str(col);
            if val.is_null() {
                Cond::all().add(Expr::col(Alias::new(col_name)).is_null())
            } else {
                Cond::all().add(Expr::col(Alias::new(col_name)).eq(json_to_sea_expr(val)))
            }
        }
        WhereNode::NotEq(col, val) => {
            let col_name = col_id_str(col);
            if val.is_null() {
                Cond::all().add(Expr::col(Alias::new(col_name)).is_not_null())
            } else {
                Cond::all().add(Expr::col(Alias::new(col_name)).ne(json_to_sea_expr(val)))
            }
        }
        WhereNode::Gt(col, val) => {
            Cond::all().add(Expr::col(Alias::new(col_id_str(col))).gt(json_to_sea_expr(val)))
        }
        WhereNode::Lt(col, val) => {
            Cond::all().add(Expr::col(Alias::new(col_id_str(col))).lt(json_to_sea_expr(val)))
        }
        WhereNode::Gte(col, val) => {
            Cond::all().add(Expr::col(Alias::new(col_id_str(col))).gte(json_to_sea_expr(val)))
        }
        WhereNode::Lte(col, val) => {
            Cond::all().add(Expr::col(Alias::new(col_id_str(col))).lte(json_to_sea_expr(val)))
        }
        WhereNode::In(col, val) => {
            let exprs = json_to_sea_exprs(val);
            Cond::all().add(Expr::col(Alias::new(col_id_str(col))).is_in(exprs))
        }
        WhereNode::NotIn(col, val) => {
            let exprs = json_to_sea_exprs(val);
            Cond::all().add(Expr::col(Alias::new(col_id_str(col))).is_not_in(exprs))
        }
        WhereNode::Between(col, val) => {
            let col_name = col_id_str(col);
            if let Some(arr) = val.as_array() {
                if arr.len() >= 2 {
                    Cond::all().add(
                        Expr::col(Alias::new(col_name))
                            .between(json_to_sea_expr(&arr[0]), json_to_sea_expr(&arr[1])),
                    )
                } else {
                    Cond::all()
                }
            } else {
                Cond::all()
            }
        }
        WhereNode::Matches(col, pattern) => {
            let col_name = col_id_str(col);
            let quoted_col = format!("\"{}\"", col_name);
            let regex_expr = dialect.format_regexp_like(&quoted_col, pattern);
            Cond::all().add(Expr::cust(&regex_expr))
        }
        WhereNode::Like(col, pattern) => {
            Cond::all().add(Expr::col(Alias::new(col_id_str(col))).like(pattern))
        }
        WhereNode::Contains(col, pattern) => {
            let escaped = pattern
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_");
            let contains_pattern = format!("%{}%", escaped);
            Cond::all().add(Expr::col(Alias::new(col_id_str(col))).like(contains_pattern))
        }
        WhereNode::IsNull(col) => Cond::all().add(Expr::col(Alias::new(col_id_str(col))).is_null()),
        WhereNode::IsNotNull(col) => {
            Cond::all().add(Expr::col(Alias::new(col_id_str(col))).is_not_null())
        }
        WhereNode::And(children) => {
            let mut cond = Cond::all();
            for child in children {
                cond = cond.add(compile_where_node(child, dialect));
            }
            cond
        }
        WhereNode::Or(children) => {
            let mut cond = Cond::any();
            for child in children {
                cond = cond.add(compile_where_node(child, dialect));
            }
            cond
        }
        WhereNode::Not(child) => Cond::all().not().add(compile_where_node(child, dialect)),
        WhereNode::Fuzzy(col, term) => {
            let col_name = col_id_str(col);
            if let Some(expanded) = expand_term(term) {
                let lower_col = format!("LOWER(\"{}\")", col_name);
                if let Some(regex_pat) = expanded.regex_pat {
                    let regex_expr = dialect.format_regexp_like(&lower_col, &regex_pat);
                    Cond::any()
                        .add(Expr::cust(&format!(
                            "{} LIKE '{}'",
                            lower_col,
                            expanded.like_pat.replace('\'', "''")
                        )))
                        .add(Expr::cust(&regex_expr))
                } else {
                    Cond::all().add(Expr::cust(&format!(
                        "{} LIKE '{}'",
                        lower_col,
                        expanded.like_pat.replace('\'', "''")
                    )))
                }
            } else {
                Cond::all()
            }
        }
        WhereNode::Fts(_term) => Cond::all(),
        WhereNode::RefFilter(ref_field, inner) => {
            let ref_col = col_id_str(ref_field);
            if let Some(ref_entity) = ref_entity_from_inner(inner) {
                let inner_cond = compile_where_node(inner, dialect);
                let ref_tbl = ref_entity.replace('-', "_");
                let mut select_stmt = sea_query::Query::select();
                select_stmt
                    .column(Alias::new("id"))
                    .from(Alias::new(&ref_tbl))
                    .cond_where(inner_cond);
                let subquery_sql = select_stmt.to_string(sea_query::PostgresQueryBuilder);
                Cond::all().add(Expr::cust(&format!(
                    "\"{}\" IN ({})",
                    ref_col, subquery_sql
                )))
            } else {
                Cond::all().add(Expr::cust("1=0"))
            }
        }
    }
}
