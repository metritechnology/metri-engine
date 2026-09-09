//! Local query engine — parses the SQL compiled by Aegis.
//!
//! infrastructure/local_s3_engine/sql_parse.rs — Fase 5 (commit B)
//!
//! Responsabilidad 1 del motor local: parsear el SQL que compila Aegis.
//! MOVIMIENTO PURO desde `local_s3_query_engine.rs` — ni una línea de
//! semántica cambiada; el corpus de caracterización viaja con el módulo.

use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) struct ProjectedColumn {
    pub expr: String,
    pub alias: String,
    pub is_aggregate: bool,
    pub agg_fn: Option<String>,
    pub agg_field: Option<String>,
}

/// Extrae el nombre de la entidad del SQL compilado por Aegis.
pub(crate) fn extract_entity_from_sql(sql: &str) -> Option<String> {
    let lower = sql.to_lowercase();

    for keyword in &["from "] {
        let mut search_pos = 0;
        while let Some(idx) = lower[search_pos..].find(keyword) {
            let abs_idx = search_pos + idx + keyword.len();
            let rest = &lower[abs_idx..];
            let table_end = rest
                .find(|c: char| c.is_whitespace() || c == ')' || c == ';')
                .unwrap_or(rest.len());
            let table_ref = &rest[..table_end];

            let table_name = if let Some(dot_idx) = table_ref.rfind('.') {
                &table_ref[dot_idx + 1..]
            } else {
                table_ref
            };

            let entity = table_name
                .trim_end_matches("_raw")
                .trim_end_matches("_rollup")
                .to_string();

            if !entity.is_empty()
                && entity != "select"
                && entity != "where"
                && entity != "("
                && !entity.starts_with('(')
            {
                return Some(entity);
            }

            search_pos = abs_idx;
        }
    }

    None
}

/// Extrae columnas de proyección del select con sus alias y metadatos de agregación.
pub(crate) fn extract_projected_columns(sql: &str) -> Vec<ProjectedColumn> {
    let lower = sql.to_lowercase();

    let select_idx = match lower.find("select ") {
        Some(idx) => idx,
        None => return Vec::new(),
    };

    let after_select = &sql[select_idx + 7..];
    let mut paren_depth = 0;
    let mut from_offset = None;
    for (i, c) in after_select.char_indices() {
        if c == '(' {
            paren_depth += 1;
        } else if c == ')' {
            paren_depth -= 1;
        } else if paren_depth == 0 {
            let remaining = &after_select[i..].to_lowercase();
            if remaining.starts_with("from ") || remaining.starts_with("from\n") {
                from_offset = Some(i);
                break;
            }
        }
    }

    let from_pos = match from_offset {
        Some(o) => o,
        None => return Vec::new(),
    };

    let select_part = after_select[..from_pos].trim();

    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0;

    for c in select_part.chars() {
        match c {
            '(' => {
                depth += 1;
                current.push(c);
            }
            ')' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    parts.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        parts.push(trimmed.to_string());
    }

    let mut projected = Vec::new();
    for part in parts {
        let alias = resolve_column_alias(&part);
        let expr = extract_expr_before_alias(&part);

        let agg_fns = vec!["count", "sum", "avg", "min", "max"];
        let mut is_aggregate = false;
        let mut agg_fn = None;
        let mut agg_field = None;
        for f in agg_fns {
            if let Some(arg) = extract_arg(&expr, f) {
                is_aggregate = true;
                agg_fn = Some(f.to_uppercase());
                let clean_arg = if arg == "*" {
                    "*".to_string()
                } else {
                    let mut col = String::new();
                    for c in arg.chars().rev() {
                        if c.is_alphanumeric() || c == '_' {
                            col.push(c);
                        } else if !col.is_empty() {
                            break;
                        }
                    }
                    col.chars().rev().collect::<String>()
                };
                agg_field = Some(clean_arg);
            }
        }

        projected.push(ProjectedColumn {
            expr,
            alias,
            is_aggregate,
            agg_fn,
            agg_field,
        });
    }

    projected
}

fn extract_expr_before_alias(part: &str) -> String {
    let lower = part.to_lowercase();
    if let Some(as_idx) = lower.rfind(" as ") {
        part[..as_idx].trim().to_string()
    } else {
        let trimmed = part.trim();
        if trimmed.contains(' ') {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if let Some(last) = parts.last() {
                let alias = last.replace('"', "").replace('\'', "");
                if !alias.contains(')') && !alias.contains('(') {
                    let alias_len = last.len();
                    return trimmed[..trimmed.len() - alias_len].trim().to_string();
                }
            }
        }
        trimmed.to_string()
    }
}

fn extract_arg(expr: &str, func_name: &str) -> Option<String> {
    let lower_expr = expr.to_lowercase();
    let func_with_paren = format!("{}(", func_name.to_lowercase());
    if let Some(idx) = lower_expr.find(&func_with_paren) {
        let start = idx + func_with_paren.len();
        let rest = &expr[start..];
        let mut depth = 1;
        let mut end = None;
        for (i, c) in rest.char_indices() {
            if c == '(' {
                depth += 1;
            } else if c == ')' {
                depth -= 1;
                if depth == 0 {
                    end = Some(i);
                    break;
                }
            }
        }
        if depth == 0 {
            if let Some(e_idx) = end {
                return Some(rest[..e_idx].trim().to_string());
            }
        }
    }
    None
}

/// Resuelve el alias de una columna SQL.
fn resolve_column_alias(expr: &str) -> String {
    let lower = expr.to_lowercase();

    if let Some(as_idx) = lower.rfind(" as ") {
        return expr[as_idx + 4..].trim().replace('"', "").replace('\'', "");
    }

    let trimmed = expr.trim();
    if trimmed.contains(' ') {
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if let Some(last) = parts.last() {
            let alias = last.replace('"', "").replace('\'', "");
            if !alias.contains(')') && !alias.contains('(') {
                return alias;
            }
        }
    }

    trimmed.to_string()
}

/// Extrae el LIMIT del SQL.
pub(crate) fn extract_limit_from_sql(sql: &str) -> Option<u32> {
    let lower = sql.to_lowercase();
    if let Some(idx) = lower.rfind("limit ") {
        let rest = &lower[idx + 6..];
        let num_str: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        num_str.parse().ok()
    } else {
        None
    }
}

/// Extrae los filtros de igualdad `col = val` y de lista `col IN (val1, val2, ...)` del SQL.
pub(crate) fn extract_filters_from_sql(sql: &str) -> HashMap<String, Value> {
    let mut filters = HashMap::new();
    let lower = sql.to_lowercase();

    // 1. Extraer filtros de igualdad col = val
    let mut search_pos = 0;
    while let Some(idx) = lower[search_pos..].find(" = ") {
        let abs_equal_idx = search_pos + idx;

        let left_part = &sql[search_pos..abs_equal_idx];
        let col_name = extract_left_column(left_part);

        let right_part = &sql[abs_equal_idx + 3..];
        if let Some((val, val_len)) = extract_right_value(right_part) {
            if let Some(col) = col_name {
                filters.insert(col, val);
            }
            search_pos = abs_equal_idx + 3 + val_len;
        } else {
            search_pos = abs_equal_idx + 3;
        }
    }

    // 2. Extraer filtros col IN (val1, val2, ...)
    let mut search_pos_in = 0;
    while let Some(idx) = lower[search_pos_in..].find(" in ") {
        let abs_in_idx = search_pos_in + idx;

        let left_part = &sql[search_pos_in..abs_in_idx];
        let col_name = extract_left_column(left_part);

        let right_part = &sql[abs_in_idx + 4..].trim_start();
        if right_part.starts_with('(') {
            if let Some(close_paren_idx) = right_part.find(')') {
                let list_content = &right_part[1..close_paren_idx];
                let mut list_vals = Vec::new();
                for item in list_content.split(',') {
                    let trimmed_item = item.trim();
                    if trimmed_item.starts_with('\'')
                        && trimmed_item.ends_with('\'')
                        && trimmed_item.len() >= 2
                    {
                        let inner_val = &trimmed_item[1..trimmed_item.len() - 1];
                        list_vals.push(Value::String(inner_val.to_string()));
                    } else if !trimmed_item.is_empty() {
                        if let Some((val, _)) = extract_right_value(trimmed_item) {
                            list_vals.push(val);
                        }
                    }
                }
                if let Some(col) = col_name {
                    if col.to_lowercase() != "not" {
                        filters.insert(col, Value::Array(list_vals));
                    }
                }
                let offset = sql[abs_in_idx + 4..].len() - right_part.len();
                search_pos_in = abs_in_idx + 4 + offset + close_paren_idx + 1;
                continue;
            }
        }
        search_pos_in = abs_in_idx + 4;
    }

    filters
}

fn extract_left_column(left_part: &str) -> Option<String> {
    let trimmed = left_part.trim();
    let mut col = String::new();
    for c in trimmed.chars().rev() {
        if c.is_alphanumeric() || c == '_' || c == '"' || c == '/' || c == '.' || c == '-' {
            col.push(c);
        } else if !col.is_empty() {
            break;
        }
    }
    if col.is_empty() {
        None
    } else {
        let cleaned = col.chars().rev().collect::<String>().replace('"', "");
        if let Some(dot_idx) = cleaned.rfind('.') {
            Some(cleaned[dot_idx + 1..].to_string())
        } else {
            Some(cleaned)
        }
    }
}

fn extract_right_value(right_part: &str) -> Option<(Value, usize)> {
    let trimmed = right_part.trim_start();
    let offset = right_part.len() - trimmed.len();

    if trimmed.starts_with('\'') {
        let rest = &trimmed[1..];
        if let Some(end_idx) = rest.find('\'') {
            let val_str = &rest[..end_idx];
            return Some((Value::String(val_str.to_string()), offset + 1 + end_idx + 1));
        }
    }
    let val_str: String = trimmed
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '.' || *c == '-')
        .collect();
    let val_len = val_str.len();
    if !val_str.is_empty() {
        let lower_val = val_str.to_lowercase();
        if lower_val == "true" {
            return Some((Value::Bool(true), offset + val_len));
        } else if lower_val == "false" {
            return Some((Value::Bool(false), offset + val_len));
        } else if lower_val == "null" {
            return Some((Value::Null, offset + val_len));
        } else if let Ok(n) = val_str.parse::<i64>() {
            return Some((Value::Number(serde_json::Number::from(n)), offset + val_len));
        } else if let Ok(f) = val_str.parse::<f64>() {
            if let Some(num) = serde_json::Number::from_f64(f) {
                return Some((Value::Number(num), offset + val_len));
            }
        }
    }
    None
}

/// Extrae el rango de tiempo del SQL si existe en la forma >= y <=.
pub(crate) fn extract_timestamp_range_from_sql(sql: &str) -> (Option<i64>, Option<i64>) {
    let lower = sql.to_lowercase();
    let mut start_ts = None;
    let mut end_ts = None;

    // Helper to extract nested numeric or string value
    fn extract_val(s: &str) -> Option<Value> {
        if let Some(start_q) = s.find('\'') {
            let rest = &s[start_q + 1..];
            if let Some(end_q) = rest.find('\'') {
                return Some(Value::String(rest[..end_q].to_string()));
            }
        }
        let chars: Vec<char> = s.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i].is_ascii_digit() {
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                let num_str: String = chars[start..i].iter().collect();
                if let Ok(num) = num_str.parse::<i64>() {
                    return Some(Value::Number(serde_json::Number::from(num)));
                }
            }
            i += 1;
        }
        None
    }

    // Parse >= for start_ts
    let mut search_pos = 0;
    while let Some(idx) = lower[search_pos..].find(">=") {
        let abs_idx = search_pos + idx;
        let left_raw = &lower[search_pos..abs_idx];
        let right = &sql[abs_idx + 2..];

        let is_time_col = left_raw.contains("timestamp")
            || left_raw.contains("created_at")
            || left_raw.contains("day");
        if is_time_col {
            if let Some(val) = extract_val(right) {
                let ts = match val {
                    Value::Number(n) => n.as_i64(),
                    Value::String(s) => {
                        if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                            naive_date
                                .and_hms_opt(0, 0, 0)
                                .map(|dt| dt.and_utc().timestamp())
                        } else {
                            s.parse::<i64>().ok()
                        }
                    }
                    _ => None,
                };
                if let Some(t) = ts {
                    start_ts = Some(start_ts.map(|old| std::cmp::min(old, t)).unwrap_or(t));
                }
            }
        }
        search_pos = abs_idx + 2;
    }

    // Parse <= for end_ts
    let mut search_pos = 0;
    while let Some(idx) = lower[search_pos..].find("<=") {
        let abs_idx = search_pos + idx;
        let left_raw = &lower[search_pos..abs_idx];
        let right = &sql[abs_idx + 2..];

        let is_time_col = left_raw.contains("timestamp")
            || left_raw.contains("created_at")
            || left_raw.contains("day");
        if is_time_col {
            if let Some(val) = extract_val(right) {
                let ts = match val {
                    Value::Number(n) => n.as_i64(),
                    Value::String(s) => {
                        if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                            naive_date
                                .and_hms_opt(23, 59, 59)
                                .map(|dt| dt.and_utc().timestamp())
                        } else {
                            s.parse::<i64>().ok()
                        }
                    }
                    _ => None,
                };
                if let Some(t) = ts {
                    end_ts = Some(end_ts.map(|old| std::cmp::max(old, t)).unwrap_or(t));
                }
            }
        }
        search_pos = abs_idx + 2;
    }

    (start_ts, end_ts)
}

pub(crate) fn clean_expr_field(expr: &str) -> String {
    let trimmed = expr.trim().replace('"', "").replace('\'', "");
    if let Some(dot_idx) = trimmed.find('.') {
        trimmed[dot_idx + 1..].to_string()
    } else {
        trimmed
    }
}

/// Parsea consultas SQL que contienen CTEs (Common Table Expressions) estructuradas como las de Janus.
/// Retorna un mapa de nombre_cte -> sql_subconsulta y la consulta de selección final.
pub(crate) fn parse_cte_queries(sql: &str) -> Option<(HashMap<String, String>, String)> {
    let lower = sql.to_lowercase();
    if !lower.trim_start().starts_with("with ") {
        return None;
    }

    let mut ctes = HashMap::new();
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();

    let mut i = 0;
    while i < n && chars[i].is_whitespace() {
        i += 1;
    }
    if i + 4 <= n && chars[i..i + 4].iter().collect::<String>().to_lowercase() == "with" {
        i += 4;
    } else {
        return None;
    }

    loop {
        while i < n && (chars[i].is_whitespace() || chars[i] == ',') {
            i += 1;
        }
        if i >= n {
            break;
        }

        let name_start = i;
        while i < n && (chars[i].is_alphanumeric() || chars[i] == '_') {
            i += 1;
        }
        let cte_name = chars[name_start..i]
            .iter()
            .collect::<String>()
            .trim()
            .to_string();
        if cte_name.is_empty() {
            break;
        }

        while i < n && chars[i].is_whitespace() {
            i += 1;
        }

        if i + 2 <= n && chars[i..i + 2].iter().collect::<String>().to_lowercase() == "as" {
            i += 2;
        } else {
            break;
        }

        while i < n && chars[i].is_whitespace() {
            i += 1;
        }

        if i < n && chars[i] == '(' {
            i += 1;
            let subquery_start = i;
            let mut depth = 1;
            while i < n && depth > 0 {
                if chars[i] == '(' {
                    depth += 1;
                } else if chars[i] == ')' {
                    depth -= 1;
                }
                i += 1;
            }
            if depth == 0 {
                let subquery = chars[subquery_start..i - 1]
                    .iter()
                    .collect::<String>()
                    .trim()
                    .to_string();
                ctes.insert(cte_name, subquery);
            } else {
                break;
            }
        } else {
            break;
        }

        while i < n && chars[i].is_whitespace() {
            i += 1;
        }
        if i < n && chars[i] == ',' {
            i += 1;
        } else {
            let final_select = chars[i..].iter().collect::<String>().trim().to_string();
            return Some((ctes, final_select));
        }
    }

    None
}

/// Evalúa una expresión de proyección del select final sobre las filas combinadas de las subconsultas.
pub(crate) fn evaluate_final_expression(
    expr: &str,
    current_row: Option<&HashMap<String, Value>>,
    prev_rows: &HashMap<String, Vec<HashMap<String, Value>>>,
    smart_rows: &HashMap<String, Vec<HashMap<String, Value>>>,
    idx: usize,
) -> Value {
    let cleaned = expr.replace('"', "").replace('\'', "").trim().to_string();

    // Detección de fórmula z_score: (c.field - smart_0.mean_field) / NULLIF(smart_0.std_field, 0)
    if cleaned.contains("z_score")
        || (cleaned.contains('-')
            && cleaned.contains('/')
            && cleaned.contains("mean_")
            && cleaned.contains("std_"))
    {
        let smart_name = if let Some(s_idx) = cleaned.find("smart_") {
            let end_idx = cleaned[s_idx..]
                .find(|c: char| !c.is_alphanumeric() && c != '_')
                .unwrap_or(cleaned[s_idx..].len());
            cleaned[s_idx..s_idx + end_idx].to_string()
        } else {
            "smart_0".to_string()
        };

        let mut field_val = 0.0;
        let mut mean_val = 0.0;
        let mut std_val = 0.0;

        if let Some(row) = current_row {
            for key in row.keys() {
                if cleaned.contains(key) {
                    field_val = match row.get(key) {
                        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                        _ => 0.0,
                    };
                    if let Some(s_list) = smart_rows.get(&smart_name) {
                        if let Some(s_row) = s_list.first() {
                            let mean_key = format!("mean_{}", key);
                            let std_key = format!("std_{}", key);
                            mean_val = s_row
                                .get(&mean_key)
                                .and_then(|v| match v {
                                    Value::Number(n) => n.as_f64(),
                                    _ => None,
                                })
                                .unwrap_or(0.0);
                            std_val = s_row
                                .get(&std_key)
                                .and_then(|v| match v {
                                    Value::Number(n) => n.as_f64(),
                                    _ => None,
                                })
                                .unwrap_or(0.0);
                        }
                    }
                    break;
                }
            }
        }

        if std_val > 0.0 {
            let z = (field_val - mean_val) / std_val;
            return serde_json::Number::from_f64(z)
                .map(Value::Number)
                .unwrap_or(Value::Null);
        } else {
            return Value::Number(serde_json::Number::from(0));
        }
    }

    // Proyecciones estándar
    let (cte_name, field_name) = if cleaned.starts_with("c.") || cleaned.contains("c.bucket") {
        ("current_data".to_string(), clean_cte_field(&cleaned, "c."))
    } else if cleaned.contains("prev_") {
        let prev_name = extract_cte_name(&cleaned, "prev_");
        (
            prev_name.clone(),
            clean_cte_field(&cleaned, &format!("{}.", prev_name)),
        )
    } else if cleaned.contains("smart_") {
        let smart_name = extract_cte_name(&cleaned, "smart_");
        (
            smart_name.clone(),
            clean_cte_field(&cleaned, &format!("{}.", smart_name)),
        )
    } else {
        ("current_data".to_string(), cleaned.clone())
    };

    if cte_name == "current_data" {
        if let Some(row) = current_row {
            row.get(&field_name).cloned().unwrap_or(Value::Null)
        } else {
            Value::Null
        }
    } else if cte_name.starts_with("prev_") {
        if let Some(list) = prev_rows.get(&cte_name) {
            if let Some(row) = list.get(idx) {
                row.get(&field_name).cloned().unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        } else {
            Value::Null
        }
    } else if cte_name.starts_with("smart_") {
        if let Some(list) = smart_rows.get(&cte_name) {
            if let Some(row) = list.first() {
                row.get(&field_name).cloned().unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        } else {
            Value::Null
        }
    } else {
        Value::Null
    }
}

fn clean_cte_field(expr: &str, prefix: &str) -> String {
    if let Some(idx) = expr.find(prefix) {
        let start = idx + prefix.len();
        let rest = &expr[start..];
        let field: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        field
    } else {
        expr.to_string()
    }
}

fn extract_cte_name(expr: &str, prefix: &str) -> String {
    if let Some(idx) = expr.find(prefix) {
        let rest = &expr[idx..];
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        name
    } else {
        prefix.to_string()
    }
}
