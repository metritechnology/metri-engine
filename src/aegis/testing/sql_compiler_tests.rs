use crate::aegis::sql::compiler::*;
use serde_json::json;
use crate::temporal::core::TimeRange;
use crate::aegis::sql::dialect::{AthenaDialect, PostgresDialect};

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
    assert!(res.sql.contains("LIMIT 10000"), "SQL debe tener LIMIT 10000, got: {}", res.sql);
    assert!(res.sql.contains("tenant_id"), "SQL debe filtrar por tenant_id, got: {}", res.sql);
    assert!(res.sql.contains("t-123"), "SQL debe incluir el valor del tenant, got: {}", res.sql);
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

#[test]
fn test_comparison_kpi_compile() {
    let ast = json!({
        "entity": "meter_reading",
        "output_cast": "KPI",
        "select": ["*"],
        "schema": {
            "entity": "meter_reading",
            "attributes": [
                {
                    "name": "timestamp",
                    "attr_type": "epoch"
                }
            ]
        },
        "where": [
            "=",
            "tenant/id",
            "golden-tenant-benchmark"
        ],
        "limit": 1000,
        "metrics": [
            {
                "entity": "meter_reading",
                "attribute": "reading_value",
                "aggregation": "avg",
                "name": "lectura promedio"
            }
        ],
        "comparisons": [
            {
                "type": "TIME_SHIFT_RELATIVE",
                "relative_amount": 1,
                "relative_granularity": "day",
                "label": "vs ayer"
            }
        ]
    });

    let time_range = TimeRange {
        start_ts: Some(1779408000),
        end_ts: Some(1779507517)
    };
    let ts_col = Some("timestamp");

    match compile_athena_sql_with_time_frame(
        &ast,
        "metri_olap",
        "golden-tenant-benchmark",
        &time_range,
        ts_col
    ) {
        Ok(res) => {
            println!("--- COMPILED COMPARISON KPI SQL ---");
            println!("{}", res.sql);
            println!("-----------------------------------");
        }
        Err(e) => {
            println!("--- COMPILATION ERROR ---");
            println!("{:?}", e);
            println!("-------------------------");
        }
    }
}

#[test]
fn test_compile_sql_with_multiple_dialects() {
    let ast = json!({
        "entity": "assets",
        "output_cast": "TABLE",
        "select": ["id"],
        "where": ["and", 
            ["=", "tenant_id", "t-123"],
            ["matches", "name", "Pump.*"]
        ]
    });

    let time_range = TimeRange { start_ts: None, end_ts: None };

    // Compilar con Athena Dialect
    let res_athena = compile_sql_with_dialect(&ast, "metrics_db", "t-123", &time_range, &AthenaDialect).unwrap();
    assert!(res_athena.sql.contains("regexp_like(\"name\", 'Pump.*')"), "Athena SQL incorrect: {}", res_athena.sql);

    // Compilar con Postgres Dialect
    let res_postgres = compile_sql_with_dialect(&ast, "metrics_db", "t-123", &time_range, &PostgresDialect).unwrap();
    assert!(res_postgres.sql.contains("\"name\" ~* 'Pump.*'"), "Postgres SQL incorrect: {}", res_postgres.sql);
}

#[test]
fn test_compile_hybrid_meter_reading_sql_scenarios() {
    use chrono::{Utc, TimeZone, NaiveTime};

    // Crear un AST IR válido para la entidad "meter_reading"
    let ast = json!({
        "entity": "meter_reading",
        "output_cast": "TABLE",
        "select": ["*"],
        "where": ["and", 
            ["=", "tenant_id", "t-golden"],
            ["=", "metric_code", "VIBRATION"]
        ]
    });

    let now = Utc::now();
    let today_utc_start = Utc
        .from_utc_datetime(&now.date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()))
        .timestamp();

    // ESCENARIO A: 100% Caliente (Hoy)
    let time_range_raw = TimeRange {
        start_ts: Some(today_utc_start + 10),
        end_ts: Some(today_utc_start + 3600),
    };
    let res_raw = compile_athena_sql_with_time_frame(&ast, "metri_olap", "t-golden", &time_range_raw, None).unwrap();
    assert!(res_raw.sql.contains("\"metri_olap\".\"meter_reading\""), "Debe apuntar a raw: {}", res_raw.sql);
    assert!(!res_raw.sql.contains("meter_reading_rollup"), "No debe contener rollup: {}", res_raw.sql);

    // ESCENARIO B: 100% Histórica (Antes de hoy)
    let time_range_rollup = TimeRange {
        start_ts: Some(today_utc_start - 7200),
        end_ts: Some(today_utc_start - 3600),
    };
    let res_rollup = compile_athena_sql_with_time_frame(&ast, "metri_olap", "t-golden", &time_range_rollup, None).unwrap();
    assert!(res_rollup.sql.contains("meter_reading_rollup"), "Debe apuntar a rollup: {}", res_rollup.sql);
    assert!(!res_rollup.sql.contains("\"metri_olap\".\"meter_reading\""), "No debe contener raw: {}", res_rollup.sql);

    // ESCENARIO C: Unión Híbrida (Cruza la frontera)
    let time_range_hybrid = TimeRange {
        start_ts: Some(today_utc_start - 3600), // 1 hora antes de hoy
        end_ts: Some(today_utc_start + 3600),  // 1 hora después de hoy
    };
    let res_hybrid = compile_athena_sql_with_time_frame(&ast, "metri_olap", "t-golden", &time_range_hybrid, None).unwrap();
    assert!(res_hybrid.sql.contains("UNION ALL"), "Debe contener UNION ALL: {}", res_hybrid.sql);
    assert!(res_hybrid.sql.contains("\"metri_olap\".\"meter_reading\""), "Debe contener raw: {}", res_hybrid.sql);
    assert!(res_hybrid.sql.contains("meter_reading_rollup"), "Debe contener rollup: {}", res_hybrid.sql);
}

#[test]
fn test_compile_hybrid_count_rewriting() {
    let ast = json!({
        "entity": "meter_reading",
        "output_cast": "KPI",
        "select": ["*"],
        "where": ["=", "tenant_id", "t-golden"],
        "metrics": [
            {
                "entity": "meter_reading",
                "attribute": "id",
                "aggregation": "COUNT"
            }
        ]
    });

    let time_range = TimeRange {
        start_ts: Some(1779408000),
        end_ts: Some(1779507517)
    };

    let res = compile_athena_sql_with_time_frame(&ast, "metri_olap", "t-golden", &time_range, None).unwrap();
    
    // Debería compilar como SUM("reading_count") debido al rewrite de entidades híbridas.
    // Además, el coalesce a 0 debería aplicarse a SUM.
    assert!(
        res.sql.to_lowercase().contains("coalesce(sum(\"reading_count\"), 0)"),
        "El SQL debería reescribir COUNT a SUM(reading_count) con coalesce. SQL compilado: {}",
        res.sql
    );
}
