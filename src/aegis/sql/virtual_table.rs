use crate::aegis::sql::dialect::SqlDialect;
use crate::aegis::sql::registry::{get_hybrid_config, HybridEntityConfig};
use crate::temporal::core::TimeRange;
use chrono::{NaiveTime, TimeZone, Utc};
use sea_query::{Alias, Expr, Query, SelectStatement, UnionType};
use std::sync::Arc;

/// Strategy pattern trait for dynamic/custom virtual table queries.
pub trait VirtualTableStrategy: Send + Sync {
    fn build_table_query(
        &self,
        database: &str,
        tenant_id: &str,
        time_range: &TimeRange,
        dialect: &dyn SqlDialect,
    ) -> SelectStatement;
}

pub struct GenericHybridStrategy {
    pub config: Arc<HybridEntityConfig>,
}

impl VirtualTableStrategy for GenericHybridStrategy {
    fn build_table_query(
        &self,
        database: &str,
        tenant_id: &str,
        time_range: &TimeRange,
        dialect: &dyn SqlDialect,
    ) -> SelectStatement {
        build_virtual_table(database, tenant_id, time_range, dialect, &self.config)
    }
}

pub fn get_strategy(entity: &str) -> Option<Arc<dyn VirtualTableStrategy>> {
    get_hybrid_config(entity)
        .map(|config| Arc::new(GenericHybridStrategy { config }) as Arc<dyn VirtualTableStrategy>)
}

/// Día `YYYY-MM-DD` de un epoch-segundos. Timestamps fuera del calendario
/// soportado por chrono (años 0–9999) se clampan a los extremos: la query
/// degrada a incluir todo el histórico en vez de paniquear con input
/// absurdo del request (PLAN_PATRON_RESULT.md, regla R2).
fn epoch_to_day(ts: i64) -> String {
    match Utc.timestamp_opt(ts, 0).single() {
        Some(dt) => dt.format("%Y-%m-%d").to_string(),
        None if ts < 0 => "0000-01-01".to_string(),
        None => "9999-12-31".to_string(),
    }
}

fn build_virtual_table(
    database: &str,
    tenant_id: &str,
    time_range: &TimeRange,
    _dialect: &dyn SqlDialect,
    config: &HybridEntityConfig,
) -> SelectStatement {
    let now = Utc::now();
    let today_utc_start = Utc
        .from_utc_datetime(&now.date_naive().and_time(NaiveTime::MIN))
        .timestamp();

    let start_ts = time_range.start_ts.unwrap_or(today_utc_start - 30 * 86400);
    let end_ts = time_range.end_ts.unwrap_or(now.timestamp());

    let attr_col = &config.attribute_column;
    let val_col = &config.value_column;
    let ts_col = &config.timestamp_column;
    let base_tbl = &config.base_table;
    let rollup_tbl = &config.rollup_table;

    if start_ts >= today_utc_start {
        // Scenario A: 100% Hot buffer query
        let mut select = Query::select();
        select
            .column(Alias::new("id"))
            .column(Alias::new(&config.tenant_column_raw))
            .expr_as(
                Expr::col(Alias::new(&config.tenant_column_raw)),
                Alias::new(&config.tenant_column_rollup),
            );

        for dim in &config.dimensions {
            select.column(Alias::new(&dim.raw_column));
        }

        select
            .column(Alias::new(attr_col))
            .column(Alias::new(val_col))
            .expr_as(Expr::col(Alias::new(val_col)), Alias::new(format!("{}_avg", val_col)))
            .expr_as(Expr::col(Alias::new(val_col)), Alias::new(format!("{}_min", val_col)))
            .expr_as(Expr::col(Alias::new(val_col)), Alias::new(format!("{}_max", val_col)))
            .expr_as(Expr::val(1i32), Alias::new("reading_count"))
            .expr_as(
                Expr::cust(format!("CAST(IF({ts_col} > 100000000000, {ts_col} / 1000.0, CAST({ts_col} AS DOUBLE)) AS BIGINT)")),
                Alias::new(ts_col)
            )
            .from((Alias::new(database), Alias::new(base_tbl)))
            .cond_where(
                sea_query::Cond::all()
                    .add(Expr::col(Alias::new(&config.tenant_column_raw)).eq(tenant_id))
                    .add(Expr::cust(format!("IF({ts_col} > 100000000000, {ts_col} / 1000.0, CAST({ts_col} AS DOUBLE)) >= {}", start_ts)))
                    .add(Expr::cust(format!("IF({ts_col} > 100000000000, {ts_col} / 1000.0, CAST({ts_col} AS DOUBLE)) <= {}", end_ts)))
            );
        select
    } else if end_ts < today_utc_start {
        // Scenario B: 100% Historical rollup query
        let start_day = epoch_to_day(start_ts);
        let end_day = epoch_to_day(end_ts);

        let id_expr = format!(
            "concat({})",
            config
                .id_rollup_fields
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(", '-', ")
        );
        let mut select = Query::select();
        select
            .expr_as(Expr::cust(&id_expr), Alias::new("id"))
            .column(Alias::new(&config.tenant_column_rollup))
            .expr_as(
                Expr::col(Alias::new(&config.tenant_column_rollup)),
                Alias::new(&config.tenant_column_raw),
            );

        for dim in &config.dimensions {
            select.expr_as(Expr::cust(&dim.rollup_expr), Alias::new(&dim.raw_column));
        }

        select
            .column(Alias::new(attr_col))
            .expr_as(
                Expr::col(Alias::new(format!("{}_avg", val_col))),
                Alias::new(val_col),
            )
            .column(Alias::new(format!("{}_avg", val_col)))
            .column(Alias::new(format!("{}_min", val_col)))
            .column(Alias::new(format!("{}_max", val_col)))
            .column(Alias::new("reading_count"))
            .expr_as(
                Expr::cust(&format!(
                    "to_unixtime(date_parse({}, '%Y-%m-%d'))",
                    config.rollup_day_column
                )),
                Alias::new(ts_col),
            )
            .from((Alias::new(database), Alias::new(rollup_tbl)))
            .cond_where(
                sea_query::Cond::all()
                    .add(Expr::col(Alias::new(&config.tenant_column_rollup)).eq(tenant_id))
                    .add(Expr::col(Alias::new(&config.rollup_day_column)).gte(start_day))
                    .add(Expr::col(Alias::new(&config.rollup_day_column)).lte(end_day)),
            );
        select
    } else {
        // Scenario C: Hybrid UNION ALL query (crosses boundaries)
        let start_day = epoch_to_day(start_ts);
        let yesterday_day = epoch_to_day(today_utc_start - 1);

        let id_expr = format!(
            "concat({})",
            config
                .id_rollup_fields
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(", '-', ")
        );
        let mut select_a = Query::select();
        select_a
            .expr_as(Expr::cust(&id_expr), Alias::new("id"))
            .column(Alias::new(&config.tenant_column_rollup))
            .expr_as(
                Expr::col(Alias::new(&config.tenant_column_rollup)),
                Alias::new(&config.tenant_column_raw),
            );

        for dim in &config.dimensions {
            select_a.expr_as(Expr::cust(&dim.rollup_expr), Alias::new(&dim.raw_column));
        }

        select_a
            .column(Alias::new(attr_col))
            .expr_as(
                Expr::col(Alias::new(format!("{}_avg", val_col))),
                Alias::new(val_col),
            )
            .column(Alias::new(format!("{}_avg", val_col)))
            .column(Alias::new(format!("{}_min", val_col)))
            .column(Alias::new(format!("{}_max", val_col)))
            .column(Alias::new("reading_count"))
            .expr_as(
                Expr::cust(&format!(
                    "to_unixtime(date_parse({}, '%Y-%m-%d'))",
                    config.rollup_day_column
                )),
                Alias::new(ts_col),
            )
            .from((Alias::new(database), Alias::new(rollup_tbl)))
            .cond_where(
                sea_query::Cond::all()
                    .add(Expr::col(Alias::new(&config.tenant_column_rollup)).eq(tenant_id))
                    .add(Expr::col(Alias::new(&config.rollup_day_column)).gte(start_day.clone()))
                    .add(
                        Expr::col(Alias::new(&config.rollup_day_column)).lte(yesterday_day.clone()),
                    ),
            );

        let mut select_b = Query::select();
        select_b
            .column(Alias::new("id"))
            .column(Alias::new(&config.tenant_column_raw))
            .expr_as(
                Expr::col(Alias::new(&config.tenant_column_raw)),
                Alias::new(&config.tenant_column_rollup),
            );

        for dim in &config.dimensions {
            select_b.column(Alias::new(&dim.raw_column));
        }

        select_b
            .column(Alias::new(attr_col))
            .column(Alias::new(val_col))
            .expr_as(Expr::col(Alias::new(val_col)), Alias::new(format!("{}_avg", val_col)))
            .expr_as(Expr::col(Alias::new(val_col)), Alias::new(format!("{}_min", val_col)))
            .expr_as(Expr::col(Alias::new(val_col)), Alias::new(format!("{}_max", val_col)))
            .expr_as(Expr::val(1i32), Alias::new("reading_count"))
            .expr_as(
                Expr::cust(format!("CAST(IF({ts_col} > 100000000000, {ts_col} / 1000.0, CAST({ts_col} AS DOUBLE)) AS BIGINT)")),
                Alias::new(ts_col)
            )
            .from((Alias::new(database), Alias::new(base_tbl)))
            .cond_where(
                sea_query::Cond::all()
                    .add(Expr::col(Alias::new(&config.tenant_column_raw)).eq(tenant_id))
                    .add(Expr::cust(format!("IF({ts_col} > 100000000000, {ts_col} / 1000.0, CAST({ts_col} AS DOUBLE)) >= {}", start_ts)))
                    .add(Expr::cust(format!("IF({ts_col} > 100000000000, {ts_col} / 1000.0, CAST({ts_col} AS DOUBLE)) <= {}", end_ts)))
                    .add(Expr::cust(&format!(
                        "format_datetime(from_unixtime(IF({ts_col} > 100000000000, {ts_col} / 1000.0, CAST({ts_col} AS DOUBLE))), 'yyyy-MM-dd') NOT IN (
                            SELECT DISTINCT {}
                            FROM \"{}\".\"{}\"
                            WHERE {} = '{}'
                              AND {} >= '{}'
                              AND {} <= '{}'
                        )",
                        config.rollup_day_column,
                        database, rollup_tbl,
                        config.tenant_column_rollup, tenant_id,
                        config.rollup_day_column, start_day,
                        config.rollup_day_column, yesterday_day
                    )))
            );

        let mut select_union = select_a;
        select_union.union(UnionType::All, select_b.to_owned());

        let mut select = Query::select();
        let mut columns = vec![
            Alias::new("id"),
            Alias::new(&config.tenant_column_rollup),
            Alias::new(&config.tenant_column_raw),
        ];
        for dim in &config.dimensions {
            columns.push(Alias::new(&dim.raw_column));
        }
        columns.push(Alias::new(attr_col));
        columns.push(Alias::new(format!("{}_avg", val_col)));
        columns.push(Alias::new(format!("{}_min", val_col)));
        columns.push(Alias::new(format!("{}_max", val_col)));
        columns.push(Alias::new("reading_count"));
        columns.push(Alias::new(ts_col));

        select
            .columns(columns)
            .expr_as(
                Expr::col(Alias::new(format!("{}_avg", val_col))),
                Alias::new(val_col),
            )
            .from_subquery(select_union, Alias::new("union_db"));
        select
    }
}
