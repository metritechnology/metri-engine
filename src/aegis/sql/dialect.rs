//! SqlDialect trait — engine-specific rendering behind a port.
//!
//! aegis/sql/dialect.rs
//! Trait SqlDialect y su implementación para Athena/Presto.
//! DIP/OCP: Permite desacoplar el compilador de consultas SQL de funciones y sintaxis específicas de Athena.

pub trait SqlDialect: Send + Sync {
    /// Formatea un operador regex match (ej: regexp_like en Athena, REGEXP en MySQL, ~* en Postgres)
    fn format_regexp_like(&self, col: &str, pattern: &str) -> String;

    /// Formatea la conversión de un valor numérico epoch a un timestamp nativo de la DB.
    fn format_from_unixtime(&self, epoch_expr: &str) -> String;

    /// Formatea la conversión condicional (soporte milisegundos y segundos) a timestamp.
    fn format_epoch_to_timestamp(&self, col: &str) -> String;

    /// Formatea el truncado de fecha (ej: date_trunc en Athena/Postgres, date_format en MySQL)
    fn format_date_trunc(&self, interval: &str, col: &str) -> String;

    /// Formatea un cálculo de percentil aproximado.
    fn format_percentile(&self, col: &str, percentile: f64) -> String;

    /// Formatea desviación estándar muestral.
    fn format_stddev_samp(&self, col: &str) -> String;

    /// Formatea varianza muestral.
    fn format_var_samp(&self, col: &str) -> String;

    /// Formatea correlación de Pearson.
    fn format_corr(&self, sec_col: &str, col: &str) -> String;

    /// Formatea pendiente de regresión lineal.
    fn format_regr_slope(&self, sec_col: &str, col: &str) -> String;

    /// Formatea una inyección coalesce para default de agregaciones.
    fn format_coalesce(&self, expr: &str, fallback: &str) -> String;

    /// Formatea la consulta EXISTS para inyección jerárquica.
    fn format_exists(&self, table: &str, tenant_id: &str, parent_col: &str) -> String;
}

pub struct AthenaDialect;

impl SqlDialect for AthenaDialect {
    fn format_regexp_like(&self, col: &str, pattern: &str) -> String {
        format!("regexp_like({}, '{}')", col, pattern.replace('\'', "''"))
    }

    fn format_from_unixtime(&self, epoch_expr: &str) -> String {
        format!("from_unixtime({})", epoch_expr)
    }

    fn format_epoch_to_timestamp(&self, col: &str) -> String {
        format!(
            "from_unixtime(IF({} > 100000000000, {} / 1000.0, CAST({} AS DOUBLE)))",
            col, col, col
        )
    }

    fn format_date_trunc(&self, interval: &str, col: &str) -> String {
        let ts_expr = self.format_epoch_to_timestamp(col);
        format!("date_trunc('{}', {})", interval, ts_expr)
    }

    fn format_percentile(&self, col: &str, percentile: f64) -> String {
        format!("approx_percentile({}, {})", col, percentile)
    }

    fn format_stddev_samp(&self, col: &str) -> String {
        format!("stddev_samp({})", col)
    }

    fn format_var_samp(&self, col: &str) -> String {
        format!("var_samp({})", col)
    }

    fn format_corr(&self, sec_col: &str, col: &str) -> String {
        format!("corr({}, {})", sec_col, col)
    }

    fn format_regr_slope(&self, sec_col: &str, col: &str) -> String {
        format!("regr_slope({}, {})", sec_col, col)
    }

    fn format_coalesce(&self, expr: &str, fallback: &str) -> String {
        format!("COALESCE({}, {})", expr, fallback)
    }

    fn format_exists(&self, table: &str, tenant_id: &str, parent_col: &str) -> String {
        format!(
            "EXISTS(SELECT 1 FROM {} WHERE _tenant = '{}' AND {} = t.id)",
            table,
            tenant_id.replace('\'', "''"),
            parent_col
        )
    }
}

/// Dialecto de prueba (ejemplo Postgres) para demostrar OCP e inyección dinámica.
#[allow(dead_code)]
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn format_regexp_like(&self, col: &str, pattern: &str) -> String {
        format!("{} ~* '{}'", col, pattern.replace('\'', "''"))
    }

    fn format_from_unixtime(&self, epoch_expr: &str) -> String {
        format!("to_timestamp({})", epoch_expr)
    }

    fn format_epoch_to_timestamp(&self, col: &str) -> String {
        format!("to_timestamp(CASE WHEN {} > 100000000000 THEN {} / 1000.0 ELSE CAST({} AS DOUBLE PRECISION) END)", col, col, col)
    }

    fn format_date_trunc(&self, interval: &str, col: &str) -> String {
        let ts_expr = self.format_epoch_to_timestamp(col);
        format!("date_trunc('{}', {})", interval, ts_expr)
    }

    fn format_percentile(&self, col: &str, percentile: f64) -> String {
        format!(
            "percentile_cont({}) WITHIN GROUP (ORDER BY {})",
            percentile, col
        )
    }

    fn format_stddev_samp(&self, col: &str) -> String {
        format!("stddev_samp({})", col)
    }

    fn format_var_samp(&self, col: &str) -> String {
        format!("var_samp({})", col)
    }

    fn format_corr(&self, sec_col: &str, col: &str) -> String {
        format!("corr({}, {})", sec_col, col)
    }

    fn format_regr_slope(&self, sec_col: &str, col: &str) -> String {
        format!("regr_slope({}, {})", sec_col, col)
    }

    fn format_coalesce(&self, expr: &str, fallback: &str) -> String {
        format!("COALESCE({}, {})", expr, fallback)
    }

    fn format_exists(&self, table: &str, tenant_id: &str, parent_col: &str) -> String {
        format!(
            "EXISTS(SELECT 1 FROM {} WHERE _tenant = '{}' AND {} = t.id)",
            table,
            tenant_id.replace('\'', "''"),
            parent_col
        )
    }
}
