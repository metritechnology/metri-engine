// aegis/sql/query_config.rs
// Declarative query configuration for OLAP engine to avoid hardcoding limits and metadata settings.

#[derive(Debug, Clone)]
pub struct QueryConfig {
    pub database: String,
    pub default_limit: u64,
    pub table_limit: u64,
    pub max_limit: u64,
    pub tenant_column: String,
    pub default_ts_column: String,
    pub system_columns: Vec<String>,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            database: std::env::var("GLUE_DATABASE_NAME")
                .unwrap_or_else(|_| "metri_olap".to_string()),
            default_limit: 1000,
            table_limit: 10000,
            max_limit: 50000,
            tenant_column: "_tenant".to_string(),
            default_ts_column: "created_at".to_string(),
            system_columns: vec![
                "_tenant".to_string(),
                "_entity".to_string(),
                "_partition_path".to_string(),
            ],
        }
    }
}
