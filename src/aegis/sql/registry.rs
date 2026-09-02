// aegis/sql/registry.rs
// Registry for dynamic configurations of hybrid/EAV/rollup entities.

use std::collections::HashMap;
use std::sync::Arc;
use lazy_static::lazy_static;

#[derive(Clone, Debug)]
pub struct DimensionConfig {
    pub name: String,
    pub raw_column: String,
    pub rollup_expr: String, // SQL expression in rollup (e.g., "asset_id", "CAST(NULL AS VARCHAR)" or "'UNKNOWN'")
}

#[derive(Clone, Debug)]
pub struct HybridEntityConfig {
    pub entity_name: String,
    pub base_table: String,
    pub rollup_table: String,
    pub tenant_column_raw: String,     // e.g., "_tenant"
    pub tenant_column_rollup: String,  // e.g., "tenant_id"
    pub timestamp_column: String,      // e.g., "timestamp"
    pub rollup_day_column: String,     // e.g., "day"
    pub id_rollup_fields: Vec<String>, // e.g., ["tenant_id", "asset_id", "day"]
    pub attribute_column: String,      // e.g., "metric_code"
    pub value_column: String,          // e.g., "reading_value"
    pub dimensions: Vec<DimensionConfig>,
    pub structural_fields: Vec<String>,
}

lazy_static! {
    static ref HYBRID_REGISTRY: HashMap<String, Arc<HybridEntityConfig>> = {
        let mut m = HashMap::new();
        m.insert(
            "meter_reading".to_string(),
            Arc::new(HybridEntityConfig {
                entity_name: "meter_reading".to_string(),
                base_table: "meter_reading".to_string(),
                rollup_table: "meter_reading_rollup".to_string(),
                tenant_column_raw: "_tenant".to_string(),
                tenant_column_rollup: "tenant_id".to_string(),
                timestamp_column: "timestamp".to_string(),
                rollup_day_column: "day".to_string(),
                id_rollup_fields: vec![
                    "tenant_id".to_string(),
                    "asset_id".to_string(),
                    "day".to_string(),
                ],
                attribute_column: "metric_code".to_string(),
                value_column: "reading_value".to_string(),
                dimensions: vec![
                    DimensionConfig {
                        name: "asset_id".to_string(),
                        raw_column: "asset_id".to_string(),
                        rollup_expr: "asset_id".to_string(),
                    },
                    DimensionConfig {
                        name: "location_id".to_string(),
                        raw_column: "location_id".to_string(),
                        rollup_expr: "CAST(NULL AS VARCHAR)".to_string(),
                    },
                    DimensionConfig {
                        name: "unit_of_measure".to_string(),
                        raw_column: "unit_of_measure".to_string(),
                        rollup_expr: "unit_of_measure".to_string(),
                    },
                    DimensionConfig {
                        name: "protocol".to_string(),
                        raw_column: "protocol".to_string(),
                        rollup_expr: "'UNKNOWN'".to_string(),
                    },
                    DimensionConfig {
                        name: "data_quality".to_string(),
                        raw_column: "data_quality".to_string(),
                        rollup_expr: "'UNKNOWN'".to_string(),
                    },
                ],
                structural_fields: vec![
                    "asset_id".to_string(),
                    "location_id".to_string(),
                    "metric_code".to_string(),
                    "reading_value".to_string(),
                    "raw_value".to_string(),
                    "unit_of_measure".to_string(),
                    "data_quality".to_string(),
                    "protocol".to_string(),
                    "source_address".to_string(),
                    "reading_value_avg".to_string(),
                    "reading_value_min".to_string(),
                    "reading_value_max".to_string(),
                    "reading_count".to_string(),
                ],
            }),
        );
        m
    };
}

pub fn get_hybrid_config(entity: &str) -> Option<Arc<HybridEntityConfig>> {
    HYBRID_REGISTRY.get(entity).cloned()
}
