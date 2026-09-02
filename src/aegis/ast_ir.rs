// aegis/ast_ir.rs
// Typed AST IR representation to decouple from raw JSON values and provide compile-time safety.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OutputCast {
    KPI,
    TIMESERIES,
    TABLE,
    PIE,
    BUBBLE,
    #[serde(rename = "CSV_EXPORT")]
    CsvExport,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WhereNode {
    Eq(String, Value),
    NotEq(String, Value),
    Gt(String, Value),
    Lt(String, Value),
    Gte(String, Value),
    Lte(String, Value),
    In(String, Value),
    NotIn(String, Value),
    Between(String, Value),
    Matches(String, String),
    Like(String, String),
    Contains(String, String),
    IsNull(String),
    IsNotNull(String),
    And(Vec<WhereNode>),
    Or(Vec<WhereNode>),
    Not(Box<WhereNode>),
    Fuzzy(String, String),
    Fts(String),
    RefFilter(String, Box<WhereNode>),
    Empty,
}

impl WhereNode {
    pub fn from_value(v: &Value) -> Result<Self, String> {
        if v.is_null() {
            return Ok(WhereNode::Empty);
        }
        let arr = v
            .as_array()
            .ok_or_else(|| "WHERE node must be an array".to_string())?;
        if arr.is_empty() {
            return Ok(WhereNode::Empty);
        }
        let op = arr[0]
            .as_str()
            .ok_or_else(|| "First element of WHERE array must be a string operator".to_string())?;
        match op {
            "=" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("= operator requires column name")?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Eq(col, val))
            }
            "not=" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("not= operator requires column name")?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::NotEq(col, val))
            }
            ">" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("> operator requires column name")?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Gt(col, val))
            }
            "<" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("< operator requires column name")?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Lt(col, val))
            }
            ">=" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or(">= operator requires column name")?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Gte(col, val))
            }
            "<=" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("<= operator requires column name")?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Lte(col, val))
            }
            "in" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("in operator requires column name")?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::In(col, val))
            }
            "not-in" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("not-in operator requires column name")?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::NotIn(col, val))
            }
            "between" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("between operator requires column name")?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Between(col, val))
            }
            "matches" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("matches operator requires column name")?
                    .to_string();
                let pattern = arr
                    .get(2)
                    .and_then(|v| v.as_str())
                    .ok_or("matches operator requires pattern string")?
                    .to_string();
                Ok(WhereNode::Matches(col, pattern))
            }
            "like" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("like operator requires column name")?
                    .to_string();
                let pattern = arr
                    .get(2)
                    .and_then(|v| v.as_str())
                    .ok_or("like operator requires pattern string")?
                    .to_string();
                Ok(WhereNode::Like(col, pattern))
            }
            "contains" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("contains operator requires column name")?
                    .to_string();
                let val = arr
                    .get(2)
                    .and_then(|v| v.as_str())
                    .ok_or("contains operator requires search string")?
                    .to_string();
                Ok(WhereNode::Contains(col, val))
            }
            "is-null" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("is-null operator requires column name")?
                    .to_string();
                Ok(WhereNode::IsNull(col))
            }
            "is-not-null" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("is-not-null operator requires column name")?
                    .to_string();
                Ok(WhereNode::IsNotNull(col))
            }
            "and" => {
                let mut children = Vec::new();
                for child in arr.iter().skip(1) {
                    children.push(Self::from_value(child)?);
                }
                Ok(WhereNode::And(children))
            }
            "or" => {
                let mut children = Vec::new();
                for child in arr.iter().skip(1) {
                    children.push(Self::from_value(child)?);
                }
                Ok(WhereNode::Or(children))
            }
            "not" => {
                let child = arr.get(1).ok_or("not operator requires child node")?;
                Ok(WhereNode::Not(Box::new(Self::from_value(child)?)))
            }
            "fuzzy" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("fuzzy operator requires column name")?
                    .to_string();
                let term = arr
                    .get(2)
                    .and_then(|v| v.as_str())
                    .ok_or("fuzzy operator requires search term")?
                    .to_string();
                Ok(WhereNode::Fuzzy(col, term))
            }
            "fts" => {
                let term = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("fts operator requires search term")?
                    .to_string();
                Ok(WhereNode::Fts(term))
            }
            "ref-filter" => {
                let ref_field = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or("ref-filter operator requires reference field")?
                    .to_string();
                let inner = arr
                    .get(2)
                    .ok_or("ref-filter operator requires inner condition")?;
                Ok(WhereNode::RefFilter(
                    ref_field,
                    Box::new(Self::from_value(inner)?),
                ))
            }
            other => Err(format!("Unsupported WHERE operator: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricDef {
    pub entity: Option<String>,
    pub attribute: Option<String>,
    pub field: Option<String>,
    pub secondary_attribute: Option<String>,
    pub secondary_field: Option<String>,
    pub aggregation: Option<String>,
    pub name: Option<String>,
    pub filter: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MeasureDef {
    pub formula: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Dimension {
    pub attribute: Option<String>,
    pub field: Option<String>,
    pub interval: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderByExpr {
    pub field: Option<String>,
    pub attribute: Option<String>,
    pub descending: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComparisonDef {
    #[serde(rename = "type")]
    pub comp_type: String,
    pub relative_granularity: Option<String>,
    pub relative_amount: Option<i64>,
    pub shortcut: Option<String>,
    pub absolute_start_ts: Option<i64>,
    pub absolute_end_ts: Option<i64>,
    pub benchmark_value: Option<f64>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HierarchyDef {
    pub parent_field: Option<String>,
    pub current_node_id: Option<Value>,
    pub inject_has_children: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaInfo {
    pub entity: String,
    pub attributes: Vec<SchemaAttribute>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaAttribute {
    pub name: String,
    pub attr_type: Option<String>,
    #[serde(rename = "type")]
    pub field_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SemanticMeasureDef {
    pub metric_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AstIr {
    pub entity: String,
    pub output_cast: OutputCast,
    pub where_clause: Option<WhereNode>,
    pub select: Option<Vec<String>>,
    pub group_by: Option<Vec<Dimension>>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub metrics: Option<Vec<MetricDef>>,
    pub measures: Option<Vec<MeasureDef>>,
    pub semantic_measures: Option<Vec<SemanticMeasureDef>>,
    pub comparisons: Option<Vec<ComparisonDef>>,
    pub hierarchy: Option<HierarchyDef>,
    pub limit: Option<u64>,
    pub schema: Option<SchemaInfo>,
}

impl AstIr {
    pub fn from_value(v: &Value) -> Result<Self, String> {
        let entity = v
            .get("entity")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Missing 'entity' field in AST".to_string())?
            .to_string();

        let output_cast_str = v
            .get("output_cast")
            .and_then(|v| v.as_str())
            .unwrap_or("KPI");

        let output_cast = match output_cast_str.to_uppercase().as_str() {
            "KPI" => OutputCast::KPI,
            "TIMESERIES" => OutputCast::TIMESERIES,
            "TABLE" => OutputCast::TABLE,
            "PIE" => OutputCast::PIE,
            "BUBBLE" => OutputCast::BUBBLE,
            "CSV_EXPORT" => OutputCast::CsvExport,
            _ => OutputCast::TABLE,
        };

        let where_clause = match v.get("where") {
            Some(w_val) => Some(WhereNode::from_value(w_val)?),
            None => None,
        };

        let select = v.get("select").and_then(|v| v.as_array()).map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    if let Some(s) = item.as_str() {
                        Some(s.to_string())
                    } else if let Some(obj) = item.as_object() {
                        obj.keys().next().cloned()
                    } else {
                        None
                    }
                })
                .collect()
        });

        let group_by: Option<Vec<Dimension>> = v
            .get("group_by")
            .or_else(|| v.get("dimensions"))
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        let order_by: Option<Vec<OrderByExpr>> = v
            .get("order_by")
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        let metrics: Option<Vec<MetricDef>> = v
            .get("metrics")
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        let measures: Option<Vec<MeasureDef>> = v
            .get("measures")
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        let semantic_measures: Option<Vec<SemanticMeasureDef>> = v
            .get("semantic_measures")
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        let comparisons: Option<Vec<ComparisonDef>> = v
            .get("comparisons")
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        let hierarchy: Option<HierarchyDef> = v
            .get("hierarchy")
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        let limit = v.get("limit").and_then(|v| v.as_u64());

        let schema: Option<SchemaInfo> = v
            .get("schema")
            .and_then(|v| serde_json::from_value(v.clone()).ok());

        Ok(Self {
            entity,
            output_cast,
            where_clause,
            select,
            group_by,
            order_by,
            metrics,
            measures,
            semantic_measures,
            comparisons,
            hierarchy,
            limit,
            schema,
        })
    }
}
