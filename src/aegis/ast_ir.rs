//! Representación intermedia tipada del AST (`AstIr`) para consultas en Aegis.
//!
//! Desacopla a los compiladores y ejecutores de la manipulación directa de estructuras
//! JSON sin esquema, proveyendo seguridad de tipos en tiempo de compilación para predicados
//! (`WhereNode`), métricas (`MetricDef`), fórmulas (`MeasureDef`), dimensiones (`Dimension`),
//! comparaciones (`ComparisonDef`) y proyecciones de salida (`OutputCast`).

use crate::domain::errors::{DomainError, ErrorCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Formato de proyección visual o tabular solicitado para el resultado de la consulta.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OutputCast {
    /// Indicador clave de rendimiento escalar con métricas principales y comparativas.
    KPI,
    /// Serie de tiempo agrupada por intervalos temporales (`buckets`).
    TIMESERIES,
    /// Tabla multidimensional de filas y columnas con soporte de paginación y jerarquías.
    TABLE,
    /// Distribución porcentual por categorías (gráfico circular o de sectores).
    PIE,
    /// Gráfico de dispersión multidimensional (ejes X, Y y tamaño de burbuja).
    BUBBLE,
    /// Flujo de exportación tabular en formato CSV.
    #[serde(rename = "CSV_EXPORT")]
    CsvExport,
}

/// Nodo recursivo del árbol de predicados de filtrado (`WHERE`) en el AST tipado.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WhereNode {
    /// Igualdad exacta (`=`) entre un atributo/columna y un valor literal.
    Eq(String, Value),
    /// Desigualdad (`not=`) entre un atributo/columna y un valor literal.
    NotEq(String, Value),
    /// Comparación estrictamente mayor que (`>`).
    Gt(String, Value),
    /// Comparación estrictamente menor que (`<`).
    Lt(String, Value),
    /// Comparación mayor o igual que (`>=`).
    Gte(String, Value),
    /// Comparación menor o igual que (`<=`).
    Lte(String, Value),
    /// Pertenencia a un conjunto de valores (`in`).
    In(String, Value),
    /// Exclusión de un conjunto de valores (`not-in`).
    NotIn(String, Value),
    /// Inclusión dentro de un rango cerrado de dos elementos (`between`).
    Between(String, Value),
    /// Coincidencia mediante expresión regular (`matches`).
    Matches(String, String),
    /// Coincidencia de patrón comodín estilo SQL (`like`).
    Like(String, String),
    /// Búsqueda de subcadena insensible a mayúsculas (`contains`).
    Contains(String, String),
    /// Verificación de ausencia de valor o nulidad (`is-null`).
    IsNull(String),
    /// Verificación de presencia de valor no nulo (`is-not-null`).
    IsNotNull(String),
    /// Conjunción lógica (`and`) de múltiples subárboles de predicados.
    And(Vec<WhereNode>),
    /// Disyunción lógica (`or`) de múltiples subárboles de predicados.
    Or(Vec<WhereNode>),
    /// Negación lógica (`not`) de un subárbol de predicado.
    Not(Box<WhereNode>),
    /// Búsqueda aproximada tolerante a errores tipográficos (`fuzzy`) sobre un atributo.
    Fuzzy(String, String),
    /// Búsqueda de texto completo (`fts`) por trigramas sobre la entidad.
    Fts(String),
    /// Filtro transversal sobre una entidad referenciada (`ref-filter`).
    RefFilter(String, Box<WhereNode>),
    /// Nodo vacío que representa ausencia de restricción de filtrado.
    Empty,
}

impl WhereNode {
    /// Parsea recursivamente un árbol de predicados en formato JSON (`Value`) hacia un `WhereNode` tipado.
    ///
    /// Retorna `ErrorCode::Aeg001` si la estructura del arreglo o el operador no son válidos.
    pub fn from_value(v: &Value) -> Result<Self, DomainError> {
        if v.is_null() {
            return Ok(WhereNode::Empty);
        }
        let arr = v
            .as_array()
            .ok_or_else(|| DomainError::aegis(ErrorCode::Aeg001, "WHERE node must be an array"))?;
        if arr.is_empty() {
            return Ok(WhereNode::Empty);
        }
        let op = arr[0].as_str().ok_or_else(|| {
            DomainError::aegis(
                ErrorCode::Aeg001,
                "First element of WHERE array must be a string operator",
            )
        })?;
        match op {
            "=" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, "= operator requires column name")
                    })?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Eq(col, val))
            }
            "not=" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, "not= operator requires column name")
                    })?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::NotEq(col, val))
            }
            ">" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, "> operator requires column name")
                    })?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Gt(col, val))
            }
            "<" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, "< operator requires column name")
                    })?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Lt(col, val))
            }
            ">=" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, ">= operator requires column name")
                    })?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Gte(col, val))
            }
            "<=" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, "<= operator requires column name")
                    })?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Lte(col, val))
            }
            "in" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, "in operator requires column name")
                    })?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::In(col, val))
            }
            "not-in" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(
                            ErrorCode::Aeg001,
                            "not-in operator requires column name",
                        )
                    })?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::NotIn(col, val))
            }
            "between" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(
                            ErrorCode::Aeg001,
                            "between operator requires column name",
                        )
                    })?
                    .to_string();
                let val = arr.get(2).cloned().unwrap_or(Value::Null);
                Ok(WhereNode::Between(col, val))
            }
            "matches" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(
                            ErrorCode::Aeg001,
                            "matches operator requires column name",
                        )
                    })?
                    .to_string();
                let pattern = arr
                    .get(2)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(
                            ErrorCode::Aeg001,
                            "matches operator requires pattern string",
                        )
                    })?
                    .to_string();
                Ok(WhereNode::Matches(col, pattern))
            }
            "like" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, "like operator requires column name")
                    })?
                    .to_string();
                let pattern = arr
                    .get(2)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(
                            ErrorCode::Aeg001,
                            "like operator requires pattern string",
                        )
                    })?
                    .to_string();
                Ok(WhereNode::Like(col, pattern))
            }
            "contains" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(
                            ErrorCode::Aeg001,
                            "contains operator requires column name",
                        )
                    })?
                    .to_string();
                let val = arr
                    .get(2)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(
                            ErrorCode::Aeg001,
                            "contains operator requires search string",
                        )
                    })?
                    .to_string();
                Ok(WhereNode::Contains(col, val))
            }
            "is-null" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(
                            ErrorCode::Aeg001,
                            "is-null operator requires column name",
                        )
                    })?
                    .to_string();
                Ok(WhereNode::IsNull(col))
            }
            "is-not-null" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(
                            ErrorCode::Aeg001,
                            "is-not-null operator requires column name",
                        )
                    })?
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
                let child = arr.get(1).ok_or_else(|| {
                    DomainError::aegis(ErrorCode::Aeg001, "not operator requires child node")
                })?;
                Ok(WhereNode::Not(Box::new(Self::from_value(child)?)))
            }
            "fuzzy" => {
                let col = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, "fuzzy operator requires column name")
                    })?
                    .to_string();
                let term = arr
                    .get(2)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, "fuzzy operator requires search term")
                    })?
                    .to_string();
                Ok(WhereNode::Fuzzy(col, term))
            }
            "fts" => {
                let term = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(ErrorCode::Aeg001, "fts operator requires search term")
                    })?
                    .to_string();
                Ok(WhereNode::Fts(term))
            }
            "ref-filter" => {
                let ref_field = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        DomainError::aegis(
                            ErrorCode::Aeg001,
                            "ref-filter operator requires reference field",
                        )
                    })?
                    .to_string();
                let inner = arr.get(2).ok_or_else(|| {
                    DomainError::aegis(
                        ErrorCode::Aeg001,
                        "ref-filter operator requires inner condition",
                    )
                })?;
                Ok(WhereNode::RefFilter(
                    ref_field,
                    Box::new(Self::from_value(inner)?),
                ))
            }
            other => Err(DomainError::aegis(
                ErrorCode::Aeg001,
                format!("Unsupported WHERE operator: {other}"),
            )),
        }
    }
}

/// Definición de una métrica agregada dentro de una consulta analítica.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricDef {
    /// Nombre de la entidad objetivo de la métrica (opcional).
    pub entity: Option<String>,
    /// Nombre canónico del atributo a agregar (ej. `"asset/cost"`).
    pub attribute: Option<String>,
    /// Alias de campo alternativo cuando se especifica sin namespace.
    pub field: Option<String>,
    /// Segundo atributo requerido para agregaciones bivariadas (ej. correlación o regresión).
    pub secondary_attribute: Option<String>,
    /// Segundo campo alternativo para agregaciones bivariadas.
    pub secondary_field: Option<String>,
    /// Función de agregación solicitada (`"SUM"`, `"AVG"`, `"COUNT"`, `"MIN"`, `"MAX"`, `"P95"`, etc.).
    pub aggregation: Option<String>,
    /// Alias de salida asignado al resultado de la métrica.
    pub name: Option<String>,
    /// Filtro condicional específico de la métrica (`FILTER (WHERE ...)`).
    pub filter: Option<Value>,
}

/// Definición de una medida calculada post-agregación mediante el motor de fórmulas.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MeasureDef {
    /// Expresión aritmética o funcional que opera sobre métricas agregadas.
    pub formula: String,
    /// Nombre o alias de salida de la medida calculada.
    pub name: String,
}

/// Dimensión de agrupación (`GROUP BY`) categórica o temporal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Dimension {
    /// Nombre del atributo de agrupación.
    pub attribute: Option<String>,
    /// Nombre de campo alternativo de agrupación.
    pub field: Option<String>,
    /// Intervalo de truncamiento temporal si la dimensión es una marca de tiempo (`"day"`, `"month"`, etc.).
    pub interval: Option<String>,
}

/// Expresión de ordenamiento (`ORDER BY`) para los resultados de la consulta.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderByExpr {
    /// Campo por el cual ordenar.
    pub field: Option<String>,
    /// Atributo alternativo por el cual ordenar.
    pub attribute: Option<String>,
    /// Indica si el ordenamiento es descendente (`true`) o ascendente (`false` / `None`).
    pub descending: Option<bool>,
}

/// Especificación de comparación temporal o de referencia en el AST.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComparisonDef {
    /// Tipo de comparación (`"TIME_SHIFT_RELATIVE"`, `"TIME_SHIFT_SHORTCUT"`, `"BENCHMARK"`, `"SMART"`, etc.).
    #[serde(rename = "type")]
    pub comp_type: String,
    /// Granularidad del desplazamiento relativo (`"day"`, `"week"`, `"month"`, `"quarter"`, `"year"`).
    pub relative_granularity: Option<String>,
    /// Cantidad de unidades a desplazar en una comparación relativa.
    pub relative_amount: Option<i64>,
    /// Identificador del atajo temporal (`"PREVIOUS_PERIOD"`, `"SAME_PERIOD_LAST_YEAR"`, etc.).
    pub shortcut: Option<String>,
    /// Límite inferior explícito en segundos epoch para comparación absoluta.
    pub absolute_start_ts: Option<i64>,
    /// Límite superior explícito en segundos epoch para comparación absoluta.
    pub absolute_end_ts: Option<i64>,
    /// Valor objetivo constante cuando `comp_type` es `"BENCHMARK"`.
    pub benchmark_value: Option<f64>,
    /// Etiqueta descriptiva opcional para la serie comparativa.
    pub label: Option<String>,
}

/// Configuración de exploración jerárquica padre-hijo (`drill-down`) sobre entidades autorreferenciadas.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HierarchyDef {
    /// Nombre del atributo que apunta al identificador del nodo padre (ej. `"parent_id"`).
    pub parent_field: Option<String>,
    /// Identificador del nodo padre actualmente expandido (`None` o `Null` para el nivel raíz).
    pub current_node_id: Option<Value>,
    /// Indica si debe calcularse e inyectarse el indicador booleano `_has_children` en cada fila.
    pub inject_has_children: Option<bool>,
}

/// Metadatos de esquema de la entidad inyectados en el AST para guiar al ejecutor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaInfo {
    /// Nombre canónico de la entidad.
    pub entity: String,
    /// Lista de descriptores de atributos pertenecientes a la entidad.
    pub attributes: Vec<SchemaAttribute>,
}

/// Descriptor de tipo de un atributo individual dentro de `SchemaInfo`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaAttribute {
    /// Nombre del atributo.
    pub name: String,
    /// Tipo semántico del atributo en el Códice.
    pub attr_type: Option<String>,
    /// Alias de tipo físico serializado como `"type"`.
    #[serde(rename = "type")]
    pub field_type: Option<String>,
}

/// Referencia a una medida semántica predefinida en el catálogo del Códice.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SemanticMeasureDef {
    /// Clave canónica de la métrica semántica.
    pub metric_key: String,
}

/// Representación intermedia tipada completa (`AstIr`) de una consulta analítica o transaccional.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AstIr {
    /// Nombre de la entidad principal consultada.
    pub entity: String,
    /// Formato de proyección de salida (`KPI`, `TIMESERIES`, `TABLE`, `PIE`, `BUBBLE`, `CsvExport`).
    pub output_cast: OutputCast,
    /// Árbol tipado de condiciones de filtrado (`WHERE`).
    pub where_clause: Option<WhereNode>,
    /// Lista de atributos solicitados en la proyección escalar (`SELECT`).
    pub select: Option<Vec<String>>,
    /// Lista de dimensiones de agrupación (`GROUP BY`).
    pub group_by: Option<Vec<Dimension>>,
    /// Lista de criterios de ordenamiento (`ORDER BY`).
    pub order_by: Option<Vec<OrderByExpr>>,
    /// Conjunto de métricas agregadas a computar.
    pub metrics: Option<Vec<MetricDef>>,
    /// Conjunto de fórmulas post-agregación a evaluar.
    pub measures: Option<Vec<MeasureDef>>,
    /// Conjunto de medidas semánticas del Códice invocadas.
    pub semantic_measures: Option<Vec<SemanticMeasureDef>>,
    /// Especificaciones de comparación temporal o benchmark.
    pub comparisons: Option<Vec<ComparisonDef>>,
    /// Configuración de navegación jerárquica (`drill-down`).
    pub hierarchy: Option<HierarchyDef>,
    /// Límite máximo de filas a retornar (`LIMIT`).
    pub limit: Option<u64>,
    /// Información de esquema de la entidad consultada.
    pub schema: Option<SchemaInfo>,
}

impl AstIr {
    /// Construye y valida un `AstIr` tipado a partir de su representación JSON (`Value`).
    ///
    /// Retorna `ErrorCode::Aeg001` si falta el campo obligatorio `"entity"` o si la cláusula
    /// `"where"` contiene operadores malformados.
    pub fn from_value(v: &Value) -> Result<Self, DomainError> {
        let entity = v
            .get("entity")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DomainError::aegis(ErrorCode::Aeg001, "Missing 'entity' field in AST"))?
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
