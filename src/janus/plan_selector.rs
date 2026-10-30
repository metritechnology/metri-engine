//! Selección del índice EAV óptimo (`plan_selector` — Paso 5 del Read Path).
//!
//! Analiza el AST IR compilado y selecciona el `EavQueryPlan` más eficiente:
//! Prioridad: `PointLookup` > `AvetRangeFilter` / `AvetSingleFilter` / `AvetIntersection` > `FtsSearch` > `AevtScan`.

use serde_json::Value;
use tracing::debug;

use crate::codice::registry::AttrType;
use crate::eav::types::datom::DatomValue;
use crate::eav::types::encoding::AvetBound;
use crate::janus::fbs;

/// Plan de ejecución seleccionado por el Plan Selector.
#[derive(Debug, Clone)]
pub enum EavQueryPlan {
    /// GET por ULID explícito → índice primario EAVT (1 lectura puntual, mínimo costo).
    PointLookup {
        /// Identificador ULID de la entidad buscada.
        entity_id: String,
    },

    /// `WHERE attr = value` (1 filtro indexado) → índice AVET.
    AvetSingleFilter {
        /// Nombre del atributo indexado.
        attr_name: String,
        /// Valor tipado a buscar en el índice AVET.
        value: DatomValue,
    },

    /// `WHERE attr1 = v1 AND attr2 = v2` → 2+ consultas AVET en paralelo + intersección `HashSet`.
    AvetIntersection {
        /// Pares `(attr_name, valor)` que deben satisfacerse simultáneamente.
        filters: Vec<(String, DatomValue)>,
    },

    /// `WHERE attr < v` / `<=` / `>` / `>=` / `BETWEEN` → recorrido BTree en `GSI-AVET`,
    /// opcionalmente intersectado con predicados de igualdad indexados (`eq_filters`).
    AvetRangeFilter {
        /// Nombre del atributo indexado del rango.
        attr_name: String,
        /// Cota inferior del rango (`Unbounded`, `Inclusive`, `Exclusive`).
        lower: AvetBound,
        /// Cota superior del rango (`Unbounded`, `Inclusive`, `Exclusive`).
        upper: AvetBound,
        /// Filtros adicionales de igualdad (`=`) sobre atributos indexados.
        eq_filters: Vec<(String, DatomValue)>,
    },

    /// Búsqueda difusa / texto completo → índice FTS de trigramas + reconstitución EAVT.
    FtsSearch {
        /// Término de búsqueda de texto libre.
        term: String,
    },

    /// Escaneo por tipo de entidad (sin filtros de igualdad indexados) → índice AEVT.
    AevtScan {
        /// Nombre canónico del tipo de entidad a escanear.
        entity_type: String,
        /// Número opcional de particiones (*shards*) para escaneo paralelo.
        shard_total: Option<u8>,
    },

    /// Búsqueda de referencias inversas → índice VAET.
    VaetLookup {
        /// Identificador de la entidad destino referenciada.
        ref_entity_id: String,
        /// Nombre opcional del atributo de tipo referencia (`Ref`).
        attr_name: Option<String>,
    },

    /// Lectura histórica *time-travel* (`as-of` un `tx_id`) → EAVT con corte `tx <= as_of_tx`.
    AsOfSnapshot {
        /// Identificador de la entidad a reconstruir en el pasado.
        entity_id: String,
        /// Identificador de transacción máximo (inclusive) del corte histórico.
        as_of_tx: u64,
    },
}

/// Selecciona el plan de ejecución EAV más eficiente a partir del AST IR.
pub fn select_plan(ast_ir: &Value) -> EavQueryPlan {
    let entity_type = ast_ir
        .get("entity")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    if let Some(ulid) = extract_ulid_filter(ast_ir) {
        debug!("[PlanSelector] → PointLookup entity_id={ulid}");
        return EavQueryPlan::PointLookup { entity_id: ulid };
    }

    if let Some(as_of) = ast_ir
        .get("as_of_tx")
        .and_then(|v| v.as_u64())
        .filter(|&t| t > 0)
    {
        let entity_id = extract_ulid_filter(ast_ir).unwrap_or_default();
        debug!("[PlanSelector] → AsOfSnapshot entity_id={entity_id} as_of_tx={as_of}");
        return EavQueryPlan::AsOfSnapshot {
            entity_id,
            as_of_tx: as_of,
        };
    }

    if let Some(term) = ast_ir.get("search").and_then(|v| v.as_str()) {
        debug!("[PlanSelector] → FtsSearch term={term}");
        return EavQueryPlan::FtsSearch {
            term: term.to_string(),
        };
    }

    let (eq_filters, range_filters) = extract_json_filters(ast_ir, &entity_type);
    select_avet_or_aevt_plan(entity_type, eq_filters, range_filters)
}

/// Selecciona el plan de ejecución EAV usando `AnalyticsRequestT` nativo (FlatBuffers).
pub fn select_plan_fbs(ast_ir: &fbs::AnalyticsRequestT) -> EavQueryPlan {
    let entity_type = ast_ir.entity.as_deref().unwrap_or("unknown").to_string();

    if let Some(ulid) = extract_ulid_fbs(ast_ir) {
        debug!("[PlanSelector] → PointLookup entity_id={ulid}");
        return EavQueryPlan::PointLookup { entity_id: ulid };
    }

    if let Some(tf) = &ast_ir.time_frame {
        if tf.end_ts > 0 && tf.type_ == fbs::TimeFrameContext_TimeFilterType::CUSTOM_RANGE {
            if let Some(entity_id) = extract_ulid_fbs(ast_ir) {
                let as_of_tx = tf.end_ts as u64;
                debug!("[PlanSelector] → AsOfSnapshot entity_id={entity_id} as_of_tx={as_of_tx}");
                return EavQueryPlan::AsOfSnapshot {
                    entity_id,
                    as_of_tx,
                };
            }
        }
    }

    if let Some(term) = ast_ir.search.as_deref().filter(|s| !s.is_empty()) {
        debug!("[PlanSelector] → FtsSearch term={term}");
        return EavQueryPlan::FtsSearch {
            term: term.to_string(),
        };
    }

    let (eq_filters, range_filters) = extract_fbs_filters(ast_ir, &entity_type);
    select_avet_or_aevt_plan(entity_type, eq_filters, range_filters)
}

fn select_avet_or_aevt_plan(
    entity_type: String,
    indexed_filters: Vec<(String, DatomValue)>,
    range_filters: Vec<(String, AvetBound, AvetBound)>,
) -> EavQueryPlan {
    if let Some((attr_name, lower, upper)) = range_filters.into_iter().next() {
        debug!("[PlanSelector] → AvetRangeFilter attr={attr_name}");
        return EavQueryPlan::AvetRangeFilter {
            attr_name,
            lower,
            upper,
            eq_filters: indexed_filters,
        };
    }
    if indexed_filters.len() > 1 {
        debug!(
            "[PlanSelector] → AvetIntersection filters={}",
            indexed_filters.len()
        );
        return EavQueryPlan::AvetIntersection {
            filters: indexed_filters,
        };
    }
    if let Some((attr_name, value)) = indexed_filters.into_iter().next() {
        debug!("[PlanSelector] → AvetSingleFilter attr={attr_name}");
        return EavQueryPlan::AvetSingleFilter { attr_name, value };
    }
    debug!("[PlanSelector] → AevtScan entity_type={entity_type}");
    EavQueryPlan::AevtScan {
        entity_type,
        shard_total: None,
    }
}

fn extract_ulid_filter(ast_ir: &Value) -> Option<String> {
    search_ulid_in_node(ast_ir.get("where")?)
}

fn search_ulid_in_node(node: &Value) -> Option<String> {
    let arr = node.as_array()?;
    if arr.len() < 3 {
        return None;
    }
    let op = arr[0].as_str().unwrap_or("");
    let field = arr[1].as_str().unwrap_or("");
    if op == "=" && field == crate::eav::writer::system_attrs::ULID_NAME {
        return arr[2].as_str().map(str::to_string);
    }
    if op == "and" || op == "or" {
        return arr.iter().skip(1).find_map(search_ulid_in_node);
    }
    None
}

/// Resuelve el `AttrType` y bandera `indexed` de un atributo consultando el Códice (`global_opt`),
/// con respaldo heurístico seguro para timestamps de sistema y pruebas unitarias sin Códice.
fn resolve_codice_attr_meta(entity_type: &str, attr_name: &str) -> (Option<AttrType>, bool) {
    if attr_name == crate::eav::writer::system_attrs::CREATED_AT_NAME
        || attr_name == crate::eav::writer::system_attrs::UPDATED_AT_NAME
    {
        return (Some(AttrType::Epoch), true);
    }
    let bare = attr_name.split('/').next_back().unwrap_or(attr_name);
    if let Some(desc) =
        crate::codice::registry::global_opt().and_then(|r| r.get_attribute(entity_type, bare))
    {
        return (Some(desc.attr_type.clone()), desc.indexed);
    }
    let is_epoch = bare.ends_with("_date")
        || bare.ends_with("_at")
        || bare.ends_with("_time")
        || bare.starts_with("scheduled_")
        || bare.starts_with("actual_")
        || bare == "timestamp";
    if is_epoch {
        (Some(AttrType::Epoch), true)
    } else {
        (None, false)
    }
}

fn coerce_datom_value_for_attr(entity_type: &str, attr_name: &str, raw: DatomValue) -> DatomValue {
    match (resolve_codice_attr_meta(entity_type, attr_name).0, raw) {
        (Some(AttrType::Epoch), DatomValue::Long(n)) => DatomValue::Instant(n),
        (Some(AttrType::Epoch), DatomValue::Double(f)) => DatomValue::Instant(f as i64),
        (Some(AttrType::Epoch), DatomValue::Str(s)) => s
            .parse::<i64>()
            .map_or(DatomValue::Str(s), DatomValue::Instant),
        (Some(AttrType::Number), DatomValue::Instant(n)) => DatomValue::Long(n),
        (Some(AttrType::Number), DatomValue::Double(f)) => DatomValue::Long(f as i64),
        (Some(AttrType::Decimal), DatomValue::Long(n) | DatomValue::Instant(n)) => {
            DatomValue::Double(n as f64)
        }
        (Some(AttrType::Uuid), DatomValue::Str(s)) => DatomValue::Uuid(s),
        (_, other) => other,
    }
}

type FilterPair = (
    Vec<(String, DatomValue)>,
    Vec<(String, AvetBound, AvetBound)>,
);

fn extract_json_filters(ast_ir: &Value, entity_type: &str) -> FilterPair {
    let Some(where_clause) = ast_ir.get("where") else {
        return (vec![], vec![]);
    };
    let mut eq_out = Vec::new();
    let mut range_out = Vec::new();
    collect_json_filters(where_clause, entity_type, &mut eq_out, &mut range_out);
    (eq_out, merge_and_prioritize_ranges(entity_type, range_out))
}

fn collect_json_filters(
    node: &Value,
    entity_type: &str,
    eq_out: &mut Vec<(String, DatomValue)>,
    range_out: &mut Vec<(String, AvetBound, AvetBound)>,
) {
    let Some(arr) = node.as_array() else { return };
    let Some(op) = arr.first().and_then(|v| v.as_str()) else {
        return;
    };
    if op == "and" {
        for c in arr.iter().skip(1) {
            collect_json_filters(c, entity_type, eq_out, range_out);
        }
        return;
    }
    let field = arr.get(1).and_then(|v| v.as_str()).unwrap_or("");
    if is_non_avet_system_field(field) {
        return;
    }
    match op {
        "=" if arr.len() >= 3 => {
            let val = coerce_datom_value_for_attr(entity_type, field, json_to_datum_value(&arr[2]));
            eq_out.push((field.to_string(), val));
        }
        "<" | "<=" | ">" | ">=" if arr.len() >= 3 => {
            let val = coerce_datom_value_for_attr(entity_type, field, json_to_datum_value(&arr[2]));
            if let Some((lo, hi)) = bounds_from_op_str(op, val) {
                range_out.push((field.to_string(), lo, hi));
            }
        }
        "between" if arr.len() >= 4 => {
            let lo = coerce_datom_value_for_attr(entity_type, field, json_to_datum_value(&arr[2]));
            let hi = coerce_datom_value_for_attr(entity_type, field, json_to_datum_value(&arr[3]));
            if !matches!(lo, DatomValue::Null) && !matches!(hi, DatomValue::Null) {
                range_out.push((
                    field.to_string(),
                    AvetBound::Inclusive(lo),
                    AvetBound::Inclusive(hi),
                ));
            }
        }
        _ => {}
    }
}

fn bounds_from_op_str(op: &str, val: DatomValue) -> Option<(AvetBound, AvetBound)> {
    if matches!(val, DatomValue::Null) {
        return None;
    }
    match op {
        "<" => Some((AvetBound::Unbounded, AvetBound::Exclusive(val))),
        "<=" => Some((AvetBound::Unbounded, AvetBound::Inclusive(val))),
        ">" => Some((AvetBound::Exclusive(val), AvetBound::Unbounded)),
        ">=" => Some((AvetBound::Inclusive(val), AvetBound::Unbounded)),
        _ => None,
    }
}

fn is_non_avet_system_field(field: &str) -> bool {
    field.is_empty()
        || field.starts_with("entity/")
        || field == crate::eav::writer::system_attrs::TENANT_NAME
        || field == "1"
        || field.contains('.')
}

fn merge_and_prioritize_ranges(
    entity_type: &str,
    raw_ranges: Vec<(String, AvetBound, AvetBound)>,
) -> Vec<(String, AvetBound, AvetBound)> {
    let mut merged: Vec<(String, AvetBound, AvetBound)> = Vec::new();
    for (attr, lo, hi) in raw_ranges {
        if let Some((_, existing_lo, existing_hi)) = merged.iter_mut().find(|(a, _, _)| *a == attr)
        {
            if !matches!(lo, AvetBound::Unbounded) {
                *existing_lo = lo;
            }
            if !matches!(hi, AvetBound::Unbounded) {
                *existing_hi = hi;
            }
        } else {
            merged.push((attr, lo, hi));
        }
    }
    merged.sort_by_key(|(attr, _, _)| !resolve_codice_attr_meta(entity_type, attr).1);
    merged
}

fn json_to_datum_value(val: &Value) -> DatomValue {
    match val {
        Value::String(s) => DatomValue::Str(s.clone()),
        Value::Number(n) => n
            .as_i64()
            .map(DatomValue::Long)
            .or_else(|| n.as_f64().map(DatomValue::Double))
            .unwrap_or(DatomValue::Null),
        Value::Bool(b) => DatomValue::Bool(*b),
        _ => DatomValue::Null,
    }
}

fn extract_ulid_fbs(ast_ir: &fbs::AnalyticsRequestT) -> Option<String> {
    ast_ir.filters.as_ref()?.iter().find_map(|f| {
        let crit = f.criteria.as_ref()?;
        (crit.op_ref.0 == fbs::FilterOperator::EQ.0 && crit.field.as_deref() == Some("entity/ulid"))
            .then(|| crit.value.as_ref().and_then(|v| v.string_val.clone()))
            .flatten()
    })
}

fn normalize_fbs_attr_name(raw_attr: &str) -> String {
    if raw_attr.starts_with("meta/") {
        raw_attr.to_string()
    } else {
        raw_attr
            .split('/')
            .next_back()
            .unwrap_or(raw_attr)
            .to_string()
    }
}

fn fbs_filter_value_to_datom(entity_type: &str, attr: &str, v: &fbs::FilterValueT) -> DatomValue {
    let raw = if let Some(s) = &v.string_val {
        s.parse::<i64>()
            .map(DatomValue::Long)
            .or_else(|_| s.parse::<bool>().map(DatomValue::Bool))
            .unwrap_or_else(|_| DatomValue::Str(s.clone()))
    } else if v.timestamp_val != 0 {
        DatomValue::Instant(v.timestamp_val)
    } else if v.number_val != 0.0 {
        if v.number_val.fract() == 0.0 {
            DatomValue::Long(v.number_val as i64)
        } else {
            DatomValue::Double(v.number_val)
        }
    } else {
        DatomValue::Null
    };
    coerce_datom_value_for_attr(entity_type, attr, raw)
}

fn extract_fbs_filters(ast_ir: &fbs::AnalyticsRequestT, entity_type: &str) -> FilterPair {
    let mut eq_out = Vec::new();
    let mut range_out = Vec::new();
    if let Some(h) = &ast_ir.hierarchy {
        if let Some(node_id) = h
            .current_node_id
            .as_deref()
            .filter(|id| !id.is_empty() && *id != "__none__")
        {
            let pf = h.parent_field.as_deref().unwrap_or("parent_id");
            let bare_attr = pf.split('/').next_back().unwrap_or(pf).to_string();
            eq_out.push((bare_attr, DatomValue::Str(node_id.to_string())));
        }
    }
    for f in ast_ir.filters.as_deref().unwrap_or(&[]) {
        collect_fbs_node(f, entity_type, &mut eq_out, &mut range_out);
    }
    (eq_out, merge_and_prioritize_ranges(entity_type, range_out))
}

fn collect_fbs_node(
    node: &fbs::FilterNodeT,
    entity_type: &str,
    eq_out: &mut Vec<(String, DatomValue)>,
    range_out: &mut Vec<(String, AvetBound, AvetBound)>,
) {
    if let Some(crit) = &node.criteria {
        let raw_attr = crit.field.as_deref().unwrap_or_default();
        if is_non_avet_system_field(raw_attr) {
            return;
        }
        let attr = normalize_fbs_attr_name(raw_attr);
        let Some(fv) = crit.value.as_deref() else {
            return;
        };
        if crit.op_ref == fbs::FilterOperator::EQ {
            let val = fbs_filter_value_to_datom(entity_type, &attr, fv);
            if !matches!(val, DatomValue::Null) {
                eq_out.push((attr, val));
            }
        } else if let Some((lo, hi)) = fbs_criteria_to_bounds(entity_type, &attr, crit.op_ref, fv) {
            range_out.push((attr, lo, hi));
        }
    } else if let Some(group) = node.group.as_ref().filter(|g| g.conjunction.0 == 1) {
        for child in group.nodes.as_deref().unwrap_or(&[]) {
            collect_fbs_node(child, entity_type, eq_out, range_out);
        }
    }
}

fn fbs_criteria_to_bounds(
    entity_type: &str,
    attr: &str,
    op: fbs::FilterOperator,
    fv: &fbs::FilterValueT,
) -> Option<(AvetBound, AvetBound)> {
    let op_str = match op {
        fbs::FilterOperator::LT => Some("<"),
        fbs::FilterOperator::LTE => Some("<="),
        fbs::FilterOperator::GT => Some(">"),
        fbs::FilterOperator::GTE => Some(">="),
        _ => None,
    };
    if let Some(op_s) = op_str {
        return bounds_from_op_str(op_s, fbs_filter_value_to_datom(entity_type, attr, fv));
    }
    if op == fbs::FilterOperator::BETWEEN {
        let vals = fv.range_values.as_ref()?.values.as_ref()?;
        if vals.len() >= 2 {
            let lo = fbs_filter_value_to_datom(entity_type, attr, &vals[0]);
            let hi = fbs_filter_value_to_datom(entity_type, attr, &vals[1]);
            if !matches!(lo, DatomValue::Null) && !matches!(hi, DatomValue::Null) {
                return Some((AvetBound::Inclusive(lo), AvetBound::Inclusive(hi)));
            }
        }
    }
    None
}
