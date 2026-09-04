// infrastructure/local_s3_query_engine/mod.rs — Motor de consultas sobre S3 para entornos locales.
// Lee los datos REALES escritos por Firehose (OlapChannel) directamente desde S3,
// sin depender de Athena (no disponible en LocalStack).
//
// Contrato:
//   - Implementa IQueryEngine (start_query / get_query_results)
//   - Schema-driven: usa Codice para coerción de tipos
//   - Entity-agnostic: funciona para cualquier entidad OLAP
//
// Fase 5 (PLAN_CORRECCIONES_PENDIENTES): descompuesto en módulos por
// responsabilidad, cada uno con su red propia:
//   - sql_parse: parseo del SQL de Aegis (entidad, proyecciones, filtros, CTEs)
//   - el pipeline en memoria (filtros, agregación, proyección) y la lectura
//     de S3 se extraen en commits propios sobre este mismo árbol.

mod sql_parse;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{Datelike, TimeZone, Timelike};
use serde_json::Value;
use tracing::info;

use crate::codice::registry::{self as codice_registry, AttrType};
use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::{IQueryEngine, QueryResults};

pub(crate) use sql_parse::*;

/// Motor de consultas local que lee datos de S3 escritos por Firehose.
pub struct LocalS3QueryEngine {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    /// Almacén en memoria de SQL por execution_id (misma estrategia que el polling de Athena).
    queries: Arc<Mutex<HashMap<String, String>>>,
    /// Caché en memoria para evitar leer repetidamente miles de archivos de S3 local en desarrollo.
    /// Clave: S3 Object Key, Valor: Lista de registros crudos JSON de ese archivo.
    cache: Mutex<HashMap<String, Vec<serde_json::Map<String, Value>>>>,
}

impl LocalS3QueryEngine {
    /// Crea un nuevo motor S3 local.
    pub async fn new(bucket: impl Into<String>) -> Self {
        let region_provider =
            aws_config::meta::region::RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::from_env().region(region_provider).load().await;

        let s3_client = if let Ok(endpoint_url) = std::env::var("AWS_ENDPOINT_URL") {
            info!("[LocalS3QueryEngine] Usando endpoint S3: {}", endpoint_url);
            let s3_config = aws_sdk_s3::config::Builder::from(&config)
                .endpoint_url(endpoint_url)
                .force_path_style(true)
                .build();
            aws_sdk_s3::Client::from_conf(s3_config)
        } else {
            aws_sdk_s3::Client::new(&config)
        };

        let b = bucket.into();
        info!("[LocalS3QueryEngine] Activo | bucket: {}", b);
        Self {
            s3_client,
            bucket: b,
            queries: Arc::new(Mutex::new(HashMap::new())),
            cache: Mutex::new(HashMap::new()),
        }
    }
}

#[derive(Clone)]
struct GroupState {
    group_values: Vec<Value>,
    agg_states: Vec<AggState>,
}

#[derive(Clone)]
struct AggState {
    count: i64,
    sum: f64,
    min: f64,
    max: f64,
    has_values: bool,
}

#[async_trait]
impl IQueryEngine for LocalS3QueryEngine {
    /// Almacena el SQL y retorna un execution_id inmediatamente.
    async fn start_query(&self, sql: &str, _database: &str) -> Result<String, DomainError> {
        info!("[LocalS3QueryEngine] start_query SQL: {}", sql);
        let exec_id = format!("local-s3-{}", uuid::Uuid::new_v4());
        if let Ok(mut map) = self.queries.lock() {
            map.insert(exec_id.clone(), sql.to_string());
        }
        Ok(exec_id)
    }

    /// Lee datos reales de S3, aplica filtros, agrupaciones y agregaciones en memoria.
    async fn get_query_results(&self, execution_id: &str) -> Result<QueryResults, DomainError> {
        let sql = {
            let map = self.queries.lock().map_err(|_| {
                DomainError::aegis(ErrorCode::Aeg004, "Lock poisoned en LocalS3QueryEngine")
            })?;
            map.get(execution_id).cloned().unwrap_or_default()
        };

        if sql.is_empty() {
            return Err(DomainError::aegis(
                ErrorCode::Aeg004,
                format!("No se encontró SQL para execution_id: {execution_id}"),
            ));
        }

        // Si la consulta empieza por WITH, es una CTE
        if let Some((ctes, final_select)) = parse_cte_queries(&sql) {
            tracing::info!(
                "[LocalS3QueryEngine] Detectada consulta CTE con {} subconsultas.",
                ctes.len()
            );

            // 1. Ejecutar cada subconsulta
            let mut current_results = None;
            let mut prev_results = HashMap::new();
            let mut smart_results = HashMap::new();

            for (name, cte_sql) in ctes {
                let res = self.execute_single_query(&cte_sql).await?;
                if name == "current_data" {
                    current_results = Some(res);
                } else if name.starts_with("prev_") {
                    prev_results.insert(name, res.rows);
                } else if name.starts_with("smart_") {
                    smart_results.insert(name, res.rows);
                }
            }

            let curr = current_results.ok_or_else(|| {
                DomainError::aegis(
                    ErrorCode::Aeg004,
                    "Subconsulta 'current_data' requerida en la CTE",
                )
            })?;

            // 2. Extraer columnas y límites de la consulta final
            let projected_info = extract_projected_columns(&final_select);
            let projected_columns: Vec<String> =
                projected_info.iter().map(|c| c.alias.clone()).collect();
            let limit = extract_limit_from_sql(&final_select);

            // 3. Unir resultados por índice (o por producto cartesiano)
            let mut rows = Vec::new();
            for (idx, curr_row) in curr.rows.iter().enumerate() {
                let mut combined = HashMap::new();
                for col in &projected_info {
                    let val = evaluate_final_expression(
                        &col.expr,
                        Some(curr_row),
                        &prev_results,
                        &smart_results,
                        idx,
                    );
                    combined.insert(col.alias.clone(), val);
                }
                rows.push(combined);
            }

            // Ordenar final por timestamp o bucket
            rows.sort_by(|a, b| {
                let ts_a = extract_ts(a);
                let ts_b = extract_ts(b);
                ts_b.cmp(&ts_a)
            });

            if let Some(lim) = limit {
                rows.truncate(lim as usize);
            }

            tracing::info!(
                "[LocalS3QueryEngine] Retornando {} filas de consulta CTE",
                rows.len()
            );
            return Ok(QueryResults {
                columns: projected_columns,
                rows,
            });
        }

        // Consulta normal sin WITH
        self.execute_single_query(&sql).await
    }
}

impl LocalS3QueryEngine {
    /// Ejecuta una única consulta analítica estándar no-CTE y retorna los resultados aggregados.
    async fn execute_single_query(&self, sql: &str) -> Result<QueryResults, DomainError> {
        // 1. Extraer entidad del SQL (FROM {db}.{entity} o FROM {entity})
        let entity = extract_entity_from_sql(sql).ok_or_else(|| {
            DomainError::aegis(ErrorCode::Aeg004, "No se pudo extraer la entidad del SQL")
        })?;

        // 2. Extraer columnas de proyección del SQL con info de agregación
        let projected_info = extract_projected_columns(sql);
        let projected_columns: Vec<String> =
            projected_info.iter().map(|c| c.alias.clone()).collect();
        if projected_columns.is_empty() {
            return Err(DomainError::aegis(
                ErrorCode::Aeg004,
                "No se pudieron extraer columnas de la consulta SQL",
            ));
        }

        // 3. Obtener schema del modelo para coerción de tipos
        let attr_types = build_attr_type_map(&entity);

        // 4. Extraer LIMIT del SQL
        let limit = extract_limit_from_sql(sql);

        // 5. Determinar filtros y rango temporal del SQL
        let filters = extract_filters_from_sql(sql);
        let (start_ts, end_ts) = extract_timestamp_range_from_sql(sql);

        let any_is_aggregate = projected_info.iter().any(|c| c.is_aggregate);

        // 6. Leer datos de S3: s3://{bucket}/iceberg-data/{entity}/
        let prefix = format!("iceberg-data/{entity}/");
        info!(
            "[LocalS3QueryEngine] Leyendo s3://{}/{} | entity={} cols={:?} limit={:?} aggregates={}",
            self.bucket, prefix, entity, projected_columns, limit, any_is_aggregate
        );

        let mut all_objects = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self
                .s3_client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }
            let list_res = req.send().await.map_err(|e| {
                DomainError::aegis(ErrorCode::Aeg004, format!("S3 list falló: {e}"))
            })?;

            if let Some(objects) = list_res.contents {
                all_objects.extend(objects);
            }

            if let Some(token) = list_res.next_continuation_token {
                continuation_token = Some(token.to_string());
            } else {
                break;
            }
        }

        let mut fail_log_count = 0;
        let mut raw_records: Vec<serde_json::Map<String, Value>> = Vec::new();
        // Límite de escaneo alto pero seguro para desarrollo local
        let max_scan_records = if any_is_aggregate {
            50000
        } else {
            limit.unwrap_or(2000) as usize
        };

        // Ordenar por fecha de modificación descendente (más recientes primero)
        all_objects.sort_by(|a, b| b.last_modified().cmp(&a.last_modified()));

        // 1. Identificar qué llaves ya están en caché y cuáles necesitamos descargar
        let mut keys_to_fetch = Vec::new();
        {
            let cache_map = self.cache.lock().map_err(|_| {
                DomainError::aegis(
                    ErrorCode::Aeg004,
                    "Lock poisoned en el cache de LocalS3QueryEngine",
                )
            })?;
            for obj in &all_objects {
                let key = match obj.key() {
                    Some(k) if !k.ends_with('/') => k.to_string(),
                    _ => continue,
                };
                if let Some(cached_rows) = cache_map.get(&key) {
                    raw_records.extend(cached_rows.clone());
                } else {
                    keys_to_fetch.push(key);
                }
            }
        }

        // 2. Descargar en paralelo solo las llaves que no están en el caché
        let mut new_records = Vec::new();
        if !keys_to_fetch.is_empty() {
            tracing::info!(
                "[LocalS3QueryEngine] {} llaves no encontradas en caché. Descargando de S3...",
                keys_to_fetch.len()
            );

            use futures::StreamExt;
            let s3_client = self.s3_client.clone();
            let bucket = self.bucket.clone();

            let mut stream = futures::stream::iter(keys_to_fetch)
                .map(move |key| {
                    let s3 = s3_client.clone();
                    let b = bucket.clone();
                    async move {
                        let get_res = s3.get_object().bucket(&b).key(&key).send().await;
                        match get_res {
                            Ok(res) => {
                                match res.body.collect().await {
                                    Ok(data) => Some((key, data.into_bytes())),
                                    Err(e) => {
                                        tracing::warn!("[LocalS3QueryEngine] body collect falló para key {}: {:?}", key, e);
                                        None
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("[LocalS3QueryEngine] get_object falló para key {}: {:?}", key, e);
                                None
                            }
                        }
                    }
                })
                .buffered(40); // 40 requests concurrently

            let mut temp_cache = HashMap::new();
            while let Some(maybe_bytes) = stream.next().await {
                if let Some((key, bytes)) = maybe_bytes {
                    let content = String::from_utf8_lossy(&bytes);
                    let mut file_records = Vec::new();
                    for line in content.lines() {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        let val: Value = match serde_json::from_str(trimmed) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if let Some(obj_map) = val.as_object() {
                            file_records.push(obj_map.clone());
                        }
                    }
                    temp_cache.insert(key, file_records.clone());
                    new_records.extend(file_records);
                }
            }

            // Guardar las nuevas llaves en la caché global
            if !temp_cache.is_empty() {
                if let Ok(mut cache_map) = self.cache.lock() {
                    for (k, v) in temp_cache {
                        cache_map.insert(k, v);
                    }
                }
            }
        }

        raw_records.extend(new_records);

        // 3. Aplicar filtros y rango de tiempo en memoria sobre todos los registros
        let mut filtered_records = Vec::new();
        for obj_map in &raw_records {
            if filtered_records.len() >= max_scan_records {
                break;
            }

            // Aplicar filtros en memoria (igualdad e IN de campos de filtros)
            let mut matches_filters = true;
            for (col, filter_val) in &filters {
                let cleaned_col = clean_expr_field(col);
                if cleaned_col == "1" || cleaned_col == "true" {
                    continue;
                }
                if let Some(record_val) = obj_map.get(&cleaned_col) {
                    let coerced = coerce_value(record_val, &cleaned_col, &attr_types);
                    match filter_val {
                        Value::Array(arr) => {
                            if !arr.contains(&coerced) {
                                if fail_log_count < 10 {
                                    tracing::info!("[LocalS3QueryEngine] Filtro IN falló: col={} cleaned_col={} record_val={:?} coerced={:?} filter_val={:?}", col, cleaned_col, record_val, coerced, filter_val);
                                    fail_log_count += 1;
                                }
                                matches_filters = false;
                                break;
                            }
                        }
                        _ => {
                            if coerced != *filter_val {
                                if fail_log_count < 10 {
                                    tracing::info!("[LocalS3QueryEngine] Filtro EQ falló: col={} cleaned_col={} record_val={:?} coerced={:?} filter_val={:?}", col, cleaned_col, record_val, coerced, filter_val);
                                    fail_log_count += 1;
                                }
                                matches_filters = false;
                                break;
                            }
                        }
                    }
                } else {
                    match filter_val {
                        Value::Array(arr) => {
                            if !arr.contains(&Value::Null) {
                                if fail_log_count < 10 {
                                    tracing::info!("[LocalS3QueryEngine] Filtro IN (no col) falló: col={} cleaned_col={} filter_val={:?}", col, cleaned_col, filter_val);
                                    fail_log_count += 1;
                                }
                                matches_filters = false;
                                break;
                            }
                        }
                        _ => {
                            if *filter_val != Value::Null {
                                if fail_log_count < 10 {
                                    tracing::info!("[LocalS3QueryEngine] Filtro EQ (no col) falló: col={} cleaned_col={} filter_val={:?}", col, cleaned_col, filter_val);
                                    fail_log_count += 1;
                                }
                                matches_filters = false;
                                break;
                            }
                        }
                    }
                }
            }
            if !matches_filters {
                continue;
            }

            // Filtrar por rango de tiempo si se especifica
            if start_ts.is_some() || end_ts.is_some() {
                let record_ts = obj_map
                    .get("timestamp")
                    .or_else(|| obj_map.get("created_at"))
                    .and_then(|v| match v {
                        Value::Number(n) => n.as_i64(),
                        Value::String(s) => s.parse::<i64>().ok(),
                        _ => None,
                    });
                if let Some(r_ts) = record_ts {
                    let r_ts_sec = if r_ts > 100000000000 {
                        r_ts / 1000
                    } else {
                        r_ts
                    };
                    if let Some(start) = start_ts {
                        let start_sec = if start > 100000000000 {
                            start / 1000
                        } else {
                            start
                        };
                        if r_ts_sec < start_sec {
                            continue;
                        }
                    }
                    if let Some(end) = end_ts {
                        let end_sec = if end > 100000000000 { end / 1000 } else { end };
                        if r_ts_sec > end_sec {
                            continue;
                        }
                    }
                }
            }

            filtered_records.push(obj_map.clone());
        }

        let raw_records = filtered_records;

        let mut rows: Vec<HashMap<String, Value>> = Vec::new();

        if any_is_aggregate {
            let group_cols: Vec<&ProjectedColumn> =
                projected_info.iter().filter(|c| !c.is_aggregate).collect();
            let agg_cols: Vec<&ProjectedColumn> =
                projected_info.iter().filter(|c| c.is_aggregate).collect();

            let mut groups: HashMap<Vec<String>, GroupState> = HashMap::new();

            for obj_map in &raw_records {
                let mut group_key = Vec::new();
                let mut group_values = Vec::new();
                for col in &group_cols {
                    let val = evaluate_column(col, obj_map, &attr_types);
                    group_key.push(serde_json::to_string(&val).unwrap_or_default());
                    group_values.push(val);
                }

                let state = groups.entry(group_key).or_insert_with(|| GroupState {
                    group_values,
                    agg_states: vec![
                        AggState {
                            count: 0,
                            sum: 0.0,
                            min: f64::MAX,
                            max: f64::MIN,
                            has_values: false,
                        };
                        agg_cols.len()
                    ],
                });

                for (i, col) in agg_cols.iter().enumerate() {
                    let agg_state = &mut state.agg_states[i];
                    let field_name = col.agg_field.as_deref().unwrap_or("*");
                    let cleaned_field = clean_expr_field(field_name);

                    if col.agg_fn.as_deref() == Some("COUNT") {
                        let should_count = if field_name == "*" {
                            true
                        } else {
                            obj_map
                                .get(&cleaned_field)
                                .map(|v| !v.is_null())
                                .unwrap_or(false)
                        };
                        if should_count {
                            agg_state.count += 1;
                            agg_state.has_values = true;
                        }
                    } else {
                        let val_opt = obj_map.get(&cleaned_field).and_then(|v| match v {
                            Value::Number(n) => n.as_f64(),
                            Value::String(s) => s.parse::<f64>().ok(),
                            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
                            _ => None,
                        });

                        if let Some(v) = val_opt {
                            match col.agg_fn.as_deref() {
                                Some("SUM") => {
                                    agg_state.sum += v;
                                    agg_state.has_values = true;
                                }
                                Some("AVG") => {
                                    agg_state.sum += v;
                                    agg_state.count += 1;
                                    agg_state.has_values = true;
                                }
                                Some("MIN") => {
                                    if v < agg_state.min {
                                        agg_state.min = v;
                                    }
                                    agg_state.has_values = true;
                                }
                                Some("MAX") => {
                                    if v > agg_state.max {
                                        agg_state.max = v;
                                    }
                                    agg_state.has_values = true;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }

            let is_groups_empty = groups.is_empty();
            for state in groups.into_values() {
                let mut row = HashMap::new();
                for (i, col) in group_cols.iter().enumerate() {
                    let val = &state.group_values[i];
                    row.insert(col.alias.clone(), val.clone());
                }
                for (i, col) in agg_cols.iter().enumerate() {
                    let agg_state = &state.agg_states[i];
                    let val = match col.agg_fn.as_deref() {
                        Some("COUNT") => Value::Number(serde_json::Number::from(agg_state.count)),
                        Some("SUM") => {
                            if agg_state.has_values {
                                Value::Number(
                                    serde_json::Number::from_f64(agg_state.sum)
                                        .unwrap_or(serde_json::Number::from(0)),
                                )
                            } else {
                                Value::Null
                            }
                        }
                        Some("AVG") => {
                            if agg_state.has_values && agg_state.count > 0 {
                                let avg = agg_state.sum / (agg_state.count as f64);
                                Value::Number(
                                    serde_json::Number::from_f64(avg)
                                        .unwrap_or(serde_json::Number::from(0)),
                                )
                            } else {
                                Value::Null
                            }
                        }
                        Some("MIN") => {
                            if agg_state.has_values {
                                Value::Number(
                                    serde_json::Number::from_f64(agg_state.min)
                                        .unwrap_or(serde_json::Number::from(0)),
                                )
                            } else {
                                Value::Null
                            }
                        }
                        Some("MAX") => {
                            if agg_state.has_values {
                                Value::Number(
                                    serde_json::Number::from_f64(agg_state.max)
                                        .unwrap_or(serde_json::Number::from(0)),
                                )
                            } else {
                                Value::Null
                            }
                        }
                        _ => Value::Null,
                    };
                    row.insert(col.alias.clone(), val);
                }
                rows.push(row);
            }
            if is_groups_empty && group_cols.is_empty() {
                let mut row = HashMap::new();
                for col in &agg_cols {
                    let val = match col.agg_fn.as_deref() {
                        Some("COUNT") => Value::Number(serde_json::Number::from(0)),
                        _ => Value::Null,
                    };
                    row.insert(col.alias.clone(), val);
                }
                rows.push(row);
            }
        } else {
            // Ruta normal no agregada
            for obj_map in &raw_records {
                let mut row = HashMap::new();
                for col in &projected_info {
                    let field_val = evaluate_column(col, obj_map, &attr_types);
                    row.insert(col.alias.clone(), field_val);
                }
                rows.push(row);
            }
        }

        // Ordenar por timestamp descendente
        rows.sort_by(|a, b| {
            let ts_a = extract_ts(a);
            let ts_b = extract_ts(b);
            ts_b.cmp(&ts_a)
        });

        if let Some(lim) = limit {
            rows.truncate(lim as usize);
        }

        info!(
            "[LocalS3QueryEngine] Retornando {} filas procesadas para entity={}",
            rows.len(),
            entity
        );

        Ok(QueryResults {
            columns: projected_columns,
            rows,
        })
    }
}

// ─── Funciones auxiliares internas (pipeline en memoria — se extraen en commit propio) ───

/// Trunca el timestamp (ms o sec) al intervalo indicado.
fn truncate_timestamp(ts_ms_or_sec: i64, interval: &str) -> i64 {
    let ts_sec = if ts_ms_or_sec > 100000000000 {
        ts_ms_or_sec / 1000
    } else {
        ts_ms_or_sec
    };

    let dt = chrono::Utc.timestamp_opt(ts_sec, 0).unwrap();
    let truncated_dt = match interval.to_lowercase().as_str() {
        "minute" => dt
            .with_second(0)
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(dt),
        "hour" => dt
            .with_minute(0)
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(dt),
        "day" => dt
            .with_hour(0)
            .and_then(|t| t.with_minute(0))
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(dt),
        "week" => {
            let days_from_monday = match dt.weekday() {
                chrono::Weekday::Mon => 0,
                chrono::Weekday::Tue => 1,
                chrono::Weekday::Wed => 2,
                chrono::Weekday::Thu => 3,
                chrono::Weekday::Fri => 4,
                chrono::Weekday::Sat => 5,
                chrono::Weekday::Sun => 6,
            };
            let truncated = dt - chrono::Duration::days(days_from_monday);
            truncated
                .with_hour(0)
                .and_then(|t| t.with_minute(0))
                .and_then(|t| t.with_second(0))
                .and_then(|t| t.with_nanosecond(0))
                .unwrap_or(dt)
        }
        "month" => dt
            .with_day(1)
            .and_then(|t| t.with_hour(0))
            .and_then(|t| t.with_minute(0))
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(dt),
        "year" => dt
            .with_month(1)
            .and_then(|t| t.with_day(1))
            .and_then(|t| t.with_hour(0))
            .and_then(|t| t.with_minute(0))
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(dt),
        _ => dt,
    };

    if ts_ms_or_sec > 100000000000 {
        truncated_dt.timestamp() * 1000
    } else {
        truncated_dt.timestamp()
    }
}

/// Evalúa el valor de la columna para un registro de S3.
fn evaluate_column(
    col: &ProjectedColumn,
    obj_map: &serde_json::Map<String, Value>,
    attr_types: &HashMap<String, AttrType>,
) -> Value {
    let lower_expr = col.expr.to_lowercase();
    if lower_expr.contains("date_trunc") {
        let interval = if lower_expr.contains("'minute'") {
            "minute"
        } else if lower_expr.contains("'hour'") {
            "hour"
        } else if lower_expr.contains("'week'") {
            "week"
        } else if lower_expr.contains("'month'") {
            "month"
        } else if lower_expr.contains("'year'") {
            "year"
        } else {
            "day"
        };

        let field_name = if lower_expr.contains("created_at") {
            "created_at"
        } else if lower_expr.contains("event_ts") {
            "event_ts"
        } else {
            "timestamp"
        };

        if let Some(raw_val) = obj_map.get(field_name) {
            let ts = match raw_val {
                Value::Number(n) => n.as_i64().unwrap_or(0),
                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                _ => 0,
            };
            if ts > 0 {
                let truncated = truncate_timestamp(ts, interval);
                return Value::Number(serde_json::Number::from(truncated));
            }
        }
        Value::Null
    } else {
        let field_name = clean_expr_field(&col.expr);
        if let Some(val) = obj_map.get(&field_name) {
            coerce_value(val, &field_name, attr_types)
        } else {
            Value::Null
        }
    }
}

/// Construye un mapa de nombre_campo → AttrType desde el schema Codice.
fn build_attr_type_map(entity: &str) -> HashMap<String, AttrType> {
    let mut map = HashMap::new();
    if let Some(reg) = codice_registry::global_opt() {
        if let Some(attrs) = reg.get_attributes(entity) {
            for attr in attrs.iter() {
                map.insert(attr.name.clone(), attr.attr_type.clone());
            }
        }
    }
    map.insert("id".to_string(), AttrType::String);
    map.insert("_tenant".to_string(), AttrType::String);
    map.insert("tenant_id".to_string(), AttrType::String);
    map.insert("created_at".to_string(), AttrType::Epoch);
    map
}

/// Coerción de valor JSON usando el schema Codice.
fn coerce_value(val: &Value, col: &str, attr_types: &HashMap<String, AttrType>) -> Value {
    let attr_type = attr_types.get(col);

    match attr_type {
        Some(AttrType::Epoch) => match val {
            Value::Number(_) => val.clone(),
            Value::String(s) => s.parse::<i64>().map(Value::from).unwrap_or(val.clone()),
            _ => val.clone(),
        },
        Some(AttrType::Number) | Some(AttrType::Decimal) => match val {
            Value::Number(_) => val.clone(),
            Value::String(s) => {
                if let Ok(n) = s.parse::<f64>() {
                    Value::from(n)
                } else {
                    val.clone()
                }
            }
            _ => val.clone(),
        },
        Some(AttrType::Boolean) => match val {
            Value::Bool(_) => val.clone(),
            Value::String(s) => Value::Bool(s.eq_ignore_ascii_case("true")),
            _ => val.clone(),
        },
        _ => val.clone(),
    }
}

/// Extrae timestamp de un row para ordenamiento.
fn extract_ts(row: &HashMap<String, Value>) -> i64 {
    row.get("timestamp")
        .or_else(|| row.get("created_at"))
        .or_else(|| row.get("bucket"))
        .or_else(|| row.get("current_bucket"))
        .and_then(|v| match v {
            Value::Number(n) => n.as_i64(),
            Value::String(s) => s.parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "../tests/local_s3_query_engine_tests.rs"]
mod local_s3_query_engine_tests;
