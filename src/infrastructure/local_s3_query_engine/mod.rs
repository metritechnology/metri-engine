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
//   - pipeline: el camino en memoria (filtros, rango temporal, agregación,
//     proyección) — puro, testeado sin S3
//   - este archivo queda como orquestador + la lectura de S3 (que se extrae
//     en el commit siguiente)

mod lake_reader;
mod pipeline;
mod sql_parse;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use tracing::info;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::{IQueryEngine, QueryResults};

pub(crate) use pipeline::*;
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
    /// Ejecuta una única consulta analítica estándar no-CTE: parsea el SQL,
    /// lee el lake de S3 y delega el pipeline en memoria.
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
        let time_range = extract_timestamp_range_from_sql(sql);

        let any_is_aggregate = projected_info.iter().any(|c| c.is_aggregate);

        // 6. Leer datos de S3: s3://{bucket}/iceberg-data/{entity}/
        let prefix = format!("iceberg-data/{entity}/");
        info!(
            "[LocalS3QueryEngine] Leyendo s3://{}/{} | entity={} cols={:?} limit={:?} aggregates={}",
            self.bucket, prefix, entity, projected_columns, limit, any_is_aggregate
        );

        let all_objects = self.list_objects(&prefix).await?;

        // Límite de escaneo alto pero seguro para desarrollo local
        let max_scan_records = if any_is_aggregate {
            50000
        } else {
            limit.unwrap_or(2000) as usize
        };

        // Ordenar por fecha de modificación descendente (más recientes primero)
        let mut all_objects = all_objects;
        all_objects.sort_by(|a, b| b.last_modified().cmp(&a.last_modified()));

        let raw_records = self.fetch_records(all_objects).await?;

        // 7. Pipeline en memoria: filtros → agregación o proyección
        let filtered = apply_filters_and_range(
            &raw_records,
            &filters,
            time_range,
            &attr_types,
            max_scan_records,
        );

        let mut rows = if any_is_aggregate {
            aggregate(&filtered, &projected_info, &attr_types)
        } else {
            project_rows(&filtered, &projected_info, &attr_types)
        };

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

#[cfg(test)]
#[path = "../tests/local_s3_query_engine_tests.rs"]
mod local_s3_query_engine_tests;
