// infrastructure/athena.rs — AthenaQueryEngine implementando IQueryEngine.
// En el stack anterior: AthenaClient SDK v2 Java.
// En Rust:    aws-sdk-athena — polling de resultados con coerción de tipos.
//
// Este módulo es el cliente PURO de AWS Athena para producción.
// En desarrollo local (LocalStack), se usa LocalS3QueryEngine en su lugar.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_athena::Client;
use serde_json::Value;
use tokio::time::sleep;
use tracing::{debug, info};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::{IQueryEngine, QueryResults};

/// Motor de consultas Athena.
pub struct AthenaQueryEngine {
    client: Client,
    workgroup: String,
    output_location: String,
    database: String,
}

impl AthenaQueryEngine {
    /// Constructor desde configuración del entorno.
    pub async fn new(
        workgroup: impl Into<String>,
        output_location: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        let region_provider =
            aws_config::meta::region::RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::from_env().region(region_provider).load().await;

        let client = if let Ok(endpoint_url) = std::env::var("AWS_ENDPOINT_URL") {
            info!(
                "[Athena] Usando endpoint override de AWS_ENDPOINT_URL: {}",
                endpoint_url
            );
            let athena_config = aws_sdk_athena::config::Builder::from(&config)
                .endpoint_url(endpoint_url)
                .build();
            Client::from_conf(athena_config)
        } else if let Ok(endpoint_url) = std::env::var("ATHENA_ENDPOINT") {
            info!(
                "[Athena] Usando endpoint override de ATHENA_ENDPOINT: {}",
                endpoint_url
            );
            let athena_config = aws_sdk_athena::config::Builder::from(&config)
                .endpoint_url(endpoint_url)
                .build();
            Client::from_conf(athena_config)
        } else {
            Client::new(&config)
        };

        let wg = workgroup.into();
        info!("[Athena] cliente activo | workgroup: {wg}");
        AthenaQueryEngine {
            client,
            workgroup: wg,
            output_location: output_location.into(),
            database: database.into(),
        }
    }
}

#[async_trait]
impl IQueryEngine for AthenaQueryEngine {
    /// Inicia una query asíncrona y retorna el execution_id.
    async fn start_query(&self, sql: &str, database: &str) -> Result<String, DomainError> {
        let db = if database.is_empty() {
            &self.database
        } else {
            database
        };

        let resp = self
            .client
            .start_query_execution()
            .query_string(sql)
            .query_execution_context(
                aws_sdk_athena::types::QueryExecutionContext::builder()
                    .database(db)
                    .build(),
            )
            .result_configuration(
                aws_sdk_athena::types::ResultConfiguration::builder()
                    .output_location(&self.output_location)
                    .build(),
            )
            .work_group(&self.workgroup)
            .send()
            .await
            .map_err(|e| {
                DomainError::aegis(ErrorCode::Aeg002, format!("Athena start_query falló: {e}"))
            })?;

        let execution_id = resp.query_execution_id.ok_or_else(|| {
            DomainError::aegis(ErrorCode::Aeg002, "Sin execution_id en respuesta Athena")
        })?;

        Ok(execution_id)
    }

    /// Espera a que la query complete y retorna los resultados.
    async fn get_query_results(&self, execution_id: &str) -> Result<QueryResults, DomainError> {
        // // Polling con back-off exponencial — mismo patrón que el el stack anterior
        let mut delay = Duration::from_millis(200);
        const MAX_POLLS: usize = 30;

        for attempt in 0..MAX_POLLS {
            let status_resp = self
                .client
                .get_query_execution()
                .query_execution_id(execution_id)
                .send()
                .await
                .map_err(|e| DomainError::aegis(ErrorCode::Aeg002, format!("Poll falló: {e}")))?;

            let state = status_resp
                .query_execution
                .as_ref()
                .and_then(|qe| qe.status.as_ref())
                .and_then(|s| s.state.as_ref());

            match state.map(|s| s.as_str()) {
                Some("SUCCEEDED") => break,
                Some("FAILED") | Some("CANCELLED") => {
                    let reason = status_resp
                        .query_execution
                        .as_ref()
                        .and_then(|qe| qe.status.as_ref())
                        .and_then(|s| s.state_change_reason.as_deref())
                        .unwrap_or("sin razón")
                        .to_string();
                    return Err(DomainError::aegis(
                        ErrorCode::Aeg002,
                        format!("Athena query fallida: {reason}"),
                    ));
                }
                _ => {
                    debug!("[Athena] polling attempt {attempt}, state: {state:?}");
                    sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
            }

            if attempt == MAX_POLLS - 1 {
                return Err(DomainError::aegis(
                    ErrorCode::Aeg003,
                    format!("Athena timeout después de {MAX_POLLS} polls — execution_id: {execution_id}"),
                ));
            }
        }

        // Leer resultados
        let results_resp = self
            .client
            .get_query_results()
            .query_execution_id(execution_id)
            .send()
            .await
            .map_err(|e| {
                DomainError::aegis(ErrorCode::Aeg004, format!("get_query_results falló: {e}"))
            })?;

        let result_set = results_resp
            .result_set
            .ok_or_else(|| DomainError::aegis(ErrorCode::Aeg004, "Sin result_set en respuesta"))?;

        // Extraer nombres de columnas desde el header (primera fila)
        let col_infos = result_set
            .result_set_metadata
            .as_ref()
            .map(|m| m.column_info())
            .unwrap_or_default();

        let columns: Vec<String> = col_infos.iter().map(|c| c.name().to_string()).collect();

        let col_types: Vec<&str> = col_infos.iter().map(|c| c.r#type()).collect();

        // Parsear filas omitiendo el header (primera fila de Athena = header)
        let data_rows = result_set.rows();
        let skip = if !data_rows.is_empty() { 1 } else { 0 };

        let rows: Vec<HashMap<String, Value>> = data_rows[skip..]
            .iter()
            .map(|row| {
                let mut map = HashMap::new();
                for (i, datum) in row.data().iter().enumerate() {
                    let col_name = columns.get(i).cloned().unwrap_or_default();
                    let col_type = col_types.get(i).copied().unwrap_or("");
                    let raw = datum.var_char_value().unwrap_or("");
                    let val = coerce_athena_value(raw, col_type);
                    map.insert(col_name, val);
                }
                map
            })
            .collect();

        Ok(QueryResults { columns, rows })
    }
}

/// Coerciona un string Athena al tipo JSON correspondiente.
fn coerce_athena_value(raw: &str, col_type: &str) -> Value {
    if raw.is_empty() {
        return Value::Null;
    }
    match col_type.to_lowercase().as_str() {
        // Enteros
        "integer" | "int" | "tinyint" | "smallint" | "bigint" => raw
            .parse::<i64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(raw.to_string())),
        // Decimales
        "double" | "float" | "real" | "decimal" | "numeric" => raw
            .parse::<f64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(raw.to_string())),
        // Boolean
        "boolean" => Value::Bool(raw.eq_ignore_ascii_case("true")),
        // Timestamps → epoch segundos (entero)
        "timestamp" | "date" => parse_athena_timestamp(raw)
            .map(Value::from)
            .unwrap_or_else(|| Value::String(raw.to_string())),
        // Strings
        _ => Value::String(raw.to_string()),
    }
}

/// Parsea timestamps de Athena a epoch segundos.
fn parse_athena_timestamp(s: &str) -> Option<i64> {
    use chrono::NaiveDateTime;

    const PATTERNS: &[&str] = &[
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ];

    for pattern in PATTERNS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, pattern) {
            return Some(dt.and_utc().timestamp());
        }
    }
    None
}
