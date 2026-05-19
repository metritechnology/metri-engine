// [PORTED_FROM: src/metri/infrastructure/athena.clj]
// infrastructure/athena.rs — AthenaQueryEngine implementando IQueryEngine.
// En Clojure: AthenaClient SDK v2 Java.
// En Rust:    aws-sdk-athena — polling de resultados con coerción de tipos.

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
/// [PORTED_FROM: (defrecord AthenaQueryEngine [^AthenaClient client workgroup output-location])]
pub struct AthenaQueryEngine {
    client:          Client,
    workgroup:       String,
    output_location: String,
    database:        String,
}

impl AthenaQueryEngine {
    /// Constructor desde configuración del entorno.
    /// [PORTED_FROM: ig/init-key :infra/athena]
    pub async fn new(
        workgroup:       impl Into<String>,
        output_location: impl Into<String>,
        database:        impl Into<String>,
    ) -> Self {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        let wg = workgroup.into();
        info!("[Athena] cliente activo | workgroup: {wg}");
        AthenaQueryEngine {
            client,
            workgroup:       wg,
            output_location: output_location.into(),
            database:        database.into(),
        }
    }
}

#[async_trait]
impl IQueryEngine for AthenaQueryEngine {
    /// Inicia una query asíncrona y retorna el execution_id.
    /// [PORTED_FROM: (start-query! [_ sql database])]
    async fn start_query(&self, sql: &str, database: &str) -> Result<String, DomainError> {
        let db = if database.is_empty() { &self.database } else { database };

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
                DomainError::aegis(
                    ErrorCode::Aeg002,
                    format!("Athena start_query falló: {e}"),
                )
            })?;

        let execution_id = resp.query_execution_id
            .ok_or_else(|| DomainError::aegis(ErrorCode::Aeg002, "Sin execution_id en respuesta Athena"))?;

        Ok(execution_id)
    }

    /// Espera a que la query complete y retorna los resultados.
    /// [PORTED_FROM: (get-query-results [_ execution-id])]
    async fn get_query_results(&self, execution_id: &str) -> Result<QueryResults, DomainError> {
        // Polling con back-off exponencial — mismo patrón que el Clojure
        // [PORTED_FROM: loop de polling implícito en el cliente Java]
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
            .map_err(|e| DomainError::aegis(ErrorCode::Aeg004, format!("get_query_results falló: {e}")))?;

        let result_set = results_resp.result_set
            .ok_or_else(|| DomainError::aegis(ErrorCode::Aeg004, "Sin result_set en respuesta"))?;

        // Extraer nombres de columnas desde el header (primera fila)
        // [PORTED_FROM: (.columnInfo (.resultSetMetadata rs))]
        let col_infos = result_set
            .result_set_metadata
            .as_ref()
            .map(|m| m.column_info())
            .unwrap_or_default();

        let columns: Vec<String> = col_infos.iter()
            .map(|c| c.name().to_string())
            .collect();

        let col_types: Vec<&str> = col_infos.iter()
            .map(|c| c.r#type())
            .collect();

        // Parsear filas omitiendo el header (primera fila de Athena = header)
        // [PORTED_FROM: (rest (.rows rs))]
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
/// [PORTED_FROM: (coerce s col-type) — todos los casos del case Clojure]
fn coerce_athena_value(raw: &str, col_type: &str) -> Value {
    if raw.is_empty() {
        return Value::Null;
    }
    match col_type.to_lowercase().as_str() {
        // Enteros
        "integer" | "int" | "tinyint" | "smallint" | "bigint" => {
            raw.parse::<i64>().map(Value::from).unwrap_or_else(|_| Value::String(raw.to_string()))
        }
        // Decimales
        "double" | "float" | "real" | "decimal" | "numeric" => {
            raw.parse::<f64>().map(Value::from).unwrap_or_else(|_| Value::String(raw.to_string()))
        }
        // Boolean
        "boolean" => Value::Bool(raw.eq_ignore_ascii_case("true")),
        // Timestamps → epoch segundos (entero)
        // [PORTED_FROM: (temporal/parse-athena-ts s)]
        "timestamp" | "date" => {
            parse_athena_timestamp(raw)
                .map(Value::from)
                .unwrap_or_else(|| Value::String(raw.to_string()))
        }
        // Strings
        _ => Value::String(raw.to_string()),
    }
}

/// Parsea timestamps de Athena a epoch segundos.
/// [PORTED_FROM: (temporal/parse-athena-ts s) — mismos 5 patrones]
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
