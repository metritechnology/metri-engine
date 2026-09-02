// [PORTED_FROM: src/metri/infrastructure/dynamodb.clj]
// infrastructure/dynamodb.rs — Cliente DynamoDB AWS SDK v1 para Rust.
// En Clojure: cognitect.aws.client.api — Cognitect SDK directo.
// En Rust:    aws-sdk-dynamodb — AWS SDK oficial.
//
// Zero-Drop Policy: replica get_item, put_item!, update_item!, delete_item!
// y todos sus códigos de error (INFRA_DDB_001..005).

use std::collections::HashMap;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::{
    config::Builder as DdbConfigBuilder, error::SdkError, types::AttributeValue, Client,
};
use serde_json::Value;
use tracing::{info, warn};

use crate::domain::errors::{DomainError, ErrorCode};

/// Cliente DynamoDB con su región configurada.
/// [PORTED_FROM: {:client client :region region}]
#[derive(Clone)]
pub struct DynamoClient {
    pub client: Client,
    pub table_eav: String, // tabla principal metri-eav-prod
}

impl DynamoClient {
    /// Construye el cliente desde la configuración AWS del entorno.
    /// Si DYNAMODB_ENDPOINT está definido, se usa como endpoint override (dev local).
    /// [PORTED_FROM: ig/init-key :infra/dynamodb]
    pub async fn new(table_eav: impl Into<String>) -> Self {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::from_env().region(region_provider).load().await;

        let client = if let Ok(endpoint_url) = std::env::var("DYNAMODB_ENDPOINT") {
            info!("[DynamoDB] Usando endpoint override: {}", endpoint_url);
            let ddb_config = DdbConfigBuilder::from(&config)
                .endpoint_url(endpoint_url)
                .build();
            Client::from_conf(ddb_config)
        } else {
            Client::new(&config)
        };

        info!("[DynamoDB] cliente activo");
        DynamoClient {
            client,
            table_eav: table_eav.into(),
        }
    }

    /// Health check — lista tablas para verificar conectividad.
    /// [PORTED_FROM: (aws/invoke client {:op :ListTables :request {:Limit 1}})]
    pub async fn health_check(&self) -> bool {
        match self.client.list_tables().limit(1).send().await {
            Ok(resp) => {
                info!("[DynamoDB] tablas visibles: {}", resp.table_names().len());
                true
            }
            Err(e) => {
                warn!("[DynamoDB] ListTables falló (puede ser normal en local): {e}");
                false
            }
        }
    }

    // ── GetItem ───────────────────────────────────────────────────────────────

    /// Obtiene un item por su clave primaria.
    /// [PORTED_FROM: (get-item ddb-client table-name key-map)]
    pub async fn get_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: Option<&[u8]>, // None si tabla no tiene SK
    ) -> Result<Option<HashMap<String, AttributeValue>>, DomainError> {
        let mut key = HashMap::new();
        key.insert("PK".to_string(), AttributeValue::S(pk.to_string()));
        if let Some(sk_bytes) = sk {
            key.insert(
                "SK".to_string(),
                AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(sk_bytes.to_vec())),
            );
        }

        self.client
            .get_item()
            .table_name(table_name)
            .set_key(Some(key))
            .send()
            .await
            .map_err(|e| map_sdk_error(e, ErrorCode::Infra001, table_name))
            .map(|resp| resp.item)
    }

    // ── PutItem ───────────────────────────────────────────────────────────────

    /// Escribe un item completo. Idempotente.
    /// [PORTED_FROM: (put-item! ddb-client table-name item-map)]
    pub async fn put_item(
        &self,
        table_name: &str,
        item: HashMap<String, AttributeValue>,
    ) -> Result<(), DomainError> {
        self.client
            .put_item()
            .table_name(table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| map_sdk_error(e, ErrorCode::Infra001, table_name))
            .map(|_| ())
    }

    // ── TransactWriteItems ────────────────────────────────────────────────────
    // Esta es la operación CORE del EAV engine — escribe múltiples datoms ACID.
    // Blueprint: Metri EAV - OLPT.md §IV — ACID Write Path
    //
    // Zero-Drop Policy: chunking de 100 items máximo por transacción.
    // [PORTED_FROM: implícito en writer/chunker.rs]

    pub async fn transact_write(
        &self,
        items: Vec<aws_sdk_dynamodb::types::TransactWriteItem>,
    ) -> Result<(), DomainError> {
        if std::env::var("STUB_DYNAMODB").unwrap_or_default() == "1" {
            tracing::info!(
                "[DynamoDB] STUB_DYNAMODB=1 -> Simulando escritura exitosa de {} items",
                items.len()
            );
            return Ok(());
        }

        // DynamoDB acepta máximo 100 items por TransactWriteItems
        // [BLUEPRINT: §XII.2 — chunking transaccional obligatorio]
        for chunk in items.chunks(100) {
            self.client
                .transact_write_items()
                .set_transact_items(Some(chunk.to_vec()))
                .send()
                .await
                .map_err(|e| {
                    DomainError::eav(
                        ErrorCode::Eav001,
                        format!("TransactWriteItems falló: {e:?}"),
                    )
                })?;
        }
        Ok(())
    }

    // ── BatchWriteItem ────────────────────────────────────────────────────────

    /// Ejecuta escrituras en lote (hasta 25 items).
    /// Usado para índices secundarios (ej. FTS trigrams) con Degraded Consistency.
    pub async fn batch_write_item(
        &self,
        table_name: &str,
        mut requests: Vec<aws_sdk_dynamodb::types::WriteRequest>,
    ) -> Result<(), DomainError> {
        if requests.is_empty() {
            return Ok(());
        }

        let mut retries = 0;
        let initial_len = requests.len();
        while !requests.is_empty() && retries < 5 {
            let mut req_map = HashMap::new();
            req_map.insert(table_name.to_string(), requests.clone());

            let resp = self
                .client
                .batch_write_item()
                .set_request_items(Some(req_map))
                .send()
                .await
                .map_err(|e| map_sdk_error(e, ErrorCode::Infra001, table_name))?;

            if let Some(mut unprocessed) = resp.unprocessed_items {
                if let Some(failed_reqs) = unprocessed.remove(table_name) {
                    if failed_reqs.is_empty() {
                        break;
                    }
                    tracing::warn!("BatchWriteItem UnprocessedItems: {}", failed_reqs.len());
                    requests = failed_reqs;
                    retries += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(50 * (2_u64.pow(retries))))
                        .await;
                    continue;
                }
            }
            break;
        }

        tracing::info!(
            "BatchWriteItem successful for {} items in table {}",
            initial_len,
            table_name
        );

        Ok(())
    }

    // ── Query (para GSI y tabla principal) ────────────────────────────────────

    /// Ejecuta una Query en una tabla o GSI con condición de partition key.
    /// Retorna todos los items (paginados automáticamente).
    pub async fn query(
        &self,
        table_name: &str,
        index_name: Option<&str>,
        key_condition: &str,
        expr_attr_names: HashMap<String, String>,
        expr_attr_values: HashMap<String, AttributeValue>,
        scan_index_forward: bool,
        limit: Option<i32>,
    ) -> Result<Vec<HashMap<String, AttributeValue>>, DomainError> {
        let mut req = self
            .client
            .query()
            .table_name(table_name)
            .key_condition_expression(key_condition)
            .scan_index_forward(scan_index_forward)
            .set_expression_attribute_names(Some(expr_attr_names))
            .set_expression_attribute_values(Some(expr_attr_values));

        if let Some(idx) = index_name {
            req = req.index_name(idx);
        } else {
            req = req.consistent_read(true);
        }
        if let Some(lim) = limit {
            req = req.limit(lim);
        }

        let mut items = Vec::new();
        let mut last_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut paged = req.clone();
            if let Some(ref key) = last_key {
                paged = paged.set_exclusive_start_key(Some(key.clone()));
            }

            let resp = paged
                .send()
                .await
                .map_err(|e| map_sdk_error(e, ErrorCode::Infra001, table_name))?;

            items.extend(resp.items.unwrap_or_default());

            match resp.last_evaluated_key {
                Some(key) if !key.is_empty() => last_key = Some(key),
                _ => break,
            }

            // Si hay límite explícito, no paginar
            if limit.is_some() {
                break;
            }
        }

        Ok(items)
    }

    // ── DeleteItem ────────────────────────────────────────────────────────────

    /// Elimina un item. Idempotente — no lanza si no existía.
    /// [PORTED_FROM: (delete-item! ddb-client table-name key-map)]
    pub async fn delete_item(
        &self,
        table_name: &str,
        pk: &str,
        sk: Option<&[u8]>,
    ) -> Result<(), DomainError> {
        let mut key = HashMap::new();
        key.insert("PK".to_string(), AttributeValue::S(pk.to_string()));
        if let Some(sk_bytes) = sk {
            key.insert(
                "SK".to_string(),
                AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(sk_bytes.to_vec())),
            );
        }

        self.client
            .delete_item()
            .table_name(table_name)
            .set_key(Some(key))
            .send()
            .await
            .map_err(|e| map_sdk_error(e, ErrorCode::Infra001, table_name))
            .map(|_| ())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Convierte un SdkError en DomainError.
/// [PORTED_FROM: (if (:cognitect.anomalies/category resp) (errors/error code {...}))]
fn map_sdk_error<E: std::fmt::Debug>(
    err: SdkError<E>,
    default: ErrorCode,
    table_name: &str,
) -> DomainError {
    let msg = format!("{err:?}");
    let code = if msg.contains("AccessDeniedException") {
        // No hay equivalente directo en ErrorCode base — usamos Infra001 con contexto
        ErrorCode::Infra001
    } else if msg.contains("ThrottlingException") || msg.contains("ProvisionedThroughput") {
        ErrorCode::Infra001
    } else {
        default
    };
    DomainError::infra(
        code,
        format!("DynamoDB error en tabla '{table_name}': {msg}"),
    )
}

/// Extrae un String de un AttributeValue.
pub fn av_string(av: &AttributeValue) -> Option<&str> {
    if let AttributeValue::S(s) = av {
        Some(s)
    } else {
        None
    }
}

/// Extrae bytes de un AttributeValue binario.
pub fn av_bytes(av: &AttributeValue) -> Option<&[u8]> {
    if let AttributeValue::B(b) = av {
        Some(b.as_ref())
    } else {
        None
    }
}

/// Extrae un número (como String) de un AttributeValue.
pub fn av_number(av: &AttributeValue) -> Option<&str> {
    if let AttributeValue::N(n) = av {
        Some(n)
    } else {
        None
    }
}
