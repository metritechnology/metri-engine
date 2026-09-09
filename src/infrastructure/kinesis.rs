//! KinesisFirehoseWriter — IStreamWriter implementation.
//!
//! KinesisFirehoseWriter implementando IStreamWriter.
//!
//! # Origin
//! En el stack anterior: `(defrecord KinesisFirehoseWriter [client])` → put-record!
//! En Rust:    aws-sdk-firehose (Firehose se mapea aquí)

use async_trait::async_trait;
use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::Record;
use aws_sdk_firehose::Client;
use tracing::info;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::IStreamWriter;

pub struct KinesisFirehoseWriter {
    client: Client,
}

impl KinesisFirehoseWriter {
    pub async fn new() -> Self {
        let region_provider =
            aws_config::meta::region::RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::from_env().region(region_provider).load().await;

        let client = if let Ok(endpoint_url) = std::env::var("AWS_ENDPOINT_URL") {
            info!(
                "[Firehose] Usando endpoint override de AWS_ENDPOINT_URL: {}",
                endpoint_url
            );
            let firehose_config = aws_sdk_firehose::config::Builder::from(&config)
                .endpoint_url(endpoint_url)
                .build();
            Client::from_conf(firehose_config)
        } else if let Ok(endpoint_url) = std::env::var("KINESIS_ENDPOINT") {
            info!(
                "[Firehose] Usando endpoint override de KINESIS_ENDPOINT: {}",
                endpoint_url
            );
            let firehose_config = aws_sdk_firehose::config::Builder::from(&config)
                .endpoint_url(endpoint_url)
                .build();
            Client::from_conf(firehose_config)
        } else {
            Client::new(&config)
        };

        info!("[Firehose] cliente Firehose activo");
        KinesisFirehoseWriter { client }
    }
}

#[async_trait]
impl IStreamWriter for KinesisFirehoseWriter {
    /// Escribe un record al stream Firehose.
    /// El payload se serializa como JSON + newline (compatible con Firehose → S3).
    async fn put_record(
        &self,
        stream_name: &str,
        _partition_key: &str,
        data: Vec<u8>,
    ) -> Result<String, DomainError> {
        // // Añadir newline al final — mismo comportamiento que el el stack anterior
        let mut payload = data;
        payload.push(b'\n');

        let record = Record::builder()
            .data(Blob::new(payload))
            .build()
            .map_err(|e| {
                DomainError::infra(
                    ErrorCode::Infra005,
                    format!("Failed to build Firehose record: {e:?}"),
                )
            })?;

        let resp = self
            .client
            .put_record()
            .delivery_stream_name(stream_name)
            .record(record)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("{e:?}");
                // Clasificación pendiente: toda falla mapea a INFRA_005.
                DomainError::infra(
                    ErrorCode::Infra005,
                    format!("Firehose PutRecord falló en '{stream_name}': {msg}"),
                )
            })?;

        // Retorna record_id como identificador del record
        Ok(resp.record_id)
    }

    /// Escribe un lote en una sola llamada PutRecordBatch.
    /// Cada payload se entrega con el mismo contrato que put_record:
    /// JSON + newline (byte a byte idéntico a nivel de record).
    async fn put_records(
        &self,
        stream_name: &str,
        mut batch: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<String>, DomainError> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let mut record_ids = Vec::with_capacity(batch.len());

        // PutRecordBatch admite hasta 500 records por llamada; el canal ya
        // parte en chunks de 50, pero el guard protege a cualquier otro
        // llamador del trait que supere el tope del API.
        for sub_batch in batch.chunks_mut(500) {
            let mut records = Vec::with_capacity(sub_batch.len());
            for (_, data) in sub_batch.iter_mut() {
                data.push(b'\n');
                let record = Record::builder()
                    .data(Blob::new(std::mem::take(data)))
                    .build()
                    .map_err(|e| {
                        DomainError::infra(
                            ErrorCode::Infra005,
                            format!("Failed to build Firehose record: {e:?}"),
                        )
                    })?;
                records.push(record);
            }

            let resp = self
                .client
                .put_record_batch()
                .delivery_stream_name(stream_name)
                .set_records(Some(records))
                .send()
                .await
                .map_err(|e| {
                    let msg = format!("{e:?}");
                    DomainError::infra(
                        ErrorCode::Infra005,
                        format!("Firehose PutRecordBatch falló en '{stream_name}': {msg}"),
                    )
                })?;

            // Firehose reporta fallos parciales por record: un record que no
            // entró es una falla del lote, no un éxito con pérdida silenciosa.
            if resp.failed_put_count() > 0 {
                return Err(DomainError::infra(
                    ErrorCode::Infra005,
                    format!(
                        "Firehose PutRecordBatch: {} records fallidos en '{stream_name}'",
                        resp.failed_put_count()
                    ),
                ));
            }

            record_ids.extend(
                resp.request_responses
                    .into_iter()
                    .filter_map(|r| r.record_id),
            );
        }

        Ok(record_ids)
    }
}

use std::sync::{Arc, Mutex};

pub struct StubStreamWriter;

impl Default for StubStreamWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl StubStreamWriter {
    pub fn new() -> Self {
        StubStreamWriter
    }
}

#[async_trait]
impl IStreamWriter for StubStreamWriter {
    async fn put_record(
        &self,
        stream_name: &str,
        _partition_key: &str,
        data: Vec<u8>,
    ) -> Result<String, DomainError> {
        let payload_str = String::from_utf8_lossy(&data);
        info!(
            stream = %stream_name,
            payload = %payload_str,
            "[StubStreamWriter] Record inyectado exitosamente en local"
        );
        Ok(format!("stub-record-{}", uuid::Uuid::new_v4()))
    }

    async fn put_records(
        &self,
        stream_name: &str,
        batch: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<String>, DomainError> {
        info!(
            stream = %stream_name,
            count = batch.len(),
            "[StubStreamWriter] Batch inyectado exitosamente en local"
        );
        Ok((0..batch.len())
            .map(|_| format!("stub-record-{}", uuid::Uuid::new_v4()))
            .collect())
    }
}

/// Test-only stream writer that captures all records in memory.
/// Used for integration testing of the audit pipeline.
#[derive(Clone)]
pub struct SpyStreamWriter {
    captured: Arc<Mutex<Vec<CapturedRecord>>>,
    batch_calls: Arc<Mutex<usize>>,
}

#[derive(Debug, Clone)]
pub struct CapturedRecord {
    pub stream_name: String,
    pub partition_key: String,
    pub data: Vec<u8>,
}

impl Default for SpyStreamWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl SpyStreamWriter {
    pub fn new() -> Self {
        Self {
            captured: Arc::new(Mutex::new(Vec::new())),
            batch_calls: Arc::new(Mutex::new(0)),
        }
    }

    /// Returns a clone of the shared capture buffer for assertion.
    pub fn captured(&self) -> Arc<Mutex<Vec<CapturedRecord>>> {
        self.captured.clone()
    }

    /// Número de llamadas a put_records recibidas — la aserción de la Puerta 1
    /// (⌈N/50⌉ llamadas en vez de N).
    pub fn batch_calls(&self) -> usize {
        *self
            .batch_calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Drains all captured records, clearing the buffer.
    pub fn drain(&self) -> Vec<CapturedRecord> {
        let mut guard = self
            .captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut *guard)
    }
}

#[async_trait]
impl IStreamWriter for SpyStreamWriter {
    async fn put_record(
        &self,
        stream_name: &str,
        partition_key: &str,
        data: Vec<u8>,
    ) -> Result<String, DomainError> {
        self.captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(CapturedRecord {
                stream_name: stream_name.to_string(),
                partition_key: partition_key.to_string(),
                data,
            });
        info!(
            stream = %stream_name,
            "[SpyStreamWriter] Captured record for stream: {}", stream_name
        );
        Ok(format!("spy-record-{}", uuid::Uuid::new_v4()))
    }

    /// Captura el lote aplastado: un CapturedRecord por record lógico, de modo
    /// que las aserciones de la suite e2e (drain + JSON por record) siguen
    /// viendo exactamente los mismos records que en el camino put_record.
    /// Añade el newline final igual que KinesisFirehoseWriter: el spy debe
    /// entregar los mismos bytes que recibiría Firehose.
    async fn put_records(
        &self,
        stream_name: &str,
        batch: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<String>, DomainError> {
        *self
            .batch_calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        let mut guard = self
            .captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut ids = Vec::with_capacity(batch.len());
        for (partition_key, mut data) in batch {
            data.push(b'\n');
            guard.push(CapturedRecord {
                stream_name: stream_name.to_string(),
                partition_key,
                data,
            });
            ids.push(format!("spy-record-{}", uuid::Uuid::new_v4()));
        }
        Ok(ids)
    }
}
