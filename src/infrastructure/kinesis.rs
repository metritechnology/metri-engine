// [PORTED_FROM: src/metri/infrastructure/kinesis.clj]
// infrastructure/kinesis.rs — KinesisFirehoseWriter implementando IStreamWriter.
// En Clojure: (defrecord KinesisFirehoseWriter [client]) → put-record!
// En Rust:    aws-sdk-firehose (Firehose se mapea aquí)

use async_trait::async_trait;
use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::Record;
use aws_sdk_firehose::Client;
use tracing::info;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::IStreamWriter;

/// [PORTED_FROM: (defrecord KinesisFirehoseWriter [client])]
pub struct KinesisFirehoseWriter {
    client: Client,
}

impl KinesisFirehoseWriter {
    /// [PORTED_FROM: ig/init-key :infra/kinesis]
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
    /// [PORTED_FROM: (put-record! [_ stream-name partition-key data])]
    async fn put_record(
        &self,
        stream_name: &str,
        _partition_key: &str,
        data: Vec<u8>,
    ) -> Result<String, DomainError> {
        // Añadir newline al final — mismo comportamiento que el Clojure
        // [PORTED_FROM: (str (json/generate-string data) "\n")]
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
                let code = if msg.contains("AccessDeniedException") {
                    ErrorCode::Infra005
                } else if msg.contains("ServiceUnavailableException") {
                    ErrorCode::Infra005
                } else {
                    ErrorCode::Infra005
                };
                DomainError::infra(
                    code,
                    format!("Firehose PutRecord falló en '{stream_name}': {msg}"),
                )
            })?;

        // Retorna record_id como identificador del record
        // [PORTED_FROM: [:ok {:sequence-number (:RecordId resp)}]]
        Ok(resp.record_id)
    }
}

use std::sync::{Arc, Mutex};

pub struct StubStreamWriter;

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
}

/// Test-only stream writer that captures all records in memory.
/// Used for integration testing of the audit pipeline.
#[derive(Clone)]
pub struct SpyStreamWriter {
    captured: Arc<Mutex<Vec<CapturedRecord>>>,
}

#[derive(Debug, Clone)]
pub struct CapturedRecord {
    pub stream_name: String,
    pub partition_key: String,
    pub data: Vec<u8>,
}

impl SpyStreamWriter {
    pub fn new() -> Self {
        Self {
            captured: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a clone of the shared capture buffer for assertion.
    pub fn captured(&self) -> Arc<Mutex<Vec<CapturedRecord>>> {
        self.captured.clone()
    }

    /// Drains all captured records, clearing the buffer.
    pub fn drain(&self) -> Vec<CapturedRecord> {
        let mut guard = self.captured.lock().unwrap();
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
        self.captured.lock().unwrap().push(CapturedRecord {
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
}
