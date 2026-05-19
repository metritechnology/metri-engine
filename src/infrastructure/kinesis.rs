// [PORTED_FROM: src/metri/infrastructure/kinesis.clj]
// infrastructure/kinesis.rs — KinesisFirehoseWriter implementando IStreamWriter.
// En Clojure: (defrecord KinesisFirehoseWriter [client]) → put-record!
// En Rust:    aws-sdk-kinesis (Firehose se mapea aquí)

use async_trait::async_trait;
use aws_sdk_kinesis::Client;
use aws_sdk_kinesis::primitives::Blob;
use tracing::{info, error};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::IStreamWriter;

/// [PORTED_FROM: (defrecord KinesisFirehoseWriter [client])]
pub struct KinesisFirehoseWriter {
    client: Client,
}

impl KinesisFirehoseWriter {
    /// [PORTED_FROM: ig/init-key :infra/kinesis]
    pub async fn new() -> Self {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        info!("[Kinesis] cliente Firehose activo");
        KinesisFirehoseWriter { client }
    }
}

#[async_trait]
impl IStreamWriter for KinesisFirehoseWriter {
    /// Escribe un record al stream Kinesis.
    /// El payload se serializa como JSON + newline (compatible con Firehose → S3).
    /// [PORTED_FROM: (put-record! [_ stream-name partition-key data])]
    async fn put_record(
        &self,
        stream_name:   &str,
        partition_key: &str,
        data:          Vec<u8>,
    ) -> Result<String, DomainError> {
        // Añadir newline al final — mismo comportamiento que el Clojure
        // [PORTED_FROM: (str (json/generate-string data) "\n")]
        let mut payload = data;
        payload.push(b'\n');

        let resp = self
            .client
            .put_record()
            .stream_name(stream_name)
            .partition_key(partition_key)
            .data(Blob::new(payload))
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
                DomainError::infra(code, format!("Kinesis PutRecord falló en '{stream_name}': {msg}"))
            })?;

        // Retorna sequence_number como identificador del record
        // [PORTED_FROM: [:ok {:sequence-number (:RecordId resp)}]]
        Ok(resp.sequence_number)
    }
}
