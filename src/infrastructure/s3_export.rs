use async_trait::async_trait;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client as S3Client;
use serde_json::Value;
use std::time::Duration;
use tracing::{error, info};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::IExportStorage;

// ── Real S3 Exporter ──────────────────────────────────────────────────────────

pub struct S3ExportStorage {
    client: S3Client,
    bucket: String,
}

impl S3ExportStorage {
    pub async fn new(bucket: String) -> Self {
        let config = aws_config::load_from_env().await;
        let client = S3Client::new(&config);
        info!("[S3ExportStorage] S3 client initialized for bucket: {bucket}");
        S3ExportStorage { client, bucket }
    }
}

#[async_trait]
impl IExportStorage for S3ExportStorage {
    async fn generate_presigned_url(
        &self,
        tenant_id: &str,
        query_key: &str,
        columns: &[crate::grpc::pb::ColumnSchema],
        rows: &[crate::grpc::pb::DataRow],
    ) -> Result<String, DomainError> {
        // 1. Generate CSV in memory buffer
        let mut wtr = csv::Writer::from_writer(vec![]);

        // Write headers
        let headers: Vec<&str> = columns.iter().map(|col| col.key.as_str()).collect();
        if let Err(e) = wtr.write_record(&headers) {
            error!("[S3ExportStorage] Failed to write CSV header: {e}");
            return Err(DomainError::new(
                ErrorCode::Infra002,
                format!("CSV Header formatting error: {e}"),
            ));
        }

        // Write row data
        for row in rows {
            let record: Vec<String> = row.values.iter().map(value_to_csv_field).collect();
            if let Err(e) = wtr.write_record(&record) {
                error!("[S3ExportStorage] Failed to write CSV record: {e}");
                return Err(DomainError::new(
                    ErrorCode::Infra002,
                    format!("CSV Row formatting error: {e}"),
                ));
            }
        }

        let csv_bytes = match wtr.into_inner() {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("[S3ExportStorage] Failed to finalize CSV buffer: {e}");
                return Err(DomainError::new(
                    ErrorCode::Infra002,
                    format!("CSV buffer finalization error: {e}"),
                ));
            }
        };

        // 2. Upload to S3
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let key = format!(
            "exports/{}/{}/{}_{}.csv",
            tenant_id,
            query_key,
            timestamp,
            ulid::Ulid::new()
        );

        info!(
            "[S3ExportStorage] Uploading CSV to S3: s3://{}/{}",
            self.bucket, key
        );

        let body = aws_sdk_s3::primitives::ByteStream::from(csv_bytes);
        if let Err(e) = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .content_type("text/csv")
            .send()
            .await
        {
            error!("[S3ExportStorage] S3 PutObject failed: {e}");
            return Err(DomainError::new(
                ErrorCode::Infra002,
                format!("S3 upload error: {e}"),
            ));
        }

        // 3. Generate Presigned URL (valid for 5 minutes)
        let presigning_config = match PresigningConfig::builder()
            .expires_in(Duration::from_secs(300))
            .build()
        {
            Ok(cfg) => cfg,
            Err(e) => {
                error!("[S3ExportStorage] Failed to build S3 presigning config: {e}");
                return Err(DomainError::new(
                    ErrorCode::Infra002,
                    format!("S3 presigning configuration error: {e}"),
                ));
            }
        };

        let presigned_req = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .presigned(presigning_config)
            .await
        {
            Ok(req) => req,
            Err(e) => {
                error!("[S3ExportStorage] S3 GetObject presigning failed: {e}");
                return Err(DomainError::new(
                    ErrorCode::Infra002,
                    format!("S3 presigned URL generation error: {e}"),
                ));
            }
        };

        let url = presigned_req.uri().to_string();
        info!("[S3ExportStorage] Successfully generated presigned URL");
        Ok(url)
    }
}

// ── Stub Exporter (Local Dev) ──────────────────────────────────────────────────

pub struct StubExportStorage;

impl Default for StubExportStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl StubExportStorage {
    pub fn new() -> Self {
        info!("[StubExportStorage] Stub storage active (local mock URLs)");
        StubExportStorage
    }
}

#[async_trait]
impl IExportStorage for StubExportStorage {
    async fn generate_presigned_url(
        &self,
        _tenant_id: &str,
        _query_key: &str,
        _columns: &[crate::grpc::pb::ColumnSchema],
        _rows: &[crate::grpc::pb::DataRow],
    ) -> Result<String, DomainError> {
        let mock_uuid = uuid::Uuid::new_v4();
        let mock_url = format!(
            "https://metri-mock-exports.s3.amazonaws.com/exports/mock_file_{mock_uuid}.csv"
        );
        info!("[StubExportStorage] Generated mock S3 URL: {mock_url}");
        Ok(mock_url)
    }
}

// ── Helper functions ──────────────────────────────────────────────────────────

fn value_to_csv_field(v: &prost_types::Value) -> String {
    use prost_types::value::Kind;
    match &v.kind {
        Some(Kind::NullValue(_)) => String::new(),
        Some(Kind::NumberValue(n)) => {
            if n.fract() == 0.0 {
                format!("{}", *n as i64)
            } else {
                format!("{}", n)
            }
        }
        Some(Kind::StringValue(s)) => s.clone(),
        Some(Kind::BoolValue(b)) => b.to_string(),
        Some(Kind::StructValue(s)) => {
            serde_json::to_string(&crate::grpc::translator::struct_to_value(s.clone()))
                .unwrap_or_default()
        }
        Some(Kind::ListValue(l)) => {
            let arr: Vec<Value> = l
                .values
                .iter()
                .map(|v| crate::grpc::translator::value_to_json(v.clone()))
                .collect();
            serde_json::to_string(&arr).unwrap_or_default()
        }
        None => String::new(),
    }
}
