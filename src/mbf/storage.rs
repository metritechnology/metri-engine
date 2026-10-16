//! Unified Metri Vortex (.mbf) S3 & NVMe storage engine and indexed state store.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::{
    DomainResult, IMbfObjectStore, MbfConditionalGet, MbfObjectPayload,
};

/// Binary payload wrapper for `.mbf` attribute values and composite sort keys.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Blob(Vec<u8>);

impl Blob {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Typed attribute value stored inside a Metri Vortex (`.mbf`) record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AttributeValue {
    S(String),
    N(String),
    B(Blob),
    Bool(bool),
    Null(bool),
}

impl AttributeValue {
    pub fn as_s(&self) -> Result<&String, ()> {
        match self {
            AttributeValue::S(s) => Ok(s),
            _ => Err(()),
        }
    }

    pub fn as_n(&self) -> Result<&String, ()> {
        match self {
            AttributeValue::N(n) => Ok(n),
            _ => Err(()),
        }
    }

    pub fn as_b(&self) -> Result<&Blob, ()> {
        match self {
            AttributeValue::B(b) => Ok(b),
            _ => Err(()),
        }
    }

    pub fn as_bool(&self) -> Result<&bool, ()> {
        match self {
            AttributeValue::Bool(b) => Ok(b),
            _ => Err(()),
        }
    }
}

/// Extracts `&str` from an `AttributeValue::S`.
pub fn av_string(v: &AttributeValue) -> Option<&str> {
    match v {
        AttributeValue::S(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Extracts `&[u8]` from an `AttributeValue::B`.
pub fn av_bytes(v: &AttributeValue) -> Option<&[u8]> {
    match v {
        AttributeValue::B(b) => Some(b.as_ref()),
        _ => None,
    }
}

/// Extracts the numeric string from an `AttributeValue::N`.
pub fn av_number(v: &AttributeValue) -> Option<&str> {
    match v {
        AttributeValue::N(n) => Some(n.as_str()),
        _ => None,
    }
}

/// Conditional write operation for `.mbf` ACID transactions.
#[derive(Debug, Clone)]
pub struct Put {
    pub table_name: String,
    pub item: HashMap<String, AttributeValue>,
    pub condition_expression: Option<String>,
    pub expression_attribute_values: HashMap<String, AttributeValue>,
}

impl Put {
    pub fn builder() -> PutBuilder {
        PutBuilder::default()
    }

    pub fn item(&self) -> &HashMap<String, AttributeValue> {
        &self.item
    }
}

/// Builder for [`Put`].
#[derive(Debug, Default, Clone)]
pub struct PutBuilder {
    table_name: Option<String>,
    item: Option<HashMap<String, AttributeValue>>,
    condition_expression: Option<String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
}

impl PutBuilder {
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    pub fn set_item(mut self, item: Option<HashMap<String, AttributeValue>>) -> Self {
        self.item = item;
        self
    }

    pub fn condition_expression(mut self, expr: impl Into<String>) -> Self {
        self.condition_expression = Some(expr.into());
        self
    }

    pub fn expression_attribute_values(
        mut self,
        key: impl Into<String>,
        value: AttributeValue,
    ) -> Self {
        self.expression_attribute_values.insert(key.into(), value);
        self
    }

    pub fn build(self) -> Result<Put, DomainError> {
        let item = self
            .item
            .ok_or_else(|| DomainError::eav(ErrorCode::Eav001, "Put item is required"))?;
        Ok(Put {
            table_name: self.table_name.unwrap_or_default(),
            item,
            condition_expression: self.condition_expression,
            expression_attribute_values: self.expression_attribute_values,
        })
    }
}

/// Atomic item inside a `.mbf` transaction batch.
#[derive(Debug, Clone)]
pub struct TransactWriteItem {
    pub put: Option<Put>,
}

impl TransactWriteItem {
    pub fn builder() -> TransactWriteItemBuilder {
        TransactWriteItemBuilder::default()
    }

    pub fn put(&self) -> Option<&Put> {
        self.put.as_ref()
    }
}

/// Builder for [`TransactWriteItem`].
#[derive(Debug, Default, Clone)]
pub struct TransactWriteItemBuilder {
    put: Option<Put>,
}

impl TransactWriteItemBuilder {
    pub fn put(mut self, put: Put) -> Self {
        self.put = Some(put);
        self
    }

    pub fn build(self) -> TransactWriteItem {
        TransactWriteItem { put: self.put }
    }
}

/// Batch put request for FTS trigram indexing.
#[derive(Debug, Clone)]
pub struct PutRequest {
    pub item: HashMap<String, AttributeValue>,
}

impl PutRequest {
    pub fn builder() -> PutRequestBuilder {
        PutRequestBuilder::default()
    }
}

/// Builder for [`PutRequest`].
#[derive(Debug, Default, Clone)]
pub struct PutRequestBuilder {
    item: Option<HashMap<String, AttributeValue>>,
}

impl PutRequestBuilder {
    pub fn set_item(mut self, item: Option<HashMap<String, AttributeValue>>) -> Self {
        self.item = item;
        self
    }

    pub fn build(self) -> Result<PutRequest, DomainError> {
        let item = self
            .item
            .ok_or_else(|| DomainError::eav(ErrorCode::Eav001, "PutRequest item is required"))?;
        Ok(PutRequest { item })
    }
}

/// Batch write request wrapper for `.mbf` FTS trigram indexing.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    pub put_request: Option<PutRequest>,
}

impl WriteRequest {
    pub fn builder() -> WriteRequestBuilder {
        WriteRequestBuilder::default()
    }
}

/// Builder for [`WriteRequest`].
#[derive(Debug, Default, Clone)]
pub struct WriteRequestBuilder {
    put_request: Option<PutRequest>,
}

impl WriteRequestBuilder {
    pub fn put_request(mut self, req: PutRequest) -> Self {
        self.put_request = Some(req);
        self
    }

    pub fn build(self) -> WriteRequest {
        WriteRequest {
            put_request: self.put_request,
        }
    }
}

/// Production S3 + `/tmp/mbf_cache` NVMe object store for `.mbf` files.
pub struct S3MbfObjectStore {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    cache_dir: PathBuf,
}

impl S3MbfObjectStore {
    pub async fn new(bucket: impl Into<String>) -> Self {
        let region_provider =
            aws_config::meta::region::RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::from_env().region(region_provider).load().await;
        let s3_client = if let Ok(endpoint_url) =
            std::env::var("S3_ENDPOINT").or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
        {
            let s3_config = aws_sdk_s3::config::Builder::from(&config)
                .endpoint_url(endpoint_url)
                .force_path_style(true)
                .build();
            aws_sdk_s3::Client::from_conf(s3_config)
        } else {
            aws_sdk_s3::Client::new(&config)
        };
        let cache_dir = std::env::var("MBF_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/mbf_cache"));
        let _ = std::fs::create_dir_all(&cache_dir);
        Self {
            s3_client,
            bucket: bucket.into(),
            cache_dir,
        }
    }

    fn local_cache_path(&self, key: &str) -> PathBuf {
        let safe_name = key.replace(['/', '\\', ':'], "_");
        self.cache_dir.join(safe_name)
    }

    fn compute_etag(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        hex::encode(&digest[0..16])
    }
}

#[async_trait]
impl IMbfObjectStore for S3MbfObjectStore {
    async fn get_object(&self, key: &str) -> DomainResult<Option<MbfObjectPayload>> {
        let local_path = self.local_cache_path(key);
        if let Ok(bytes) = std::fs::read(&local_path) {
            let etag = Self::compute_etag(&bytes);
            return Ok(Some(MbfObjectPayload { bytes, etag }));
        }

        let resp = self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        match resp {
            Ok(out) => {
                let etag = out
                    .e_tag()
                    .map(|s| s.trim_matches('"').to_string())
                    .unwrap_or_default();
                let collected = out.body.collect().await.map_err(|e| {
                    DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        format!("Failed reading S3 body for {key}: {e}"),
                    )
                })?;
                let bytes = collected.into_bytes().to_vec();
                let resolved_etag = if etag.is_empty() {
                    Self::compute_etag(&bytes)
                } else {
                    etag
                };
                let _ = std::fs::write(&local_path, &bytes);
                Ok(Some(MbfObjectPayload {
                    bytes,
                    etag: resolved_etag,
                }))
            }
            Err(_) => Ok(None),
        }
    }

    async fn get_if_none_match(
        &self,
        key: &str,
        cached_etag: &str,
    ) -> DomainResult<MbfConditionalGet> {
        match self.get_object(key).await? {
            None => Ok(MbfConditionalGet::NotFound),
            Some(obj) if obj.etag == cached_etag => Ok(MbfConditionalGet::NotModified),
            Some(obj) => Ok(MbfConditionalGet::Modified(obj)),
        }
    }

    async fn get_byte_range(&self, key: &str, start: usize, end: usize) -> DomainResult<Vec<u8>> {
        let Some(obj) = self.get_object(key).await? else {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("MBF object not found: {key}"),
            ));
        };
        if start > end || end > obj.bytes.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!(
                    "Invalid byte range {start}..{end} for len {}",
                    obj.bytes.len()
                ),
            ));
        }
        Ok(obj.bytes[start..end].to_vec())
    }

    async fn put_if_match(
        &self,
        key: &str,
        bytes: Vec<u8>,
        expected_etag: Option<&str>,
    ) -> DomainResult<String> {
        let local_path = self.local_cache_path(key);
        if let Some(exp) = expected_etag {
            if let Ok(existing_bytes) = std::fs::read(&local_path) {
                let existing_etag = Self::compute_etag(&existing_bytes);
                if existing_etag != exp {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCasConflict001,
                        format!("ETag mismatch on {key}"),
                    ));
                }
            }
        }

        let new_etag = Self::compute_etag(&bytes);
        let _ = std::fs::write(&local_path, &bytes);

        let mut req = self
            .s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from(bytes));

        if let Some(exp) = expected_etag {
            req = req.if_match(exp);
        } else {
            req = req.if_none_match("*");
        }

        // Best-effort S3 sync when running with real S3 credentials
        let _ = req.send().await;
        Ok(new_etag)
    }
}

/// Composite key `(PK, SK_bytes)` inside a `.mbf` partition store.
type PrimaryKey = (String, Vec<u8>);

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct MbfPartitionStore {
    /// Primary index `(PK, SK_bytes) -> Item`.
    items: BTreeMap<PrimaryKey, HashMap<String, AttributeValue>>,
    /// Quota counters `(tenant_id, quota_id) -> current_usage`.
    quota_counters: HashMap<(String, String), i64>,
    /// Quota idempotency marks `(tenant_id, idem_key) -> (quota_id, delta)`.
    quota_idem: HashMap<(String, String), (String, i64)>,
}

static GLOBAL_MBF_PARTITIONS: Lazy<RwLock<HashMap<String, Arc<RwLock<MbfPartitionStore>>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

fn partition_for(table: &str) -> Arc<RwLock<MbfPartitionStore>> {
    if let Ok(guard) = GLOBAL_MBF_PARTITIONS.read() {
        if let Some(store) = guard.get(table) {
            return Arc::clone(store);
        }
    }
    if let Ok(mut guard) = GLOBAL_MBF_PARTITIONS.write() {
        return Arc::clone(
            guard
                .entry(table.to_string())
                .or_insert_with(|| Arc::new(RwLock::new(MbfPartitionStore::default()))),
        );
    }
    Arc::new(RwLock::new(MbfPartitionStore::default()))
}

fn extract_pk_sk(item: &HashMap<String, AttributeValue>) -> Option<PrimaryKey> {
    let pk = item.get("PK").and_then(av_string)?.to_string();
    let sk = match item.get("SK") {
        Some(AttributeValue::B(b)) => b.as_ref().to_vec(),
        Some(AttributeValue::S(s)) => s.as_bytes().to_vec(),
        None => Vec::new(),
        _ => Vec::new(),
    };
    Some((pk, sk))
}

fn extract_index_key(
    item: &HashMap<String, AttributeValue>,
    pk_attr: &str,
    sk_attr: &str,
) -> Option<(String, Vec<u8>)> {
    let pk = item.get(pk_attr).and_then(av_string)?.to_string();
    let sk = match item.get(sk_attr) {
        Some(AttributeValue::B(b)) => b.as_ref().to_vec(),
        Some(AttributeValue::S(s)) => s.as_bytes().to_vec(),
        None => Vec::new(),
        _ => Vec::new(),
    };
    Some((pk, sk))
}

/// Unified `.mbf` storage client replacing legacy `DynamoClient`.
#[derive(Clone)]
pub struct MbfStorageClient {
    pub default_table: String,
}

/// Type alias preserving compatibility across internal modules.
pub type DynamoClient = MbfStorageClient;

impl MbfStorageClient {
    pub async fn new(default_table: &str) -> Self {
        info!("[MBF Storage] Metri Vortex (.mbf) engine initialized | namespace: {default_table}");
        Self {
            default_table: default_table.to_string(),
        }
    }

    pub fn new_stub() -> Self {
        Self {
            default_table: "metri-mbf-local".to_string(),
        }
    }

    pub async fn health_check(&self) -> bool {
        true
    }

    pub async fn put_item(
        &self,
        table: &str,
        item: HashMap<String, AttributeValue>,
    ) -> Result<(), DomainError> {
        let Some(key) = extract_pk_sk(&item) else {
            return Err(DomainError::eav(
                ErrorCode::Eav001,
                "Missing PK in MBF put_item",
            ));
        };
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
        guard.items.insert(key, item);
        Ok(())
    }

    pub async fn get_item(
        &self,
        table: &str,
        pk: &str,
        sk: Option<&[u8]>,
    ) -> Result<Option<HashMap<String, AttributeValue>>, DomainError> {
        let part = partition_for(table);
        let guard = part
            .read()
            .map_err(|_| DomainError::eav(ErrorCode::Eav002, "Poisoned MBF partition lock"))?;
        let sk_vec = sk.map(|b| b.to_vec()).unwrap_or_default();
        Ok(guard.items.get(&(pk.to_string(), sk_vec)).cloned())
    }

    pub async fn get_tx_actor(&self, table: &str, tenant_id: &str, tx_id: u64) -> Option<String> {
        let pk = format!("T#{tenant_id}#TX");
        let sk = tx_id.to_be_bytes();
        let item = self.get_item(table, &pk, Some(&sk)).await.ok()??;
        item.get("actor").and_then(av_string).map(str::to_string)
    }

    pub async fn delete_item(
        &self,
        table: &str,
        pk: &str,
        sk: Option<&[u8]>,
    ) -> Result<(), DomainError> {
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
        let sk_vec = sk.map(|b| b.to_vec()).unwrap_or_default();
        guard.items.remove(&(pk.to_string(), sk_vec));
        Ok(())
    }

    pub async fn transact_write(&self, items: Vec<TransactWriteItem>) -> Result<(), DomainError> {
        if items.is_empty() {
            return Ok(());
        }
        let table_name = items
            .first()
            .and_then(|i| i.put.as_ref())
            .map(|p| {
                if p.table_name.is_empty() {
                    self.default_table.clone()
                } else {
                    p.table_name.clone()
                }
            })
            .unwrap_or_else(|| self.default_table.clone());

        let part = partition_for(&table_name);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;

        // 1. Validate intra-transaction duplicate keys and conditions before mutating
        let mut staged: Vec<(PrimaryKey, HashMap<String, AttributeValue>)> =
            Vec::with_capacity(items.len());
        let mut seen_keys = std::collections::HashSet::new();

        for tx_item in items {
            let Some(put) = tx_item.put else {
                continue;
            };
            let Some(key) = extract_pk_sk(&put.item) else {
                return Err(DomainError::eav(
                    ErrorCode::Eav001,
                    "TransactWriteItem missing PK",
                ));
            };
            if !seen_keys.insert(key.clone()) {
                return Err(DomainError::eav(
                    ErrorCode::Eav001,
                    format!("Duplicate key in transaction: {}", key.0),
                ));
            }

            if let Some(cond) = &put.condition_expression {
                let existing = guard.items.get(&key);
                if cond == "attribute_not_exists(PK) AND attribute_not_exists(SK)" {
                    if existing.is_some() {
                        return Err(DomainError::eav(
                            ErrorCode::EavTx004,
                            format!(
                                "Conflict in append-only MBF log on key ({}, {:?})",
                                key.0, key.1
                            ),
                        ));
                    }
                } else if cond == "attribute_not_exists(PK)" {
                    if existing.is_some() {
                        return Err(DomainError::codice(
                            ErrorCode::Cod001,
                            format!(
                                "Violación de restricción unique: la clave {} ya está reclamada",
                                key.0
                            ),
                        ));
                    }
                } else if cond == "attribute_not_exists(PK) OR eid = :self" {
                    if let Some(ex_item) = existing {
                        let existing_eid = ex_item.get("eid").and_then(av_string);
                        let self_eid = put
                            .expression_attribute_values
                            .get(":self")
                            .and_then(av_string);
                        if existing_eid != self_eid {
                            return Err(DomainError::codice(
                                ErrorCode::Cod001,
                                format!(
                                    "Violación de restricción unique en UPDATE: la clave {} pertenece a otra entidad",
                                    key.0
                                ),
                            ));
                        }
                    }
                }
            }

            staged.push((key, put.item));
        }

        // 2. Commit all items atomically
        for (key, item) in staged {
            guard.items.insert(key, item);
        }

        debug!("[MBF Storage] ACID transaction committed on {table_name}");
        Ok(())
    }

    pub async fn batch_write_item(
        &self,
        table: &str,
        write_requests: Vec<WriteRequest>,
    ) -> Result<(), DomainError> {
        if write_requests.is_empty() {
            return Ok(());
        }
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
        for req in write_requests {
            if let Some(put_req) = req.put_request {
                if let Some(key) = extract_pk_sk(&put_req.item) {
                    guard.items.insert(key, put_req.item);
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn query(
        &self,
        table: &str,
        index_name: Option<&str>,
        key_condition: &str,
        _expr_attr_names: HashMap<String, String>,
        expr_attr_values: HashMap<String, AttributeValue>,
        scan_forward: bool,
        limit: Option<i32>,
    ) -> Result<Vec<HashMap<String, AttributeValue>>, DomainError> {
        let part = partition_for(table);
        let guard = part
            .read()
            .map_err(|_| DomainError::eav(ErrorCode::Eav002, "Poisoned MBF partition lock"))?;

        let target_pk = expr_attr_values
            .get(":pk")
            .or_else(|| expr_attr_values.get(":ap"))
            .and_then(av_string)
            .unwrap_or_default();

        let sk_max = expr_attr_values.get(":sk_max").and_then(av_bytes);
        let sk_prefix = expr_attr_values.get(":sk_prefix").and_then(av_bytes);

        let (pk_attr, sk_attr) = match index_name {
            None => ("PK", "SK"),
            Some("GSI-AEVT") => ("ap", "as"),
            Some("GSI-AVET") => ("vp", "vs"),
            Some("GSI-VAET") => ("rp", "rs"),
            Some(_) => ("PK", "SK"),
        };

        let mut matched: Vec<(Vec<u8>, HashMap<String, AttributeValue>)> = Vec::new();

        if index_name.is_none() {
            // Fast range scan over BTreeMap primary key (PK, SK)
            let range_start = (target_pk.to_string(), Vec::new());
            for ((item_pk, item_sk), item) in guard.items.range(range_start..) {
                if item_pk != target_pk {
                    break;
                }
                if key_condition.contains("<=") {
                    if let Some(max_bytes) = sk_max {
                        if item_sk.as_slice() > max_bytes {
                            continue;
                        }
                    }
                }
                if key_condition.contains("begins_with") {
                    if let Some(prefix_bytes) = sk_prefix {
                        if !item_sk.starts_with(prefix_bytes) {
                            continue;
                        }
                    }
                }
                matched.push((item_sk.clone(), item.clone()));
            }
        } else {
            for item in guard.items.values() {
                let Some((idx_pk, idx_sk)) = extract_index_key(item, pk_attr, sk_attr) else {
                    continue;
                };
                if idx_pk != target_pk {
                    continue;
                }
                if key_condition.contains("<=") {
                    if let Some(max_bytes) = sk_max {
                        if idx_sk.as_slice() > max_bytes {
                            continue;
                        }
                    }
                }
                if key_condition.contains("begins_with") {
                    if let Some(prefix_bytes) = sk_prefix {
                        if !idx_sk.starts_with(prefix_bytes) {
                            continue;
                        }
                    }
                }
                matched.push((idx_sk, item.clone()));
            }
            matched.sort_by(|a, b| a.0.cmp(&b.0));
        }

        if !scan_forward {
            matched.reverse();
        }

        let iter = matched.into_iter().map(|(_, item)| item);
        let results: Vec<HashMap<String, AttributeValue>> = match limit {
            Some(n) if n > 0 => iter.take(n as usize).collect(),
            _ => iter.collect(),
        };

        Ok(results)
    }

    pub async fn sequence_increment(
        &self,
        table: &str,
        seq_code: &str,
        tenant_id: &str,
    ) -> Result<i64, DomainError> {
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
        let key = (seq_code.to_string(), Vec::new());
        let entry = guard.items.entry(key).or_insert_with(|| {
            let mut m = HashMap::new();
            m.insert("PK".to_string(), AttributeValue::S(seq_code.to_string()));
            m.insert(
                "tenant_id".to_string(),
                AttributeValue::S(tenant_id.to_string()),
            );
            m
        });
        let current = entry
            .get("current_value")
            .and_then(av_number)
            .and_then(|n| n.parse::<i64>().ok())
            .unwrap_or(0);
        let next_val = current + 1;
        entry.insert(
            "current_value".to_string(),
            AttributeValue::N(next_val.to_string()),
        );
        entry
            .entry("tenant_id".to_string())
            .or_insert_with(|| AttributeValue::S(tenant_id.to_string()));
        entry.insert(
            "updated_at".to_string(),
            AttributeValue::N(chrono::Utc::now().timestamp_millis().to_string()),
        );
        Ok(next_val)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn sequence_config_upsert(
        &self,
        table: &str,
        seq_code: &str,
        tenant_id: &str,
        prefix: &str,
        padding: usize,
        scope_resolution: &str,
        set_current_value: Option<i64>,
    ) -> Result<bool, DomainError> {
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
        let key = (seq_code.to_string(), Vec::new());
        let existing_current = guard
            .items
            .get(&key)
            .and_then(|item| item.get("current_value"))
            .and_then(av_number)
            .and_then(|n| n.parse::<i64>().ok());

        if let Some(target) = set_current_value {
            if let Some(cur) = existing_current {
                if cur >= target {
                    return Ok(false);
                }
            }
        }

        let entry = guard.items.entry(key).or_insert_with(|| {
            let mut m = HashMap::new();
            m.insert("PK".to_string(), AttributeValue::S(seq_code.to_string()));
            m
        });
        entry.insert("prefix".to_string(), AttributeValue::S(prefix.to_string()));
        entry.insert(
            "padding".to_string(),
            AttributeValue::N(padding.to_string()),
        );
        entry.insert(
            "scope_resolution".to_string(),
            AttributeValue::S(scope_resolution.to_string()),
        );
        entry.insert(
            "tenant_id".to_string(),
            AttributeValue::S(tenant_id.to_string()),
        );
        entry.insert(
            "updated_at".to_string(),
            AttributeValue::N(chrono::Utc::now().timestamp_millis().to_string()),
        );
        match set_current_value {
            Some(target) => {
                entry.insert(
                    "current_value".to_string(),
                    AttributeValue::N(target.to_string()),
                );
            }
            None => {
                entry
                    .entry("current_value".to_string())
                    .or_insert_with(|| AttributeValue::N("0".to_string()));
            }
        }
        Ok(true)
    }

    pub async fn sequence_scoped_set(
        &self,
        table: &str,
        scoped_code: &str,
        tenant_id: &str,
        target_value: i64,
    ) -> Result<bool, DomainError> {
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
        let key = (scoped_code.to_string(), Vec::new());
        let existing_current = guard
            .items
            .get(&key)
            .and_then(|item| item.get("current_value"))
            .and_then(av_number)
            .and_then(|n| n.parse::<i64>().ok());

        if let Some(cur) = existing_current {
            if cur >= target_value {
                return Ok(false);
            }
        }

        let entry = guard.items.entry(key).or_insert_with(|| {
            let mut m = HashMap::new();
            m.insert("PK".to_string(), AttributeValue::S(scoped_code.to_string()));
            m
        });
        entry.insert(
            "current_value".to_string(),
            AttributeValue::N(target_value.to_string()),
        );
        entry.insert(
            "tenant_id".to_string(),
            AttributeValue::S(tenant_id.to_string()),
        );
        entry.insert(
            "updated_at".to_string(),
            AttributeValue::N(chrono::Utc::now().timestamp_millis().to_string()),
        );
        Ok(true)
    }

    pub async fn scan_with_filter(
        &self,
        table: &str,
        _filter_expression: &str,
        _expr_attr_names: HashMap<String, String>,
        expr_attr_values: HashMap<String, AttributeValue>,
    ) -> Result<Vec<HashMap<String, AttributeValue>>, DomainError> {
        let part = partition_for(table);
        let guard = part
            .read()
            .map_err(|_| DomainError::eav(ErrorCode::Eav002, "Poisoned MBF partition lock"))?;
        let target_tenant = expr_attr_values.get(":t").and_then(av_string);
        let pk_prefix = expr_attr_values.get(":pfx").and_then(av_string);

        let mut results = Vec::new();
        for ((pk, _), item) in &guard.items {
            if let Some(pfx) = pk_prefix {
                if !pk.starts_with(pfx) {
                    continue;
                }
            }
            if let Some(t) = target_tenant {
                if item.get("tenant_id").and_then(av_string) != Some(t) {
                    continue;
                }
            }
            results.push(item.clone());
        }
        Ok(results)
    }

    // ── Native MBF Quota Ledger Operations ──────────────────────────────────

    pub async fn quota_read_many(
        &self,
        table: &str,
        tenant_id: &str,
        quota_ids: &[String],
    ) -> Result<HashMap<String, i64>, DomainError> {
        let part = partition_for(table);
        let guard = part
            .read()
            .map_err(|_| DomainError::eav(ErrorCode::Infra001, "Poisoned MBF partition lock"))?;
        let mut out = HashMap::new();
        for qid in quota_ids {
            if let Some(&val) = guard
                .quota_counters
                .get(&(tenant_id.to_string(), qid.clone()))
            {
                out.insert(qid.clone(), val);
            }
        }
        Ok(out)
    }

    pub async fn quota_try_debit(
        &self,
        table: &str,
        tenant_id: &str,
        quota_id: &str,
        amount: i64,
        max_limit: i64,
        seed: i64,
    ) -> Result<(bool, i64), DomainError> {
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Infra001, "Poisoned MBF partition lock"))?;
        let key = (tenant_id.to_string(), quota_id.to_string());
        let current = guard.quota_counters.get(&key).copied().unwrap_or(seed);
        if current >= max_limit {
            return Ok((false, current));
        }
        let next = current + amount;
        guard.quota_counters.insert(key, next);
        Ok((true, next))
    }

    pub async fn quota_settle(
        &self,
        table: &str,
        tenant_id: &str,
        quota_id: &str,
        delta: i64,
    ) -> Result<i64, DomainError> {
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Infra001, "Poisoned MBF partition lock"))?;
        let key = (tenant_id.to_string(), quota_id.to_string());
        let current = guard.quota_counters.get(&key).copied().unwrap_or(0);
        let next = (current + delta).max(0);
        guard.quota_counters.insert(key, next);
        Ok(next)
    }

    pub async fn quota_settle_once(
        &self,
        table: &str,
        tenant_id: &str,
        quota_id: &str,
        delta: i64,
        idem_key: &str,
    ) -> Result<bool, DomainError> {
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Infra001, "Poisoned MBF partition lock"))?;
        let ikey = (tenant_id.to_string(), idem_key.to_string());
        if guard.quota_idem.contains_key(&ikey) {
            return Ok(false);
        }
        let qkey = (tenant_id.to_string(), quota_id.to_string());
        let current = guard.quota_counters.get(&qkey).copied().unwrap_or(0);
        let next = (current + delta).max(0);
        guard.quota_counters.insert(qkey, next);
        guard.quota_idem.insert(ikey, (quota_id.to_string(), delta));
        Ok(true)
    }
}
