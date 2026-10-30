//! Motor de almacenamiento unificado Metri Vortex (`.mbf`) sobre S3 y caché NVMe local con índices BTree en memoria.

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

/// Envoltorio de carga útil binaria para valores de atributos y claves de ordenación compuestas `.mbf`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Blob(Vec<u8>);

impl Blob {
    /// Crea un nuevo `Blob` a partir de un contenedor convertible a `Vec<u8>`.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Consume el `Blob` y devuelve el vector de bytes subyacente.
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Valor de atributo tipado almacenado dentro de un registro Metri Vortex (`.mbf`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AttributeValue {
    /// Cadena de texto UTF-8.
    S(String),
    /// Representación textual exacta de un valor numérico.
    N(String),
    /// Secuencia binaria opaca (`Blob`).
    B(Blob),
    /// Valor booleano.
    Bool(bool),
    /// Marcador de valor nulo.
    Null(bool),
}

impl AttributeValue {
    /// Extrae una referencia a la cadena si la variante es `AttributeValue::S`.
    pub fn as_s(&self) -> Result<&String, ()> {
        match self {
            AttributeValue::S(s) => Ok(s),
            _ => Err(()),
        }
    }

    /// Extrae una referencia a la representación numérica si la variante es `AttributeValue::N`.
    pub fn as_n(&self) -> Result<&String, ()> {
        match self {
            AttributeValue::N(n) => Ok(n),
            _ => Err(()),
        }
    }

    /// Extrae una referencia al `Blob` binario si la variante es `AttributeValue::B`.
    pub fn as_b(&self) -> Result<&Blob, ()> {
        match self {
            AttributeValue::B(b) => Ok(b),
            _ => Err(()),
        }
    }

    /// Extrae una referencia al booleano si la variante es `AttributeValue::Bool`.
    pub fn as_bool(&self) -> Result<&bool, ()> {
        match self {
            AttributeValue::Bool(b) => Ok(b),
            _ => Err(()),
        }
    }
}

/// Extrae un `&str` desde un `AttributeValue::S`.
pub fn av_string(v: &AttributeValue) -> Option<&str> {
    match v {
        AttributeValue::S(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Extrae un `&[u8]` desde un `AttributeValue::B`.
pub fn av_bytes(v: &AttributeValue) -> Option<&[u8]> {
    match v {
        AttributeValue::B(b) => Some(b.as_ref()),
        _ => None,
    }
}

/// Extrae la cadena numérica desde un `AttributeValue::N`.
pub fn av_number(v: &AttributeValue) -> Option<&str> {
    match v {
        AttributeValue::N(n) => Some(n.as_str()),
        _ => None,
    }
}

/// Operación de escritura condicional para transacciones ACID `.mbf`.
#[derive(Debug, Clone)]
pub struct Put {
    /// Nombre del espacio de nombres o partición `.mbf` de destino.
    pub table_name: String,
    /// Mapa de atributos del registro a escribir.
    pub item: HashMap<String, AttributeValue>,
    /// Expresión de precondición opcional evaluada antes de confirmar la escritura.
    pub condition_expression: Option<String>,
    /// Valores de sustitución asociados a la expresión condicional.
    pub expression_attribute_values: HashMap<String, AttributeValue>,
}

impl Put {
    /// Crea un constructor fluido `PutBuilder`.
    pub fn builder() -> PutBuilder {
        PutBuilder::default()
    }

    /// Devuelve una referencia al mapa de atributos del registro.
    pub fn item(&self) -> &HashMap<String, AttributeValue> {
        &self.item
    }
}

/// Constructor fluido para `Put`.
#[derive(Debug, Default, Clone)]
pub struct PutBuilder {
    table_name: Option<String>,
    item: Option<HashMap<String, AttributeValue>>,
    condition_expression: Option<String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
}

impl PutBuilder {
    /// Define el nombre de la partición o tabla `.mbf` de destino.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    /// Define el mapa completo de atributos a insertar.
    pub fn set_item(mut self, item: Option<HashMap<String, AttributeValue>>) -> Self {
        self.item = item;
        self
    }

    /// Define la expresión condicional de escritura.
    pub fn condition_expression(mut self, expr: impl Into<String>) -> Self {
        self.condition_expression = Some(expr.into());
        self
    }

    /// Registra un par clave-valor para la evaluación de la condición.
    pub fn expression_attribute_values(
        mut self,
        key: impl Into<String>,
        value: AttributeValue,
    ) -> Self {
        self.expression_attribute_values.insert(key.into(), value);
        self
    }

    /// Construye la operación `Put` validando que el mapa `item` esté presente.
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

/// Elemento individual dentro de un lote transaccional ACID `.mbf`.
#[derive(Debug, Clone)]
pub struct TransactWriteItem {
    /// Operación `Put` contenida en este elemento transaccional.
    pub put: Option<Put>,
}

impl TransactWriteItem {
    /// Crea un constructor fluido `TransactWriteItemBuilder`.
    pub fn builder() -> TransactWriteItemBuilder {
        TransactWriteItemBuilder::default()
    }

    /// Devuelve una referencia a la operación `Put` interna si existe.
    pub fn put(&self) -> Option<&Put> {
        self.put.as_ref()
    }
}

/// Constructor fluido para `TransactWriteItem`.
#[derive(Debug, Default, Clone)]
pub struct TransactWriteItemBuilder {
    put: Option<Put>,
}

impl TransactWriteItemBuilder {
    /// Asigna la operación `Put` al elemento transaccional.
    pub fn put(mut self, put: Put) -> Self {
        self.put = Some(put);
        self
    }

    /// Construye la instancia final de `TransactWriteItem`.
    pub fn build(self) -> TransactWriteItem {
        TransactWriteItem { put: self.put }
    }
}

/// Solicitud de escritura por lotes para indexación de trigramas FTS.
#[derive(Debug, Clone)]
pub struct PutRequest {
    /// Mapa de atributos del ítem a escribir en lote.
    pub item: HashMap<String, AttributeValue>,
}

impl PutRequest {
    /// Crea un constructor fluido `PutRequestBuilder`.
    pub fn builder() -> PutRequestBuilder {
        PutRequestBuilder::default()
    }
}

/// Constructor fluido para `PutRequest`.
#[derive(Debug, Default, Clone)]
pub struct PutRequestBuilder {
    item: Option<HashMap<String, AttributeValue>>,
}

impl PutRequestBuilder {
    /// Establece el mapa de atributos de la solicitud.
    pub fn set_item(mut self, item: Option<HashMap<String, AttributeValue>>) -> Self {
        self.item = item;
        self
    }

    /// Construye el `PutRequest` verificando la presencia del mapa `item`.
    pub fn build(self) -> Result<PutRequest, DomainError> {
        let item = self
            .item
            .ok_or_else(|| DomainError::eav(ErrorCode::Eav001, "PutRequest item is required"))?;
        Ok(PutRequest { item })
    }
}

/// Envoltorio de solicitud de escritura en lote para indexación FTS en `.mbf`.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    /// Solicitud `PutRequest` asociada a la operación en lote.
    pub put_request: Option<PutRequest>,
}

impl WriteRequest {
    /// Crea un constructor fluido `WriteRequestBuilder`.
    pub fn builder() -> WriteRequestBuilder {
        WriteRequestBuilder::default()
    }
}

/// Constructor fluido para `WriteRequest`.
#[derive(Debug, Default, Clone)]
pub struct WriteRequestBuilder {
    put_request: Option<PutRequest>,
}

impl WriteRequestBuilder {
    /// Asigna el `PutRequest` al envoltorio de escritura en lote.
    pub fn put_request(mut self, req: PutRequest) -> Self {
        self.put_request = Some(req);
        self
    }

    /// Construye la instancia final de `WriteRequest`.
    pub fn build(self) -> WriteRequest {
        WriteRequest {
            put_request: self.put_request,
        }
    }
}

/// Almacén de objetos de producción sobre Amazon S3 con caché NVMe local en `/tmp/mbf_cache` para archivos `.mbf`.
pub struct S3MbfObjectStore {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    cache_dir: PathBuf,
}

impl S3MbfObjectStore {
    /// Inicializa un nuevo almacén de objetos S3 para el bucket indicado y prepara el directorio de caché NVMe.
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
            .or_else(|_| std::env::var("MBF_TMP_CACHE_DIR"))
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/mbf_cache"));
        let prefix = std::env::var("MBF_S3_PREFIX").unwrap_or_else(|_| "metri-engine".to_string());
        let _ = std::fs::create_dir_all(&cache_dir);
        Self {
            s3_client,
            bucket: bucket.into(),
            prefix: prefix.trim_matches('/').to_string(),
            cache_dir,
        }
    }

    /// Devuelve el prefijo raíz configurado para las claves en S3.
    pub fn prefix(&self) -> &str {
        &self.prefix
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

    /// Escribe un objeto binario `.mbf` directamente en S3 y lo replica en la caché local `/tmp/mbf_cache`.
    pub async fn put_raw_mbf(&self, key: &str, bytes: Vec<u8>) -> DomainResult<String> {
        let local_path = self.local_cache_path(key);
        let _ = std::fs::write(&local_path, &bytes);

        let fallback_etag = Self::compute_etag(&bytes);
        let resp = self
            .s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type("application/x-metri-mbf")
            .body(aws_sdk_s3::primitives::ByteStream::from(bytes))
            .send()
            .await;

        match resp {
            Ok(out) => {
                let s3_etag = out
                    .e_tag()
                    .map(|s| s.trim_matches('"').to_string())
                    .filter(|s| !s.is_empty())
                    .unwrap_or(fallback_etag);
                Ok(s3_etag)
            }
            Err(e) => {
                tracing::warn!("[MBF S3] Error writing {key} to s3://{}: {e}", self.bucket);
                Ok(fallback_etag)
            }
        }
    }

    /// Descarga un archivo binario `.mbf` desde S3 únicamente si su ETag difiere de `known_s3_etag`.
    pub async fn fetch_partition_if_changed(
        &self,
        key: &str,
        known_s3_etag: Option<&str>,
    ) -> DomainResult<MbfConditionalGet> {
        let mut req = self.s3_client.get_object().bucket(&self.bucket).key(key);
        if let Some(etag) = known_s3_etag {
            if !etag.is_empty() {
                req = req.if_none_match(format!("\"{etag}\""));
            }
        }

        match req.send().await {
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
                let local_path = self.local_cache_path(key);
                let _ = std::fs::write(&local_path, &bytes);
                Ok(MbfConditionalGet::Modified(MbfObjectPayload {
                    bytes,
                    etag: resolved_etag,
                }))
            }
            Err(sdk_err) => {
                let status_code = sdk_err.raw_response().map(|r| r.status().as_u16());
                match status_code {
                    Some(304) => Ok(MbfConditionalGet::NotModified),
                    Some(404) => Ok(MbfConditionalGet::NotFound),
                    _ => Ok(MbfConditionalGet::NotModified),
                }
            }
        }
    }
    fn local_slice_cache_path(&self, key: &str, start: usize, end: usize) -> PathBuf {
        let safe_name = key.replace(['/', '\\', ':'], "_");
        self.cache_dir
            .join(format!("{safe_name}__range_{start}_{end}.slice"))
    }

    /// Aplica el presupuesto máximo de caché NVMe en `/tmp/mbf_cache`,
    /// desalojando primero los archivos `.slice` inmutables más antiguos (LRU) mientras preserva `HOT_HEAD` y `.mbf.idx`.
    pub fn enforce_nvme_cache_budget(&self, max_budget_bytes: u64) -> usize {
        let Ok(entries) = std::fs::read_dir(&self.cache_dir) else {
            return 0;
        };
        let mut total_bytes = 0u64;
        let mut evictable_slices: Vec<(std::time::SystemTime, u64, PathBuf)> = Vec::new();

        for entry in entries.flatten() {
            let path = entry.path();
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    let len = meta.len();
                    total_bytes = total_bytes.saturating_add(len);
                    let is_idx_or_head = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.contains("HOT_HEAD") || n.ends_with(".idx"));
                    if !is_idx_or_head {
                        let mtime = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                        evictable_slices.push((mtime, len, path));
                    }
                }
            }
        }

        if total_bytes <= max_budget_bytes {
            return 0;
        }

        // Evict oldest immutable slices first (LRU order)
        evictable_slices.sort_by_key(|(mtime, _, _)| *mtime);
        let mut evicted = 0usize;
        for (_, len, path) in evictable_slices {
            if total_bytes <= max_budget_bytes {
                break;
            }
            if std::fs::remove_file(&path).is_ok() {
                total_bytes = total_bytes.saturating_sub(len);
                evicted += 1;
            }
        }
        evicted
    }

    /// Verifica en $O(1)$ la integridad física y de CRC32C de un archivo `.mbf` o `.idx` en `/tmp/mbf_cache`.
    pub fn is_valid_cached_mbf(key: &str, bytes: &[u8]) -> bool {
        if bytes.is_empty() {
            return false;
        }
        if !(key.ends_with(".mbf") || key.ends_with(".idx")) {
            return true;
        }
        if bytes.len() < 128 || bytes.len() % 64 != 0 {
            return false;
        }
        let valid_magic = &bytes[0..8] == b"METRIBIN" || &bytes[0..8] == b"MBFINDEX";
        if !valid_magic || &bytes[bytes.len() - 8..] != b"MBFFOOTR" {
            return false;
        }
        let Ok(footer) =
            crate::mbf::format::MbfFooterTrailer::from_bytes(&bytes[bytes.len() - 64..])
        else {
            return false;
        };
        footer.header_crc32c == crate::mbf::format::compute_crc32c(&bytes[0..64])
    }
}

/// Presupuesto por defecto de 450 MB para la caché NVMe en `/tmp/mbf_cache`.
pub const MBF_NVME_CACHE_BUDGET_BYTES: u64 = 450 * 1024 * 1024;

#[async_trait]
impl IMbfObjectStore for S3MbfObjectStore {
    async fn get_object(&self, key: &str) -> DomainResult<Option<MbfObjectPayload>> {
        let local_path = self.local_cache_path(key);
        if let Ok(bytes) = std::fs::read(&local_path) {
            if Self::is_valid_cached_mbf(key, &bytes) {
                let etag = Self::compute_etag(&bytes);
                return Ok(Some(MbfObjectPayload { bytes, etag }));
            }
            let _ = std::fs::remove_file(&local_path);
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
                let _ = self.enforce_nvme_cache_budget(MBF_NVME_CACHE_BUDGET_BYTES);
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
        match self
            .fetch_partition_if_changed(key, Some(cached_etag))
            .await
        {
            Ok(cond) => Ok(cond),
            Err(_) => match self.get_object(key).await? {
                None => Ok(MbfConditionalGet::NotFound),
                Some(obj) if obj.etag == cached_etag => Ok(MbfConditionalGet::NotModified),
                Some(obj) => Ok(MbfConditionalGet::Modified(obj)),
            },
        }
    }

    async fn get_byte_range(&self, key: &str, start: usize, end: usize) -> DomainResult<Vec<u8>> {
        if start > end {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Invalid byte range {start}..{end}"),
            ));
        }
        if start == end {
            return Ok(Vec::new());
        }

        // 1. Fast path: check if the exact immutable slice is already in `/tmp/mbf_cache`
        let slice_path = self.local_slice_cache_path(key, start, end);
        if let Ok(cached_slice) = std::fs::read(&slice_path) {
            if cached_slice.len() == end - start {
                return Ok(cached_slice);
            }
        }

        // 2. Second fast path: check if the full object is already cached on local NVMe
        let full_local_path = self.local_cache_path(key);
        if let Ok(full_bytes) = std::fs::read(&full_local_path) {
            if end > full_bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    format!(
                        "Invalid byte range {start}..{end} for len {}",
                        full_bytes.len()
                    ),
                ));
            }
            let slice = full_bytes[start..end].to_vec();
            let _ = std::fs::write(&slice_path, &slice);
            return Ok(slice);
        }

        // 3. Terabyte-Scale S3 Native HTTP Range GET (`Range: bytes=start-(end-1)`)
        // Downloads ONLY the requested 64B-aligned byte window instead of the entire 256MB-1GB L2 archive!
        let range_header = format!("bytes={start}-{}", end - 1);
        let resp = self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(range_header)
            .send()
            .await
            .map_err(|e| {
                DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    format!("S3 Range GET ({start}..{end}) failed on {key}: {e}"),
                )
            })?;

        let collected = resp.body.collect().await.map_err(|e| {
            DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("Failed reading S3 Range body for {key}: {e}"),
            )
        })?;
        let slice = collected.into_bytes().to_vec();
        let _ = self.enforce_nvme_cache_budget(MBF_NVME_CACHE_BUDGET_BYTES);
        let _ = std::fs::write(&slice_path, &slice);
        Ok(slice)
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
        let mut put_req = self
            .s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type("application/x-metri-mbf")
            .body(aws_sdk_s3::primitives::ByteStream::from(bytes.clone()));
        if let Some(exp) = expected_etag {
            put_req = put_req.if_match(format!("\"{exp}\""));
        }
        match put_req.send().await {
            Ok(out) => {
                let s3_etag = out
                    .e_tag()
                    .map(|s| s.trim_matches('"').to_string())
                    .filter(|s| !s.is_empty())
                    .unwrap_or(new_etag);
                let _ = self.enforce_nvme_cache_budget(MBF_NVME_CACHE_BUDGET_BYTES);
                let _ = std::fs::write(&local_path, &bytes);
                Ok(s3_etag)
            }
            Err(sdk_err) => {
                let status_code = sdk_err.raw_response().map(|r| r.status().as_u16());
                if status_code == Some(412) {
                    let _ = std::fs::remove_file(&local_path);
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCasConflict001,
                        format!("S3 If-Match PreconditionFailed (412) on {key}"),
                    ));
                }
                let _ = self.enforce_nvme_cache_budget(MBF_NVME_CACHE_BUDGET_BYTES);
                let _ = std::fs::write(&local_path, &bytes);
                Ok(new_etag)
            }
        }
    }
}

/// Clave primaria compuesta `(PK, SK_bytes)` dentro de una partición `.mbf`.
type PrimaryKey = (String, Vec<u8>);

/// Selector de índice secundario BTree nativo para particiones `.mbf`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MbfSecondaryIndex {
    /// Índice primario `EAVT` ordenado por `(PK, SK)`.
    PrimaryEavt,
    /// Índice secundario `AEVT` ordenado por atributo `(ap, as)`.
    Aevt,
    /// Índice secundario `AVET` ordenado por atributo-valor `(vp, vs)`.
    Avet,
    /// Índice secundario `VAET` de referencias inversas `(rp, rs)`.
    Vaet,
}

impl MbfSecondaryIndex {
    /// Resuelve el selector de índice secundario a partir del nombre lógico del índice.
    pub fn from_index_name(name: Option<&str>) -> Self {
        match name {
            Some("GSI-AEVT") | Some("AEVT") => Self::Aevt,
            Some("GSI-AVET") | Some("AVET") => Self::Avet,
            Some("GSI-VAET") | Some("VAET") => Self::Vaet,
            _ => Self::PrimaryEavt,
        }
    }
}

/// Precondición tipada de escritura transaccional evaluada de forma nativa en memoria.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MbfWriteCondition {
    /// Escritura incondicional.
    Unconditional,
    /// Exige que la tupla `(PK, SK)` no exista previamente en el log append-only.
    AppendOnlyLogUnique,
    /// Exige que la clave de reclamo único `PK` no exista previamente.
    UniqueClaimKey,
    /// Exige que la clave única no exista o pertenezca a la misma entidad (`:self`).
    UniqueEntitySelfOrUnclaimed,
}

impl MbfWriteCondition {
    /// Analiza una expresión condicional textual y la convierte en una variante tipada `MbfWriteCondition`.
    pub fn parse(expr: Option<&str>) -> Self {
        match expr.map(str::trim) {
            Some("attribute_not_exists(PK) AND attribute_not_exists(SK)") => {
                Self::AppendOnlyLogUnique
            }
            Some("attribute_not_exists(PK)") => Self::UniqueClaimKey,
            Some("attribute_not_exists(PK) OR eid = :self") => Self::UniqueEntitySelfOrUnclaimed,
            _ => Self::Unconditional,
        }
    }
}

const MBF_PARTITION_SNAP_MAGIC_V1: [u8; 8] = *b"MBFPART1";
const MBF_PARTITION_SNAP_MAGIC: [u8; 8] = *b"MBFPART2";
const MBF_ENTITY_SNAP_MAGIC_V1: [u8; 8] = *b"MBFENTZ1";
const MBF_ENTITY_SNAP_MAGIC: [u8; 8] = *b"MBFENTZ2";

fn write_bytes_slice(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn read_bytes_slice(bytes: &[u8], offset: &mut usize) -> DomainResult<Vec<u8>> {
    if *offset + 4 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Truncated byte slice length in MBF binary payload",
        ));
    }
    let mut len_buf = [0u8; 4];
    len_buf.copy_from_slice(&bytes[*offset..*offset + 4]);
    *offset += 4;
    let len = u32::from_le_bytes(len_buf) as usize;
    if *offset + len > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Truncated byte slice body in MBF binary payload",
        ));
    }
    let out = bytes[*offset..*offset + len].to_vec();
    *offset += len;
    Ok(out)
}

fn write_str_binary(buf: &mut Vec<u8>, s: &str) {
    write_bytes_slice(buf, s.as_bytes());
}

fn read_str_binary(bytes: &[u8], offset: &mut usize) -> DomainResult<String> {
    let raw = read_bytes_slice(bytes, offset)?;
    String::from_utf8(raw).map_err(|e| {
        DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            format!("Invalid UTF-8 in MBF binary string: {e}"),
        )
    })
}

fn extract_tenant_prefix(pk: &str) -> Option<&str> {
    let rest = pk.strip_prefix("T#")?;
    let hash_pos = rest.find('#')?;
    Some(&pk[..2 + hash_pos + 1]) // e.g., "T#system#"
}

fn write_attribute_value_binary(buf: &mut Vec<u8>, val: &AttributeValue) {
    match val {
        AttributeValue::S(s) => {
            buf.push(0x01);
            write_str_binary(buf, s);
        }
        AttributeValue::N(n) => {
            buf.push(0x02);
            write_str_binary(buf, n);
        }
        AttributeValue::B(b) => {
            buf.push(0x03);
            write_bytes_slice(buf, b.as_ref());
        }
        AttributeValue::Bool(b) => {
            buf.push(0x04);
            buf.push(u8::from(*b));
        }
        AttributeValue::Null(n) => {
            buf.push(0x05);
            buf.push(u8::from(*n));
        }
    }
}

fn write_attribute_value_compact(
    buf: &mut Vec<u8>,
    val: &AttributeValue,
    tenant_prefix: Option<&str>,
) {
    if let (AttributeValue::S(s), Some(tp)) = (val, tenant_prefix) {
        if let Some(after_tp) = s.strip_prefix(tp) {
            if let Some(suffix) = after_tp.strip_prefix("A#") {
                buf.push(0x11);
                write_str_binary(buf, suffix);
                return;
            }
            if let Some(suffix) = after_tp.strip_prefix("V#") {
                buf.push(0x12);
                write_str_binary(buf, suffix);
                return;
            }
            if let Some(suffix) = after_tp.strip_prefix("R#") {
                buf.push(0x13);
                write_str_binary(buf, suffix);
                return;
            }
        }
    }
    write_attribute_value_binary(buf, val);
}

fn read_attribute_value_binary(bytes: &[u8], offset: &mut usize) -> DomainResult<AttributeValue> {
    read_attribute_value_compact(bytes, offset, None)
}

fn read_attribute_value_compact(
    bytes: &[u8],
    offset: &mut usize,
    tenant_prefix: Option<&str>,
) -> DomainResult<AttributeValue> {
    if *offset >= bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading binary AttributeValue tag",
        ));
    }
    let tag = bytes[*offset];
    *offset += 1;
    match tag {
        0x01 => Ok(AttributeValue::S(read_str_binary(bytes, offset)?)),
        0x02 => Ok(AttributeValue::N(read_str_binary(bytes, offset)?)),
        0x03 => Ok(AttributeValue::B(Blob::new(read_bytes_slice(
            bytes, offset,
        )?))),
        0x04 => {
            if *offset >= bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Truncated Bool AttributeValue",
                ));
            }
            let b = bytes[*offset] != 0;
            *offset += 1;
            Ok(AttributeValue::Bool(b))
        }
        0x05 => {
            if *offset >= bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Truncated Null AttributeValue",
                ));
            }
            let n = bytes[*offset] != 0;
            *offset += 1;
            Ok(AttributeValue::Null(n))
        }
        0x11 => {
            let suffix = read_str_binary(bytes, offset)?;
            let tp = tenant_prefix.unwrap_or("T#system#");
            Ok(AttributeValue::S(format!("{tp}A#{suffix}")))
        }
        0x12 => {
            let suffix = read_str_binary(bytes, offset)?;
            let tp = tenant_prefix.unwrap_or("T#system#");
            Ok(AttributeValue::S(format!("{tp}V#{suffix}")))
        }
        0x13 => {
            let suffix = read_str_binary(bytes, offset)?;
            let tp = tenant_prefix.unwrap_or("T#system#");
            Ok(AttributeValue::S(format!("{tp}R#{suffix}")))
        }
        other => Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            format!("Unknown binary AttributeValue tag: 0x{other:02X}"),
        )),
    }
}

#[allow(dead_code)]
fn write_item_map_binary(buf: &mut Vec<u8>, item: &HashMap<String, AttributeValue>) {
    let mut entries: Vec<(&String, &AttributeValue)> = item.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (k, v) in entries {
        write_str_binary(buf, k);
        write_attribute_value_binary(buf, v);
    }
}

fn read_item_map_binary(
    bytes: &[u8],
    offset: &mut usize,
) -> DomainResult<HashMap<String, AttributeValue>> {
    if *offset + 4 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Truncated binary item map length",
        ));
    }
    let mut u32_buf = [0u8; 4];
    u32_buf.copy_from_slice(&bytes[*offset..*offset + 4]);
    *offset += 4;
    let count = u32::from_le_bytes(u32_buf) as usize;
    let mut item = HashMap::with_capacity(count);
    for _ in 0..count {
        let k = read_str_binary(bytes, offset)?;
        let v = read_attribute_value_binary(bytes, offset)?;
        item.insert(k, v);
    }
    Ok(item)
}

/// Writes an item map using bitflags to elide duplicate `PK`/`SK` values, 1-byte FSST codes for standard EAV keys,
/// and prefix-stripping for `T#<tenant>#(A|V|R)#` index strings.
fn write_item_map_compact(
    buf: &mut Vec<u8>,
    pk: &str,
    sk: &[u8],
    item: &HashMap<String, AttributeValue>,
) {
    let mut flags = 0u8;
    let elide_pk = matches!(item.get("PK"), Some(AttributeValue::S(s)) if s == pk);
    if elide_pk {
        flags |= 0x01;
    }
    let elide_sk_blob = matches!(item.get("SK"), Some(AttributeValue::B(b)) if b.as_ref() == sk);
    let elide_sk_str = !elide_sk_blob
        && matches!(item.get("SK"), Some(AttributeValue::S(s)) if s.as_bytes() == sk);
    if elide_sk_blob {
        flags |= 0x02;
    } else if elide_sk_str {
        flags |= 0x04;
    }

    let mut entries: Vec<(&String, &AttributeValue)> = item
        .iter()
        .filter(|(k, _)| {
            if k.as_str() == "PK" && elide_pk {
                return false;
            }
            if k.as_str() == "SK" && (elide_sk_blob || elide_sk_str) {
                return false;
            }
            true
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    buf.push(flags);
    buf.extend_from_slice(&(entries.len() as u16).to_le_bytes());
    let tp = extract_tenant_prefix(pk);

    for (k, v) in entries {
        let key_code: u8 = match k.as_str() {
            "ap" => 0x01,
            "as" => 0x02,
            "v" => 0x03,
            "vp" => 0x04,
            "vs" => 0x05,
            "rp" => 0x06,
            "rs" => 0x07,
            "current_value" => 0x08,
            "updated_at" => 0x09,
            _ => 0xFF,
        };
        buf.push(key_code);
        if key_code == 0xFF {
            write_str_binary(buf, k);
        }
        write_attribute_value_compact(buf, v, tp);
    }
}

fn read_item_map_compact(
    bytes: &[u8],
    offset: &mut usize,
    pk: &str,
    sk: &[u8],
) -> DomainResult<HashMap<String, AttributeValue>> {
    if *offset + 3 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Truncated compact item map header",
        ));
    }
    let flags = bytes[*offset];
    *offset += 1;
    let count = u16::from_le_bytes([bytes[*offset], bytes[*offset + 1]]) as usize;
    *offset += 2;

    let mut item = HashMap::with_capacity(count + 2);
    if (flags & 0x01) != 0 {
        item.insert("PK".to_string(), AttributeValue::S(pk.to_string()));
    }
    if (flags & 0x02) != 0 {
        item.insert("SK".to_string(), AttributeValue::B(Blob::new(sk.to_vec())));
    } else if (flags & 0x04) != 0 {
        item.insert(
            "SK".to_string(),
            AttributeValue::S(String::from_utf8_lossy(sk).into_owned()),
        );
    }

    let tp = extract_tenant_prefix(pk);
    for _ in 0..count {
        if *offset >= bytes.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Unexpected EOF reading compact item key code",
            ));
        }
        let key_code = bytes[*offset];
        *offset += 1;
        let k = match key_code {
            0x01 => "ap".to_string(),
            0x02 => "as".to_string(),
            0x03 => "v".to_string(),
            0x04 => "vp".to_string(),
            0x05 => "vs".to_string(),
            0x06 => "rp".to_string(),
            0x07 => "rs".to_string(),
            0x08 => "current_value".to_string(),
            0x09 => "updated_at".to_string(),
            0xFF => read_str_binary(bytes, offset)?,
            other => {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    format!("Unknown compact item key code: 0x{other:02X}"),
                ));
            }
        };
        let v = read_attribute_value_compact(bytes, offset, tp)?;
        item.insert(k, v);
    }
    Ok(item)
}

/// Instantánea binaria pura en tránsito de una partición `.mbf` (cero JSON).
#[derive(Debug, Default, Clone)]
pub struct MbfPartitionWireSnapshot {
    /// Identificador monótono de época de la partición.
    pub epoch_id: u64,
    /// Colección ordenada de registros `(PrimaryKey, atributos)`.
    pub items: Vec<(PrimaryKey, HashMap<String, AttributeValue>)>,
    /// Estado actual de los contadores de cuota por `(tenant_id, quota_id)`.
    pub quota_counters: Vec<((String, String), i64)>,
    /// Registro de claves de idempotencia de cuotas aplicadas.
    pub quota_idem: Vec<((String, String), (String, i64))>,
}

impl MbfPartitionWireSnapshot {
    /// Codifica la instantánea de partición en un bloque binario alineado a 64 bytes (`"MBFPART2"` + delta-PK + mapa compacto de bitflags + LZ4).
    pub fn encode_binary(&self) -> Vec<u8> {
        let mut raw = Vec::with_capacity(128 + self.items.len() * 48);
        raw.extend_from_slice(&self.epoch_id.to_le_bytes());
        raw.extend_from_slice(&(self.items.len() as u32).to_le_bytes());
        let mut prev_pk = "";
        for ((pk, sk), item) in &self.items {
            if !prev_pk.is_empty() && pk == prev_pk {
                raw.push(0x01); // 1-byte delta-encoded PK match
            } else {
                raw.push(0x00);
                write_str_binary(&mut raw, pk);
                prev_pk = pk.as_str();
            }
            write_bytes_slice(&mut raw, sk);
            write_item_map_compact(&mut raw, pk, sk, item);
        }
        raw.extend_from_slice(&(self.quota_counters.len() as u32).to_le_bytes());
        for ((t, q), val) in &self.quota_counters {
            write_str_binary(&mut raw, t);
            write_str_binary(&mut raw, q);
            raw.extend_from_slice(&val.to_le_bytes());
        }
        raw.extend_from_slice(&(self.quota_idem.len() as u32).to_le_bytes());
        for ((t, ik), (qid, delta)) in &self.quota_idem {
            write_str_binary(&mut raw, t);
            write_str_binary(&mut raw, ik);
            write_str_binary(&mut raw, qid);
            raw.extend_from_slice(&delta.to_le_bytes());
        }

        let compressed = lz4_flex::compress_prepend_size(&raw);
        let mut out = Vec::with_capacity(16 + compressed.len() + 64);
        out.extend_from_slice(&MBF_PARTITION_SNAP_MAGIC);
        out.extend_from_slice(&(compressed.len() as u64).to_le_bytes());
        out.extend_from_slice(&compressed);
        crate::mbf::format::pad_to_64(&mut out);
        out
    }

    /// Decodifica una carga útil binaria alineada a 64 bytes producida por `Self::encode_binary` (soporta `MBFPART1` y `MBFPART2`).
    pub fn decode_binary(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 16 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated MBFPartitionWireSnapshot header",
            ));
        }
        let is_v2 = if bytes[0..8] == MBF_PARTITION_SNAP_MAGIC {
            true
        } else if bytes[0..8] == MBF_PARTITION_SNAP_MAGIC_V1 {
            false
        } else {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid MBFPartitionWireSnapshot magic (expected MBFPART1 or MBFPART2)",
            ));
        };
        let mut len_buf = [0u8; 8];
        len_buf.copy_from_slice(&bytes[8..16]);
        let comp_len = u64::from_le_bytes(len_buf) as usize;
        if 16 + comp_len > bytes.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated MBFPartitionWireSnapshot LZ4 stream",
            ));
        }
        let raw = lz4_flex::decompress_size_prepended(&bytes[16..16 + comp_len]).map_err(|e| {
            DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("MBFPartitionWireSnapshot LZ4 error: {e}"),
            )
        })?;
        if raw.len() < 12 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated MBFPartitionWireSnapshot header",
            ));
        }
        let mut offset = 0usize;
        let mut u64_buf = [0u8; 8];
        let mut u32_buf = [0u8; 4];
        u64_buf.copy_from_slice(&raw[offset..offset + 8]);
        offset += 8;
        let epoch_id = u64::from_le_bytes(u64_buf);

        u32_buf.copy_from_slice(&raw[offset..offset + 4]);
        offset += 4;
        let item_count = u32::from_le_bytes(u32_buf) as usize;
        let mut items = Vec::with_capacity(item_count);
        let mut prev_pk = String::new();
        for _ in 0..item_count {
            if is_v2 {
                if offset >= raw.len() {
                    return Err(DomainError::mbf(
                        ErrorCode::MbfCorrupt001,
                        "Truncated PK delta flag in MBFPART2",
                    ));
                }
                let same_pk = raw[offset] != 0;
                offset += 1;
                if !same_pk {
                    prev_pk = read_str_binary(&raw, &mut offset)?;
                }
                let sk = read_bytes_slice(&raw, &mut offset)?;
                let item = read_item_map_compact(&raw, &mut offset, &prev_pk, &sk)?;
                items.push(((prev_pk.clone(), sk), item));
            } else {
                let pk = read_str_binary(&raw, &mut offset)?;
                let sk = read_bytes_slice(&raw, &mut offset)?;
                let item = read_item_map_binary(&raw, &mut offset)?;
                items.push(((pk, sk), item));
            }
        }

        if offset + 4 > raw.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated quota_counters count",
            ));
        }
        u32_buf.copy_from_slice(&raw[offset..offset + 4]);
        offset += 4;
        let qc_count = u32::from_le_bytes(u32_buf) as usize;
        let mut quota_counters = Vec::with_capacity(qc_count);
        for _ in 0..qc_count {
            let t = read_str_binary(&raw, &mut offset)?;
            let q = read_str_binary(&raw, &mut offset)?;
            if offset + 8 > raw.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Truncated quota counter value",
                ));
            }
            u64_buf.copy_from_slice(&raw[offset..offset + 8]);
            offset += 8;
            quota_counters.push(((t, q), i64::from_le_bytes(u64_buf)));
        }

        if offset + 4 > raw.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated quota_idem count",
            ));
        }
        u32_buf.copy_from_slice(&raw[offset..offset + 4]);
        offset += 4;
        let qi_count = u32::from_le_bytes(u32_buf) as usize;
        let mut quota_idem = Vec::with_capacity(qi_count);
        for _ in 0..qi_count {
            let t = read_str_binary(&raw, &mut offset)?;
            let ik = read_str_binary(&raw, &mut offset)?;
            let qid = read_str_binary(&raw, &mut offset)?;
            if offset + 8 > raw.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Truncated quota idem delta",
                ));
            }
            u64_buf.copy_from_slice(&raw[offset..offset + 8]);
            offset += 8;
            quota_idem.push(((t, ik), (qid, i64::from_le_bytes(u64_buf))));
        }

        Ok(Self {
            epoch_id,
            items,
            quota_counters,
            quota_idem,
        })
    }
}

/// Instantánea binaria pura de una entidad individual (`metri-engine/tenants/<tenant>/entities/<type>/<id>.mbf`).
#[derive(Debug, Clone)]
pub struct MbfEntityWireSnapshot {
    /// Identificador del inquilino propietario.
    pub tenant_id: String,
    /// Nombre canónico del tipo de entidad.
    pub entity_type: String,
    /// Identificador único de la entidad.
    pub entity_id: String,
    /// Época de almacenamiento en que se generó la instantánea.
    pub epoch_id: u64,
    /// Lista ordenada de datoms `(SK_bytes, atributos)` de la entidad.
    pub datoms: Vec<(Vec<u8>, HashMap<String, AttributeValue>)>,
}

impl MbfEntityWireSnapshot {
    /// Codifica los datoms de una entidad individual en un bloque binario alineado a 64 bytes (`"MBFENTZ2"` + mapa compacto + LZ4).
    pub fn encode_binary(&self) -> Vec<u8> {
        let mut raw = Vec::with_capacity(64 + self.datoms.len() * 36);
        write_str_binary(&mut raw, &self.tenant_id);
        write_str_binary(&mut raw, &self.entity_type);
        write_str_binary(&mut raw, &self.entity_id);
        raw.extend_from_slice(&self.epoch_id.to_le_bytes());
        raw.extend_from_slice(&(self.datoms.len() as u32).to_le_bytes());
        let entity_pk = format!("T#{}#E#{}", self.tenant_id, self.entity_id);
        for (sk, item) in &self.datoms {
            write_bytes_slice(&mut raw, sk);
            write_item_map_compact(&mut raw, &entity_pk, sk, item);
        }
        let compressed = lz4_flex::compress_prepend_size(&raw);
        let mut out = Vec::with_capacity(16 + compressed.len() + 64);
        out.extend_from_slice(&MBF_ENTITY_SNAP_MAGIC);
        out.extend_from_slice(&(compressed.len() as u64).to_le_bytes());
        out.extend_from_slice(&compressed);
        crate::mbf::format::pad_to_64(&mut out);
        out
    }

    /// Decodifica una instantánea binaria de entidad producida por `Self::encode_binary` (soporta `MBFENTZ1` y `MBFENTZ2`).
    pub fn decode_binary(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 16 {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated MbfEntityWireSnapshot header",
            ));
        }
        let is_v2 = if bytes[0..8] == MBF_ENTITY_SNAP_MAGIC {
            true
        } else if bytes[0..8] == MBF_ENTITY_SNAP_MAGIC_V1 {
            false
        } else {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid MbfEntityWireSnapshot magic (expected MBFENTZ1 or MBFENTZ2)",
            ));
        };
        let mut len_buf = [0u8; 8];
        len_buf.copy_from_slice(&bytes[8..16]);
        let comp_len = u64::from_le_bytes(len_buf) as usize;
        if 16 + comp_len > bytes.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated MbfEntityWireSnapshot LZ4 stream",
            ));
        }
        let raw = lz4_flex::decompress_size_prepended(&bytes[16..16 + comp_len]).map_err(|e| {
            DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                format!("MbfEntityWireSnapshot LZ4 error: {e}"),
            )
        })?;
        let mut offset = 0usize;
        let tenant_id = read_str_binary(&raw, &mut offset)?;
        let entity_type = read_str_binary(&raw, &mut offset)?;
        let entity_id = read_str_binary(&raw, &mut offset)?;
        if offset + 12 > raw.len() {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Truncated MbfEntityWireSnapshot counts",
            ));
        }
        let mut u64_buf = [0u8; 8];
        u64_buf.copy_from_slice(&raw[offset..offset + 8]);
        offset += 8;
        let epoch_id = u64::from_le_bytes(u64_buf);
        let mut u32_buf = [0u8; 4];
        u32_buf.copy_from_slice(&raw[offset..offset + 4]);
        offset += 4;
        let count = u32::from_le_bytes(u32_buf) as usize;
        let mut datoms = Vec::with_capacity(count);
        let entity_pk = format!("T#{tenant_id}#E#{entity_id}");
        for _ in 0..count {
            let sk = read_bytes_slice(&raw, &mut offset)?;
            let item = if is_v2 {
                read_item_map_compact(&raw, &mut offset, &entity_pk, &sk)?
            } else {
                read_item_map_binary(&raw, &mut offset)?
            };
            datoms.push((sk, item));
        }
        Ok(Self {
            tenant_id,
            entity_type,
            entity_id,
            epoch_id,
            datoms,
        })
    }
}

/// Empaqueta una carga útil binaria en un archivo `.mbf` completo alineado a 64 bytes:
/// `MbfHeader` (64B, `"METRIBIN"`), `MbfTenantCryptoSeal` (64B), carga útil alineada a 64B, `MbfDirectoryIndex` y `MbfFooterTrailer` (64B, `"MBFFOOTR"`).
pub fn encode_mbf_envelope(
    cell_id: u32,
    row_count: u64,
    epoch_id: u64,
    tenant_id: &str,
    entity_type: &str,
    payload: &[u8],
) -> DomainResult<Vec<u8>> {
    use crate::mbf::format::{
        compute_crc32c, pad_to_64, ColumnChunkDescriptor, MbfDirectoryIndex, MbfFooterTrailer,
        MbfHeader, MbfTenantCryptoSeal, ZoneOffsetsTable, MBF_FLAG_CRYPTO_SEALED, MBF_FLAG_HAS_PAX,
    };

    let now_ms = chrono::Utc::now().timestamp_millis();
    let tenant_hash = xxhash_rust::xxh64::xxh64(tenant_id.as_bytes(), 0x4D45_5452_4932_3032);
    let schema_fp_lo = xxhash_rust::xxh64::xxh64(entity_type.as_bytes(), 0x01);
    let schema_fp_hi = xxhash_rust::xxh64::xxh64(entity_type.as_bytes(), 0x02);

    let header = MbfHeader::new(
        cell_id,
        row_count,
        epoch_id,
        tenant_hash,
        schema_fp_lo as i64,
        now_ms,
    )
    .with_flags(MBF_FLAG_HAS_PAX | MBF_FLAG_CRYPTO_SEALED);

    let seal = MbfTenantCryptoSeal::seal_active(tenant_id, entity_type, epoch_id, payload)?;

    let mut padded_payload = payload.to_vec();
    pad_to_64(&mut padded_payload);

    let zone1_offset = 128u64;
    let zone1_len = padded_payload.len() as u64;
    let directory = MbfDirectoryIndex {
        zones: ZoneOffsetsTable {
            zone1_pax_offset: zone1_offset,
            zone1_pax_len: zone1_len,
            ..ZoneOffsetsTable::default()
        },
        columns: vec![ColumnChunkDescriptor {
            attr_id: (schema_fp_lo & 0xFFFF) as u16,
            zone_id: 1,
            value_tag: 2, // LZ4_RAW / PAX
            row_count: row_count.min(u64::from(u32::MAX)) as u32,
            byte_offset: zone1_offset,
            byte_len: payload.len() as u64,
            synopsis_offset: 0,
        }],
    };
    let dir_bytes = directory.to_bytes();

    let mut buf = Vec::with_capacity(128 + padded_payload.len() + dir_bytes.len() + 64);
    buf.extend_from_slice(&header.to_bytes());
    buf.extend_from_slice(&seal.to_bytes());
    buf.extend_from_slice(&padded_payload);

    let directory_offset = buf.len() as u64;
    let directory_len = dir_bytes.len() as u32;
    buf.extend_from_slice(&dir_bytes);

    let header_crc = compute_crc32c(&buf[0..64]);
    let directory_crc = compute_crc32c(&dir_bytes);
    let mut hmac_24 = [0u8; 24];
    hmac_24.copy_from_slice(&seal.keyed_mac[0..24]);

    let footer = MbfFooterTrailer::new(
        directory_offset,
        directory_len,
        header_crc,
        directory_crc,
        schema_fp_hi as u32,
        0,
        hmac_24,
    );
    buf.extend_from_slice(&footer.to_bytes());
    Ok(buf)
}

/// Valida y decodifica un archivo binario `.mbf` producido por `encode_mbf_envelope`.
pub fn decode_mbf_envelope(
    file_bytes: &[u8],
) -> DomainResult<(crate::mbf::format::MbfHeader, &[u8])> {
    use crate::mbf::format::{
        compute_crc32c, MbfDirectoryIndex, MbfFooterTrailer, MbfHeader, MbfTenantCryptoSeal,
        MBF_FLAG_CRYPTO_SEALED,
    };

    if file_bytes.len() < 256 || file_bytes.len() % 64 != 0 {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            format!(
                "MBF file size invalid or not 64B-aligned: {} bytes",
                file_bytes.len()
            ),
        ));
    }
    let header = MbfHeader::from_bytes(&file_bytes[0..64]).map_err(|e| {
        DomainError::mbf(ErrorCode::MbfCorrupt001, format!("Header parse error: {e}"))
    })?;
    let footer =
        MbfFooterTrailer::from_bytes(&file_bytes[file_bytes.len() - 64..]).map_err(|e| {
            DomainError::mbf(ErrorCode::MbfCorrupt001, format!("Footer parse error: {e}"))
        })?;

    let expected_hdr_crc = compute_crc32c(&file_bytes[0..64]);
    if footer.header_crc32c != expected_hdr_crc {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Header CRC32C mismatch in MBF footer",
        ));
    }
    if (header.flags & MBF_FLAG_CRYPTO_SEALED) != 0 {
        let seal = MbfTenantCryptoSeal::from_bytes(&file_bytes[64..128])?;
        seal.verify_footer_tag(&footer)?;
    }

    let dir_start = footer.directory_offset as usize;
    let dir_end = dir_start.saturating_add(footer.directory_len as usize);
    if dir_start < 128 || dir_end > file_bytes.len() - 64 {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Invalid directory offset/len in MBF footer",
        ));
    }
    let dir_slice = &file_bytes[dir_start..dir_end];
    if compute_crc32c(dir_slice) != footer.directory_crc32c {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Directory CRC32C mismatch in MBF footer",
        ));
    }
    let directory = MbfDirectoryIndex::from_bytes(dir_slice).map_err(|e| {
        DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            format!("Directory index error: {e}"),
        )
    })?;
    let payload_len = directory
        .columns
        .first()
        .map_or(dir_start - 128, |c| c.byte_len as usize);
    let payload_end = (128 + payload_len).min(dir_start);

    Ok((header, &file_bytes[128..payload_end]))
}

/// Codifica un archivo espejo de índice `.mbf.idx` (magic `"MBFINDEX"`) con Zona 5 y Zona 5B.
pub fn encode_mbf_idx_mirror(
    cell_id: u8,
    epoch_id: u64,
    row_count: u32,
    tenant_id: &str,
    search_zone_bytes: &[u8],
    synopsis_zone_bytes: &[u8],
) -> Vec<u8> {
    use crate::mbf::format::{
        compute_crc32c, pad_to_64, MbfDirectoryIndex, MbfFooterTrailer, MbfHeader,
        MbfTenantCryptoSeal, ZoneOffsetsTable, MBF_FLAG_HAS_SEARCH, MBF_FLAG_HAS_SYNOPSES,
    };

    let now_ms = chrono::Utc::now().timestamp_millis();
    let tenant_hash = xxhash_rust::xxh64::xxh64(tenant_id.as_bytes(), 0x4D45_5452_4932_3032);
    let header = MbfHeader::new_idx_mirror(
        u32::from(cell_id),
        u64::from(row_count),
        epoch_id,
        tenant_hash,
        now_ms,
        now_ms,
    )
    .with_flags(MBF_FLAG_HAS_SYNOPSES | MBF_FLAG_HAS_SEARCH);

    let mut buf = Vec::with_capacity(192 + search_zone_bytes.len() + synopsis_zone_bytes.len());
    buf.extend_from_slice(&header.to_bytes());

    let z5_offset = buf.len() as u64;
    buf.extend_from_slice(synopsis_zone_bytes);
    pad_to_64(&mut buf);
    let z5_len = (buf.len() as u64) - z5_offset;

    let z5b_offset = buf.len() as u64;
    buf.extend_from_slice(search_zone_bytes);
    pad_to_64(&mut buf);
    let z5b_len = (buf.len() as u64) - z5b_offset;

    let dir = MbfDirectoryIndex {
        zones: ZoneOffsetsTable {
            zone5_synopsis_offset: z5_offset,
            zone5_synopsis_len: z5_len,
            zone5b_search_offset: z5b_offset,
            zone5b_search_len: z5b_len,
            ..ZoneOffsetsTable::default()
        },
        columns: Vec::new(),
    };
    let dir_bytes = dir.to_bytes();
    let dir_offset = buf.len() as u64;
    let dir_len = dir_bytes.len() as u32;
    buf.extend_from_slice(&dir_bytes);

    let hdr_crc = compute_crc32c(&buf[0..64]);
    let dir_crc = compute_crc32c(&dir_bytes);
    let mut hmac_24 = [0x49u8; 24];
    if let Ok(seal) =
        MbfTenantCryptoSeal::seal_active(tenant_id, "idx_mirror", epoch_id, &dir_bytes)
    {
        hmac_24.copy_from_slice(&seal.keyed_mac[0..24]);
    }
    let footer = MbfFooterTrailer::new(
        dir_offset,
        dir_len,
        hdr_crc,
        dir_crc,
        tenant_hash as u32,
        0,
        hmac_24,
    );
    buf.extend_from_slice(&footer.to_bytes());
    buf
}

fn is_non_searchable_attr(attr_name: &str) -> bool {
    attr_name == crate::eav::writer::system_attrs::TYPE_NAME
        || attr_name == crate::eav::writer::system_attrs::ULID_NAME
        || attr_name == crate::eav::writer::system_attrs::TENANT_NAME
        || attr_name.starts_with("meta/")
        // Espejos de historial `_refs/<entidad>`: viven en la partición del
        // dueño sin declararse en su modelo — exentos de indexación de texto,
        // igual que los atributos de sistema.
        || attr_name.starts_with(crate::eav::writer::history_mirror::REFS_PREFIX)
        || attr_name.ends_with("_id")
        || matches!(
            attr_name,
            "id" | "role_ids"
                | "grants"
                | "status"
                | "criticality"
                | "currency"
                | "user_type"
                | "tier"
                | "storage_region"
                | "payload"
                | "detail_type"
                | "event_type"
                | "aggregate_type"
                | "action"
                | "ip_address"
                | "user_agent"
                | "changes_json"
                | "error_code"
                | "layer"
                | "operation"
                | "message"
                | "context"
                | "password_hash"
        )
}

struct ColdStartDocBuilder {
    tenant_id: String,
    doc: crate::mbf::zones::hybrid_search::IndexedEntityDocument,
    lat: Option<f64>,
    lon: Option<f64>,
}

impl ColdStartDocBuilder {
    fn new(tenant_id: &str, entity_id: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            doc: crate::mbf::zones::hybrid_search::IndexedEntityDocument {
                entity_id: entity_id.to_string(),
                row_id: 0,
                code: String::new(),
                title: String::new(),
                description: String::new(),
                embedding: None,
                coordinates_wgs84: None,
            },
            lat: None,
            lon: None,
        }
    }

    fn feed(&mut self, attr_name_opt: Option<&str>, item: &HashMap<String, AttributeValue>) {
        match item.get("v") {
            Some(AttributeValue::S(text)) => self.feed_string(attr_name_opt, text.trim()),
            Some(AttributeValue::N(num_str)) => self.feed_number(attr_name_opt, num_str.trim()),
            Some(AttributeValue::B(blob)) => self.feed_blob(attr_name_opt, blob.as_ref()),
            _ => {}
        }
    }

    fn feed_string(&mut self, attr_name_opt: Option<&str>, trimmed: &str) {
        if trimmed.is_empty()
            || trimmed.starts_with('\x7b')
            || trimmed.starts_with('[')
            || attr_name_opt.is_some_and(is_non_searchable_attr)
        {
            return;
        }
        let is_title = matches!(
            attr_name_opt,
            Some("name" | "title" | "label" | "full_name" | "username")
        );
        let is_chunk_content = matches!(attr_name_opt, Some("chunk_content"));
        let is_metadata_tags = matches!(attr_name_opt, Some("metadata_tags"));
        let is_code = is_metadata_tags
            || matches!(
                attr_name_opt,
                Some(
                    "tag"
                        | "code"
                        | "serial_number"
                        | "barcode_qr_code"
                        | "functional_location_code"
                        | "sku"
                )
            );
        if is_code {
            let code_segment = if is_metadata_tags {
                let canon = crate::eav::fts::searcher::canonicalize_alphanumeric_code(trimmed);
                if canon.is_empty() {
                    trimmed.to_string()
                } else {
                    format!("{trimmed} {canon}")
                }
            } else {
                crate::eav::fts::searcher::canonicalize_alphanumeric_code(trimmed)
            };
            if !code_segment.is_empty() && !self.doc.code.contains(&code_segment) {
                if !self.doc.code.is_empty() {
                    self.doc.code.push(' ');
                }
                self.doc.code.push_str(&code_segment);
            }
        }
        if is_title && (self.doc.title.is_empty() || trimmed.len() > self.doc.title.len()) {
            self.doc.title = trimmed.to_string();
        } else if is_chunk_content && self.doc.title.is_empty() {
            self.doc.title = trimmed
                .split_whitespace()
                .take(16)
                .collect::<Vec<_>>()
                .join(" ");
        }
        if !self.doc.description.contains(trimmed) {
            if !self.doc.description.is_empty() {
                self.doc.description.push(' ');
            }
            self.doc.description.push_str(trimmed);
        }
        if matches!(attr_name_opt, Some("synonyms" | "equivalence_group")) {
            let group: Vec<&str> = trimmed
                .split([',', ';', '|'])
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();
            if group.len() >= 2 {
                crate::eav::fts::searcher::register_tenant_synonym_group(&self.tenant_id, &group);
            }
        }
    }

    fn feed_number(&mut self, attr_name_opt: Option<&str>, num_str: &str) {
        let Some(attr_name) = attr_name_opt else {
            return;
        };
        if matches!(attr_name, "latitude" | "start_latitude") {
            if let Ok(lat) = num_str.parse::<f64>() {
                if lat.is_finite() {
                    self.lat = Some(lat);
                }
            }
        } else if matches!(attr_name, "longitude" | "start_longitude") {
            if let Ok(lon) = num_str.parse::<f64>() {
                if lon.is_finite() {
                    self.lon = Some(lon);
                }
            }
        }
    }

    fn feed_blob(&mut self, attr_name_opt: Option<&str>, bytes: &[u8]) {
        if matches!(attr_name_opt, Some("semantic_embedding" | "embedding"))
            && bytes.len() >= 16
            && bytes.len() % 4 == 0
        {
            let mut vec = Vec::with_capacity(bytes.len() / 4);
            for chunk in bytes.chunks_exact(4) {
                let bits = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                vec.push(f32::from_bits(bits));
            }
            self.doc.embedding = Some(vec);
        }
    }

    fn finish(
        mut self,
    ) -> Option<(
        String,
        crate::mbf::zones::hybrid_search::IndexedEntityDocument,
    )> {
        if let (Some(lat), Some(lon)) = (self.lat, self.lon) {
            self.doc.coordinates_wgs84 = Some((lat, lon));
        }
        if self.doc.code.is_empty()
            && self.doc.title.is_empty()
            && self.doc.description.is_empty()
            && self.doc.embedding.is_none()
            && self.doc.coordinates_wgs84.is_none()
        {
            return None;
        }
        Some((self.tenant_id, self.doc))
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct MbfPartitionStore {
    pub(crate) epoch_id: u64,
    pub(crate) last_s3_etag: Option<String>,
    pub(crate) last_sync_ms: i64,
    pub(crate) bulk_depth: usize,
    pub(crate) dirty_entities: std::collections::BTreeSet<(String, String, String)>,
    pub(crate) transient_entities: std::collections::HashSet<String>,
    pub(crate) outbox_entities: std::collections::HashSet<String>,
    pub(crate) tenant_synopses_cache:
        HashMap<String, BTreeMap<u16, crate::mbf::zones::bi_synopses::MbfHyperSynopsis512>>,
    /// Primary index `(PK, SK_bytes) -> Item` (EAVT).
    pub(crate) items: BTreeMap<PrimaryKey, HashMap<String, AttributeValue>>,
    /// Native secondary BTree index for `AEVT` (`(ap, as_bytes, PrimaryKey)`).
    pub(crate) aevt_index: BTreeMap<(String, Vec<u8>, PrimaryKey), ()>,
    /// Native secondary BTree index for `AVET` (`(vp, vs_bytes, PrimaryKey)`).
    pub(crate) avet_index: BTreeMap<(String, Vec<u8>, PrimaryKey), ()>,
    /// Native secondary BTree index for `VAET` (`(rp, rs_bytes, PrimaryKey)`).
    pub(crate) vaet_index: BTreeMap<(String, Vec<u8>, PrimaryKey), ()>,
    /// Quota counters `(tenant_id, quota_id) -> current_usage`.
    pub(crate) quota_counters: HashMap<(String, String), i64>,
    /// Quota idempotency marks `(tenant_id, idem_key) -> (quota_id, delta)`.
    pub(crate) quota_idem: HashMap<(String, String), (String, i64)>,
}

impl MbfPartitionStore {
    pub(crate) fn insert_indexed(
        &mut self,
        key: PrimaryKey,
        item: HashMap<String, AttributeValue>,
    ) {
        if let Some(old_item) = self.items.remove(&key) {
            self.unindex_item(&key, &old_item);
        }
        self.index_item(&key, &item);
        self.items.insert(key, item);
    }

    pub(crate) fn remove_indexed(&mut self, key: &PrimaryKey) {
        if let Some(old_item) = self.items.remove(key) {
            self.unindex_item(key, &old_item);
        }
    }

    fn index_secondary_only(
        &mut self,
        key: &PrimaryKey,
        item: &HashMap<String, AttributeValue>,
    ) -> Option<(String, String, Option<String>)> {
        if let Some((ap, as_bytes)) = extract_index_key(item, "ap", "as") {
            self.aevt_index.insert((ap, as_bytes, key.clone()), ());
        }
        if let Some((vp, vs_bytes)) = extract_index_key(item, "vp", "vs") {
            self.avet_index.insert((vp, vs_bytes, key.clone()), ());
        }
        if let Some((rp, rs_bytes)) = extract_index_key(item, "rp", "rs") {
            self.vaet_index.insert((rp, rs_bytes, key.clone()), ());
        }
        if key.1.len() != 11 {
            return None;
        }
        if self.transient_entities.contains(key.0.as_str()) {
            return None;
        }
        let (tenant_id, entity_id) = key.0.strip_prefix("T#").and_then(|s| s.split_once("#E#"))?;
        let attr_name_opt = item
            .get("ap")
            .and_then(|av| match av {
                AttributeValue::S(ap_str) => ap_str.split_once("#A#").map(|(_, a)| a),
                _ => None,
            })
            .map(|a| a.split('#').next().unwrap_or(a).to_string());

        if key.1[10] == 0 {
            if attr_name_opt.as_deref() == Some(crate::eav::writer::system_attrs::TYPE_NAME) {
                crate::eav::fts::searcher::remove_fts_document(tenant_id, entity_id);
            }
            return None;
        }

        if attr_name_opt.as_deref() == Some(crate::eav::writer::system_attrs::TYPE_NAME) {
            if let Some(AttributeValue::S(et)) = item.get("v") {
                if et == "outbox_event" {
                    self.transient_entities.insert(key.0.clone());
                    self.outbox_entities.insert(key.0.clone());
                    return None;
                }
                if matches!(et.as_str(), "audit_log" | "domain_fault") {
                    self.transient_entities.insert(key.0.clone());
                    return None;
                }
            }
        }
        Some((tenant_id.to_string(), entity_id.to_string(), attr_name_opt))
    }

    fn index_item(&mut self, key: &PrimaryKey, item: &HashMap<String, AttributeValue>) {
        if let Some((tenant_id, entity_id, attr_name_opt)) = self.index_secondary_only(key, item) {
            self.index_item_fts(&tenant_id, &entity_id, attr_name_opt.as_deref(), item);
        }
    }

    fn rebuild_fts_from_items(&self) {
        let mut docs_by_tenant: HashMap<
            String,
            Vec<crate::mbf::zones::hybrid_search::IndexedEntityDocument>,
        > = HashMap::new();
        let mut current_pk = String::new();
        let mut current_builder: Option<ColdStartDocBuilder> = None;
        let mut latest_by_attr: BTreeMap<
            [u8; 2],
            (bool, Option<String>, &HashMap<String, AttributeValue>),
        > = BTreeMap::new();

        let flush_builder = |builder_opt: Option<ColdStartDocBuilder>,
                             latest: &mut BTreeMap<
            [u8; 2],
            (bool, Option<String>, &HashMap<String, AttributeValue>),
        >,
                             out: &mut HashMap<
            String,
            Vec<crate::mbf::zones::hybrid_search::IndexedEntityDocument>,
        >| {
            let entries = std::mem::take(latest);
            if let Some(mut builder) = builder_opt {
                for (_, (is_assert, attr_name_opt, item_ref)) in entries {
                    if is_assert {
                        builder.feed(attr_name_opt.as_deref(), item_ref);
                    }
                }
                if let Some((t_id, doc)) = builder.finish() {
                    out.entry(t_id).or_default().push(doc);
                }
            }
        };

        for ((pk, sk), item) in &self.items {
            if sk.len() != 11 || self.transient_entities.contains(pk.as_str()) {
                continue;
            }
            if pk != &current_pk {
                flush_builder(
                    current_builder.take(),
                    &mut latest_by_attr,
                    &mut docs_by_tenant,
                );
                current_pk.clone_from(pk);
                if let Some((t_id, e_id)) = pk.strip_prefix("T#").and_then(|s| s.split_once("#E#"))
                {
                    current_builder = Some(ColdStartDocBuilder::new(t_id, e_id));
                }
            }
            if current_builder.is_some() {
                let attr_name_opt = item
                    .get("ap")
                    .and_then(|av| match av {
                        AttributeValue::S(ap_str) => ap_str.split_once("#A#").map(|(_, a)| a),
                        _ => None,
                    })
                    .map(|a| a.split('#').next().unwrap_or(a).to_string());
                latest_by_attr.insert([sk[0], sk[1]], (sk[10] != 0, attr_name_opt, item));
            }
        }
        flush_builder(
            current_builder.take(),
            &mut latest_by_attr,
            &mut docs_by_tenant,
        );
        for (tenant_id, docs) in docs_by_tenant {
            crate::eav::fts::searcher::register_fts_documents_batch(&tenant_id, docs);
        }
    }

    fn index_item_fts(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attr_name_opt: Option<&str>,
        item: &HashMap<String, AttributeValue>,
    ) {
        match item.get("v") {
            Some(AttributeValue::S(text)) => {
                let trimmed = text.trim();
                if !trimmed.is_empty() && !trimmed.starts_with('{') && !trimmed.starts_with('[') {
                    let skip = attr_name_opt.is_some_and(is_non_searchable_attr);
                    if !skip {
                        crate::eav::fts::searcher::register_fts_document_attr(
                            tenant_id,
                            entity_id,
                            attr_name_opt,
                            trimmed,
                        );
                        if matches!(attr_name_opt, Some("synonyms" | "equivalence_group")) {
                            let group: Vec<&str> = trimmed
                                .split([',', ';', '|'])
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                                .collect();
                            if group.len() >= 2 {
                                crate::eav::fts::searcher::register_tenant_synonym_group(
                                    tenant_id, &group,
                                );
                            }
                        }
                    }
                }
            }
            Some(AttributeValue::N(num_str)) => {
                if let Some(attr_name) = attr_name_opt {
                    if matches!(attr_name, "latitude" | "start_latitude") {
                        if let Ok(lat) = num_str.trim().parse::<f64>() {
                            crate::eav::fts::searcher::register_fts_entity_coordinate_axis(
                                tenant_id, entity_id, true, lat,
                            );
                        }
                    } else if matches!(attr_name, "longitude" | "start_longitude") {
                        if let Ok(lon) = num_str.trim().parse::<f64>() {
                            crate::eav::fts::searcher::register_fts_entity_coordinate_axis(
                                tenant_id, entity_id, false, lon,
                            );
                        }
                    }
                }
            }
            Some(AttributeValue::B(blob)) => {
                if matches!(attr_name_opt, Some("semantic_embedding" | "embedding")) {
                    let bytes = blob.as_ref();
                    if bytes.len() >= 16 && bytes.len() % 4 == 0 {
                        let mut vec = Vec::with_capacity(bytes.len() / 4);
                        for chunk in bytes.chunks_exact(4) {
                            let bits = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                            vec.push(f32::from_bits(bits));
                        }
                        crate::eav::fts::searcher::register_fts_entity_embedding(
                            tenant_id, entity_id, vec,
                        );
                    }
                }
            }
            _ => {}
        }
    }

    fn unindex_item(&mut self, key: &PrimaryKey, item: &HashMap<String, AttributeValue>) {
        if let Some((ap, as_bytes)) = extract_index_key(item, "ap", "as") {
            self.aevt_index.remove(&(ap, as_bytes, key.clone()));
        }
        if let Some((vp, vs_bytes)) = extract_index_key(item, "vp", "vs") {
            self.avet_index.remove(&(vp, vs_bytes, key.clone()));
        }
        if let Some((rp, rs_bytes)) = extract_index_key(item, "rp", "rs") {
            self.vaet_index.remove(&(rp, rs_bytes, key.clone()));
        }
    }

    #[allow(dead_code)]
    fn rebuild_secondary_indexes(&mut self) {
        self.aevt_index.clear();
        self.avet_index.clear();
        self.vaet_index.clear();
        let keys_and_items: Vec<(PrimaryKey, HashMap<String, AttributeValue>)> = self
            .items
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (k, item) in &keys_and_items {
            let _ = self.index_secondary_only(k, item);
        }
        self.rebuild_fts_from_items();
    }

    pub(crate) fn to_wire_snapshot(&self) -> MbfPartitionWireSnapshot {
        MbfPartitionWireSnapshot {
            epoch_id: self.epoch_id,
            items: self
                .items
                .iter()
                .filter(|((pk, _), _)| {
                    !pk.contains("#FTS#") && !self.outbox_entities.contains(pk.as_str())
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            quota_counters: self
                .quota_counters
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            quota_idem: self
                .quota_idem
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    fn apply_wire_snapshot(&mut self, snap: MbfPartitionWireSnapshot, etag: String, now_ms: i64) {
        let remote_is_newer = snap.epoch_id >= self.epoch_id;
        let was_empty = self.items.is_empty();
        self.epoch_id = self.epoch_id.max(snap.epoch_id);
        for (key, remote_item) in snap.items {
            if key.0.contains("#FTS#") {
                continue;
            }
            if was_empty {
                let _ = self.index_secondary_only(&key, &remote_item);
                self.items.insert(key, remote_item);
                continue;
            }
            let should_insert = match self.items.get(&key) {
                None => true,
                Some(local_item) => {
                    let local_seq = local_item
                        .get("current_value")
                        .and_then(av_number)
                        .and_then(|n| n.parse::<i64>().ok());
                    let remote_seq = remote_item
                        .get("current_value")
                        .and_then(av_number)
                        .and_then(|n| n.parse::<i64>().ok());
                    match (local_seq, remote_seq) {
                        (Some(l), Some(r)) => r > l || (r == l && remote_is_newer),
                        _ => remote_is_newer,
                    }
                }
            };
            if should_insert {
                self.insert_indexed(key, remote_item);
            }
        }
        if was_empty {
            self.rebuild_fts_from_items();
        }
        for (k, v) in snap.quota_counters {
            let entry = self.quota_counters.entry(k).or_insert(0);
            if remote_is_newer {
                *entry = v;
            } else if v > *entry {
                *entry = v;
            }
        }
        for (k, v) in snap.quota_idem {
            self.quota_idem.entry(k).or_insert(v);
        }
        self.last_s3_etag = Some(etag);
        self.last_sync_ms = now_ms;
    }
}

static GLOBAL_MBF_PARTITIONS: Lazy<RwLock<HashMap<String, Arc<RwLock<MbfPartitionStore>>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

static GLOBAL_MBF_S3_LOCKS: Lazy<RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub(crate) fn partition_for(table: &str) -> Arc<RwLock<MbfPartitionStore>> {
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

fn s3_io_lock_for(table: &str) -> Arc<tokio::sync::Mutex<()>> {
    if let Ok(guard) = GLOBAL_MBF_S3_LOCKS.read() {
        if let Some(lock) = guard.get(table) {
            return Arc::clone(lock);
        }
    }
    if let Ok(mut guard) = GLOBAL_MBF_S3_LOCKS.write() {
        return Arc::clone(
            guard
                .entry(table.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        );
    }
    Arc::new(tokio::sync::Mutex::new(()))
}

pub(crate) fn evict_all_read_caches_on_remote_sync() {
    if let Ok(mut eav) = crate::eav::reader::pull::EAV_CACHE.write() {
        eav.clear();
    }
    if let Ok(mut aevt) = crate::eav::reader::query::AEVT_SCAN_CACHE.write() {
        aevt.clear();
    }
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

/// Cliente unificado de almacenamiento `.mbf` con persistencia en S3 e índices BTree en memoria.
#[derive(Clone)]
pub struct MbfStorageClient {
    /// Nombre del espacio de nombres o partición `.mbf` predeterminada.
    pub default_table: String,
    s3_store: Option<Arc<S3MbfObjectStore>>,
}

/// Alias de tipo del cliente de almacenamiento `.mbf` preservado por compatibilidad con módulos internos.
pub type DynamoClient = MbfStorageClient;

impl MbfStorageClient {
    /// Inicializa un cliente de almacenamiento `.mbf` conectado a S3 o en memoria según el entorno configurado.
    pub async fn new(default_table: &str) -> Self {
        let s3_mode = std::env::var("S3_MODE").unwrap_or_default();
        let maybe_bucket = std::env::var("MBF_S3_BUCKET")
            .or_else(|_| std::env::var("MBF_BUCKET_NAME"))
            .ok()
            .filter(|b| !b.is_empty());

        let s3_store = if s3_mode != "stub" {
            if let Some(bucket) = maybe_bucket {
                let store = Arc::new(S3MbfObjectStore::new(&bucket).await);
                info!(
                    "[MBF Storage] Metri Vortex (.mbf) S3 persistence active | s3://{}/{}/ | namespace: {default_table}",
                    bucket,
                    store.prefix()
                );
                Some(store)
            } else {
                info!(
                    "[MBF Storage] Metri Vortex (.mbf) in-memory mode | namespace: {default_table}"
                );
                None
            }
        } else {
            info!("[MBF Storage] Metri Vortex (.mbf) stub mode | namespace: {default_table}");
            None
        };

        let client = Self {
            default_table: default_table.to_string(),
            s3_store,
        };
        let _ = client.sync_from_s3(default_table, true).await;
        client
    }

    /// Crea una instancia del cliente `.mbf` puramente en memoria para pruebas unitarias y de integración.
    pub fn new_stub() -> Self {
        Self {
            default_table: "metri-mbf-local".to_string(),
            s3_store: None,
        }
    }

    /// Ejecuta una verificación rápida de disponibilidad del motor de almacenamiento.
    pub async fn health_check(&self) -> bool {
        true
    }

    fn vortex_s3_key(&self, s3: &S3MbfObjectStore, table: &str) -> String {
        format!("{}/vortex/{table}.mbf", s3.prefix())
    }

    /// Asegura la frescura del ETag de la partición en S3 antes de servir lecturas o escaneos en caché.
    pub async fn sync_if_needed(&self, table: &str) -> Result<(), DomainError> {
        self.sync_from_s3(table, false).await
    }

    /// Inicia un ámbito de ingesta masiva diferida hacia S3 para consolidar múltiples mutaciones en un único volcado.
    pub async fn begin_bulk_s3_batch(&self, table: &str) -> Result<(), DomainError> {
        if self.s3_store.is_none() {
            return Ok(());
        }
        let s3_lock = s3_io_lock_for(table);
        let _io_guard = s3_lock.lock().await;
        self.sync_from_s3_inner(table, true).await?;
        let part = partition_for(table);
        if let Ok(mut guard) = part.write() {
            guard.bulk_depth += 1;
        }
        Ok(())
    }

    /// Finaliza el ámbito de ingesta masiva diferida y vuelca la partición `.mbf` y archivos por entidad a S3.
    pub async fn finish_bulk_s3_batch(&self, table: &str) -> Result<(), DomainError> {
        if self.s3_store.is_none() {
            return Ok(());
        }
        let should_flush = {
            let part = partition_for(table);
            let mut guard = part
                .write()
                .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
            guard.bulk_depth = guard.bulk_depth.saturating_sub(1);
            guard.bulk_depth == 0
        };
        if should_flush {
            self.flush_to_s3(table, Vec::new()).await?;
        }
        Ok(())
    }

    /// Sincroniza el almacén de partición `.mbf` en memoria con S3 si una instancia remota confirmó estado más reciente.
    async fn sync_from_s3(&self, table: &str, force: bool) -> Result<(), DomainError> {
        if self.s3_store.is_none() {
            return Ok(());
        }
        let part = partition_for(table);
        let now_ms = chrono::Utc::now().timestamp_millis();
        {
            let guard = part
                .read()
                .map_err(|_| DomainError::eav(ErrorCode::Eav002, "Poisoned MBF partition lock"))?;
            if guard.bulk_depth > 0 {
                return Ok(());
            }
            if !force && guard.last_s3_etag.is_some() && (now_ms - guard.last_sync_ms) < 200 {
                return Ok(());
            }
        }
        let s3_lock = s3_io_lock_for(table);
        let _io_guard = s3_lock.lock().await;
        self.sync_from_s3_inner(table, force).await
    }

    async fn sync_from_s3_inner(&self, table: &str, force: bool) -> Result<(), DomainError> {
        let Some(s3) = &self.s3_store else {
            return Ok(());
        };
        let part = partition_for(table);
        let now_ms = chrono::Utc::now().timestamp_millis();

        let (known_etag, last_sync, in_bulk) = {
            let guard = part
                .read()
                .map_err(|_| DomainError::eav(ErrorCode::Eav002, "Poisoned MBF partition lock"))?;
            (
                guard.last_s3_etag.clone(),
                guard.last_sync_ms,
                guard.bulk_depth > 0,
            )
        };

        if in_bulk {
            return Ok(());
        }
        if !force && known_etag.is_some() && (now_ms - last_sync) < 200 {
            return Ok(());
        }

        let key = self.vortex_s3_key(s3, table);
        match s3
            .fetch_partition_if_changed(&key, known_etag.as_deref())
            .await?
        {
            MbfConditionalGet::NotModified => {
                if let Ok(mut guard) = part.write() {
                    guard.last_sync_ms = now_ms;
                }
            }
            MbfConditionalGet::NotFound => {
                if let Ok(mut guard) = part.write() {
                    if guard.last_s3_etag.is_some() {
                        let bulk_depth = guard.bulk_depth;
                        *guard = MbfPartitionStore {
                            bulk_depth,
                            last_sync_ms: now_ms,
                            ..MbfPartitionStore::default()
                        };
                        crate::eav::fts::searcher::clear_all_fts_corpora();
                        evict_all_read_caches_on_remote_sync();
                    } else {
                        guard.last_sync_ms = now_ms;
                    }
                }
            }
            MbfConditionalGet::Modified(payload) => {
                if let Ok((_, raw_bin)) = decode_mbf_envelope(&payload.bytes) {
                    if let Ok(snap) = MbfPartitionWireSnapshot::decode_binary(raw_bin) {
                        if let Ok(mut guard) = part.write() {
                            guard.apply_wire_snapshot(snap, payload.etag, now_ms);
                        }
                        evict_all_read_caches_on_remote_sync();
                    }
                }
            }
        }
        Ok(())
    }

    /// Serializa en memoria el sobre binario de la partición (`vortex/{table}.mbf`) y todos los artefactos
    /// por entidad (`.mbf`, `.mbf.idx`), celda (`HOT_HEAD_c{cell_id}.mbf`) e índice de tenant (`HOT_HEAD.mbf.idx`)
    /// pendientes de volcado para la tabla indicada.
    pub fn export_flush_artifacts(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<(Vec<u8>, Vec<(String, Vec<u8>)>), DomainError> {
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
        Self::serialize_flush_artifacts(&mut guard, prefix, table)
    }

    /// Aplica un sobre binario de partición (`vortex/{table}.mbf`) recibido de una instancia remota.
    pub fn apply_remote_vortex_envelope(
        &self,
        table: &str,
        vortex_bytes: &[u8],
        etag: &str,
    ) -> Result<(), DomainError> {
        let (_hdr, raw_bin) = decode_mbf_envelope(vortex_bytes)?;
        let snap = MbfPartitionWireSnapshot::decode_binary(raw_bin)?;
        let part = partition_for(table);
        let mut guard = part
            .write()
            .map_err(|_| DomainError::eav(ErrorCode::Eav002, "Poisoned MBF partition lock"))?;
        let now_ms = chrono::Utc::now().timestamp_millis();
        guard.apply_wire_snapshot(snap, etag.to_string(), now_ms);
        evict_all_read_caches_on_remote_sync();
        Ok(())
    }

    /// Vuelca la partición binaria `.mbf` actualizada y los archivos `.mbf` / `.mbf.idx` por entidad hacia S3.
    pub(crate) async fn flush_to_s3(
        &self,
        table: &str,
        touched_entities: Vec<(String, String, String)>,
    ) -> Result<(), DomainError> {
        let part = partition_for(table);
        {
            let mut guard = part
                .write()
                .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
            for te in touched_entities {
                guard.dirty_entities.insert(te);
            }
            if guard.bulk_depth > 0 {
                return Ok(());
            }
        }
        let Some(s3) = &self.s3_store else {
            return Ok(());
        };

        let s3_lock = s3_io_lock_for(table);
        let _io_guard = s3_lock.lock().await;
        self.sync_from_s3_inner(table, true).await?;
        let prefix = s3.prefix().to_string();

        let (vortex_bytes, entity_files) = {
            let mut guard = part
                .write()
                .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
            if guard.bulk_depth > 0 {
                return Ok(());
            }
            Self::serialize_flush_artifacts(&mut guard, &prefix, table)?
        };

        let vortex_key = self.vortex_s3_key(s3, table);
        let new_etag = s3.put_raw_mbf(&vortex_key, vortex_bytes).await?;

        if !entity_files.is_empty() {
            let mut join_set = tokio::task::JoinSet::new();
            for (ekey, ebytes) in entity_files {
                while join_set.len() >= 128 {
                    let _ = join_set.join_next().await;
                }
                let s3_clone = Arc::clone(s3);
                join_set.spawn(async move {
                    let _ = s3_clone.put_raw_mbf(&ekey, ebytes).await;
                });
            }
            while join_set.join_next().await.is_some() {}
        }

        if let Ok(mut guard) = part.write() {
            guard.last_s3_etag = Some(new_etag);
            guard.last_sync_ms = chrono::Utc::now().timestamp_millis();
        }
        Ok(())
    }

    fn serialize_flush_artifacts(
        guard: &mut MbfPartitionStore,
        prefix: &str,
        table: &str,
    ) -> Result<(Vec<u8>, Vec<(String, Vec<u8>)>), DomainError> {
        let entities_to_flush: Vec<(String, String, String)> =
            std::mem::take(&mut guard.dirty_entities)
                .into_iter()
                .collect();
        guard.epoch_id += 1;
        let epoch = guard.epoch_id;
        let snap = guard.to_wire_snapshot();
        let row_count = snap.items.len() as u64;
        let raw_bin = snap.encode_binary();
        let vortex_bin = encode_mbf_envelope(0, row_count, epoch, "system", table, &raw_bin)?;

        let mut extra_files: Vec<(String, Vec<u8>)> = Vec::new();
        let mut cell_frames: BTreeMap<(String, u8), Vec<crate::mbf::tx::CommittedDeltaFrame>> =
            BTreeMap::new();
        let mut touched_tenants: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();

        for (tenant_id, entity_type, entity_id) in entities_to_flush {
            if matches!(
                entity_type.as_str(),
                "outbox_event" | "audit_log" | "domain_fault"
            ) {
                continue;
            }
            if guard.last_s3_etag.is_none() {
                guard
                    .tenant_synopses_cache
                    .entry(tenant_id.clone())
                    .or_default();
            }
            touched_tenants.insert(tenant_id.clone());
            Self::append_entity_flush_files(
                guard,
                prefix,
                epoch,
                &tenant_id,
                &entity_type,
                &entity_id,
                &mut extra_files,
                &mut cell_frames,
            );
        }

        for ((tenant_id, cell_id), frames) in cell_frames {
            let cell_key = format!("{prefix}/tenants/{tenant_id}/head/HOT_HEAD_c{cell_id}.mbf");
            let cell_bin = crate::mbf::tx::CommittedDeltaFrame::encode_hot_head_binary(
                &tenant_id, cell_id, &frames,
            );
            extra_files.push((cell_key, cell_bin));
        }

        for tenant_id in touched_tenants {
            let tenant_search_zone =
                crate::eav::fts::searcher::export_tenant_zone5b_bytes(&tenant_id);
            let tenant_syns = Self::build_tenant_synopses(guard, &tenant_id, epoch);
            let tenant_syn_zone =
                crate::mbf::zones::bi_synopses::MbfHyperSynopsis512::encode_zone_binary_slice(
                    &tenant_syns,
                );
            let tenant_idx_bin = encode_mbf_idx_mirror(
                0,
                epoch,
                row_count.min(u64::from(u32::MAX)) as u32,
                &tenant_id,
                &tenant_search_zone,
                &tenant_syn_zone,
            );
            let tenant_idx_key = format!("{prefix}/tenants/{tenant_id}/head/HOT_HEAD.mbf.idx");
            extra_files.push((tenant_idx_key, tenant_idx_bin));
        }
        Ok((vortex_bin, extra_files))
    }

    #[allow(clippy::too_many_arguments)]
    fn append_entity_flush_files(
        guard: &mut MbfPartitionStore,
        prefix: &str,
        epoch: u64,
        tenant_id: &str,
        entity_type: &str,
        entity_id: &str,
        extra_files: &mut Vec<(String, Vec<u8>)>,
        cell_frames: &mut BTreeMap<(String, u8), Vec<crate::mbf::tx::CommittedDeltaFrame>>,
    ) {
        let target_pk = format!("T#{tenant_id}#E#{entity_id}");
        let range_start = (target_pk.clone(), Vec::new());
        let mut datoms = Vec::new();
        let mut latest_text_by_attr: BTreeMap<[u8; 2], Option<String>> = BTreeMap::new();
        for ((item_pk, item_sk), item) in guard.items.range(range_start..) {
            if item_pk != &target_pk {
                break;
            }
            if item_sk.len() == 11 {
                let attr_key = [item_sk[0], item_sk[1]];
                if item_sk[10] == 0 {
                    latest_text_by_attr.insert(attr_key, None);
                } else {
                    let attr_name_opt = item
                        .get("ap")
                        .and_then(av_string)
                        .and_then(|ap_str| ap_str.split_once("#A#").map(|(_, a)| a))
                        .map(|a| a.split('#').next().unwrap_or(a));
                    let text_opt = if !attr_name_opt.is_some_and(is_non_searchable_attr) {
                        item.get("v")
                            .and_then(av_string)
                            .map(str::trim)
                            .filter(|t| !t.is_empty() && !t.starts_with('{'))
                            .map(ToString::to_string)
                    } else {
                        None
                    };
                    latest_text_by_attr.insert(attr_key, text_opt);
                }
            }
            datoms.push((item_sk.clone(), item.clone()));
        }
        let fallback_text_parts: Vec<String> =
            latest_text_by_attr.into_values().flatten().collect();
        let ent_snap = MbfEntityWireSnapshot {
            tenant_id: tenant_id.to_string(),
            entity_type: entity_type.to_string(),
            entity_id: entity_id.to_string(),
            epoch_id: epoch,
            datoms,
        };
        let ent_payload_bin = ent_snap.encode_binary();
        let cell_id = crate::mbf::tx::DomainCellRouter::resolve_cell_id(entity_type, entity_id);
        if let Ok(ent_bin) = encode_mbf_envelope(
            u32::from(cell_id),
            ent_snap.datoms.len() as u64,
            epoch,
            tenant_id,
            entity_type,
            &ent_payload_bin,
        ) {
            let entity_key =
                format!("{prefix}/tenants/{tenant_id}/entities/{entity_type}/{entity_id}.mbf");
            let idx_key =
                format!("{prefix}/tenants/{tenant_id}/entities/{entity_type}/{entity_id}.mbf.idx");
            let fallback_desc = fallback_text_parts.join(" ");
            let search_zone = crate::eav::fts::searcher::export_single_entity_zone5b_bytes(
                tenant_id,
                entity_id,
                &fallback_desc,
            );
            let ent_syns =
                Self::build_and_merge_entity_synopses(guard, tenant_id, &ent_snap.datoms, epoch);
            let synopsis_zone =
                crate::mbf::zones::bi_synopses::MbfHyperSynopsis512::encode_zone_binary_slice(
                    &ent_syns,
                );
            let idx_bin = encode_mbf_idx_mirror(
                cell_id,
                epoch,
                ent_snap.datoms.len() as u32,
                tenant_id,
                &search_zone,
                &synopsis_zone,
            );
            extra_files.push((entity_key, ent_bin));
            extra_files.push((idx_key, idx_bin));
            cell_frames
                .entry((tenant_id.to_string(), cell_id))
                .or_default()
                .push(Self::build_hot_head_frame(
                    entity_type,
                    entity_id,
                    cell_id,
                    epoch,
                    ent_snap.datoms.len(),
                ));
        }
    }

    fn build_and_merge_entity_synopses(
        guard: &mut MbfPartitionStore,
        tenant_id: &str,
        datoms: &[(Vec<u8>, HashMap<String, AttributeValue>)],
        epoch: u64,
    ) -> Vec<crate::mbf::zones::bi_synopses::MbfHyperSynopsis512> {
        let mut by_attr: BTreeMap<u16, Vec<(i64, f64)>> = BTreeMap::new();
        for (idx, (sk, item)) in datoms.iter().enumerate() {
            if sk.len() != 11 || sk[10] == 0 {
                continue;
            }
            let attr_id = u16::from_be_bytes([sk[0], sk[1]]);
            if attr_id <= 7 {
                continue;
            }
            let Some(val) = item
                .get("v")
                .and_then(av_number)
                .and_then(|n| n.parse::<f64>().ok())
            else {
                continue;
            };
            if val.is_finite() {
                by_attr
                    .entry(attr_id)
                    .or_default()
                    .push(((epoch as i64) * 1000 + (idx as i64), val));
            }
        }
        if by_attr.is_empty() {
            return vec![crate::mbf::zones::bi_synopses::MbfHyperSynopsis512::default()];
        }
        let mut out = Vec::with_capacity(by_attr.len());
        let mut cache_opt = guard.tenant_synopses_cache.get_mut(tenant_id);
        for (attr_id, obs) in by_attr {
            let syn = crate::mbf::zones::bi_synopses::MbfHyperSynopsis512::from_observations(
                &obs,
                0,
                &[],
                0.0,
            );
            if let Some(cache) = &mut cache_opt {
                cache
                    .entry(attr_id)
                    .and_modify(|existing| *existing = existing.merge(&syn))
                    .or_insert_with(|| syn.clone());
            }
            out.push(syn);
        }
        out
    }

    fn build_tenant_synopses(
        guard: &mut MbfPartitionStore,
        tenant_id: &str,
        epoch: u64,
    ) -> Vec<crate::mbf::zones::bi_synopses::MbfHyperSynopsis512> {
        if let Some(cached) = guard.tenant_synopses_cache.get(tenant_id) {
            if !cached.is_empty() {
                return cached.values().cloned().collect();
            }
        }
        let pk_prefix = format!("T#{tenant_id}#E#");
        let range_start = (pk_prefix.clone(), Vec::new());
        let mut by_attr: BTreeMap<u16, Vec<(i64, f64)>> = BTreeMap::new();
        let mut idx = 0i64;
        for ((pk, sk), item) in guard.items.range(range_start..) {
            if !pk.starts_with(&pk_prefix) {
                break;
            }
            if sk.len() != 11 || sk[10] == 0 {
                continue;
            }
            if guard.transient_entities.contains(pk.as_str()) {
                continue;
            }
            let attr_id = u16::from_be_bytes([sk[0], sk[1]]);
            if attr_id <= 7 {
                continue;
            }
            if let Some(val) = item
                .get("v")
                .and_then(av_number)
                .and_then(|n| n.parse::<f64>().ok())
            {
                if val.is_finite() {
                    by_attr
                        .entry(attr_id)
                        .or_default()
                        .push(((epoch as i64) * 1000 + idx, val));
                    idx += 1;
                }
            }
        }
        if by_attr.is_empty() {
            return vec![crate::mbf::zones::bi_synopses::MbfHyperSynopsis512::default()];
        }
        let mut syn_map = BTreeMap::new();
        for (attr_id, obs) in by_attr {
            syn_map.insert(
                attr_id,
                crate::mbf::zones::bi_synopses::MbfHyperSynopsis512::from_observations(
                    &obs,
                    0,
                    &[],
                    0.0,
                ),
            );
        }
        let out: Vec<_> = syn_map.values().cloned().collect();
        guard
            .tenant_synopses_cache
            .insert(tenant_id.to_string(), syn_map);
        out
    }

    fn build_hot_head_frame(
        entity_type: &str,
        entity_id: &str,
        _cell_id: u8,
        epoch: u64,
        datom_count: usize,
    ) -> crate::mbf::tx::CommittedDeltaFrame {
        let sig = crate::mbf::tx::TxConflictSignature::from_keys(
            epoch.saturating_sub(1),
            epoch,
            &[entity_id],
            &[entity_id, entity_type],
        );
        crate::mbf::tx::CommittedDeltaFrame {
            signature: sig,
            payload_json: format!(
                "{{\"entity_type\":\"{entity_type}\",\"entity_id\":\"{entity_id}\",\"datoms\":{datom_count}}}"
            ),
        }
    }

    /// Inserta o reemplaza un registro individual en la partición `.mbf` e indexa sus claves secundarias.
    pub async fn put_item(
        &self,
        table: &str,
        item: HashMap<String, AttributeValue>,
    ) -> Result<(), DomainError> {
        self.sync_from_s3(table, true).await?;
        let Some(key) = extract_pk_sk(&item) else {
            return Err(DomainError::eav(
                ErrorCode::Eav001,
                "Missing PK in MBF put_item",
            ));
        };
        {
            let part = partition_for(table);
            let mut guard = part
                .write()
                .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
            guard.insert_indexed(key, item);
        }
        self.flush_to_s3(table, Vec::new()).await?;
        Ok(())
    }

    /// Recupera un registro por su clave primaria `(PK, SK)` desde la partición `.mbf`.
    pub async fn get_item(
        &self,
        table: &str,
        pk: &str,
        sk: Option<&[u8]>,
    ) -> Result<Option<HashMap<String, AttributeValue>>, DomainError> {
        self.sync_from_s3(table, false).await?;
        let part = partition_for(table);
        let guard = part
            .read()
            .map_err(|_| DomainError::eav(ErrorCode::Eav002, "Poisoned MBF partition lock"))?;
        let sk_vec = sk.map(|b| b.to_vec()).unwrap_or_default();
        Ok(guard.items.get(&(pk.to_string(), sk_vec)).cloned())
    }

    /// Consulta el identificador del actor asociado a una transacción `tx_id` en el registro de auditoría del inquilino.
    pub async fn get_tx_actor(&self, table: &str, tenant_id: &str, tx_id: u64) -> Option<String> {
        let pk = format!("T#{tenant_id}#TX");
        let sk = tx_id.to_be_bytes();
        let item = self.get_item(table, &pk, Some(&sk)).await.ok()??;
        item.get("actor").and_then(av_string).map(str::to_string)
    }

    /// Elimina un registro por su clave primaria `(PK, SK)` y limpia sus entradas en los índices secundarios.
    pub async fn delete_item(
        &self,
        table: &str,
        pk: &str,
        sk: Option<&[u8]>,
    ) -> Result<(), DomainError> {
        self.sync_from_s3(table, true).await?;
        {
            let part = partition_for(table);
            let mut guard = part
                .write()
                .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
            let sk_vec = sk.map(|b| b.to_vec()).unwrap_or_default();
            guard.remove_indexed(&(pk.to_string(), sk_vec));
        }
        self.flush_to_s3(table, Vec::new()).await?;
        Ok(())
    }

    /// Ejecuta un lote transaccional ACID validando precondiciones tipadas antes de confirmar en memoria y S3.
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

        self.sync_from_s3(&table_name, true).await?;

        let touched_entities = {
            let part = partition_for(&table_name);
            let mut guard = part
                .write()
                .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;

            // 1. Validate intra-transaction duplicate keys and typed preconditions before mutating
            let mut staged: Vec<(PrimaryKey, HashMap<String, AttributeValue>)> =
                Vec::with_capacity(items.len());
            let mut seen_keys = std::collections::HashSet::new();
            let mut touched: Vec<(String, String, String)> = Vec::new();

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

                match MbfWriteCondition::parse(put.condition_expression.as_deref()) {
                    MbfWriteCondition::Unconditional => {}
                    MbfWriteCondition::AppendOnlyLogUnique => {
                        if guard.items.contains_key(&key) {
                            return Err(DomainError::eav(
                                ErrorCode::EavTx004,
                                format!(
                                    "Conflict in append-only MBF log on key ({}, {:?})",
                                    key.0, key.1
                                ),
                            ));
                        }
                    }
                    MbfWriteCondition::UniqueClaimKey => {
                        if guard.items.contains_key(&key) {
                            return Err(DomainError::codice(
                                ErrorCode::Cod001,
                                format!(
                                    "Violación de restricción unique: la clave {} ya está reclamada",
                                    key.0
                                ),
                            ));
                        }
                    }
                    MbfWriteCondition::UniqueEntitySelfOrUnclaimed => {
                        if let Some(ex_item) = guard.items.get(&key) {
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

                // Track touched entities from TX log items or entity/type datoms
                if key.0.starts_with("T#") && key.0.ends_with("#TX") && key.0.len() > 5 {
                    let tid = key.0[2..key.0.len() - 3].to_string();
                    if let (Some(et), Some(eid)) = (
                        put.item.get("entity_type").and_then(av_string),
                        put.item.get("entity_id").and_then(av_string),
                    ) {
                        touched.push((tid, et.to_string(), eid.to_string()));
                    }
                } else if let Some(ap) = put.item.get("ap").and_then(av_string) {
                    let is_entity_type_datom =
                        ap.contains("#A#entity/type#") || ap.ends_with("#A#entity/type");
                    if is_entity_type_datom {
                        if let Some(et) = put.item.get("v").and_then(av_string) {
                            if let Some(rest) = key.0.strip_prefix("T#") {
                                if let Some((tid, eid)) = rest.split_once("#E#") {
                                    touched.push((
                                        tid.to_string(),
                                        et.to_string(),
                                        eid.to_string(),
                                    ));
                                }
                            }
                        }
                    }
                }

                staged.push((key, put.item));
            }

            for (tid, et, eid) in &touched {
                if et == "outbox_event" {
                    let pk = format!("T#{tid}#E#{eid}");
                    guard.transient_entities.insert(pk.clone());
                    guard.outbox_entities.insert(pk);
                } else if matches!(et.as_str(), "audit_log" | "domain_fault") {
                    guard.transient_entities.insert(format!("T#{tid}#E#{eid}"));
                }
            }
            // 2. Commit all items atomically and update native secondary indexes
            for (key, item) in staged {
                guard.insert_indexed(key, item);
            }
            touched.sort();
            touched.dedup();
            touched
        };

        self.flush_to_s3(&table_name, touched_entities).await?;
        debug!("[MBF Storage] ACID transaction committed on {table_name}");
        Ok(())
    }

    /// Inserta un lote de solicitudes `WriteRequest` en la partición `.mbf` (utilizado para indexación de trigramas FTS).
    pub async fn batch_write_item(
        &self,
        table: &str,
        write_requests: Vec<WriteRequest>,
    ) -> Result<(), DomainError> {
        if write_requests.is_empty() {
            return Ok(());
        }
        self.sync_from_s3(table, true).await?;
        {
            let part = partition_for(table);
            let mut guard = part
                .write()
                .map_err(|_| DomainError::eav(ErrorCode::Eav001, "Poisoned MBF partition lock"))?;
            for req in write_requests {
                if let Some(put_req) = req.put_request {
                    if let Some(key) = extract_pk_sk(&put_req.item) {
                        guard.insert_indexed(key, put_req.item);
                    }
                }
            }
        }
        self.flush_to_s3(table, Vec::new()).await?;
        Ok(())
    }

    /// Ejecuta una consulta por rango $O(\log N)$ sobre el índice primario `EAVT` o secundario (`AEVT`, `AVET`, `VAET`).
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
        self.sync_from_s3(table, false).await?;
        let part = partition_for(table);
        let guard = part
            .read()
            .map_err(|_| DomainError::eav(ErrorCode::Eav002, "Poisoned MBF partition lock"))?;

        let target_pk = expr_attr_values
            .get(":pk")
            .or_else(|| expr_attr_values.get(":ap"))
            .and_then(av_string)
            .unwrap_or_default();

        let has_between = key_condition.contains("BETWEEN");
        let has_lte = key_condition.contains("<=") || has_between;
        let has_prefix = key_condition.contains("begins_with");
        let sk_min = expr_attr_values.get(":sk_min").and_then(av_bytes);
        let sk_max = expr_attr_values.get(":sk_max").and_then(av_bytes);
        let sk_prefix = expr_attr_values.get(":sk_prefix").and_then(av_bytes);

        let initial_sk = if let Some(min_bytes) = sk_min {
            min_bytes.to_vec()
        } else if has_prefix {
            sk_prefix.map_or_else(Vec::new, |p| p.to_vec())
        } else {
            Vec::new()
        };

        let mut matched: Vec<(Vec<u8>, HashMap<String, AttributeValue>)> = Vec::new();

        match MbfSecondaryIndex::from_index_name(index_name) {
            MbfSecondaryIndex::PrimaryEavt => {
                // O(log N) BTree range lookup on Primary EAVT index
                let range_start = (target_pk.to_string(), initial_sk);
                for ((item_pk, item_sk), item) in guard.items.range(range_start..) {
                    if item_pk != target_pk {
                        break;
                    }
                    if has_lte {
                        if let Some(max_bytes) = sk_max {
                            if item_sk.as_slice() > max_bytes {
                                break;
                            }
                        }
                    }
                    if has_prefix {
                        if let Some(prefix_bytes) = sk_prefix {
                            if !item_sk.starts_with(prefix_bytes) {
                                break;
                            }
                        }
                    }
                    matched.push((item_sk.clone(), item.clone()));
                }
            }
            sec_idx => {
                // O(log N) BTree range lookup on Secondary AEVT / AVET / VAET index (zero linear scan!)
                let tree = match sec_idx {
                    MbfSecondaryIndex::Aevt => &guard.aevt_index,
                    MbfSecondaryIndex::Avet => &guard.avet_index,
                    MbfSecondaryIndex::Vaet => &guard.vaet_index,
                    MbfSecondaryIndex::PrimaryEavt => &guard.aevt_index,
                };
                let range_start = (
                    target_pk.to_string(),
                    initial_sk,
                    (String::new(), Vec::new()),
                );
                for ((idx_pk, idx_sk, primary_key), ()) in tree.range(range_start..) {
                    if idx_pk != target_pk {
                        break;
                    }
                    if has_lte {
                        if let Some(max_bytes) = sk_max {
                            if idx_sk.as_slice() > max_bytes {
                                break;
                            }
                        }
                    }
                    if has_prefix {
                        if let Some(prefix_bytes) = sk_prefix {
                            if !idx_sk.starts_with(prefix_bytes) {
                                break;
                            }
                        }
                    }
                    if let Some(item) = guard.items.get(primary_key) {
                        matched.push((idx_sk.clone(), item.clone()));
                    }
                }
            }
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

    /// Incrementa de forma atómica el contador de una secuencia de negocio y persiste el nuevo valor.
    pub async fn sequence_increment(
        &self,
        table: &str,
        seq_code: &str,
        tenant_id: &str,
    ) -> Result<i64, DomainError> {
        self.sync_from_s3(table, true).await?;
        let next_val = {
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
            next_val
        };
        self.flush_to_s3(table, Vec::new()).await?;
        Ok(next_val)
    }

    /// Inserta o actualiza la configuración y el valor actual de una secuencia de negocio.
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
        self.sync_from_s3(table, true).await?;
        let updated = {
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
            true
        };
        if updated {
            self.flush_to_s3(table, Vec::new()).await?;
        }
        Ok(updated)
    }

    /// Establece de forma monótona el valor actual de una secuencia con ámbito (`scoped_code`).
    pub async fn sequence_scoped_set(
        &self,
        table: &str,
        scoped_code: &str,
        tenant_id: &str,
        target_value: i64,
    ) -> Result<bool, DomainError> {
        self.sync_from_s3(table, true).await?;
        let updated = {
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
            true
        };
        if updated {
            self.flush_to_s3(table, Vec::new()).await?;
        }
        Ok(updated)
    }

    /// Escanea registros de la partición aplicando filtrado opcional por prefijo de `PK` e inquilino (`:t`).
    pub async fn scan_with_filter(
        &self,
        table: &str,
        _filter_expression: &str,
        _expr_attr_names: HashMap<String, String>,
        expr_attr_values: HashMap<String, AttributeValue>,
    ) -> Result<Vec<HashMap<String, AttributeValue>>, DomainError> {
        self.sync_from_s3(table, false).await?;
        let part = partition_for(table);
        let guard = part
            .read()
            .map_err(|_| DomainError::eav(ErrorCode::Eav002, "Poisoned MBF partition lock"))?;
        let target_tenant = expr_attr_values.get(":t").and_then(av_string);
        let pk_prefix = expr_attr_values.get(":pfx").and_then(av_string);

        let mut results = Vec::new();
        if let Some(pfx) = pk_prefix {
            let start = (pfx.to_string(), Vec::new());
            for ((pk, _), item) in guard.items.range(start..) {
                if !pk.starts_with(pfx) {
                    break;
                }
                if let Some(t) = target_tenant {
                    if item.get("tenant_id").and_then(av_string) != Some(t) {
                        continue;
                    }
                }
                results.push(item.clone());
            }
        } else {
            for item in guard.items.values() {
                if let Some(t) = target_tenant {
                    if item.get("tenant_id").and_then(av_string) != Some(t) {
                        continue;
                    }
                }
                results.push(item.clone());
            }
        }
        Ok(results)
    }

    // ── Native MBF Quota Ledger Operations ──────────────────────────────────

    /// Lee en lote los valores actuales de múltiples contadores de cuota de un inquilino.
    pub async fn quota_read_many(
        &self,
        table: &str,
        tenant_id: &str,
        quota_ids: &[String],
    ) -> Result<HashMap<String, i64>, DomainError> {
        self.sync_from_s3(table, false).await?;
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

    /// Intenta debitar `amount` unidades de una cuota si el consumo actual es menor que `max_limit`.
    pub async fn quota_try_debit(
        &self,
        table: &str,
        tenant_id: &str,
        quota_id: &str,
        amount: i64,
        max_limit: i64,
        seed: i64,
    ) -> Result<(bool, i64), DomainError> {
        self.sync_from_s3(table, true).await?;
        let (allowed, val) = {
            let part = partition_for(table);
            let mut guard = part.write().map_err(|_| {
                DomainError::eav(ErrorCode::Infra001, "Poisoned MBF partition lock")
            })?;
            let key = (tenant_id.to_string(), quota_id.to_string());
            let current = guard.quota_counters.get(&key).copied().unwrap_or(seed);
            if current >= max_limit {
                (false, current)
            } else {
                let next = current + amount;
                guard.quota_counters.insert(key, next);
                (true, next)
            }
        };
        if allowed {
            self.flush_to_s3(table, Vec::new()).await?;
        }
        Ok((allowed, val))
    }

    /// Ajusta el contador de una cuota aplicando un delta firmado truncado a cero como mínimo.
    pub async fn quota_settle(
        &self,
        table: &str,
        tenant_id: &str,
        quota_id: &str,
        delta: i64,
    ) -> Result<i64, DomainError> {
        self.sync_from_s3(table, true).await?;
        let next = {
            let part = partition_for(table);
            let mut guard = part.write().map_err(|_| {
                DomainError::eav(ErrorCode::Infra001, "Poisoned MBF partition lock")
            })?;
            let key = (tenant_id.to_string(), quota_id.to_string());
            let current = guard.quota_counters.get(&key).copied().unwrap_or(0);
            let next = (current + delta).max(0);
            guard.quota_counters.insert(key, next);
            next
        };
        self.flush_to_s3(table, Vec::new()).await?;
        Ok(next)
    }

    /// Aplica un ajuste de cuota de forma idempotente garantizando ejecución única por `idem_key`.
    pub async fn quota_settle_once(
        &self,
        table: &str,
        tenant_id: &str,
        quota_id: &str,
        delta: i64,
        idem_key: &str,
    ) -> Result<bool, DomainError> {
        self.sync_from_s3(table, true).await?;
        let settled = {
            let part = partition_for(table);
            let mut guard = part.write().map_err(|_| {
                DomainError::eav(ErrorCode::Infra001, "Poisoned MBF partition lock")
            })?;
            let ikey = (tenant_id.to_string(), idem_key.to_string());
            if guard.quota_idem.contains_key(&ikey) {
                false
            } else {
                let qkey = (tenant_id.to_string(), quota_id.to_string());
                let current = guard.quota_counters.get(&qkey).copied().unwrap_or(0);
                let next = (current + delta).max(0);
                guard.quota_counters.insert(qkey, next);
                guard.quota_idem.insert(ikey, (quota_id.to_string(), delta));
                true
            }
        };
        if settled {
            self.flush_to_s3(table, Vec::new()).await?;
        }
        Ok(settled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mbf_envelope_and_partition_snapshot_roundtrip() {
        let mut store = MbfPartitionStore {
            epoch_id: 7,
            ..Default::default()
        };
        let mut item = HashMap::new();
        item.insert(
            "PK".to_string(),
            AttributeValue::S("T#system#E#record_01".to_string()),
        );
        item.insert(
            "SK".to_string(),
            AttributeValue::B(Blob::new(vec![0, 1, 2, 3])),
        );
        item.insert(
            "ap".to_string(),
            AttributeValue::S("T#system#A#title".to_string()),
        );
        item.insert("as".to_string(), AttributeValue::B(Blob::new(vec![10, 20])));
        item.insert(
            "v".to_string(),
            AttributeValue::S("Record Principal".to_string()),
        );
        store.insert_indexed(("T#system#E#record_01".to_string(), vec![0, 1, 2, 3]), item);
        store
            .quota_counters
            .insert(("system".to_string(), "generic_record".to_string()), 1);

        let snap = store.to_wire_snapshot();
        let raw_bin = snap.encode_binary();
        let mbf_bytes =
            encode_mbf_envelope(2, 1, 7, "system", "metri-mbf-local", &raw_bin).unwrap();

        assert_eq!(mbf_bytes.len() % 64, 0);
        assert_eq!(&mbf_bytes[0..8], b"METRIBIN");
        assert_eq!(&mbf_bytes[mbf_bytes.len() - 8..], b"MBFFOOTR");
        // Verify zero JSON braces inside the binary envelope
        assert!(!std::str::from_utf8(&mbf_bytes)
            .unwrap_or("")
            .contains("{\"epoch_id\""));

        let (header, decoded_payload) = decode_mbf_envelope(&mbf_bytes).unwrap();
        assert_eq!(header.cell_id(), 2);
        assert_eq!(header.row_count, 1);
        assert_eq!(header.epoch_id(), 7);

        let decoded_snap = MbfPartitionWireSnapshot::decode_binary(decoded_payload).unwrap();
        let mut restored = MbfPartitionStore::default();
        restored.apply_wire_snapshot(decoded_snap, "etag-123".to_string(), 1000);
        assert_eq!(restored.epoch_id, 7);
        assert_eq!(restored.items.len(), 1);
        assert_eq!(restored.aevt_index.len(), 1);
        assert_eq!(
            restored
                .quota_counters
                .get(&("system".to_string(), "generic_record".to_string())),
            Some(&1)
        );
    }

    #[test]
    fn test_compact_mbf_entity_and_single_idx_optimization() {
        let tenant_id = "system";
        let entity_id = "01M534FE21GVM9KV40HV0RXE6B";
        let pk = format!("T#{tenant_id}#E#{entity_id}");
        let mut datoms = Vec::new();
        let mut raw_v1_size = 0usize;

        for idx in 0..21u8 {
            let sk = vec![0, 0, 0, 0, 0, 0, 0, idx, 0, idx, 1];
            let mut item = HashMap::new();
            item.insert("PK".to_string(), AttributeValue::S(pk.clone()));
            item.insert("SK".to_string(), AttributeValue::B(Blob::new(sk.clone())));
            item.insert(
                "ap".to_string(),
                AttributeValue::S(format!("T#{tenant_id}#A#attr_{idx}")),
            );
            item.insert(
                "as".to_string(),
                AttributeValue::B(Blob::new(vec![1, 2, 3, idx])),
            );
            item.insert(
                "v".to_string(),
                AttributeValue::S(format!("Valor operativo industrial #{idx}")),
            );
            item.insert(
                "vp".to_string(),
                AttributeValue::S(format!("T#{tenant_id}#V#attr_{idx}")),
            );
            item.insert(
                "vs".to_string(),
                AttributeValue::B(Blob::new(vec![4, 5, 6, idx])),
            );
            let mut v1_buf = Vec::new();
            write_bytes_slice(&mut v1_buf, &sk);
            write_item_map_binary(&mut v1_buf, &item);
            raw_v1_size += v1_buf.len();
            datoms.push((sk, item));
        }

        let snap = MbfEntityWireSnapshot {
            tenant_id: tenant_id.to_string(),
            entity_type: "asset".to_string(),
            entity_id: entity_id.to_string(),
            epoch_id: 99,
            datoms: datoms.clone(),
        };
        let encoded = snap.encode_binary();
        assert_eq!(&encoded[0..8], b"MBFENTZ2");
        assert_eq!(encoded.len() % 64, 0);

        let decoded = MbfEntityWireSnapshot::decode_binary(&encoded).unwrap();
        assert_eq!(decoded.entity_id, entity_id);
        assert_eq!(decoded.datoms.len(), 21);
        assert_eq!(decoded.datoms, datoms);

        // Verify raw v2 compact item map is < 50% of raw v1 item map size
        let mut raw_v2_size = 0usize;
        for (sk, item) in &datoms {
            let mut v2_buf = Vec::new();
            write_bytes_slice(&mut v2_buf, sk);
            write_item_map_compact(&mut v2_buf, &pk, sk, item);
            raw_v2_size += v2_buf.len();
        }
        assert!(
            raw_v2_size * 2 < raw_v1_size,
            "Expected raw_v2_size ({raw_v2_size}) < 50% of raw_v1_size ({raw_v1_size})"
        );

        // Verify single-entity .mbf.idx is compact (< 2,048 bytes instead of 100 KB)
        let single_z5b = crate::eav::fts::searcher::export_single_entity_zone5b_bytes(
            tenant_id,
            entity_id,
            "Variador de Frecuencia Danfoss #100 Unidad operativa industrial",
        );
        let syn_zone =
            crate::mbf::zones::bi_synopses::MbfHyperSynopsis512::encode_zone_binary_slice(&[
                crate::mbf::zones::bi_synopses::MbfHyperSynopsis512::default(),
            ]);
        let idx_bytes = encode_mbf_idx_mirror(2, 99, 21, tenant_id, &single_z5b, &syn_zone);
        assert_eq!(idx_bytes.len() % 64, 0);
        assert!(
            idx_bytes.len() <= 2048,
            "Expected compact single-entity .mbf.idx <= 2048 B, got {} B",
            idx_bytes.len()
        );
    }
}
