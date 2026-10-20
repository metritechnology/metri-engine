//! Puertos del dominio — interfaces estáticas tipadas para inversión de dependencias (DIP).
//!
//! Define los contratos formales (`ISessionStore`, `ISqsBus`, `IEventBus`, `ICedarContext`,
//! `IAstCompiler`, `IAegisEngine`, `IExportStorage`, `IQueryCache`, `QueryCacheInvalidator`,
//! `IMbfObjectStore`, `IClock`) implementados por la capa de infraestructura.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::domain::errors::DomainError;

// ── Tipo alias para resultados Railway-oriented ───────────────────────────────
/// Alias estándar de resultado para operaciones de dominio bajo el patrón Railway (`Result<T, DomainError>`).
pub type DomainResult<T> = Result<T, DomainError>;

// ── Sesión / Token HMAC ───────────────────────────────────────────────────────

/// Sesión verificada — resultado de `get_session()`.
#[derive(Debug, Clone)]
pub struct Session {
    /// Identificador del tenant autenticado en la sesión.
    pub tenant_id: String,
    /// Identificador del usuario o principal autenticado.
    pub user_id: String,
    /// Identificador único del token (`jti`) para control de revocación.
    pub jti: String,
    /// Instante de expiración de la sesión en segundos desde el epoch Unix.
    pub exp: i64,
}

/// `ISessionStore` — verifica tokens HMAC-SHA256 y gestiona la lista de revocación.
#[async_trait]
pub trait ISessionStore: Send + Sync {
    /// Verifica token `mk_...` → `Session` o `None` si es inválido, expirado o revocado.
    async fn get_session(&self, token: &str) -> DomainResult<Option<Session>>;

    /// Revoca un token añadiendo su `jti` a la lista de revocación con tiempo de vida (`ttl_seconds`).
    async fn revoke_session(&self, jti: &str, ttl_seconds: u64) -> DomainResult<()>;

    /// Quita un `jti` de la lista de revocación (des-revocar). Operación idempotente.
    async fn unrevoke_session(&self, jti: &str) -> DomainResult<()>;
}

// ── SQS FIFO Bus ──────────────────────────────────────────────────────────────

/// Mensaje recibido del bus SQS.
#[derive(Debug, Clone)]
pub struct SqsMessage {
    /// Identificador de recibo requerido para confirmar la eliminación del mensaje en la cola.
    pub receipt_handle: String,
    /// Cuerpo serializado del mensaje recibido.
    pub body: String,
    /// Identificador único del mensaje asignado por el bus de colas.
    pub message_id: String,
}

/// `ISqsBus` — bus de mensajes FIFO.
#[async_trait]
pub trait ISqsBus: Send + Sync {
    /// Publica un mensaje en la cola FIFO.
    async fn publish(&self, payload: &str, group_id: &str, dedup_id: &str) -> DomainResult<String>; // retorna message_id

    /// Recibe hasta `max_count` mensajes.
    async fn receive_messages(&self, max_count: u32) -> DomainResult<Vec<SqsMessage>>;

    /// Confirma el procesamiento eliminando el mensaje.
    async fn delete_message(&self, receipt_handle: &str) -> DomainResult<()>;
}

// ── Event Bus (EventBridge) ───────────────────────────────────────────────────

/// `IEventBus` — bus de eventos de dominio.
#[async_trait]
pub trait IEventBus: Send + Sync {
    /// Publica un evento de dominio. Retorna `event_id`.
    async fn put_event(
        &self,
        event_bus_name: &str,
        source: &str,
        detail_type: &str,
        detail: Value,
    ) -> DomainResult<String>;
}

// ── Cedar Authorization Context ───────────────────────────────────────────────

/// Contexto de autorización Cedar resuelto para una petición autenticada.
#[derive(Debug, Clone)]
pub struct CedarCtx {
    /// Identificador del tenant validado para la operación.
    pub tenant_id: String,
    /// Identificador del usuario o principal que ejecuta la petición.
    pub user_id: String,
    /// Conjunto de roles asignados al principal dentro del tenant.
    pub roles: Vec<String>,
    /// Fronteras de dominio ABAC y restricciones jerárquicas aplicables al principal.
    pub domain_boundaries: HashMap<String, Value>,
}

/// `ICedarContext` — autorización Zero-Trust.
#[async_trait]
pub trait ICedarContext: Send + Sync {
    /// Evalúa el request contra políticas Cedar.
    async fn intercept(&self, request: &Value) -> DomainResult<CedarCtx>;
}

// ── AST Compiler (Janus) ──────────────────────────────────────────────────────

/// `IAstCompiler` — compilador puro de AST IR. Sin E/S ni estado mutable.
pub trait IAstCompiler: Send + Sync {
    /// Transforma descriptor gRPC + `CedarCtx` → AST IR inmutable.
    /// Invariante: `ast_ir["where"]` siempre incluye el filtro de aislamiento `tenant_id`.
    fn compile_ast(&self, query_descriptor: &Value, cedar_ctx: &CedarCtx) -> DomainResult<Value>;
}

// ── Aegis Engine ──────────────────────────────────────────────────────────────

/// Fragmento de resultado analítico emitido en el flujo de respuesta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultChunk {
    /// Carga útil JSON con las filas o métricas del fragmento actual.
    pub data: Value,
    /// Indica si este fragmento cierra la secuencia de resultados de la consulta.
    pub is_last: bool,
}

/// `IAegisEngine` — motor analítico unificado sobre AST IR (`.mbf` HTAP).
#[async_trait]
pub trait IAegisEngine: Send + Sync {
    /// Ejecuta el AST IR contra el motor analítico y retorna la secuencia de fragmentos de resultado.
    async fn transmute(&self, ast_ir: Value) -> DomainResult<Vec<ResultChunk>>;
}

// ── Export Storage (S3) ──────────────────────────────────────────────────────

/// `IExportStorage` — exportación de datos a almacenamiento externo (S3 / stub).
#[async_trait]
pub trait IExportStorage: Send + Sync {
    /// Genera la URL firmada para la descarga temporal de un archivo exportado.
    async fn generate_presigned_url(
        &self,
        tenant_id: &str,
        query_key: &str,
        columns: &[crate::grpc::pb::ColumnSchema],
        rows: &[crate::grpc::pb::DataRow],
    ) -> Result<String, crate::domain::errors::DomainError>;
}

// ── Query Result Cache (Janus read path) ─────────────────────────────────────

/// Entrada cacheada del read path. `expires_at` es epoch-segs LÓGICO —
/// verificado por la app en cada lectura; la expiración física la hace
/// el almacenamiento subyacente sobre su atributo y jamás participa en la decisión de servir.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Carga útil JSON de la respuesta de consulta almacenada en caché.
    pub payload: serde_json::Value,
    /// Identificador del tenant propietario de la entrada (candado anti-fuga entre tenants).
    pub tenant_id: String,
    /// Instante de expiración lógico en segundos desde el epoch Unix.
    pub expires_at: i64,
    /// Sello de generación del tenant al escribir (invalidación por escritura).
    pub generation: Option<u64>,
    /// Marca de tiempo en milisegundos epoch en que se almacenó la entrada.
    pub stored_at_ms: i64,
}

/// Escritura de una entrada de caché — best-effort: un fallo nunca debe
/// ser observable como error de query.
#[derive(Debug, Clone)]
pub struct CachePut {
    /// Identificador del tenant propietario de la consulta cacheada.
    pub tenant_id: String,
    /// PK completo: `QC#v1#<sha256>`.
    pub key: String,
    /// Carga útil JSON del resultado de consulta a almacenar.
    pub payload: serde_json::Value,
    /// TTL efectivo en segundos (jitter ya aplicado por el llamador).
    pub ttl_secs: u64,
}

/// `IQueryCache` — caché KV de resultados de consulta del read path.
/// Implementaciones: adaptador MBF en `infrastructure`, Null Object y
/// fakes en `janus::cache` / tests.
#[async_trait]
pub trait IQueryCache: Send + Sync {
    /// `Ok(None)` = miss (ausente, vencido, tenant distinto o generación
    /// invalidada). El llamador decide la degradación ante `Err` (fail-open).
    async fn get(&self, key: &str, tenant_id: &str) -> DomainResult<Option<CacheEntry>>;

    /// Almacena una entrada. Idempotente (último-gana).
    async fn put(&self, entry: &CachePut) -> DomainResult<()>;
}

/// `QueryCacheInvalidator` — invalidación lógica tras escritura (ISP: el write
/// path solo necesita esto, no lectura/escritura de entradas).
#[async_trait]
pub trait QueryCacheInvalidator: Send + Sync {
    /// Invalida lógicamente todas las entradas del tenant (bump de
    /// generación). Best-effort: el TTL sigue siendo la cota dura.
    async fn invalidate_tenant(&self, tenant_id: &str) -> DomainResult<()>;
}

// ── Metri Vortex (.mbf) Object Store & Deterministic Clock Ports (ISP) ───────

/// Payload de un objeto `.mbf` recuperado con su ETag de versión atómica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MbfObjectPayload {
    /// Contenido binario completo del archivo o celda `.mbf`.
    pub bytes: Vec<u8>,
    /// Etiqueta de entidad (`ETag`) de versión para control de concurrencia optimista CAS.
    pub etag: String,
}

/// Resultado de una lectura condicional con `If-None-Match`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MbfConditionalGet {
    /// El ETag coincide (`HTTP 304 Not Modified`): la caché en `/tmp` NVMe sigue vigente.
    NotModified,
    /// El objeto cambió (`HTTP 200 OK`): entrega los nuevos bytes y el nuevo ETag.
    Modified(MbfObjectPayload),
    /// El objeto aún no existe en el bucket.
    NotFound,
}

/// Puerto de almacenamiento de objetos optimizado para `.mbf` (S3 / LocalFS / ChaosTest).
#[async_trait]
pub trait IMbfObjectStore: Send + Sync {
    /// Obtiene un objeto completo junto con su `ETag`. Retorna `Ok(None)` si no existe.
    async fn get_object(&self, key: &str) -> DomainResult<Option<MbfObjectPayload>>;

    /// Revalida un objeto usando `If-None-Match: <cached_etag>`.
    async fn get_if_none_match(
        &self,
        key: &str,
        cached_etag: &str,
    ) -> DomainResult<MbfConditionalGet>;

    /// Obtiene un sub-rango exacto de bytes (`Range: bytes=start..end`).
    async fn get_byte_range(&self, key: &str, start: usize, end: usize) -> DomainResult<Vec<u8>>;

    /// Escritura atómica condicional Compare-And-Swap (`If-Match` / `If-None-Match: *`).
    /// Si `expected_etag` es `None`, exige que el objeto no exista previamente.
    /// Si el ETag actual no coincide, retorna `Err(DomainError::mbf(ErrorCode::MbfCasConflict001, ...))`.
    async fn put_if_match(
        &self,
        key: &str,
        bytes: Vec<u8>,
        expected_etag: Option<&str>,
    ) -> DomainResult<String>;
}

/// Puerto de reloj determinista para pruebas de cierres fiscales, DST y series temporales.
pub trait IClock: Send + Sync {
    /// Retorna el timestamp actual en milisegundos epoch UTC.
    fn now_epoch_ms(&self) -> i64;
}
