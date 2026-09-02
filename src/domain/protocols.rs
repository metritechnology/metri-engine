// [PORTED_FROM: src/metri/domain/protocols.clj]
// Equivalencia: ISessionStore, ISQSBus, IQueryEngine, IStreamWriter,
//               IEventBus, ICedarContext, IASTCompiler, IAegisEngine
//
// En Clojure: defprotocol → interfaz dinámica (duck typing)
// En Rust:    trait → interfaz estática tipada
//
// Zero-Drop Policy: todos los métodos de cada defprotocol están presentes.
// Los contratos Railway [:ok ...] | [:error ...] se mapean a Result<T, DomainError>.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::domain::errors::DomainError;

// ── Tipo alias para resultados Railway-oriented ───────────────────────────────
pub type DomainResult<T> = Result<T, DomainError>;

// ── Sesión / Token HMAC ───────────────────────────────────────────────────────

/// Sesión verificada — resultado de get_session()
/// Equivale al mapa {:tenant-id :user-id :jti :exp} de Clojure.
#[derive(Debug, Clone)]
pub struct Session {
    pub tenant_id: String,
    pub user_id: String,
    pub jti: String,
    pub exp: i64,
}

/// ISessionStore — verifica tokens HMAC-SHA256 y gestiona blacklist.
/// [PORTED_FROM: (defprotocol ISessionStore)]
#[async_trait]
pub trait ISessionStore: Send + Sync {
    /// Verifica token `mk_...` → Session o None si inválido/expirado/revocado.
    /// [PORTED_FROM: (get-session [store token])]
    async fn get_session(&self, token: &str) -> DomainResult<Option<Session>>;

    /// REVOCAR: añade el token a la blacklist DynamoDB.
    /// [PORTED_FROM: (put-session! [store token session-map ttl-seconds])]
    async fn revoke_session(&self, jti: &str, ttl_seconds: u64) -> DomainResult<()>;

    /// Quita un jti de la blacklist (des-revocar). Idempotente.
    /// [PORTED_FROM: (del-session! [store token-or-jti])]
    async fn unrevoke_session(&self, jti: &str) -> DomainResult<()>;
}

// ── SQS FIFO Bus ──────────────────────────────────────────────────────────────

/// Mensaje recibido del bus SQS.
#[derive(Debug, Clone)]
pub struct SqsMessage {
    pub receipt_handle: String,
    pub body: String,
    pub message_id: String,
}

/// ISQSBus — bus de mensajes FIFO.
/// [PORTED_FROM: (defprotocol ISQSBus)]
#[async_trait]
pub trait ISqsBus: Send + Sync {
    /// Publica un mensaje en la cola FIFO.
    /// [PORTED_FROM: (publish! [bus payload group-id dedup-id])]
    async fn publish(&self, payload: &str, group_id: &str, dedup_id: &str) -> DomainResult<String>; // retorna message_id

    /// Recibe hasta `max_count` mensajes.
    /// [PORTED_FROM: (receive-messages [bus max-count])]
    async fn receive_messages(&self, max_count: u32) -> DomainResult<Vec<SqsMessage>>;

    /// Confirma el procesamiento eliminando el mensaje.
    /// [PORTED_FROM: (delete-message! [bus receipt-handle])]
    async fn delete_message(&self, receipt_handle: &str) -> DomainResult<()>;
}

// ── Query Engine (Athena) ─────────────────────────────────────────────────────

/// Resultado de una consulta analítica Athena.
#[derive(Debug, Clone)]
pub struct QueryResults {
    pub columns: Vec<String>,
    pub rows: Vec<HashMap<String, Value>>,
}

/// IQueryEngine — motor analítico (Athena / stub).
/// [PORTED_FROM: (defprotocol IQueryEngine)]
#[async_trait]
pub trait IQueryEngine: Send + Sync {
    /// Inicia query asíncrona. Retorna execution_id.
    /// [PORTED_FROM: (start-query! [engine sql database])]
    async fn start_query(&self, sql: &str, database: &str) -> DomainResult<String>;

    /// Espera y retorna resultados.
    /// [PORTED_FROM: (get-query-results [engine execution-id])]
    async fn get_query_results(&self, execution_id: &str) -> DomainResult<QueryResults>;
}

// ── Stream Writer (Kinesis) ───────────────────────────────────────────────────

/// IStreamWriter — escritura a Kinesis Firehose.
/// [PORTED_FROM: (defprotocol IStreamWriter)]
#[async_trait]
pub trait IStreamWriter: Send + Sync {
    /// Escribe un record al stream. Retorna sequence_number.
    /// [PORTED_FROM: (put-record! [writer stream-name partition-key data])]
    async fn put_record(
        &self,
        stream_name: &str,
        partition_key: &str,
        data: Vec<u8>,
    ) -> DomainResult<String>;
}

// ── Event Bus (EventBridge) ───────────────────────────────────────────────────

/// IEventBus — bus de eventos de dominio.
/// [PORTED_FROM: (defprotocol IEventBus)]
#[async_trait]
pub trait IEventBus: Send + Sync {
    /// Publica un evento de dominio. Retorna event_id.
    /// [PORTED_FROM: (put-event! [bus event-bus-name source detail-type detail])]
    async fn put_event(
        &self,
        event_bus_name: &str,
        source: &str,
        detail_type: &str,
        detail: Value,
    ) -> DomainResult<String>;
}

// ── Cedar Authorization Context ───────────────────────────────────────────────

/// Contexto de autorización Cedar.
/// Equivale al mapa {:tenant-id :user-id :roles :domain-boundaries} de Clojure.
#[derive(Debug, Clone)]
pub struct CedarCtx {
    pub tenant_id: String,
    pub user_id: String,
    pub roles: Vec<String>,
    pub domain_boundaries: HashMap<String, Value>,
}

/// ICedarContext — autorización Zero-Trust.
/// [PORTED_FROM: (defprotocol ICedarContext)]
#[async_trait]
pub trait ICedarContext: Send + Sync {
    /// Evalúa el request contra políticas Cedar.
    /// [PORTED_FROM: (intercept [this request])]
    async fn intercept(&self, request: &Value) -> DomainResult<CedarCtx>;
}

// ── AST Compiler (Janus) ──────────────────────────────────────────────────────

/// IASTCompiler — compilador puro de AST IR. Sin I/O. Sin estado.
/// [PORTED_FROM: (defprotocol IASTCompiler)]
pub trait IAstCompiler: Send + Sync {
    /// Transforma descriptor gRPC + cedar-ctx → AST IR inmutable.
    /// Invariante: ast_ir["where"] siempre incluye tenant_id filter.
    /// [PORTED_FROM: (compile-ast [this query-descriptor cedar-ctx])]
    fn compile_ast(&self, query_descriptor: &Value, cedar_ctx: &CedarCtx) -> DomainResult<Value>;
}

// ── Aegis Engine ──────────────────────────────────────────────────────────────

/// Chunk de resultado analítico — item de la secuencia lazy.
/// Equivale a cada [:ok chunk-map] del lazy-seq Clojure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultChunk {
    pub data: Value,
    pub is_last: bool,
}

/// IAegisEngine — motor analítico: AST IR → Datalog (OLTP) o SQL (OLAP).
/// [PORTED_FROM: (defprotocol IAegisEngine)]
#[async_trait]
pub trait IAegisEngine: Send + Sync {
    /// Ejecuta el AST IR contra el motor correcto.
    /// Retorna stream de chunks (equivalente a lazy-seq Clojure).
    /// [PORTED_FROM: (transmute! [this ast-ir])]
    async fn transmute(&self, ast_ir: Value) -> DomainResult<Vec<ResultChunk>>;
}

// ── Export Storage (S3) ──────────────────────────────────────────────────────

/// IExportStorage — exportación de datos a almacenamiento externo (S3 / stub).
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
