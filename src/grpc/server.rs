// grpc/server.rs — Inicializador del Servidor gRPC Tonic
// SRP: Configura el servidor HTTP/2 y los interceptores para AWS Lambda.

use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::info;

use super::service::MetriGrpcService;
use super::pb::metri_service_server::MetriServiceServer;

pub async fn start_lambda_grpc_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let port = std::env::var("GRPC_PORT").unwrap_or_else(|_| "9090".to_string());
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    info!("Iniciando gRPC Server (Tonic-Web) en {}...", addr);
    
    // Lee la tabla EAV y el prefijo OLAP desde env vars (alineado con template.yaml)
    let eav_table    = std::env::var("EAV_TABLE_NAME").unwrap_or_else(|_| "metri-eav-local".to_string());
    let olap_prefix  = std::env::var("KINESIS_STREAM_PREFIX").unwrap_or_else(|_| "metri-olap-stream".to_string());

    let ddb_client = Arc::new(crate::infrastructure::dynamodb::DynamoClient::new(&eav_table).await);
    let query_exec = crate::eav::reader::query::EavQueryExecutor::new(Arc::clone(&ddb_client), &eav_table);
    let pull_read  = crate::eav::reader::pull::EavReader::new(Arc::clone(&ddb_client), &eav_table);
    let oltp_exec  = crate::aegis::oltp::executor::OltpExecutor::new(query_exec, pull_read);

    // ── Write Path — channel registry ────────────────────────────────────────
    // OLTP: EavWriter → TransactWriteItems DynamoDB (tabla: EAV_TABLE_NAME)
    let eav_writer   = crate::eav::writer::EavWriter::new(Arc::clone(&ddb_client), &eav_table);
    let oltp_channel: Arc<dyn crate::janus_router::router::IWriteChannel> =
        Arc::new(crate::janus_router::oltp_channel::OltpChannel::new(eav_writer));

    // OLAP: Columnar Nativo (Firehose STUB — FASE 4: real Firehose client)
    let olap_channel: Arc<dyn crate::janus_router::router::IWriteChannel> =
        Arc::new(crate::janus_router::olap_channel::OlapChannel::new(&olap_prefix));


    let mut channel_registry: std::collections::HashMap<
        crate::codice::registry::EngineChannel,
        Arc<dyn crate::janus_router::router::IWriteChannel>,
    > = std::collections::HashMap::new();
    channel_registry.insert(crate::codice::registry::EngineChannel::Oltp, oltp_channel);
    channel_registry.insert(crate::codice::registry::EngineChannel::Olap, Arc::clone(&olap_channel));

    let janus_router = Arc::new(crate::janus_router::router::JanusRouter::new(channel_registry));

    // ── Interceptors y Pipeline ──────────────────────────────────────────────
    let audit_interceptor = Arc::new(crate::infrastructure::audit::interceptor::AuditInterceptorImpl::new(Arc::clone(&olap_channel)));

    let grpc_service = MetriGrpcService::new(oltp_exec, janus_router, audit_interceptor);
    let auth_interceptor = super::interceptors::auth_waf_interceptor;
    let service = MetriServiceServer::with_interceptor(grpc_service, auth_interceptor);
    let web_service = tonic_web::enable(service);

    // FASE 10: Habilitar gRPC Server Reflection para testing (grpcurl)
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(super::pb::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    Server::builder()
        .accept_http1(true)
        .add_service(reflection_service)
        .add_service(web_service)
        .serve(addr)
        .await?;
    
    Ok(())
}
