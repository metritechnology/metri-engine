// [PORTED_FROM: src/metri/lambda/handler.clj + src/metri/main.clj]
// main.rs — Lambda entry point para AWS Lambda ARM64 (provided.al2023)
// Runtime: Tonic gRPC server sobre Lambda Function URL
//
// En Clojure: (ig/init ...) arrancaba el servidor JVM con Integrant.
// En Rust: composición estática en bootstrap — zero DI runtime overhead.
extern crate alloc;

mod codice;
mod domain;
mod eav;
mod otel;
mod infrastructure;
mod grpc;
mod janus;        // FASE 2 ✓ — Read Path (query pipeline)
mod janus_router; // FASE 2 ✓ — Write Path (JanusRouter + channels)
mod iop;          // FASE 2 ✓
mod aegis;        // FASE 3 — SQL/Datalog compiler
mod temporal;   // FASE 3 ✓ — Primitivas temporales (port de metres.temporal.*)
mod eda;          // FASE 3 — Event-Driven Architecture
mod cedar;        // FASE 3 — cedar-policy ABAC

use std::path::Path;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Inicializar telemetría OTel ────────────────────────────────────────
    // [PORTED_FROM: ig/init-key :otel/tracer]
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("RUST_LOG")
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()           // JSON logs — compatible con AWS CloudWatch structured logging
        .init();

    info!("Metri Engine (Rust) — bootstrap iniciando");

    // ── 2. Bootstrap CodeRegistry — FASE 1 crítico ───────────────────────────
    // [PORTED_FROM: ig/init-key :codice/registry]
    // El registry se carga desde S3 o filesystem una sola vez en cold start.
    let models_dir_env = std::env::var("CODICE_MODELS_DIR")
        .unwrap_or_else(|_| "config/models".to_string());
    let models_dir = Path::new(&models_dir_env);

    match codice::CodeRegistry::build(models_dir) {
        Ok((registry, event_rules)) => {
            info!(
                entity_count = registry.entity_count(),
                event_rules  = event_rules.len(),
                "Códice: registry compilado"
            );
            codice::init_global(registry);
        }
        Err(e) => {
            error!("FATAL: Códice no pudo compilar el registry: {e:?}");
            // En Lambda, si el registry falla, el cold start falla → el invoke falla con 500.
            // Este es el comportamiento correcto (fail-fast de bootstrap).
            std::process::exit(1);
        }
    }

    // ── 3. Inicializar gRPC server (Tonic) ──────────────────────────────
    // [PORTED_FROM: grpc/server.clj]
    if let Err(e) = grpc::server::start_lambda_grpc_server().await {
        error!("Error inicializando gRPC Server: {:?}", e);
    }

    // ── 4. TODO: Inicializar Metri EAV Engine ────────────────────────────────
    // [PORTED_FROM: infrastructure/datahike.clj — REEMPLAZADO]
    // Semana 5-8: write path + read path + 4 índices DynamoDB
    info!("TODO: Metri EAV Engine — FASE 1 pendiente");

    // Placeholder: Lambda espera events desde el runtime
    // lambda_runtime::run(handler).await?;

    info!("Metri Engine (Rust) — bootstrap completado");
    Ok(())
}
