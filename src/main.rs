// [PORTED_FROM: src/metri/lambda/handler.clj + src/metri/main.clj]
// main.rs — Lambda entry point para AWS Lambda ARM64 (provided.al2023)
// Runtime: Tonic gRPC server sobre Lambda Function URL

use std::path::Path;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Inicializar telemetría OTel ────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("RUST_LOG")
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()           // JSON logs — compatible con AWS CloudWatch structured logging
        .init();

    info!("Metri Engine (Rust) — bootstrap iniciando");

    // ── 2. Bootstrap CodeRegistry — FASE 1 crítico ───────────────────────────
    let models_dir_env = std::env::var("CODICE_MODELS_DIR")
        .unwrap_or_else(|_| "config/models".to_string());
    let models_dir = Path::new(&models_dir_env);

    match metri_engine::codice::CodeRegistry::build(models_dir) {
        Ok((registry, event_rules)) => {
            info!(
                entity_count = registry.entity_count(),
                event_rules  = event_rules.len(),
                "Códice: registry compilado"
            );
            metri_engine::codice::init_global(registry);
        }
        Err(e) => {
            error!("FATAL: Códice no pudo compilar el registry: {e:?}");
            std::process::exit(1);
        }
    }

    // ── 2.5. Bootstrap ErrorCatalog ──────────────────────────────────────────
    let error_catalog_path_env = std::env::var("ERROR_CATALOG_PATH")
        .unwrap_or_else(|_| "config/errors/error_catalog.toml".to_string());
    let error_catalog_path = Path::new(&error_catalog_path_env);

    match metri_engine::domain::ErrorCatalog::load(error_catalog_path) {
        Ok(catalog) => {
            info!(
                version = %catalog.version,
                entries = catalog.entry_count(),
                "ErrorCatalog: catalog compilado"
            );
            metri_engine::domain::init_error_catalog(catalog);
        }
        Err(e) => {
            error!("FATAL: ErrorCatalog no pudo cargarse: {e:?}");
            std::process::exit(1);
        }
    }

    // ── 3. Inicializar gRPC server (Tonic) ──────────────────────────────
    if let Err(e) = metri_engine::grpc::server::start_lambda_grpc_server().await {
        error!("Error inicializando gRPC Server: {:?}", e);
    }

    info!("Metri Engine (Rust) — bootstrap completado");
    Ok(())
}
