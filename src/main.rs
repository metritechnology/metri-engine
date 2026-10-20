//! Punto de entrada de `metri-engine` para AWS Lambda — binario `bootstrap`.
//!
//! El runtime `provided.al2023` de AWS Lambda exige que el ejecutable se denomine
//! `bootstrap`. La secuencia de inicialización aplica validación *fail-fast*
//! (ADR-005): cualquier anomalía en esquemas o catálogo de errores aborta el
//! proceso con `exit(1)` y registro estructurado JSON antes de aceptar tráfico:
//!
//! 1. Telemetría y suscriptor de trazas JSON compatible con AWS CloudWatch (`RUST_LOG`).
//! 2. [`metri_engine::codice`] — compila el `CodeRegistry` desde `CODICE_MODELS_DIR`
//!    (por defecto `config/models`) e instala la instancia estática `OnceLock`.
//! 3. [`metri_engine::domain`] — carga y valida el `ErrorCatalog` desde
//!    `ERROR_CATALOG_PATH` (`config/errors/error_catalog.toml`).
//! 4. [`metri_engine::grpc`] — inicia el servidor gRPC Tonic sobre Lambda Function URL
//!    o socket TCP local.

// Cerradura final del patrón Result: prohibido unwrap/expect/panic en código
// no-test. Los únicos sitios permitidos son las invariantes documentadas en
// scripts/dev/result_pattern_allowlist.json. Bajo cfg(test) se desactiva.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented
    )
)]
use std::path::Path;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // ── 1. Inicializar telemetría OTel ────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json() // JSON logs — compatible con AWS CloudWatch structured logging
        .init();

    info!("Metri Engine (Rust) — bootstrap iniciando");

    // ── 2. Bootstrap CodeRegistry — FASE 1 crítico ───────────────────────────
    let models_dir_env =
        std::env::var("CODICE_MODELS_DIR").unwrap_or_else(|_| "config/models".to_string());
    let models_dir = Path::new(&models_dir_env);

    match metri_engine::codice::CodeRegistry::build(models_dir) {
        Ok((registry, event_rules)) => {
            info!(
                entity_count = registry.entity_count(),
                event_rules = event_rules.len(),
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
        error!("FATAL: gRPC Server falló: {e:?}");
        std::process::exit(1);
    }

    info!("Metri Engine (Rust) — bootstrap completado");
}
