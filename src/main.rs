//! metri-engine Lambda entry point — the `bootstrap` binary.
//!
//! Punto de arranque de la función Lambda: `provided.al2023` exige que el
//! binario se llame `bootstrap`. La secuencia es fail-fast — cada paso
//! aborta con `exit(1)` y log estructurado si falla:
//!
//! 1. Telemetría OTel (logs JSON para CloudWatch, filtro por `RUST_LOG`).
//! 2. [`metri_engine::codice`] — compila el `CodeRegistry` desde
//!    `CODICE_MODELS_DIR` (default `config/models`) y lo instala global.
//! 3. [`metri_engine::domain`] — carga el `ErrorCatalog` TOML desde
//!    `ERROR_CATALOG_PATH` y lo instala global.
//! 4. [`metri_engine::grpc`] — levanta el servidor gRPC sobre la Lambda
//!    Function URL.

// Cerradura final del patrón Result (PLAN_PATRON_RESULT.md §4.4): prohibido
// unwrap/expect/panic en código no-test. Los únicos sitios permitidos son las
// invariantes documentadas en scripts/dev/result_pattern_allowlist.json, cada
// una con su #[allow] y comentario. Bajo cfg(test) se desactiva: los tests
// usan unwrap/expect libremente.
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
