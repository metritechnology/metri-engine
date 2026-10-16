//! metri-engine — the data engine of the metri CMMS/BI platform.
//!
//! Motor de datos 100 % Rust de la plataforma metri: una única Lambda ARM64
//! (`provided.al2023`) que expone un servidor gRPC (Tonic) y unifica en un
//! solo proceso el canal transaccional y el analítico sobre **Metri Vortex (`.mbf`)**:
//!
//! - **HTAP Unificado (`.mbf`)** — almacenamiento binario columnar y transaccional
//!   sobre S3 con caché NVMe en `/tmp` y `MbfStorageClient` en memoria, con datoms
//!   inmutables (EAVT/AEVT/AVET/VAET), transacciones ACID, full-text search y
//!   ejecución analítica vectorizada sin compilación SQL ni motores externos.
//! - **Seguridad** — autorización ABAC embebida (Cedar), aislamiento por
//!   tenant fail-closed, cifrado por tenant (AES-256-GCM + HKDF) y censura
//!   zero-trust del tenant maestro.
//! - **Cuotas** — ledger atómico por tenant con reservas para consumo de IA.
//!
//! # Architecture
//!
//! ```text
//!   clientes gRPC (metri-app · system-bff · Metri Q Assistant)
//!        │  interceptors: HMAC · sesión · Cedar (fail-closed)
//!        ├─ janus ──────── read path ───▶ aegis (mbf/oltp) ──▶ eav / mbf (S3 + NVMe)
//!        ├─ janus_router ─ write path ──▶ eav (transact) ───▶ mbf · outbox ▶ eda
//!        └─ iop ────────── ingesta ─────▶ cedar → quota → janus_router
//!              codice (esquemas SSOT) · quota (ledger) · infrastructure (AWS)
//! ```
//!
//! # Modules
//!
//! | Módulo | Rol |
//! |---|---|
//! | [`mbf`] | Motor binario HTAP unificado Metri Vortex (`.mbf`): formato columnar, cifrado, S3 CAS y storage en memoria |
//! | [`janus`] | Read path: queries → AST IR en FlatBuffers, agregaciones, multi-series, FTS |
//! | [`janus_router`] | Write path: routing transaccional `.mbf`, sagas (`scheduled_job`), ULIDs monotónicos |
//! | [`aegis`] | Ejecutores `.mbf`/OLTP sobre AST IR + motor de fórmulas sandboxeado |
//! | [`eav`] | Motor de datoms inmutables sobre `.mbf`: `types`, `reader`, `writer` y `fts` |
//! | [`codice`] | Registro SSOT de los modelos JSON (`config/models/`) |
//! | [`cedar`] | Autorización ABAC (Cedar Policy), cache de principals |
//! | [`quota`] | Cuotas atómicas por tenant: ledger, reservas IA, sweeper |
//! | [`iop`] | Pipeline de ingesta por pasos: Cedar → Quota → Janus |
//! | [`eda`] | Eventos: outbox, fault detection (`sherlog`), routing EventBridge/SQS |
//! | [`grpc`] | Servidor gRPC/Tonic-Web (`MetriService`, `MoiraRoutingService`, `QuotaService`, `AgentConfigService`) |
//! | [`temporal`] | Primitivas temporales timezone-aware |
//! | [`infrastructure`] | Adaptadores AWS SDK (S3, SQS, EventBridge), sesiones HMAC y auditoría |
//! | [`application`] | Puertos de aplicación (DIP) |
//! | [`domain`] | Errores canónicos, eventos de dominio, protocolos |
//! | [`otel`] | Trazas OpenTelemetry (OTLP) |
//!
//! # Origin
//!
//! Evolución nativa en Rust hacia el motor binario unificado `.mbf`.
//! Índice de la documentación de referencia generada en `docs/reference/`.
//!
//! # Documentation
//!
//! `cargo doc --no-deps` genera la referencia completa y `cargo test --doc`
//! ejecuta los ejemplos.

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
extern crate alloc;

pub mod aegis; // Ejecutores .mbf/OLTP y motor de fórmulas
pub mod application; // Puertos de aplicación (DIP) — contratos metri-contracts
pub mod cedar; // Autorización ABAC Cedar Policy
pub mod codice; // Registro SSOT de esquemas JSON
pub mod domain; // Errores canónicos, eventos y puertos de dominio
pub mod eav; // Datoms inmutables sobre MbfStorageClient (.mbf)
pub mod eda; // Event-Driven Architecture (Moira + Outbox)
pub mod grpc; // Servidor y servicios gRPC (Tonic)
pub mod infrastructure; // Adaptadores AWS SDK y sesiones HMAC
pub mod iop; // Pipeline de orquestación de escritura (Cedar → Quota → Janus)
pub mod janus; // Read Path (query pipeline)
pub mod janus_router; // Write Path (JanusRouter + OltpChannel)
pub mod mbf; // Metri Vortex (.mbf) Unified HTAP Binary Engine
pub mod otel; // Trazas OpenTelemetry
pub mod quota; // QuotaGuard — contador con autoridad + resolución de cuota vigente
pub mod temporal; // Primitivas temporales timezone-aware
