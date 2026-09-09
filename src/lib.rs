//! metri-engine — the data engine of the metri CMMS/BI platform.
//!
//! Motor de datos 100 % Rust de la plataforma metri: una única Lambda ARM64
//! (`provided.al2023`) que expone un servidor gRPC (Tonic) y combina en un
//! solo proceso el canal transaccional y el analítico:
//!
//! - **OLTP** — motor EAV inmutable sobre DynamoDB single-table (datoms
//!   EAVT/AEVT/AVET/VAET, transacciones ACID, full-text search, jerarquías).
//! - **OLAP** — data lake Parquet en S3 + Glue + Athena, `tenant_id` como
//!   primera partición obligatoria.
//! - **Seguridad** — autorización ABAC embebida (Cedar), aislamiento por
//!   tenant fail-closed, censura zero-trust del tenant maestro.
//! - **Cuotas** — ledger atómico por tenant con reservas para consumo de IA.
//!
//! # Architecture
//!
//! ```text
//!   clientes gRPC (metri-app · system-bff · Metri Q Assistant)
//!        │  interceptors: HMAC · sesión · Cedar (fail-closed)
//!        ├─ janus ──────── read path ───▶ aegis (sql/oltp) ──▶ eav / Athena
//!        ├─ janus_router ─ write path ──▶ eav (transact) ─▶ outbox ▶ eda
//!        └─ iop ────────── ingesta ─────▶ cedar → quota → janus
//!              codice (esquemas SSOT) · quota (ledger) · infrastructure (AWS)
//! ```
//!
//! # Modules
//!
//! | Módulo | Rol |
//! |---|---|
//! | [`janus`] | Read path: queries → AST IR en FlatBuffers, agregaciones, multi-series, FTS |
//! | [`janus_router`] | Write path: routing OLTP/OLAP, sagas, particionado, ULIDs |
//! | [`aegis`] | Compilador SQL para Athena + executor OLTP + motor de fórmulas |
//! | [`eav`] | Motor de almacenamiento EAV inmutable: datoms, transacciones, FTS |
//! | [`codice`] | Registro SSOT de los modelos JSON (`config/models/`) |
//! | [`cedar`] | Autorización ABAC (Cedar Policy 3), cache de principals |
//! | [`quota`] | Cuotas atómicas por tenant: ledger, reservas IA, sweeper |
//! | [`iop`] | Pipeline de ingesta por pasos: Cedar → Quota → Janus |
//! | [`eda`] | Eventos: outbox, fault detection, routing EventBridge/SQS |
//! | [`temporal`] | Primitivas temporales timezone-aware |
//! | [`infrastructure`] | Adaptadores AWS SDK + motor local de query S3 |
//! | [`application`] | Puertos de aplicación (DIP) |
//! | [`domain`] | Errores canónicos, eventos de dominio, protocolos |
//! | [`otel`] | Trazas OpenTelemetry (OTLP) |
//!
//! # Origin
//!
//! Port del stack Clojure anterior; cada módulo narra su mapeo pieza a pieza
//! en su sección `# Origin`. Índice completo de la documentación en
//! `docs/README.md`; decisiones de diseño en `docs/architecture/adr/`.
//!
//! # Documentation
//!
//! `cargo doc --no-deps` genera la referencia completa y `cargo test --doc`
//! ejecuta los ejemplos. Reglas de documentación:
//! `docs/architecture/PLAN_DOCUMENTACION.md`.

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

pub mod aegis; // FASE 3 — SQL/Datalog compiler
pub mod application; // Puertos de aplicación (DIP) — contratos metri-contracts
pub mod cedar;
pub mod codice;
pub mod domain;
pub mod eav;
pub mod eda; // FASE 3 — Event-Driven Architecture
pub mod grpc;
pub mod infrastructure;
pub mod iop; // FASE 2 ✓
pub mod janus; // FASE 2 ✓ — Read Path (query pipeline)
pub mod janus_router; // FASE 2 ✓ — Write Path (JanusRouter + channels)
pub mod otel;
pub mod quota; // QuotaGuard — contador con autoridad + resolución de cuota vigente
pub mod temporal; // FASE 3 ✓ — Primitivas temporales (port de metres.temporal.*) // FASE 3 — cedar-policy ABAC
