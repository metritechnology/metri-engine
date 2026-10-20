//! `metri-engine` — Motor de datos HTAP unificado de la plataforma Metri (CMMS / BI / IoT).
//!
//! Motor de datos 100 % Rust desplegado como una única función AWS Lambda ARM64
//! (`provided.al2023`, binario `bootstrap`) que expone un servidor gRPC (Tonic) y
//! unifica en un solo proceso el canal transaccional ACID y el canal analítico
//! vectorizado sobre **Metri Vortex (`.mbf`)**:
//!
//! - **HTAP Unificado (`.mbf`)** — almacenamiento binario columnar y transaccional
//!   sobre Amazon S3 con caché NVMe en `/tmp` y `MbfStorageClient` en memoria, con datoms
//!   inmutables (EAVT/AEVT/AVET/VAET), transacciones ACID por escritura condicional (`ETag`),
//!   búsqueda híbrida (trigramas, difusa y vectorial) y ejecución analítica vectorizada
//!   sin compilación SQL ni motores externos.
//! - **Seguridad Zero-Trust** — autorización ABAC embebida (Cedar Policy), aislamiento por
//!   tenant fail-closed (`TenantGuard`), autenticación HMAC en tiempo constante y censura
//!   del tenant maestro (`system`).
//! - **Cuotas Atómicas** — libro mayor (`QuotaLedger`) con autoridad atómica por tenant y
//!   reservas de consumo para agentes de IA.
//!
//! # Arquitectura General
//!
//! ```text
//!   clientes gRPC (metri-app · system-bff · Metri Q Assistant)
//!        │  interceptores: HMAC · sesión · Cedar (fail-closed)
//!        ├─ janus ──────── read path ───▶ aegis (mbf/oltp/formula) ──▶ eav / mbf (S3 + NVMe)
//!        ├─ janus_router ─ write path ──▶ eav (transact) ────────────▶ mbf · outbox ▶ eda
//!        └─ iop ────────── ingesta ─────▶ cedar → quota → janus_router
//!              codice (esquemas SSOT) · quota (ledger) · infrastructure (AWS)
//! ```
//!
//! # Mapa de Módulos
//!
//! | Módulo | Rol |
//! |---|---|
//! | [`mbf`] | Motor binario HTAP unificado Metri Vortex (`.mbf`): formato de 64B, 8 zonas físicas, transacciones MVCC/CAS y almacenamiento |
//! | [`janus`] | Camino de lectura (Read Path): normalización, validación, selección de plan, compilación a AST IR en FlatBuffers y agregación |
//! | [`janus_router`] | Camino de escritura (Write Path): enrutamiento transaccional `.mbf`, sagas declarativas (`scheduled_job`) y ULIDs monotónicos |
//! | [`aegis`] | Ejecutores `.mbf` y OLTP sobre AST IR + motor de fórmulas matemáticas sandboxeado |
//! | [`eav`] | Motor de datoms inmutables sobre `.mbf`: `types`, `reader`, `writer` y `fts` |
//! | [`codice`] | Registro SSOT de los 60 modelos JSON (`config/models/`): coerción, validación, perfiles matemáticos y secuencias |
//! | [`cedar`] | Autorización ABAC Zero-Trust (Cedar Policy), grafo de principal y cachés de sesión |
//! | [`quota`] | Cuotas atómicas por tenant: resolución, libro mayor (`QuotaLedger`), reservas IA y `sweeper` |
//! | [`iop`] | Pipeline de orquestación de ingesta por pasos: Cedar → Quota → Janus |
//! | [`eda`] | Arquitectura orientada a eventos: drenaje de outbox y reglas de ruteo (`moira`) |
//! | [`grpc`] | Servidor gRPC/Tonic-Web (`MetriService`, `MoiraRoutingService`, `QuotaService`, `AgentConfigService`) |
//! | [`temporal`] | Primitivas temporales canónicas conscientes de zona horaria (`chrono-tz`) |
//! | [`infrastructure`] | Adaptadores AWS SDK (S3, SQS, EventBridge), sesiones HMAC, `TenantGuard` y auditoría |
//! | [`application`] | Puertos de aplicación (Principio de Inversión de Dependencias — DIP) |
//! | [`domain`] | Errores canónicos (`DomainError`/`ErrorCatalog`), eventos de dominio y protocolos |
//! | [`otel`] | Trazas distribuidas OpenTelemetry (OTLP) |
//!
//! # Documentación Unificada
//!
//! El índice maestro de la documentación arquitectónica, guías, componentes y referencia
//! vive en `docs/README.md` y la referencia autogenerada en `docs/05-referencia/`.
//! `cargo doc --no-deps` genera la referencia de API del crate y `cargo test --doc`
//! verifica los ejemplos.

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
