//! Enrutador de escritura (`janus_router`) — Write Path transaccional del motor `metri-engine`.
//!
//! Componentes principales:
//! - `router`       — `JanusRouter` y el contrato de canal `IWriteChannel`.
//! - `oltp_channel` — Canal transaccional ACID sobre `.mbf` (`EavWriter`).
//! - `saga`         — Proyección y reconciliación declarativa de `scheduled_job` en la misma TX.
//! - `ulid`         — Generador ULID monotónico en Crockford Base32.

pub mod oltp_channel; // — .mbf ACID
pub mod router;
pub mod saga; // — scheduled_job en la misma TX
pub mod ulid;
