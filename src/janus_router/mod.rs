//! janus_router — the write path of the metri engine.
//!
//! janus_router/ — Write Path del Metri Engine.
//!
//! Componentes:
//! - [`router`]       — `JanusRouter` + `IWriteChannel`
//! - [`oltp_channel`] — Canal transaccional ACID sobre `.mbf` (`EavWriter`)
//! - [`saga`]         — Orquestación de `scheduled_job` en la misma TX
//! - [`ulid`]         — Generador ULID monotónico

pub mod oltp_channel; // — .mbf ACID
pub mod router;
pub mod saga; // — scheduled_job en la misma TX
pub mod ulid;
