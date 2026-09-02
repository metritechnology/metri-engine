// janus_router/ — Write Path del Metri Engine.
// [PORTED_FROM: src/metri/janus_router/]
//
// Componentes:
//   router        — JanusRouter + IWriteChannel (core.clj)
//   ulid          — Generador ULID monotónico (ulid.clj)
//   partition     — Estrategias S3/Hive (partition.clj)
//   oltp_channel  — Canal ACID EAV/DynamoDB (channels/oltp.clj)
//   olap_channel  — Canal Columnar Firehose (channels/olap.clj)

pub mod router;
pub mod ulid;
pub mod partition;
pub mod oltp_channel;   // [PORTED_FROM: channels/oltp.clj] — EAV ACID
pub mod saga;           // [PORTED_FROM: projections/saga.clj] — scheduled_job en la misma TX
pub mod olap_channel;   // [PORTED_FROM: channels/olap.clj] — Firehose OLAP
