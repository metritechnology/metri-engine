// janus_router/ — Write Path del Metri Engine.
//
// Componentes:
//   router        — JanusRouter + IWriteChannel (core.clj)
//   ulid          — Generador ULID monotónico (ulid.clj)
//   partition     — Estrategias S3/Hive (partition.clj)
//   oltp_channel  — Canal ACID EAV/DynamoDB (channels/oltp.clj)
//   olap_channel  — Canal Columnar Firehose (channels/olap.clj)

pub mod olap_channel;
pub mod oltp_channel; // — EAV ACID
pub mod partition;
pub mod router;
pub mod saga; // — scheduled_job en la misma TX
pub mod ulid; // — Firehose OLAP
