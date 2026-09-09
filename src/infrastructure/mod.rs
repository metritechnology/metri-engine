//! Infrastructure — outbound adapters: AWS SDK clients and local emulations.
//!
//! Cada adaptador implementa un puerto de [`crate::application`] o de
//! [`crate::domain`]; los interruptores `*_MODE` permiten levantar el
//! engine completo sin AWS (modo stub) — ver `docs/guides/desarrollo-local.md`.
pub mod athena;
pub mod audit;
pub mod domain_event_bus;
pub mod dynamodb;
pub mod eventbridge;
pub mod glue;
pub mod kinesis;
pub mod local_s3_query_engine;
pub mod s3_export;
pub mod seeder;
pub mod session_store;
pub mod sqs;
pub mod tenant_guard;
