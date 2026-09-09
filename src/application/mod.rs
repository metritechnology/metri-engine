//! Application ports — consumer-bounded traits (DIP).
//!
//! application — casos de uso y puertos de la aplicación.
//!
//! DIP: lo que aquí se define son TRAITS delimitados por la necesidad del
//! consumidor. Las implementaciones concretas (EventBridge, outbox, DynamoDB)
//! viven en infrastructure/ y se cablean en los composition roots
//! (grpc/bootstrap.rs, grpc/server.rs). Los tests componen fakes sin AWS.

pub mod ports;
