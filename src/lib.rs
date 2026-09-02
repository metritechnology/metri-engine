extern crate alloc;

pub mod codice;
pub mod domain;
pub mod eav;
pub mod otel;
pub mod infrastructure;
pub mod grpc;
pub mod janus;        // FASE 2 ✓ — Read Path (query pipeline)
pub mod janus_router; // FASE 2 ✓ — Write Path (JanusRouter + channels)
pub mod iop;          // FASE 2 ✓
pub mod quota;        // QuotaGuard — contador con autoridad + resolución de cuota vigente
pub mod aegis;        // FASE 3 — SQL/Datalog compiler
pub mod temporal;     // FASE 3 ✓ — Primitivas temporales (port de metres.temporal.*)
pub mod eda;          // FASE 3 — Event-Driven Architecture
pub mod cedar;        // FASE 3 — cedar-policy ABAC
