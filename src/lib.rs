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
