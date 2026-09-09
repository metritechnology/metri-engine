//! cedar — the Zero-Trust PDP over cedar-policy (ADR-002).
//!
//! cedar — PDP Zero-Trust sobre cedar-policy (ADR-002).
//!
//! Estructura: `authn` (tokens HMAC + step1), `pipeline` (composición de pasos
//! y fachadas intercept/get_principal_data), `principal_graph` (step2/step3),
//! `evaluator` (PDP step4), `engine` (fachada cedar-policy), `rules` (reglas
//! de sistema), `cache` (principals e invalidación), `ports` (contratos DI),
//! `request`/`resource_hydrator` (transporte y hidratación ABAC), `types`.
//!
//! La API pública del módulo se re-exporta en la raíz: los consumidores hacen
//! `use crate::cedar::{intercept, PrincipalData, ...}`.

pub mod authn;
pub mod cache;
pub mod engine;
pub mod evaluator;
pub mod pipeline;
pub mod ports;
pub mod principal_graph;
pub mod request;
pub mod resource_hydrator;
pub mod rules;
pub mod types;

#[cfg(test)]
mod tests;

pub use authn::{step1_extract_token, verify_hmac_token_local_in_step, HmacTokenVerifier};
pub use cache::invalidation::BroadcastBus;
pub use cache::principal::{InMemoryPrincipalCache, MAX_PRINCIPAL_CACHE_SIZE};
pub use cache::session::{InMemorySessionStore, MAX_SESSION_CACHE_SIZE};
pub use engine::CedarAuthorizer;
pub(crate) use evaluator::is_known_domain;
pub(crate) use evaluator::principal_grant_keys;
pub use evaluator::step4_evaluate_cedar;
pub use pipeline::{get_principal_data, intercept};
pub use ports::{EntityReader, PolicyStore, PrincipalCache};
pub use principal_graph::{assemble_principal_graph, step2_query_oltp, step3_consolidate};
pub use request::AuthRequest;
pub use rules::{
    is_master_tenant, step3b_validate_time_window, AuthenticationPolicy, SystemSecurityRules,
};
pub use types::{CedarContext, InvalidationMsg, PrincipalData, RoleBoundary, TimeRestriction};
