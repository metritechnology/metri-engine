// cedar/cache/principal.rs — Caché en memoria de principals consolidados.
//
// Entradas con TTL: la staleness máxima de una decisión por caché queda
// acotada incluso si el suscriptor de invalidación pierde mensajes (ventana
// de `Lagged`) — se prefiere re-consultar el grafo antes que autorizar con
// roles obsoletos.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::cedar::cache::invalidation::spawn_invalidation_task;
use crate::cedar::types::PrincipalData;
use crate::domain::errors::DomainError;

pub const MAX_PRINCIPAL_CACHE_SIZE: usize = 5_000;

/// Vida máxima de una entrada. Cubre la ventana de mensajes de invalidación
/// perdidos (Lagged) y acota decisiones autorizadas con datos viejos.
pub const PRINCIPAL_CACHE_TTL: Duration = Duration::from_secs(300);

pub(crate) struct Entry {
    pub(crate) principal: PrincipalData,
    stored_at: Instant,
}

pub struct InMemoryPrincipalCache {
    cache: Arc<RwLock<HashMap<String, Entry>>>,
}

impl Default for InMemoryPrincipalCache {
    fn default() -> Self {
        Self::new(std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)))
    }
}

impl InMemoryPrincipalCache {
    /// `bus` es la instancia compartida con los mutadores que publican la
    /// invalidación — sin canales globales.
    pub fn new(bus: std::sync::Arc<dyn crate::cedar::ports::InvalidationBus>) -> Self {
        let cache = Arc::new(RwLock::new(HashMap::<String, Entry>::new()));
        spawn_invalidation_task(bus, Arc::clone(&cache));
        Self { cache }
    }
}

#[async_trait]
impl crate::cedar::ports::PrincipalCache for InMemoryPrincipalCache {
    async fn lookup_principal(&self, user_id: &str) -> Option<PrincipalData> {
        if let Ok(mut lock) = self.cache.write() {
            match lock.get(user_id) {
                Some(entry) if entry.stored_at.elapsed() < PRINCIPAL_CACHE_TTL => {
                    return Some(entry.principal.clone());
                }
                _ => {
                    // Ausente o vencida: fuera, para que la próxima escritura
                    // refresque `stored_at` en lugar de conservar la vieja.
                    lock.remove(user_id);
                }
            }
        }
        None
    }

    async fn store_principal(
        &self,
        user_id: &str,
        principal: PrincipalData,
    ) -> Result<(), DomainError> {
        if let Ok(mut lock) = self.cache.write() {
            if lock.len() >= MAX_PRINCIPAL_CACHE_SIZE && !lock.contains_key(user_id) {
                let to_remove: Vec<String> = lock
                    .keys()
                    .take(MAX_PRINCIPAL_CACHE_SIZE / 5)
                    .cloned()
                    .collect();
                for k in to_remove {
                    lock.remove(&k);
                }
            }
            lock.insert(
                user_id.to_string(),
                Entry {
                    principal,
                    stored_at: Instant::now(),
                },
            );
        }
        Ok(())
    }

    async fn evict_user(&self, user_id: &str) -> Result<(), DomainError> {
        if let Ok(mut lock) = self.cache.write() {
            lock.remove(user_id);
        }
        Ok(())
    }

    async fn evict_by_role(&self, role_id: &str) -> Result<(), DomainError> {
        if let Ok(mut lock) = self.cache.write() {
            lock.retain(|_, v| !v.principal.roles.contains(role_id));
        }
        Ok(())
    }
}
