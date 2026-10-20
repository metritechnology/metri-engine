//! Almacén de sesiones en memoria (`InMemorySessionStore`) exclusivo para pruebas y desarrollo.
//!
//! Nunca se instancia en producción (`server.rs` monta `HmacTokenStore`).
//! Respeta el contrato de sustitución de Liskov (`ISessionStore`): un `jti` revocado
//! mediante `revoke_session` deja de autenticar inmediatamente.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use async_trait::async_trait;

use crate::domain::errors::DomainError;
use crate::domain::protocols::{ISessionStore, Session};

/// Capacidad máxima de sesiones almacenadas en memoria antes de desalojar entradas antiguas.
pub const MAX_SESSION_CACHE_SIZE: usize = 10_000;

/// Implementación en memoria de `ISessionStore` con soporte de revocación por `jti` para pruebas.
pub struct InMemorySessionStore {
    sessions: RwLock<HashMap<String, Session>>,
    revoked: RwLock<HashSet<String>>,
}

impl Default for InMemorySessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemorySessionStore {
    /// Crea un almacén de sesiones en memoria vacío.
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            revoked: RwLock::new(HashSet::new()),
        }
    }

    /// Inserta o actualiza una sesión asociada al token indicado en el almacén en memoria.
    pub fn insert(&self, token: &str, session: Session) {
        if let Ok(mut lock) = self.sessions.write() {
            if lock.len() >= MAX_SESSION_CACHE_SIZE && !lock.contains_key(token) {
                let to_remove: Vec<String> = lock
                    .keys()
                    .take(MAX_SESSION_CACHE_SIZE / 5)
                    .cloned()
                    .collect();
                for k in to_remove {
                    lock.remove(&k);
                }
            }
            lock.insert(token.to_string(), session);
        }
    }
}

#[async_trait]
impl ISessionStore for InMemorySessionStore {
    async fn get_session(&self, token: &str) -> Result<Option<Session>, DomainError> {
        if let Ok(lock) = self.sessions.read() {
            if let Some(session) = lock.get(token) {
                let revoked = self
                    .revoked
                    .read()
                    .map(|r| r.contains(&session.jti))
                    .unwrap_or(false);
                return Ok(if revoked { None } else { Some(session.clone()) });
            }
        }
        Ok(None)
    }

    async fn revoke_session(&self, jti: &str, _ttl_seconds: u64) -> Result<(), DomainError> {
        if let Ok(mut lock) = self.revoked.write() {
            lock.insert(jti.to_string());
        }
        Ok(())
    }

    async fn unrevoke_session(&self, jti: &str) -> Result<(), DomainError> {
        if let Ok(mut lock) = self.revoked.write() {
            lock.remove(jti);
        }
        Ok(())
    }
}
