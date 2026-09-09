//! In-memory session store — tests and development only.
//!
//! Session store en memoria.
//!
//! ÚNICAMENTE para tests y desarrollo: nunca se construye en producción
//! (server.rs monta `HmacTokenStore`). A diferencia de antes, `revoke_session`
//! respeta el contrato del trait (LSP): un jti revocado ya no autentica.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use async_trait::async_trait;

use crate::domain::errors::DomainError;
use crate::domain::protocols::{ISessionStore, Session};

pub const MAX_SESSION_CACHE_SIZE: usize = 10_000;

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
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            revoked: RwLock::new(HashSet::new()),
        }
    }

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
