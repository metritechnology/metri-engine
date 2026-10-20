//! Bus y suscriptor de invalidación reactiva de cachés de autorización.
//!
//! Los mutadores (`bulk`, `transact`, `composite`) publican `InvalidationMsg` y este
//! suscriptor expulsa al principal de la caché — así como a los usuarios que lo referencian
//! por rol o grupo — y delega la expulsión de `EAV_CACHE` en su propio módulo.
//! Si el canal `broadcast` pierde mensajes (`Lagged`), registra la advertencia y continúa
//! consumiendo mientras el TTL de la caché acota cualquier obsolescencia residual.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tracing::info;

use crate::cedar::cache::principal::Entry;
use crate::cedar::types::InvalidationMsg;

/// Bus concreto sobre un canal broadcast de tokio.
pub struct BroadcastBus {
    tx: tokio::sync::broadcast::Sender<InvalidationMsg>,
}

impl BroadcastBus {
    /// Crea un nuevo bus de invalidación en memoria con la capacidad de cola especificada.
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = tokio::sync::broadcast::channel(capacity);
        BroadcastBus { tx }
    }
}

impl crate::cedar::ports::InvalidationBus for BroadcastBus {
    fn publish(&self, msg: InvalidationMsg) {
        // Sin suscriptores no hay nada que invalidar — no es un error.
        let _ = self.tx.send(msg);
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<InvalidationMsg> {
        self.tx.subscribe()
    }
}

/// Levanta el suscriptor de invalidación para UNA caché de principals. Se
/// llama explícitamente al construir `InMemoryPrincipalCache`, con el bus
/// inyectado por la raíz de composición.
pub(crate) fn spawn_invalidation_task(
    bus: Arc<dyn crate::cedar::ports::InvalidationBus>,
    cache: Arc<RwLock<HashMap<String, Entry>>>,
) {
    tokio::spawn(async move {
        let mut rx = bus.subscribe();
        loop {
            match rx.recv().await {
                Ok(msg) => evict(&cache, msg),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        "[CacheInvalidation] Lagged: {n} mensajes de invalidación perdidos — \
                         las entradas expiran por TTL y acotan la staleness"
                    );
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

fn evict(cache: &RwLock<HashMap<String, Entry>>, msg: InvalidationMsg) {
    // Evict from EAV_CACHE — vía la función de su propio módulo, sin candados cruzados.
    crate::eav::reader::pull::evict_cached_entity(&msg.tenant_id, &msg.entity_id);

    if let Ok(mut principal_lock) = cache.write() {
        match msg.entity_type.as_str() {
            "user" => {
                principal_lock.remove(&msg.entity_id);
                info!(
                    "[CacheInvalidation] Evicted User {} from Principal Cache",
                    msg.entity_id
                );
            }
            "role" => {
                let before_len = principal_lock.len();
                principal_lock.retain(|_, v| !v.principal.roles.contains(&msg.entity_id));
                let evicted_count = before_len - principal_lock.len();
                info!(
                    "[CacheInvalidation] Evicted {} users having Role {} from Principal Cache",
                    evicted_count, msg.entity_id
                );
            }
            "user_group" => {
                let before_len = principal_lock.len();
                principal_lock.retain(|_, v| !v.principal.groups.contains(&msg.entity_id));
                let evicted_count = before_len - principal_lock.len();
                info!(
                    "[CacheInvalidation] Evicted {} users in Group {} from Principal Cache",
                    evicted_count, msg.entity_id
                );
            }
            _ => {}
        }
    }
}
