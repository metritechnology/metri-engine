//! Tests for `quota::fakes.rs`.
// quota/tests/fakes.rs — Dobles compartidos por los tests de cuota.
//
// El contador falso reproduce lo que hace DynamoDB en lo que importa para
// decidir: el techo, el arranque desde el `seed` mientras el item no existe, el
// suelo en cero y —lo que da sentido a toda la fase 2— la marca de idempotencia
// de `settle_once`.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::quota::{DebitOutcome, QuotaCounter, SettleOutcome};

/// Contador en memoria con las mismas garantías que el de DynamoDB.
#[derive(Default)]
pub struct FakeCounter {
    /// `None` mientras el contador no existe, igual que el item.
    usage: Mutex<HashMap<String, i64>>,
    /// Claves de idempotencia ya consumidas.
    applied: Mutex<HashSet<String>>,
    /// Cuando está puesto, toda operación falla con ese mensaje.
    down: Mutex<Option<String>>,
    /// Lecturas en lote realizadas. Sirve para comprobar que una página se
    /// resuelve con una llamada y no con una por fila.
    reads: Mutex<usize>,
}

impl FakeCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Contador que ya existe con este consumo.
    pub fn at(quota_id: &str, n: i64) -> Self {
        let c = Self::default();
        c.usage.lock().unwrap().insert(quota_id.to_string(), n);
        c
    }

    pub fn usage_of(&self, quota_id: &str) -> Option<i64> {
        self.usage.lock().unwrap().get(quota_id).copied()
    }

    pub fn break_it(&self, msg: &str) {
        *self.down.lock().unwrap() = Some(msg.to_string());
    }

    pub fn fix_it(&self) {
        *self.down.lock().unwrap() = None;
    }

    /// Cuántas claves de idempotencia se han consumido.
    pub fn applied_keys(&self) -> usize {
        self.applied.lock().unwrap().len()
    }

    /// Cuántas lecturas en lote se han pedido.
    pub fn read_calls(&self) -> usize {
        *self.reads.lock().unwrap()
    }

    fn check_down(&self) -> Result<(), DomainError> {
        match self.down.lock().unwrap().as_ref() {
            Some(msg) => Err(DomainError::new(ErrorCode::Infra001, msg.clone())),
            None => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl QuotaCounter for FakeCounter {
    async fn try_debit(
        &self,
        _tenant_id: &str,
        quota_id: &str,
        amount: i64,
        max_limit: i64,
        seed: i64,
    ) -> Result<DebitOutcome, DomainError> {
        self.check_down()?;
        let mut usage = self.usage.lock().unwrap();
        let actual = *usage.get(quota_id).unwrap_or(&seed);
        if actual >= max_limit {
            return Ok(DebitOutcome::Exhausted {
                current_usage: actual,
                limit: max_limit,
            });
        }
        usage.insert(quota_id.to_string(), actual + amount);
        Ok(DebitOutcome::Debited {
            new_usage: actual + amount,
        })
    }

    async fn settle(
        &self,
        _tenant_id: &str,
        quota_id: &str,
        delta: i64,
    ) -> Result<i64, DomainError> {
        self.check_down()?;
        let mut usage = self.usage.lock().unwrap();
        let nuevo = (usage.get(quota_id).copied().unwrap_or(0) + delta).max(0);
        usage.insert(quota_id.to_string(), nuevo);
        Ok(nuevo)
    }

    async fn read_many(
        &self,
        _tenant_id: &str,
        quota_ids: &[String],
    ) -> Result<HashMap<String, i64>, DomainError> {
        *self.reads.lock().unwrap() += 1;
        self.check_down()?;
        let usage = self.usage.lock().unwrap();
        Ok(quota_ids
            .iter()
            .filter_map(|id| usage.get(id).map(|n| (id.clone(), *n)))
            .collect())
    }

    async fn settle_once(
        &self,
        tenant_id: &str,
        quota_id: &str,
        delta: i64,
        idem_key: &str,
    ) -> Result<SettleOutcome, DomainError> {
        self.check_down()?;
        {
            let mut applied = self.applied.lock().unwrap();
            if !applied.insert(idem_key.to_string()) {
                return Ok(SettleOutcome::AlreadyApplied);
            }
        }
        self.settle(tenant_id, quota_id, delta).await?;
        Ok(SettleOutcome::Applied)
    }
}
