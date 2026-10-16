//! Atomic counter with ceiling, backed by MbfStorageClient (.mbf engine).
//!
//! Contador atómico con techo respaldado por MbfStorageClient (.mbf).

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{info, warn};

use crate::domain::errors::DomainError;
use crate::infrastructure::dynamodb::DynamoClient;

/// Qué pasó al intentar consumir una unidad de cuota.
#[derive(Debug, Clone, PartialEq)]
pub enum DebitOutcome {
    /// Debitado. `new_usage` es el valor autoritativo tras el incremento.
    Debited { new_usage: i64 },
    /// El techo lo impidió. `current_usage` es el valor real en el momento del
    /// rechazo, no la lectura previa de quien llamó.
    Exhausted { current_usage: i64, limit: i64 },
}

/// Qué pasó al aplicar un ajuste con clave de idempotencia.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleOutcome {
    /// El apunte se aplicó ahora.
    Applied,
    /// Ya estaba aplicado por esta misma clave. No se ha tocado el contador.
    AlreadyApplied,
}

/// Contador de cuota por tenant y periodo, atómico frente a concurrencia.
#[derive(Clone)]
pub struct QuotaLedger {
    ddb: Arc<DynamoClient>,
    table: String,
}

#[async_trait::async_trait]
pub trait QuotaCounter: Send + Sync {
    async fn try_debit(
        &self,
        tenant_id: &str,
        quota_id: &str,
        amount: i64,
        max_limit: i64,
        seed: i64,
    ) -> Result<DebitOutcome, DomainError>;

    async fn settle(&self, tenant_id: &str, quota_id: &str, delta: i64)
        -> Result<i64, DomainError>;

    async fn settle_once(
        &self,
        tenant_id: &str,
        quota_id: &str,
        delta: i64,
        idem_key: &str,
    ) -> Result<SettleOutcome, DomainError>;

    async fn read_many(
        &self,
        tenant_id: &str,
        quota_ids: &[String],
    ) -> Result<HashMap<String, i64>, DomainError>;

    async fn release(
        &self,
        tenant_id: &str,
        quota_id: &str,
        amount: i64,
    ) -> Result<(), DomainError> {
        self.settle(tenant_id, quota_id, -amount).await.map(|_| ())
    }
}

impl QuotaLedger {
    pub fn new(ddb: Arc<DynamoClient>, table: impl Into<String>) -> Self {
        QuotaLedger {
            ddb,
            table: table.into(),
        }
    }
}

#[async_trait::async_trait]
impl QuotaCounter for QuotaLedger {
    async fn read_many(
        &self,
        tenant_id: &str,
        quota_ids: &[String],
    ) -> Result<HashMap<String, i64>, DomainError> {
        self.ddb
            .quota_read_many(&self.table, tenant_id, quota_ids)
            .await
    }

    async fn try_debit(
        &self,
        tenant_id: &str,
        quota_id: &str,
        amount: i64,
        max_limit: i64,
        seed: i64,
    ) -> Result<DebitOutcome, DomainError> {
        let (debited, usage) = self
            .ddb
            .quota_try_debit(&self.table, tenant_id, quota_id, amount, max_limit, seed)
            .await?;
        if debited {
            info!(
                tenant = %tenant_id, quota = %quota_id, amount, new_usage = usage,
                "[QuotaLedger] Débito atómico confirmado"
            );
            Ok(DebitOutcome::Debited { new_usage: usage })
        } else {
            warn!(
                tenant = %tenant_id, quota = %quota_id, current_usage = usage, limit = max_limit,
                "[QuotaLedger] Débito rechazado por el techo"
            );
            Ok(DebitOutcome::Exhausted {
                current_usage: usage,
                limit: max_limit,
            })
        }
    }

    async fn settle(
        &self,
        tenant_id: &str,
        quota_id: &str,
        delta: i64,
    ) -> Result<i64, DomainError> {
        let new_usage = self
            .ddb
            .quota_settle(&self.table, tenant_id, quota_id, delta)
            .await?;
        info!(tenant = %tenant_id, quota = %quota_id, delta, new_usage, "[QuotaLedger] Ajuste aplicado");
        Ok(new_usage)
    }

    async fn settle_once(
        &self,
        tenant_id: &str,
        quota_id: &str,
        delta: i64,
        idem_key: &str,
    ) -> Result<SettleOutcome, DomainError> {
        let applied = self
            .ddb
            .quota_settle_once(&self.table, tenant_id, quota_id, delta, idem_key)
            .await?;
        if applied {
            info!(
                tenant = %tenant_id, quota = %quota_id, delta, idem = %idem_key,
                "[QuotaLedger] Apunte idempotente aplicado"
            );
            Ok(SettleOutcome::Applied)
        } else {
            info!(
                tenant = %tenant_id, quota = %quota_id, idem = %idem_key,
                "[QuotaLedger] Apunte ya aplicado por esta clave — no se repite"
            );
            Ok(SettleOutcome::AlreadyApplied)
        }
    }
}

#[cfg(test)]
#[path = "tests/ledger_tests.rs"]
mod tests;
