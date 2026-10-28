//! Paso 2 del pipeline IOP (`QuotaGuardStep`) — verificación rápida y débito atómico de cuota.
//!
//! Responsabilidades:
//! 1. Filtrar en `O(1)` las operaciones exentas de límite (`UPDATE`, `DELETE`, `UPSERT`).
//! 2. Localizar la cuota vigente del tenant y dominio mediante `QuotaResolver`.
//! 3. Debitar la cuota de forma atómica contra `QuotaLedger` (`QuotaCounter`), que evalúa
//!    techo e incremento en una sola operación condicional.
//! 4. Anotar la reserva en `IopContext::quota_reservation` para que el orquestador pueda
//!    compensarla (`compensate`) si un paso posterior del pipeline falla.

use serde_json::json;
use tracing::{error, info, warn};

use crate::aegis::oltp::executor::OltpExecutor;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::iop::core::{IopContext, IopStep};
use crate::quota::{DebitOutcome, OltpQueryRunner, QuotaCounter, QuotaLedger, QuotaResolver};

/// Wrapper IopStep para el control de cuotas por tenant (Paso 2).
pub struct QuotaGuardStep<E = OltpExecutor, C = QuotaLedger> {
    /// Localiza la cuota vigente. Es una LECTURA: dice cuál es la cuota y cuál
    /// su techo, nunca si queda sitio.
    resolver: QuotaResolver<E>,
    /// Quien decide si hay sitio, y el único sitio donde se apunta.
    counter: C,
}

impl<E, C> QuotaGuardStep<E, C>
where
    E: OltpQueryRunner,
    C: QuotaCounter,
{
    /// Construye un nuevo `QuotaGuardStep` con el ejecutor de consultas OLTP y el contador atómico.
    pub fn new(oltp_executor: E, counter: C) -> Self {
        info!("[QuotaStep] Inicializando QuotaGuardStep con débito atómico sobre el Ledger");
        Self {
            resolver: QuotaResolver::new(oltp_executor),
            counter,
        }
    }
}

#[async_trait::async_trait]
impl<E, C> IopStep for QuotaGuardStep<E, C>
where
    E: OltpQueryRunner + Send + Sync,
    C: QuotaCounter + Send + Sync,
{
    /// Verifica la cuota del tenant para la operación de forma transaccional.
    /// Operaciones monitoreadas: CREATE (WRITE_COUNT), GET (READ_COUNT).
    /// UPDATE, DELETE, UPSERT → pass-through inmediato sin llamadas a base de datos.
    #[tracing::instrument(
        name = "iop.step2.quota.start",
        skip(self, ctx),
        fields(
            tenant_id = %ctx.tenant_id,
            error.code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty
        )
    )]
    async fn execute(&self, ctx: IopContext) -> Result<IopContext, DomainError> {
        crate::iop::core::record_step_span_error(self.execute_inner(ctx).await)
    }

    /// Devuelve la unidad debitada cuando el pipeline no llegó a escribir.
    ///
    /// El débito ocurre en el paso 2 y la escritura de verdad en el 3. Entre
    /// ambos cabe un fallo, y sin esto el contador se quedaba arriba: un tenant
    /// con errores intermitentes de escritura agotaba su plan sin haber creado
    /// nada, y el contador solo bajaba al cambiar de periodo.
    async fn compensate(&self, ctx: &IopContext) {
        let Some(reservation) = &ctx.quota_reservation else {
            return;
        };

        let Some(id) = reservation.get("id").and_then(|v| v.as_str()) else {
            error!(
                tenant = %ctx.tenant_id,
                reserva = %reservation,
                "[QuotaStep] Reserva sin id — no se puede devolver, el contador queda alto"
            );
            return;
        };

        let debit_units = reservation
            .get("debit")
            .and_then(|v| v.as_i64())
            .filter(|&n| n > 0)
            .unwrap_or(1);

        info!(
            tenant = %ctx.tenant_id,
            quota_id = %id,
            units = debit_units,
            "[QuotaStep] Devolviendo la reserva — el pipeline no llegó a escribir"
        );

        if let Err(e) = self.counter.release(&ctx.tenant_id, id, debit_units).await {
            // No se propaga: quien está delante necesita leer el error que
            // tumbó su operación, no el de la limpieza. Pero esto deja el
            // contador un punto por encima de la realidad, y eso sí merece
            // nivel de error para que alguien lo vea.
            error!(
                tenant = %ctx.tenant_id,
                quota_id = %id,
                error = ?e,
                "[QuotaStep] NO se pudo devolver la reserva — el contador queda alto"
            );
        }
    }
}

impl<E, C> QuotaGuardStep<E, C>
where
    E: OltpQueryRunner + Send + Sync,
    C: QuotaCounter + Send + Sync,
{
    async fn execute_inner(&self, mut ctx: IopContext) -> Result<IopContext, DomainError> {
        let op = ctx.operation.to_uppercase();

        // ── Fast-path: master tenant, cuenta del sistema o recurso ilimitado → bypass de cuota ──
        if ctx.is_super_master
            || ctx.user_id == "usr_system_bff"
            || ctx.user_id == "usr_master"
            || crate::cedar::SystemSecurityRules::is_quota_exempt(&ctx.entity_type)
        {
            info!(
                tenant = %ctx.tenant_id,
                user = %ctx.user_id,
                entity = %ctx.entity_type,
                "[QuotaStep] Bypass de cuota (master, cuenta del sistema o recurso ilimitado)"
            );
            return Ok(ctx);
        }

        // ── Fast-path: operación no monitoreada → pass-through O(1) ──────────
        if !matches!(op.as_str(), "CREATE" | "GET") {
            info!(
                tenant    = %ctx.tenant_id,
                operation = %op,
                "[QuotaStep] Pass-through O(1) — sin afectación de cuota"
            );
            return Ok(ctx);
        }

        // El fast-path de arriba ya filtró el resto de operaciones; este
        // brazo es defensivo: pass-through en vez de pánico (R3).
        let limit_type = match op.as_str() {
            "CREATE" => "WRITE_COUNT",
            "GET" => "READ_COUNT",
            _ => return Ok(ctx),
        };

        let domain = &ctx.entity_type;

        // ── Localizar la cuota vigente de este Tenant y Dominio ───────────────
        info!(
            tenant = %ctx.tenant_id,
            domain = %domain,
            limit_type = %limit_type,
            "[QuotaStep] Consultando cuotas en el Ledger EAV nativo"
        );

        let active_quota = self
            .resolver
            .active_quota(&ctx.tenant_id, domain, limit_type)
            .await?;

        match active_quota {
            None => {
                warn!(
                    tenant = %ctx.tenant_id,
                    domain = %domain,
                    limit_type = %limit_type,
                    "[QuotaStep] Cuota no configurada para el periodo actual"
                );
                Err(DomainError::new(
                    ErrorCode::Quota001,
                    format!(
                        "No quota configured for tenant={} resource_domain={} limit_type={}",
                        ctx.tenant_id, domain, limit_type
                    ),
                )
                .with_stage("quota"))
            }
            Some(quota) => {
                let id = quota.id.as_str();
                // Contra qué contador se apunta. Casi siempre es el id de la
                // fila; cuando el ciclo se renovó solo, o cuando gobierna la
                // cuota por defecto del tenant, es un contador derivado — ver
                // `quota/resolver.rs::QuotaSpec::counter_id`.
                let counter_id = quota.counter_id.as_str();
                let max_limit = quota.max_limit;
                let current_usage = quota.seed_usage;
                let period_key = quota.period_key.as_str();

                let debit_units = ctx
                    .request
                    .get("data")
                    .and_then(|d| d.as_array())
                    .map(|a| a.len() as i64)
                    .filter(|&n| n > 0)
                    .unwrap_or(1);

                // Techo e incremento en una sola escritura condicional. La
                // lectura de arriba sirve para localizar la cuota y su límite;
                // NO para decidir. Decidir con ella era la carrera.
                let outcome = self
                    .counter
                    // Se debita por cantidad de registros creados o leídos (1 en Transact, N en BulkIngest).
                    .try_debit(
                        &ctx.tenant_id,
                        counter_id,
                        debit_units,
                        max_limit,
                        current_usage,
                    )
                    .await?;

                match outcome {
                    DebitOutcome::Exhausted {
                        current_usage,
                        limit,
                    } => {
                        warn!(
                            tenant = %ctx.tenant_id,
                            domain = %domain,
                            limit = limit,
                            current = current_usage,
                            "[QuotaStep] Cuota agotada para el tenant"
                        );

                        let error = DomainError::new(
                            ErrorCode::Quota001,
                            "Quota exhausted — upgrade plan or wait for reset",
                        )
                        .with_stage("quota")
                        // Este contexto llega al cliente por `Status.error_context`,
                        // que es lo que le permite decir «48 de 50» en vez de
                        // «error al guardar».
                        .with_context(json!({
                            "limit": limit,
                            "current_usage": current_usage,
                            "resource_domain": domain,
                            "period_key": period_key
                        }));

                        Err(error)
                    }
                    DebitOutcome::Debited { new_usage } => {
                        // Aquí ya no se escribe nada más. El consumo vive en el
                        // contador y `domain_quota.current_usage` se calcula al
                        // leerlo: ver `quota/projection.rs`.

                        // «pending», no «confirmed»: el débito solo queda firme
                        // cuando el pipeline entero termina bien. Quien lo
                        // confirma o lo devuelve es el orquestador, en
                        // `iop/core.rs`.
                        ctx.quota_reservation = Some(json!({
                            // `id` es contra qué se debitó, porque es lo que hay
                            // que devolver si el pipeline no llega a escribir.
                            // `quota_id` es la fila de configuración, para poder
                            // leer la traza.
                            "id":       counter_id,
                            "quota_id": id,
                            "debit":  debit_units,
                            "domain": domain.clone(),
                            "period_key": period_key,
                            "new_usage": new_usage,
                            "status": "pending"
                        }));

                        Ok(ctx)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/quota_step_tests.rs"]
mod tests;
