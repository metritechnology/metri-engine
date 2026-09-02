// iop/quota_step.rs — IopStep wrapper para QuotaGuard real.
//
// Responsabilidades:
//   1. Filtrar fast-path O(1) de operaciones no limitadas (UPDATE, DELETE, UPSERT).
//   2. Localizar la cuota vigente del tenant y el dominio con `QuotaResolver`.
//   3. Debitar la cuota de forma ATÓMICA contra `QuotaLedger`, que resuelve
//      techo e incremento en una sola escritura condicional de DynamoDB.
//   4. Dejar la reserva anotada en el contexto para que el orquestador pueda
//      devolverla si un paso posterior del pipeline falla.
//
// Este paso corre en CADA create y CADA get del motor, así que lo que cuesta
// aquí lo paga todo el mundo. Hasta la fase 3 pagaba además una
// `TransactWriteItems` completa sobre `domain_quota` para dejar una copia
// legible del contador; ahora ese número se calcula al leerlo
// (`quota/projection.rs`) y este camino se queda en una sola escritura
// condicional.
//
// Hasta la revisión de cuotas esto era un leer-modificar-escribir: se consultaba
// `current_usage`, se sumaba uno en memoria y se escribía el resultado absoluto.
// Dos altas simultáneas en el límite menos uno leían las dos el mismo valor y
// pasaban las dos. Ahora la decisión la toma DynamoDB dentro de la propia
// escritura; ver `quota/ledger.rs` para por qué el contador no puede vivir en el
// log de datoms.
//
// La resolución de «qué cuota aplica hoy» ya no vive aquí: estaba duplicada con
// `grpc/quota_service.rs` y las dos copias habían divergido. Ahora es
// `quota/resolver.rs`, y este archivo se ocupa solo de la política del pipeline
// —qué operaciones cuentan, quién queda exento, qué se devuelve al compensar—.

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
        match self.execute_inner(ctx).await {
            Ok(c) => Ok(c),
            Err(e) => {
                let span = tracing::Span::current();
                span.record("error.code", e.code.canonical_code());
                span.record("otel.status_code", "ERROR");
                Err(e)
            }
        }
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

        info!(
            tenant = %ctx.tenant_id,
            quota_id = %id,
            "[QuotaStep] Devolviendo la reserva — el pipeline no llegó a escribir"
        );

        if let Err(e) = self.counter.release(&ctx.tenant_id, id, 1).await {
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
            || crate::cedar::authorizer::SystemSecurityRules::is_quota_exempt(&ctx.entity_type)
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

        let limit_type = match op.as_str() {
            "CREATE" => "WRITE_COUNT",
            "GET" => "READ_COUNT",
            _ => unreachable!(),
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

                // Techo e incremento en una sola escritura condicional. La
                // lectura de arriba sirve para localizar la cuota y su límite;
                // NO para decidir. Decidir con ella era la carrera.
                let outcome = self
                    .counter
                    // Una unidad: aquí se cuenta por registro creado o leído.
                    .try_debit(&ctx.tenant_id, counter_id, 1, max_limit, current_usage)
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
                            "debit":  1,
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
