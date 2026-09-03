// grpc/quota_service.rs — Implementación de gRPC para QuotaService.
//
// Responsabilidades:
//   1. Ofrecer endpoints atómicos para que el MCP Proxy reserve y concilie tokens.
//   2. Apoyarse en `quota::QuotaResolver` para localizar la cuota vigente y en
//      `quota::QuotaLedger` para aplicarla.
//   3. Guardar cada reserva en un store compartido, de modo que la concilie la
//      réplica que la abrió o cualquier otra.
//
// LO QUE CAMBIÓ Y POR QUÉ
// ───────────────────────
// Hasta la fase 2 las reservas vivían en un `HashMap` de este proceso y el
// `reservation_id` llevaba dentro, sin firma, las cifras con las que luego se
// conciliaba. Eso significaba tres cosas:
//
//   · Una conciliación que aterrizaba en otra réplica no encontraba el ticket,
//     asumía que ya había expirado y cargaba el consumo real — mientras la
//     réplica dueña reintegraba además la estimación entera. Se devolvía de más.
//   · Un reinicio se llevaba las reservas en vuelo y su débito quedaba aplicado
//     para siempre. Cada despliegue filtraba cuota.
//   · Cualquier cliente podía fabricar un ticket y aplicar apuntes sobre la
//     cuota de otro tenant.
//
// Ahora el ticket es un ULID opaco, lo que se concilia sale del item guardado, y
// el tenant del item se contrasta con el de la sesión autenticada.

use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

use super::pb::quota_service_server::QuotaService;
use super::pb::{
    ReconcileTokensRequest, ReconcileTokensResponse, ReserveTokensRequest, ReserveTokensResponse,
    Status as PbStatus,
};
use crate::aegis::oltp::executor::OltpExecutor;
use crate::eav::writer::EavWriter;
use crate::grpc::interceptors::AuthenticatedSession;
use crate::quota::reservations::late_charge_key;
use crate::quota::{
    settlement_key, split_wire_id, wire_id, ClaimResult, CloseReason, DebitOutcome,
    OltpQueryRunner, QuotaCounter, QuotaResolver, Reservation, ReservationStore,
};

/// Cuánto vive una reserva antes de que el barrido la recoja.
///
/// Configurable porque el valor correcto depende del modelo: una generación
/// larga puede pasar de 90 s, y entonces la reserva se devuelve antes de que el
/// cliente concilie. La cuenta acaba cuadrando —el cargo tardío la corrige—,
/// pero durante esa ventana el techo no protegió.
const DEFAULT_RESERVATION_TTL_SECS: i64 = 90;

/// Cuánto se reserva una conciliación el derecho a cerrar el ticket.
const RECONCILE_LEASE: Duration = Duration::from_secs(30);

/// Cuentas de servicio que pueden operar sobre cuotas de otros tenants.
/// Las mismas que ya saltan el control de cuota en `iop/quota_step.rs`.
const SYSTEM_USERS: [&str; 2] = ["usr_system_bff", "usr_master"];

pub struct QuotaServiceImpl<E = OltpExecutor> {
    /// Localiza la cuota vigente del modelo. La misma pieza que usa el paso IOP.
    resolver: QuotaResolver<E>,
    /// Donde vive el consumo. Es el único sitio: `domain_quota.current_usage`
    /// se calcula al leerlo, ver `quota/projection.rs`.
    counter: Arc<dyn QuotaCounter>,
    /// Dónde viven las reservas en vuelo. Compartido entre réplicas.
    store: Arc<dyn ReservationStore>,
    reservation_ttl: i64,
}

impl QuotaServiceImpl<OltpExecutor> {
    pub fn new(
        oltp_executor: OltpExecutor,
        eav_writer: EavWriter,
        store: Arc<dyn ReservationStore>,
    ) -> Self {
        let counter: Arc<dyn QuotaCounter> = Arc::new(eav_writer.quota_ledger());

        let reservation_ttl = std::env::var("QUOTA_RESERVATION_TTL_SECS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_RESERVATION_TTL_SECS);

        info!(
            ttl_reserva_s = reservation_ttl,
            "[QuotaService] Inicializando QuotaServiceImpl con reservas distribuidas"
        );

        // El barrido sustituye al recolector que recorría el mapa local. Todas
        // las réplicas lo ejecutan; el `claim` decide cuál cierra cada reserva.
        crate::quota::sweeper::spawn(Arc::clone(&store), Arc::clone(&counter));

        Self {
            resolver: QuotaResolver::new(oltp_executor),
            counter,
            store,
            reservation_ttl,
        }
    }
}

impl<E: OltpQueryRunner> QuotaServiceImpl<E> {
    /// Composición explícita, para poder ejercitar el servicio con dobles.
    pub fn with_parts(
        oltp_executor: E,
        counter: Arc<dyn QuotaCounter>,
        store: Arc<dyn ReservationStore>,
        reservation_ttl: i64,
    ) -> Self {
        Self {
            resolver: QuotaResolver::new(oltp_executor),
            counter,
            store,
            reservation_ttl,
        }
    }

    /// La sesión que el interceptor dejó en la petición.
    ///
    /// Fail-closed, igual que el resto de servicios: sin sesión no se sirve.
    fn session<T>(request: &Request<T>) -> Result<AuthenticatedSession, Status> {
        request
            .extensions()
            .get::<AuthenticatedSession>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Petición no autenticada [Fail-Closed]"))
    }

    /// ¿Puede esta sesión tocar la cuota de `target_tenant`?
    ///
    /// Antes no se comprobaba: el tenant salía del cuerpo de la petición en la
    /// reserva y del propio ticket en la conciliación, así que un cliente
    /// autenticado podía apuntar consumo contra la cuota de cualquier otro.
    ///
    /// Las cuentas de sistema y el tenant maestro siguen pudiendo operar en
    /// nombre de otros: es como llama hoy el BFF. Si el proxy de IA resultara
    /// usar una tercera forma, `QUOTA_TENANT_MISMATCH=warn` deja pasar la
    /// petición dejando traza, para poder verlo en producción antes de cerrar.
    fn authorize(session: &AuthenticatedSession, target_tenant: &str) -> Result<(), Status> {
        if session.tenant_id == target_tenant
            || crate::cedar::is_master_tenant(&session.tenant_id)
            || SYSTEM_USERS.contains(&session.user_id.as_str())
        {
            return Ok(());
        }

        if std::env::var("QUOTA_TENANT_MISMATCH").unwrap_or_default() == "warn" {
            warn!(
                sesion_tenant = %session.tenant_id,
                objetivo = %target_tenant,
                usuario = %session.user_id,
                "[QuotaService] Cuota de otro tenant — se deja pasar por QUOTA_TENANT_MISMATCH=warn"
            );
            return Ok(());
        }

        warn!(
            sesion_tenant = %session.tenant_id,
            objetivo = %target_tenant,
            usuario = %session.user_id,
            "[QuotaService] Rechazada: la sesión no puede operar sobre la cuota de otro tenant"
        );
        Err(Status::permission_denied(
            "La sesión no puede operar sobre la cuota de otro tenant",
        ))
    }

    fn ok_status() -> Option<PbStatus> {
        Some(PbStatus {
            success: true,
            error_code: String::new(),
            error_message: String::new(),
            error_context: None,
        })
    }

    fn rejected(
        code: &str,
        message: String,
        context: Option<serde_json::Value>,
    ) -> ReserveTokensResponse {
        ReserveTokensResponse {
            status: Some(PbStatus {
                success: false,
                error_code: code.to_string(),
                error_message: message,
                error_context: context.map(|v| crate::grpc::translator::value_to_struct(&v)),
            }),
            reservation_id: String::new(),
            allowed_tokens: 0,
        }
    }
}

#[tonic::async_trait]
impl<E> QuotaService for QuotaServiceImpl<E>
where
    E: OltpQueryRunner + Send + Sync + 'static,
{
    /// Paso 1: Reserva de tokens pre-flight (optimista)
    #[tracing::instrument(name = "grpc.quota.reserve", skip(self, request))]
    async fn reserve_tokens(
        &self,
        request: Request<ReserveTokensRequest>,
    ) -> Result<Response<ReserveTokensResponse>, Status> {
        let session = Self::session(&request)?;
        let req = request.into_inner();
        Self::authorize(&session, &req.tenant_id)?;

        info!(
            tenant_id = %req.tenant_id,
            model_urn = %req.model_urn,
            estimated = req.estimated_tokens,
            "[QuotaService] Recibida solicitud de reserva de tokens"
        );

        // Una estimación negativa debitaría en negativo, es decir, regalaría
        // cuota. El valor lo pone el cliente, así que se comprueba aquí.
        if req.estimated_tokens <= 0 {
            return Err(Status::invalid_argument(
                "estimated_tokens debe ser mayor que cero",
            ));
        }

        // ── 1. Cuota vigente ─────────────────────────────────────────────────
        let quota = match self
            .resolver
            .active_quota(&req.tenant_id, &req.model_urn, "TOKEN_COUNT")
            .await
        {
            Ok(Some(q)) => q,
            Ok(None) => {
                warn!(
                    tenant_id = %req.tenant_id,
                    model_urn = %req.model_urn,
                    "[QuotaService] Cuota no configurada para el periodo actual"
                );
                return Ok(Response::new(Self::rejected(
                    "QUOTA_NOT_CONFIGURED",
                    format!(
                        "No quota configured for tenant={} model={}",
                        req.tenant_id, req.model_urn
                    ),
                    None,
                )));
            }
            Err(e) => {
                error!("[QuotaService] Falló consulta EAV de cuotas: {:?}", e);
                return Err(Status::internal(format!("Error consultando cuota: {e}")));
            }
        };

        // ── 2. Abrir el ticket ANTES de debitar ──────────────────────────────
        //
        // Este orden es lo que impide que una muerte a mitad deje tokens
        // debitados sin nadie que los reclame: si el proceso cae aquí, el
        // barrido encuentra una reserva con `debited = false` y la cierra sin
        // tocar el contador.
        let id = ulid::Ulid::new().to_string();
        let reservation = Reservation {
            id: id.clone(),
            tenant_id: req.tenant_id.clone(),
            quota_id: quota.id.clone(),
            estimated: req.estimated_tokens,
            debited: false,
            expires_at: chrono::Utc::now().timestamp() + self.reservation_ttl,
        };

        if let Err(e) = self.store.open(&reservation).await {
            error!("[QuotaService] No se pudo registrar la reserva: {:?}", e);
            return Err(Status::internal(format!("Error abriendo la reserva: {e}")));
        }

        // ── 3. Techo y débito, en una sola escritura condicional ─────────────
        let outcome = match self
            .counter
            .try_debit(
                &req.tenant_id,
                &quota.id,
                req.estimated_tokens,
                quota.max_limit,
                quota.seed_usage,
            )
            .await
        {
            Ok(o) => o,
            Err(e) => {
                error!("[QuotaService] Falló el débito de tokens: {:?}", e);
                // La reserva se queda abierta con `debited = false`. Si el
                // débito no llegó a aplicarse, el barrido la cierra sin tocar
                // nada; si sí llegó y lo que falló fue la respuesta, se pierde
                // esa estimación hasta el cambio de periodo. Cerrar esa ventana
                // del todo exige que el propio débito lleve clave, y eso es otro
                // cambio.
                return Err(Status::internal(format!("Fallo escribiendo débito: {e}")));
            }
        };

        let new_usage = match outcome {
            DebitOutcome::Exhausted {
                current_usage,
                limit,
            } => {
                warn!(
                    tenant_id = %req.tenant_id,
                    current = current_usage,
                    max = limit,
                    "[QuotaService] Cuota de tokens agotada"
                );
                // El ticket nunca llegó a valer nada: fuera del índice de
                // barrido, para que nadie intente devolver un débito que no se
                // aplicó.
                if let Err(e) = self
                    .store
                    .close(&req.tenant_id, &id, CloseReason::Rejected)
                    .await
                {
                    warn!(reserva = %id, error = ?e, "[QuotaService] Reserva rechazada no cerrada — la cerrará el barrido");
                }
                return Ok(Response::new(Self::rejected(
                    "QUOTA_EXHAUSTED",
                    "Token quota has been fully exhausted".to_string(),
                    // Las cifras son las del contador en el momento del
                    // rechazo, no las de la lectura previa.
                    Some(json!({
                        "limit": limit,
                        "current_usage": current_usage,
                        "resource_domain": req.model_urn,
                    })),
                )));
            }
            DebitOutcome::Debited { new_usage } => new_usage,
        };

        // ── 4. Dejar constancia de que el débito se aplicó ───────────────────
        //
        // Sin esta marca el barrido creería que no hay nada que devolver. Como
        // aquí SÍ sabemos que el débito entró, si la marca falla lo deshacemos
        // en el acto en vez de dejar que alguien lo adivine luego.
        if let Err(e) = self.store.mark_debited(&req.tenant_id, &id).await {
            error!(
                reserva = %id, error = ?e,
                "[QuotaService] No se pudo marcar el débito — se deshace la reserva"
            );
            if let Err(e) = self
                .counter
                .settle_once(
                    &req.tenant_id,
                    &quota.id,
                    -req.estimated_tokens,
                    &settlement_key(&id),
                )
                .await
            {
                error!(
                    reserva = %id, cuota = %quota.id, error = ?e,
                    "[QuotaService] Tampoco se pudo devolver el débito — el contador queda alto"
                );
            }
            let _ = self
                .store
                .close(&req.tenant_id, &id, CloseReason::Rejected)
                .await;
            return Err(Status::internal(
                "No se pudo registrar la reserva de tokens",
            ));
        }

        info!(
            tenant_id = %req.tenant_id,
            quota_id = %quota.id,
            reserva = %id,
            new_usage = new_usage,
            "[QuotaService] Débito atómico de tokens aplicado"
        );

        Ok(Response::new(ReserveTokensResponse {
            status: Self::ok_status(),
            reservation_id: wire_id(&req.tenant_id, &id),
            allowed_tokens: req.estimated_tokens,
        }))
    }

    /// Paso 4: Conciliación post-flight
    #[tracing::instrument(name = "grpc.quota.reconcile", skip(self, request))]
    async fn reconcile_tokens(
        &self,
        request: Request<ReconcileTokensRequest>,
    ) -> Result<Response<ReconcileTokensResponse>, Status> {
        let session = Self::session(&request)?;
        let req = request.into_inner();

        info!(
            res_id = %req.reservation_id,
            input = req.actual_input_tokens,
            output = req.actual_output_tokens,
            "[QuotaService] Recibida solicitud de conciliación"
        );

        // Un consumo negativo devolvería cuota. Lo pone el cliente.
        if req.actual_input_tokens < 0 || req.actual_output_tokens < 0 {
            return Err(Status::invalid_argument(
                "Los tokens consumidos no pueden ser negativos",
            ));
        }
        let actual_consumed = req.actual_input_tokens + req.actual_output_tokens;

        // ── 1. Localizar el ticket ───────────────────────────────────────────
        let Some((tenant_hint, id)) = split_wire_id(&req.reservation_id) else {
            // Formato de cuatro partes: lo emitió una versión anterior, que
            // guardaba las reservas en memoria. Su dueño —si sigue vivo— las
            // devolverá al vencer, así que aquí solo se apunta lo consumido.
            return self
                .reconcile_legacy(&session, &req.reservation_id, actual_consumed)
                .await;
        };
        Self::authorize(&session, tenant_hint)?;

        let claim = self
            .store
            .claim(tenant_hint, id, RECONCILE_LEASE)
            .await
            .map_err(|e| {
                error!("[QuotaService] Falló el claim de la reserva: {:?}", e);
                Status::internal(format!("Error leyendo la reserva: {e}"))
            })?;

        let reservation = match claim {
            ClaimResult::Claimed(r) => r,

            // Ya se cerró. Qué hacer depende de por qué, y por eso el store lo
            // guarda: dar la misma respuesta a los cuatro casos era justo lo
            // que producía la doble devolución.
            ClaimResult::Closed(r, reason) => {
                return self.reconcile_late(r, reason, actual_consumed).await;
            }

            // Otro la está cerrando ahora mismo. En un instante estará cerrada
            // y se podrá decidir con certeza; ahora no.
            ClaimResult::Leased(r) => {
                warn!(reserva = %r.id, "[QuotaService] Reserva en curso — se pide reintento");
                return Err(Status::aborted(
                    "La reserva se está conciliando en este momento; reintenta",
                ));
            }

            ClaimResult::NotFound => {
                warn!(res_id = %req.reservation_id, "[QuotaService] Reserva inexistente");
                return Err(Status::not_found(format!(
                    "No existe la reserva {}",
                    req.reservation_id
                )));
            }
        };

        // El tenant que manda es el del item, no el del ticket.
        Self::authorize(&session, &reservation.tenant_id)?;

        // ── 2. La diferencia entre lo apuntado y lo gastado ──────────────────
        //
        // Positivo carga, negativo devuelve. Si el débito nunca llegó a
        // aplicarse solo se apunta el consumo real: no hay estimación que
        // corregir.
        let delta = if reservation.debited {
            actual_consumed - reservation.estimated
        } else {
            actual_consumed
        };

        if let Err(e) = self
            .counter
            .settle_once(
                &reservation.tenant_id,
                &reservation.quota_id,
                delta,
                &settlement_key(&reservation.id),
            )
            .await
        {
            error!("[QuotaService] Fallo en la conciliación: {:?}", e);
            // Sin cerrar: al vencer el lease, el barrido la recoge. La clave de
            // idempotencia hace que reintentarlo no cuente dos veces.
            return Err(Status::internal(format!(
                "Fallo escribiendo conciliación: {e}"
            )));
        }

        if let Err(e) = self
            .store
            .close(
                &reservation.tenant_id,
                &reservation.id,
                CloseReason::Settled,
            )
            .await
        {
            // El apunte ya está hecho y marcado. Que el cierre falle solo
            // significa que el barrido la verá vencida y encontrará la marca.
            warn!(reserva = %reservation.id, error = ?e, "[QuotaService] Conciliada pero no cerrada");
        }

        let tokens_returned = (reservation.estimated - actual_consumed).max(0);
        info!(
            quota_id = %reservation.quota_id,
            reserva = %reservation.id,
            delta = delta,
            "[QuotaService] Uso final conciliado"
        );

        Ok(Response::new(ReconcileTokensResponse {
            status: Self::ok_status(),
            tokens_consumed: actual_consumed,
            tokens_returned: if reservation.debited {
                tokens_returned
            } else {
                0
            },
        }))
    }
}

impl<E: OltpQueryRunner> QuotaServiceImpl<E> {
    /// Conciliación de una reserva que ya estaba cerrada.
    async fn reconcile_late(
        &self,
        r: Reservation,
        reason: CloseReason,
        actual_consumed: i64,
    ) -> Result<Response<ReconcileTokensResponse>, Status> {
        match reason {
            // El barrido ya devolvió la estimación entera porque venció. Lo
            // gastado de verdad sigue sin apuntarse, y este es el momento.
            // Clave propia: el apunte de cierre ya se consumió.
            CloseReason::Expired => {
                warn!(
                    reserva = %r.id, consumido = actual_consumed,
                    "[QuotaService] Conciliación tardía — la reserva ya había vencido; se apunta lo consumido"
                );
                if actual_consumed > 0 {
                    if let Err(e) = self
                        .counter
                        .settle_once(
                            &r.tenant_id,
                            &r.quota_id,
                            actual_consumed,
                            &late_charge_key(&r.id),
                        )
                        .await
                    {
                        error!("[QuotaService] Falló el cargo tardío: {:?}", e);
                        return Err(Status::internal(format!("Fallo apuntando el consumo: {e}")));
                    }
                }
                Ok(Response::new(ReconcileTokensResponse {
                    status: Self::ok_status(),
                    tokens_consumed: actual_consumed,
                    tokens_returned: r.estimated,
                }))
            }

            // Ya se concilió. Repetir la petición no debe cambiar nada, así que
            // se responde lo mismo en vez de un error: es un reintento, no un
            // fallo del cliente.
            CloseReason::Settled => {
                info!(reserva = %r.id, "[QuotaService] Conciliación repetida — sin efecto");
                Ok(Response::new(ReconcileTokensResponse {
                    status: Self::ok_status(),
                    tokens_consumed: actual_consumed,
                    tokens_returned: (r.estimated - actual_consumed).max(0),
                }))
            }

            // Nunca hubo débito que corregir: o el techo la rechazó, o venció
            // antes de aplicarse. Apuntar aquí el consumo sería cobrar por una
            // reserva que el sistema negó.
            CloseReason::Rejected | CloseReason::Abandoned => {
                warn!(
                    reserva = %r.id, razon = reason.as_str(),
                    "[QuotaService] Conciliación de una reserva que nunca llegó a debitar"
                );
                Err(Status::failed_precondition(format!(
                    "La reserva {} no llegó a aplicarse ({})",
                    r.id,
                    reason.as_str()
                )))
            }
        }
    }

    /// Conciliación de un ticket con el formato anterior al store compartido.
    ///
    /// Provisional: en cuanto pase un despliegue completo no puede quedar
    /// ninguno vivo —duraban 90 s— y esta función se retira.
    async fn reconcile_legacy(
        &self,
        session: &AuthenticatedSession,
        wire: &str,
        actual_consumed: i64,
    ) -> Result<Response<ReconcileTokensResponse>, Status> {
        let parts: Vec<&str> = wire.split(':').collect();
        if parts.len() != 4 {
            return Err(Status::invalid_argument(
                "Formato de reservation_id inválido",
            ));
        }
        let quota_id = parts[0];
        let tenant_id = parts[2];
        Self::authorize(session, tenant_id)?;

        warn!(
            res_id = %wire,
            "[QuotaService] Ticket con el formato anterior — se apunta el consumo y \
             la réplica que lo emitió devolverá la estimación al vencer"
        );

        if actual_consumed > 0 {
            if let Err(e) = self
                .counter
                .settle_once(tenant_id, quota_id, actual_consumed, &late_charge_key(wire))
                .await
            {
                error!("[QuotaService] Falló la conciliación heredada: {:?}", e);
                return Err(Status::internal(format!(
                    "Fallo escribiendo conciliación: {e}"
                )));
            }
        }

        Ok(Response::new(ReconcileTokensResponse {
            status: Self::ok_status(),
            tokens_consumed: actual_consumed,
            tokens_returned: 0,
        }))
    }
}

#[cfg(test)]
#[path = "tests/quota_service_tests.rs"]
mod tests;
