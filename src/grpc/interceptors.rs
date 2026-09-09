//! gRPC interceptors — WAF limits, HMAC auth, Cedar fail-closed.
//!
//! WAF + Auth gRPC middleware
//! Intercepta peticiones para asegurar Zero-Trust y límites WAF.

use tonic::{Request, Status};
use tracing::debug;

use crate::cedar::authn::HmacTokenVerifier;

#[derive(Clone, Debug)]
pub struct AuthenticatedSession {
    pub tenant_id: String,
    pub user_id: String,
    pub jti: String,
}

/// Interceptor WAF y Auth para proteger los endpoints gRPC (Fail-Closed estricto).
pub fn auth_waf_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    // 1. Extraer token preferentemente desde el metadato "sid" o "authorization"
    let token = if let Some(sid_header) = req.metadata().get("sid").and_then(|v| v.to_str().ok()) {
        sid_header.trim().to_string()
    } else if let Some(auth_header) = req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
    {
        let auth_str = auth_header.trim();
        auth_str
            .strip_prefix("Bearer ")
            .or_else(|| auth_str.strip_prefix("bearer "))
            .unwrap_or(auth_str)
            .trim()
            .to_string()
    } else {
        return Err(Status::unauthenticated(
            "Missing authorization or sid header [Fail-Closed]",
        ));
    };

    // 2. Verificar la firma HMAC del token localmente (sub-0.1ms sin red) —
    //    la implementación única vive en cedar::authn (D1).
    let session = verify_hmac_token_local(&token)
        .ok_or_else(|| Status::unauthenticated("Invalid or expired session token [Fail-Closed]"))?;

    // 3. Inyectar sesión autenticada en las extensiones de la petición gRPC
    req.extensions_mut().insert(session);

    debug!("[gRPC-Auth] Petición autenticada exitosamente");
    Ok(req)
}

/// Verificación del interceptor: estricta contra la configuración del
/// arranque, con tolerancia de reloj según el entorno (300 s en producción,
/// 24 h en local — robustez ante suspensión del host). La revocación se
/// comprueba más adelante, en el camino Cedar contra el session store.
fn verify_hmac_token_local(raw_token: &str) -> Option<AuthenticatedSession> {
    let is_local = crate::domain::config::engine_config()
        .environment
        .as_deref()
        .map(|v| v == "local" || v == "development")
        .unwrap_or(false);
    let skew_tolerance: i64 = if is_local { 86_400 } else { 300 };

    HmacTokenVerifier::from_engine_config()
        .verify(raw_token, skew_tolerance)
        .map(|t| AuthenticatedSession {
            tenant_id: t.tenant_id,
            user_id: t.user_id,
            jti: t.jti,
        })
}
