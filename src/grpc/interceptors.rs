//! Interceptores gRPC de borde: límites WAF y autenticación HMAC fail-closed.
//!
//! Verifica en tiempo constante la firma de cada token `mk_` antes de alcanzar los handlers
//! e inyecta `AuthenticatedSession` en las extensiones de la petición.

use constant_time_eq::constant_time_eq;
use tonic::{Request, Status};
use tracing::debug;

use crate::cedar::authn::HmacTokenVerifier;

/// Sesión autenticada extraída del token HMAC e inyectada en las extensiones gRPC (`Request::extensions`).
#[derive(Clone, Debug)]
pub struct AuthenticatedSession {
    /// Identificador del tenant autenticado en el token.
    pub tenant_id: String,
    /// Identificador del usuario autenticado en el token.
    pub user_id: String,
    /// Identificador único del token (`jti`).
    pub jti: String,
}

/// Verifica en tiempo constante la cabecera `x-metri-origin-token` inyectada por CloudFront.
///
/// Si `expected` es `Some(secret)` (configurado en producción vía `ORIGIN_VERIFY_TOKEN`),
/// exige que la cabecera coincida byte a byte en tiempo constante, bloqueando cualquier
/// invocación directa a la URL de Lambda que intente evadir CloudFront y AWS WAF.
pub fn verify_origin_token(expected: Option<&str>, provided: Option<&str>) -> Result<(), Status> {
    if let Some(expected_token) = expected.filter(|s| !s.is_empty()) {
        let Some(provided_token) = provided.map(str::trim).filter(|s| !s.is_empty()) else {
            return Err(Status::permission_denied(
                "Missing x-metri-origin-token header [WAF Bypass Blocked]",
            ));
        };
        if !constant_time_eq(expected_token.as_bytes(), provided_token.as_bytes()) {
            return Err(Status::permission_denied(
                "Invalid x-metri-origin-token header [WAF Bypass Blocked]",
            ));
        }
    }
    Ok(())
}

/// Interceptor WAF y Auth para proteger los endpoints gRPC (Fail-Closed estricto).
pub fn auth_waf_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    // 0. Verificar origen CloudFront (anti-Origin-Bypass) si ORIGIN_VERIFY_TOKEN está activo
    let provided_origin = req
        .metadata()
        .get("x-metri-origin-token")
        .and_then(|v| v.to_str().ok());
    verify_origin_token(
        crate::domain::config::engine_config()
            .origin_verify_token
            .as_deref(),
        provided_origin,
    )?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn origin_token_desactivado_en_local_permite_trafico() {
        assert!(verify_origin_token(None, None).is_ok());
        assert!(verify_origin_token(Some(""), None).is_ok());
    }

    #[test]
    fn origin_token_activo_bloquea_ausencia_o_discrepancia() {
        let secret = Some("metri-cf-origin-guard-v1-test-secret-0001");

        let missing = verify_origin_token(secret, None);
        assert!(missing.is_err());
        assert_eq!(missing.unwrap_err().code(), Code::PermissionDenied);

        let wrong = verify_origin_token(secret, Some("otro-token-invalido"));
        assert!(wrong.is_err());
        assert_eq!(wrong.unwrap_err().code(), Code::PermissionDenied);

        let valid = verify_origin_token(secret, Some("metri-cf-origin-guard-v1-test-secret-0001"));
        assert!(valid.is_ok());
    }
}
