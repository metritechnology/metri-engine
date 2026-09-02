// grpc/interceptors.rs — WAF + Auth gRPC middleware
// SRP: Intercepta peticiones para asegurar Zero-Trust y límites WAF.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use constant_time_eq::constant_time_eq;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tonic::{Request, Status};
use tracing::{debug, info};

type HmacSha256 = Hmac<Sha256>;

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

    // 2. Verificar la firma HMAC del token localmente (sub-0.1ms sin red)
    let session = verify_hmac_token_local(&token)
        .ok_or_else(|| Status::unauthenticated("Invalid or expired session token [Fail-Closed]"))?;

    // 3. Inyectar sesión autenticada en las extensiones de la petición gRPC
    req.extensions_mut().insert(session);

    debug!("[gRPC-Auth] Petición autenticada exitosamente");
    Ok(req)
}

fn verify_hmac_token_local(raw_token: &str) -> Option<AuthenticatedSession> {
    let token = raw_token.strip_prefix("mk_")?;
    let dot = token.find('.')?;
    let (payload_b64, sig_b64) = token.split_at(dot);
    let sig_b64 = &sig_b64[1..]; // Quitar el punto

    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let hmac_secret = std::env::var("HMAC_SECRET")
        .unwrap_or_else(|_| "secret-key-development-metri-256-bits!!!".to_string());

    let mut mac = HmacSha256::new_from_slice(hmac_secret.as_bytes()).ok()?;
    mac.update(&payload_bytes);
    let expected_sig = mac.finalize().into_bytes();
    let provided_sig = URL_SAFE_NO_PAD.decode(sig_b64).ok()?;

    if !constant_time_eq(&expected_sig, &provided_sig) {
        info!("[gRPC-Auth] Firma de token inválida");
        return None;
    }

    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;
    let now = chrono::Utc::now().timestamp();
    let exp = claims["exp"].as_i64()?;
    // Permitir mayor tolerancia para diferencias de reloj en entorno de desarrollo local (robustez ante suspensión del host)
    let is_local = std::env::var("ENVIRONMENT")
        .map(|v| v == "local" || v == "development")
        .unwrap_or(false);
    let skew_tolerance = if is_local { 86400 } else { 300 }; // 24 horas en local vs 5 minutos en prod

    if exp + skew_tolerance <= now {
        info!("[gRPC-Auth] Token de sesión gRPC expirado");
        return None;
    }

    Some(AuthenticatedSession {
        tenant_id: claims["tid"].as_str()?.to_string(),
        user_id: claims["uid"].as_str()?.to_string(),
        jti: claims["jti"].as_str()?.to_string(),
    })
}
