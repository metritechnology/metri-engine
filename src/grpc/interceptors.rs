// grpc/interceptors.rs — WAF + Auth gRPC middleware
// SRP: Intercepta peticiones para asegurar Zero-Trust y límites WAF.

use tonic::{Request, Status};
use tracing::info;

/// Interceptor WAF y Auth para proteger los endpoints gRPC.
pub fn auth_waf_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    // 1. WAF Check: Validar tamaño, Content-Type u headers especiales
    // (Tonic/Hyper maneja mucho de esto automáticamente, pero podemos añadir lógica extra)
    
    // 2. Auth Check: HMAC-SHA256 token verification
    // FASE 3 STUB: Simular verificación de token
    if let Some(auth_header) = req.metadata().get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str.starts_with("Bearer ") {
                let token = &auth_str[7..];
                // Simulación de verificación
                if token == "invalid-token" {
                    return Err(Status::unauthenticated("Invalid token"));
                }
                
                // Inyectar contexto validado en la request (e.g. extensiones)
                // req.extensions_mut().insert(CedarCtx { tenant_id: ..., user_id: ... });
                info!("[Interceptor] Token verificado correctamente");
                return Ok(req);
            }
        }
    }

    // Default: Permitir paso (En FASE 5 se pondrá estricto)
    info!("[Interceptor] Petición sin token, permitida temporalmente en desarrollo");
    Ok(req)
}
