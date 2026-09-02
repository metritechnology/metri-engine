// [PORTED_FROM: src/metri/infrastructure/session_store.clj]
// infrastructure/session_store.rs — HMACTokenStore.
// En Clojure: (defrecord HMACTokenStore [secret ddb-client table-name])
// En Rust:    struct HmacTokenStore — implementa ISessionStore
//
// Zero-Drop Policy: replica verify_signature con constant-time comparison,
// blacklist check en DynamoDB, revocación y des-revocación.

use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use constant_time_eq::constant_time_eq;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tracing::{debug, info, warn};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::{ISessionStore, Session};
use crate::infrastructure::dynamodb::DynamoClient;

type HmacSha256 = Hmac<Sha256>;

/// HMACTokenStore — verificación local sub-0.1ms, revocación en DynamoDB.
/// [PORTED_FROM: (defrecord HMACTokenStore [secret ddb-client table-name])]
pub struct HmacTokenStore {
    secret: Vec<u8>, // HMAC-SHA256 secret
    ddb: Arc<DynamoClient>,
    table_name: String, // tabla de blacklist (REVOKED#<jti>)
}

impl HmacTokenStore {
    /// Constructor. El secret viene de Secrets Manager o variable de entorno.
    /// [PORTED_FROM: ig/init-key :infra/session-store]
    pub fn new(secret: Vec<u8>, ddb: Arc<DynamoClient>, table_name: impl Into<String>) -> Self {
        info!("[HMAC] Session store activo — verificación local, sin red");
        HmacTokenStore {
            secret,
            ddb,
            table_name: table_name.into(),
        }
    }

    /// Verifica la firma y la expiración del token. No consulta blacklist.
    /// [PORTED_FROM: (verify-signature secret raw-token)]
    fn verify_signature(&self, raw_token: &str) -> Option<Session> {
        // El token tiene formato: mk_<base64url(payload)>.<base64url(HMAC)>
        // [PORTED_FROM: (when (starts-with? raw-token "mk_") ...)]
        let token = raw_token.strip_prefix("mk_")?;
        let dot = token.find('.')?;
        let (payload_b64, sig_b64) = token.split_at(dot);
        let sig_b64 = &sig_b64[1..]; // quitar el punto

        // Decodificar payload
        let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;

        // Calcular HMAC esperado
        // [PORTED_FROM: (hmac-sha256 secret payload-bytes)]
        let mut mac = HmacSha256::new_from_slice(&self.secret).ok()?;
        mac.update(&payload_bytes);
        let expected_sig = mac.finalize().into_bytes();

        // Decodificar firma provista
        let provided_sig = URL_SAFE_NO_PAD.decode(sig_b64).ok()?;

        // Comparación en tiempo constante — previene timing attacks
        // [PORTED_FROM: (constant-time-eq? expected-sig provided-sig)]
        if !constant_time_eq(&expected_sig, &provided_sig) {
            debug!("[HMAC] Firma inválida");
            return None;
        }

        // Parsear claims
        let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;
        let now = chrono::Utc::now().timestamp();

        // Verificar expiración
        // [PORTED_FROM: (when (> (:exp claims) now) ...)]
        let exp = claims["exp"].as_i64()?;
        if exp <= now {
            debug!("[HMAC] Token expirado");
            return None;
        }

        Some(Session {
            tenant_id: claims["tid"].as_str()?.to_string(),
            user_id: claims["uid"].as_str()?.to_string(),
            jti: claims["jti"].as_str()?.to_string(),
            exp,
        })
    }

    /// Verifica si un jti está en la blacklist de DynamoDB.
    /// [PORTED_FROM: (blacklisted? ddb-client table-name jti)]
    async fn is_blacklisted(&self, jti: &str) -> bool {
        let pk = format!("REVOKED#{jti}");
        match self
            .ddb
            .get_item(&self.table_name, &pk, Some(b"REVOKED"))
            .await
        {
            Ok(Some(_)) => true, // encontrado → revocado
            Ok(None) => false,   // no encontrado → válido
            Err(e) => {
                warn!("[HMAC] Error verificando blacklist: {e:?}");
                false // fail-open en error de infra (disponibilidad > seguridad)
            }
        }
    }
}

#[async_trait]
impl ISessionStore for HmacTokenStore {
    /// Verifica token y retorna Session si es válido.
    /// [PORTED_FROM: (get-session [_ raw-token])]
    async fn get_session(&self, token: &str) -> Result<Option<Session>, DomainError> {
        // 1. Verificación local de firma — sin red, < 0.1ms
        let claims = match self.verify_signature(token) {
            Some(c) => c,
            None => return Ok(None),
        };

        // 2. Blacklist check — solo si la firma es válida
        // [PORTED_FROM: (if (blacklisted? ...) nil claims)]
        if self.is_blacklisted(&claims.jti).await {
            warn!("[HMAC] Token revocado, jti: {}", claims.jti);
            return Ok(None);
        }

        Ok(Some(claims))
    }

    /// REVOCAR: añade el jti a la blacklist DynamoDB con TTL.
    /// [PORTED_FROM: (put-session! [_ raw-token _ ttl-seconds])]
    async fn revoke_session(&self, jti: &str, ttl_seconds: u64) -> Result<(), DomainError> {
        let pk = format!("REVOKED#{jti}");
        let expires = chrono::Utc::now().timestamp() as u64 + ttl_seconds;

        info!("[HMAC] Revocando token, jti: {jti}");

        use aws_sdk_dynamodb::types::AttributeValue;
        let mut item = std::collections::HashMap::new();
        item.insert("PK".to_string(), AttributeValue::S(pk));
        item.insert(
            "SK".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(b"REVOKED".to_vec())),
        );
        item.insert("ttl".to_string(), AttributeValue::N(expires.to_string()));

        self.ddb
            .put_item(&self.table_name, item)
            .await
            .map_err(|e| {
                DomainError::infra(ErrorCode::Infra001, format!("Revocación falló: {e:?}"))
            })
    }

    /// Quita un jti de la blacklist (des-revocar). Idempotente.
    /// [PORTED_FROM: (del-session! [_ jti])]
    async fn unrevoke_session(&self, jti: &str) -> Result<(), DomainError> {
        let pk = format!("REVOKED#{jti}");
        self.ddb
            .delete_item(&self.table_name, &pk, Some(b"REVOKED"))
            .await
    }
}

// ── Primitivas de emisión de tokens ──────────────────────────────────────────

/// Emite un token HMAC firmado.
/// [PORTED_FROM: (issue-token secret {:keys [tenant-id user-id ttl-seconds jti]})]
pub fn issue_token(
    secret: &[u8],
    tenant_id: &str,
    user_id: &str,
    ttl_seconds: u64,
    jti: Option<String>,
) -> String {
    let now = chrono::Utc::now().timestamp();
    let jti = jti.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let payload = serde_json::json!({
        "tid": tenant_id,
        "uid": user_id,
        "iat": now,
        "exp": now + ttl_seconds as i64,
        "jti": jti,
    });

    let payload_bytes = payload.to_string().into_bytes();
    let payload_b64 = URL_SAFE_NO_PAD.encode(&payload_bytes);

    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC key de longitud inválida");
    mac.update(&payload_bytes);
    let sig = mac.finalize().into_bytes();
    let sig_b64 = URL_SAFE_NO_PAD.encode(sig);

    format!("mk_{payload_b64}.{sig_b64}")
}
