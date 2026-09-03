// infrastructure/session_store.rs — HMACTokenStore.
// En el stack anterior: (defrecord HMACTokenStore [secret ddb-client table-name])
// En Rust:    struct HmacTokenStore — implementa ISessionStore
//
// Zero-Drop Policy: replica verify_signature con constant-time comparison,
// blacklist check en DynamoDB, revocación y des-revocación.

use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tracing::{info, warn};

use crate::cedar::authn::{HmacTokenVerifier, VerifiedToken};

type HmacSha256 = Hmac<Sha256>;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::{ISessionStore, Session};
use crate::infrastructure::dynamodb::DynamoClient;

/// HMACTokenStore — verificación local sub-0.1ms, revocación en DynamoDB.
pub struct HmacTokenStore {
    verifier: HmacTokenVerifier,
    ddb: Arc<DynamoClient>,
    table_name: String, // tabla de blacklist (REVOKED#<jti>)
}

impl HmacTokenStore {
    /// Constructor. El secret viene de Secrets Manager o variable de entorno.
    pub fn new(secret: Vec<u8>, ddb: Arc<DynamoClient>, table_name: impl Into<String>) -> Self {
        info!("[HMAC] Session store activo — verificación local, sin red");
        HmacTokenStore {
            verifier: HmacTokenVerifier::new(secret),
            ddb,
            table_name: table_name.into(),
        }
    }

    /// Verifica la firma y la expiración del token (estricto, sin skew). No
    /// consulta blacklist — la implementación única del formato vive en
    /// cedar::authn.
    fn verify_signature(&self, raw_token: &str) -> Option<Session> {
        self.verifier
            .verify(raw_token, 0)
            .map(
                |VerifiedToken {
                     tenant_id,
                     user_id,
                     jti,
                     exp,
                 }| Session {
                    tenant_id,
                    user_id,
                    jti,
                    exp,
                },
            )
    }

    /// Verifica si un jti está en la blacklist de DynamoDB.
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
    async fn get_session(&self, token: &str) -> Result<Option<Session>, DomainError> {
        // 1. Verificación local de firma — sin red, < 0.1ms
        let claims = match self.verify_signature(token) {
            Some(c) => c,
            None => return Ok(None),
        };

        // 2. Blacklist check — solo si la firma es válida
        if self.is_blacklisted(&claims.jti).await {
            warn!("[HMAC] Token revocado, jti: {}", claims.jti);
            return Ok(None);
        }

        Ok(Some(claims))
    }

    /// REVOCAR: añade el jti a la blacklist DynamoDB con TTL.
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
    async fn unrevoke_session(&self, jti: &str) -> Result<(), DomainError> {
        let pk = format!("REVOKED#{jti}");
        self.ddb
            .delete_item(&self.table_name, &pk, Some(b"REVOKED"))
            .await
    }
}

// ── Primitivas de emisión de tokens ──────────────────────────────────────────

/// Emite un token HMAC firmado.
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
