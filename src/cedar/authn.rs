// cedar/authn.rs — Autenticación de tokens HMAC (formato mk_).
//
// UNA implementación de formato + firma + comparación en tiempo constante.
// Antes había tres copias que derivaban por separado (el camino Cedar leía el
// secreto de la variable de entorno cruda, el interceptor gRPC usaba
// engine_config con tolerancia de reloj, y el session store guardaba el
// suyo) — la fuente única es ahora la configuración del arranque.
//
// El guard fail-closed que exige un secreto fuerte en producción vive en
// `domain::config::resolve_hmac_secret` y se aplica UNA vez en `server.rs`;
// aquí no hay segunda validación de entorno por petición.

use std::sync::OnceLock;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use constant_time_eq::constant_time_eq;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Secreto de desarrollo. Nunca válido en producción: `resolve_hmac_secret`
/// aborta el arranque si el entorno no es no-productivo conocido y el secreto
/// falta o es débil. Vive aquí solo como default del entorno local.
pub const DEV_HMAC_SECRET: &str = "secret-key-development-metri-256-bits!!!";

/// Verificador de tokens mk_. Barato de clonar; para el proceso entero existe
/// una única instancia sobre la configuración del arranque (`from_engine_config`).
#[derive(Clone)]
pub struct HmacTokenVerifier {
    secret: Vec<u8>,
}

/// Claims verificados de un token mk_.
#[derive(Debug, Clone)]
pub struct VerifiedToken {
    pub tenant_id: String,
    pub user_id: String,
    pub jti: String,
    pub exp: i64,
}

impl HmacTokenVerifier {
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        HmacTokenVerifier {
            secret: secret.into(),
        }
    }

    /// Verificador del proceso: construido una sola vez desde `engine_config`.
    pub fn from_engine_config() -> &'static HmacTokenVerifier {
        static VERIFIER: OnceLock<HmacTokenVerifier> = OnceLock::new();
        VERIFIER.get_or_init(|| {
            HmacTokenVerifier::new(
                crate::domain::config::engine_config()
                    .hmac_secret
                    .clone()
                    .unwrap_or_else(|| DEV_HMAC_SECRET.to_string()),
            )
        })
    }

    /// Verifica formato, firma (en tiempo constante) y expiración del token.
    ///
    /// `skew_seconds` tolera desvíos de reloj: los interceptores gRPC permiten
    /// 300 s en producción y 24 h en local (robustez ante suspensión del
    /// host); el camino Cedar y el session store son estrictos (0).
    pub fn verify(&self, raw_token: &str, skew_seconds: i64) -> Option<VerifiedToken> {
        // Formato: mk_<base64url(payload)>.<base64url(HMAC)>
        let token = raw_token.strip_prefix("mk_")?;
        let dot = token.find('.')?;
        let (payload_b64, sig_b64) = token.split_at(dot);
        let sig_b64 = &sig_b64[1..];

        let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;

        let mut mac = HmacSha256::new_from_slice(&self.secret).ok()?;
        mac.update(&payload_bytes);
        let expected_sig = mac.finalize().into_bytes();
        let provided_sig = URL_SAFE_NO_PAD.decode(sig_b64).ok()?;

        if !constant_time_eq(&expected_sig, &provided_sig) {
            return None;
        }

        let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;
        let now = chrono::Utc::now().timestamp();
        let exp = claims["exp"].as_i64()?;
        if exp + skew_seconds <= now {
            return None;
        }

        Some(VerifiedToken {
            tenant_id: claims["tid"].as_str()?.to_string(),
            user_id: claims["uid"].as_str()?.to_string(),
            jti: claims["jti"].as_str()?.to_string(),
            exp,
        })
    }
}


// ── step1: extracción de token y consulta de sesión ────────────────────────

use crate::cedar::request::AuthRequest;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::{ISessionStore, Session};

/// Verificación HMAC estricta del camino Cedar (sin tolerancia de reloj).
///
/// Envoltorio fino sobre `authn::HmacTokenVerifier` — la implementación única
/// del formato mk_ (D1). El secreto viene de la configuración del arranque.
pub fn verify_hmac_token_local_in_step(raw_token: &str) -> Option<Session> {
    HmacTokenVerifier::from_engine_config()
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


// --- Public Helper Interceptor Methods ---


pub async fn step1_extract_token(
    auth: &AuthRequest<'_>,
    valkey_store: &dyn ISessionStore,
) -> Result<Session, DomainError> {
    let Some(token) = auth.session_token() else {
        return Err(
            DomainError::new(ErrorCode::Auth401, "Missing authorization or sid header")
                .with_stage("cedar"),
        );
    };

    // Rechazo temprano sin red de tokens mk_ mal firmados o expirados.
    // S1: el fast-path YA NO devuelve la sesión aquí — la revocación (blacklist
    // por jti) vive en el session store y ninguna firma válida se la salta.
    if token.starts_with("mk_") && verify_hmac_token_local_in_step(&token).is_none() {
        return Err(
            DomainError::new(ErrorCode::Auth401, "Invalid or expired token").with_stage("cedar"),
        );
    }

    let session = valkey_store.get_session(&token).await?.ok_or_else(|| {
        DomainError::new(ErrorCode::Auth401, "Invalid or expired token").with_stage("cedar")
    })?;

    if session.user_id.is_empty() || session.tenant_id.is_empty() {
        return Err(
            DomainError::new(ErrorCode::Auth401, "Malformed session payload from Valkey")
                .with_stage("cedar"),
        );
    }

    Ok(session)
}


#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Acuña tokens mk_ para tests de todo el crate (cedar, interceptores).
    pub(crate) fn mint_for_tests(
        verifier: &HmacTokenVerifier,
        exp: i64,
        jti: &str,
        tenant_id: &str,
        user_id: &str,
    ) -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        let claims = serde_json::json!({
            "exp": exp,
            "iat": exp - 3600,
            "jti": jti,
            "tid": tenant_id,
            "uid": user_id
        });
        let payload = serde_json::to_vec(&claims).unwrap();
        let mut mac = HmacSha256::new_from_slice(&verifier.secret).unwrap();
        mac.update(&payload);
        let sig = mac.finalize().into_bytes();
        format!(
            "mk_{}.{}",
            URL_SAFE_NO_PAD.encode(&payload),
            URL_SAFE_NO_PAD.encode(sig)
        )
    }

    fn mint(verifier: &HmacTokenVerifier, exp: i64) -> String {
        mint_for_tests(verifier, exp, "jti_test", "tnt_01", "usr_01")
    }

    #[test]
    fn token_valido_se_verifica_sin_skew() {
        let verifier = HmacTokenVerifier::new("secreto-de-test-de-al-menos-32-bytes!!!".as_bytes());
        let exp = chrono::Utc::now().timestamp() + 3600;
        let token = mint(&verifier, exp);
        let verified = verifier.verify(&token, 0).expect("token válido");
        assert_eq!(verified.tenant_id, "tnt_01");
        assert_eq!(verified.user_id, "usr_01");
        assert_eq!(verified.jti, "jti_test");
        assert_eq!(verified.exp, exp);
    }

    #[test]
    fn firma_invalida_se_rechaza() {
        let verifier = HmacTokenVerifier::new("secreto-de-test-de-al-menos-32-bytes!!!".as_bytes());
        let exp = chrono::Utc::now().timestamp() + 3600;
        let token = mint(&verifier, exp);
        let otro = HmacTokenVerifier::new("otro-secreto-de-test-de-32-bytes!!!!!!".as_bytes());
        assert!(otro.verify(&token, 0).is_none());
    }

    #[test]
    fn expirado_stricto_se_rechaza_y_con_skew_generoso_pasa() {
        let verifier = HmacTokenVerifier::new("secreto-de-test-de-al-menos-32-bytes!!!".as_bytes());
        let exp = chrono::Utc::now().timestamp() - 60; // expiró hace un minuto
        let token = mint(&verifier, exp);
        assert!(verifier.verify(&token, 0).is_none(), "estricto: rechazado");
        assert!(
            verifier.verify(&token, 86_400).is_some(),
            "skew de 24h (entorno local): aceptado"
        );
    }

    #[test]
    fn formato_malformado_se_rechaza() {
        let verifier = HmacTokenVerifier::new("secreto-de-test-de-al-menos-32-bytes!!!".as_bytes());
        assert!(verifier.verify("", 0).is_none());
        assert!(verifier.verify("sin_prefijo", 0).is_none());
        assert!(verifier.verify("mk_sinpunto", 0).is_none());
        assert!(verifier.verify("mk_!!!.!!!", 0).is_none());
    }
}
