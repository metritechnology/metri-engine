//! Configuración global del motor (`EngineConfig`), leída una única vez en el arranque.
//!
//! Centraliza la resolución de variables de entorno en `EngineConfig::from_env()`
//! almacenada en un `OnceLock` estático y accesible vía `engine_config()`.
//! Incluye los candados puros `resolve_hmac_secret` y `resolve_dev_auth_bypass`
//! con política fail-closed para entornos productivos.

use std::sync::OnceLock;

use crate::domain::errors::{DomainError, ErrorCode};

/// Tope del RPC ListEntities: configuración de servicio, no una constante de
/// código.
pub const DEFAULT_MAX_LIST_LIMIT: i32 = 5_000;

/// Clave legada de sellado `.mbf` preservada en el Key-Ring para lectura retrocompatible.
pub const LEGACY_MBF_SEAL_KEY: &[u8] = b"METRI_VORTEX_MBF_SEAL_KEY_2026";

/// Configuración global e inmutable del motor resuelta durante el arranque.
#[derive(Debug, Clone, PartialEq)]
pub struct EngineConfig {
    /// Tenant maestro ( Zero-Trust censura a este tenant los dominios de negocio).
    pub master_tenant_id: String,
    /// Tope duro de `ListEntities` mientras no exista paginación real (L4).
    pub max_list_limit: i32,
    /// `ENVIRONMENT` tal como llegó al arranque (None = ausente). El interceptor
    /// local lo consulta por petición; leerlo de acá, no del entorno.
    pub environment: Option<String>,
    /// `HMAC_SECRET` tal como llegó al arranque (None = ausente).
    pub hmac_secret: Option<String>,
    /// Clave maestra activa para `MbfTenantCryptoSeal` (`MBF_SEAL_KEY` o derivada de `HMAC_SECRET`).
    pub mbf_seal_key: Option<String>,
    /// Token compartido de borde (`ORIGIN_VERIFY_TOKEN`) inyectado por CloudFront en `X-Metri-Origin-Token`.
    pub origin_verify_token: Option<String>,
    /// Tabla lógica para `sequence_registry` (`SEQUENCE_REGISTRY_TABLE`).
    pub sequence_registry_table: String,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            master_tenant_id: "system".to_string(),
            max_list_limit: DEFAULT_MAX_LIST_LIMIT,
            environment: None,
            hmac_secret: None,
            mbf_seal_key: None,
            origin_verify_token: None,
            sequence_registry_table: "metri-sequence-registry".to_string(),
        }
    }
}

impl EngineConfig {
    /// Lee la configuración desde el entorno. Un solo concepto, los dos nombres
    /// históricos aceptados: `MASTER_TENANT_ID` (canónico) y
    /// `METRI_MASTER_TENANT_ID` (legado del executor).
    pub fn from_env() -> Self {
        let master_tenant_id = std::env::var("MASTER_TENANT_ID")
            .or_else(|_| std::env::var("METRI_MASTER_TENANT_ID"))
            .unwrap_or_else(|_| "system".to_string());
        let sequence_registry_table = std::env::var("SEQUENCE_REGISTRY_TABLE")
            .unwrap_or_else(|_| "metri-sequence-registry".to_string());
        let hmac_secret = std::env::var("HMAC_SECRET").ok();
        let mbf_seal_key = std::env::var("MBF_SEAL_KEY")
            .ok()
            .filter(|k| !k.is_empty())
            .or_else(|| {
                hmac_secret
                    .as_deref()
                    .filter(|k| !k.is_empty())
                    .map(derive_mbf_seal_key_from_hmac)
            });
        let origin_verify_token = std::env::var("ORIGIN_VERIFY_TOKEN")
            .ok()
            .filter(|k| !k.is_empty());
        Self {
            master_tenant_id,
            max_list_limit: DEFAULT_MAX_LIST_LIMIT,
            environment: std::env::var("ENVIRONMENT").ok(),
            hmac_secret,
            mbf_seal_key,
            origin_verify_token,
            sequence_registry_table,
        }
    }

    /// Constructor explícito para tests y para la raíz de composición.
    pub fn from_parts(master_tenant_id: impl Into<String>, max_list_limit: i32) -> Self {
        Self {
            master_tenant_id: master_tenant_id.into(),
            max_list_limit,
            environment: None,
            hmac_secret: None,
            mbf_seal_key: None,
            origin_verify_token: None,
            sequence_registry_table: "metri-sequence-registry".to_string(),
        }
    }

    /// Devuelve la clave maestra activa para sellar bloques `.mbf` (`MbfTenantCryptoSeal`).
    /// Si existe `mbf_seal_key` o `hmac_secret` configurados en el entorno, los utiliza;
    /// en modo local/test sin variables definidas, recurre a `LEGACY_MBF_SEAL_KEY`.
    pub fn active_mbf_seal_key(&self) -> &[u8] {
        if let Some(ref k) = self.mbf_seal_key {
            if !k.is_empty() {
                return k.as_bytes();
            }
        }
        if let Some(ref h) = self.hmac_secret {
            if !h.is_empty() {
                return h.as_bytes();
            }
        }
        LEGACY_MBF_SEAL_KEY
    }
}

static ENGINE_CONFIG: OnceLock<EngineConfig> = OnceLock::new();

/// Configuración vigente. La primera llamada (arranque) fija el valor.
pub fn engine_config() -> &'static EngineConfig {
    ENGINE_CONFIG.get_or_init(EngineConfig::from_env)
}

/// Fija la configuración explícitamente en el arranque. Falla si ya estaba
/// fijada — nadie reconfigura un engine vivo.
pub fn init_engine_config(cfg: EngineConfig) -> Result<(), EngineConfig> {
    ENGINE_CONFIG.set(cfg)
}

/// Secreto de desarrollo: nunca válido fuera de entornos no productivos.
const DEV_HMAC_SECRET: &str = "secret-key-development-metri-256-bits!!!";

/// Decide el secreto HMAC según el entorno. **Falla cerrado.**
///
/// La Fase 4 invierte la lógica anterior: solo `development`/`dev`/`local`/
/// `test` usan el secreto por defecto; cualquier valor de `ENVIRONMENT`
/// ausente, desconocido (un typo, un entorno nuevo) o productivo exige un
/// secreto fuerte — y aborta el arranque si no lo hay.
///
/// # Errors
///
/// `ErrorCode::InfraConfig001` si el entorno productivo no tiene secreto, o
/// lo tiene débil/por defecto — el llamador (bootstrap) hace fail-fast.
pub fn resolve_hmac_secret(
    environment: Option<&str>,
    hmac_secret: Option<&str>,
) -> Result<String, DomainError> {
    let known_non_prod = matches!(environment, Some("development" | "dev" | "local" | "test"));
    if !known_non_prod {
        let secret = hmac_secret.ok_or_else(|| {
            DomainError::new(
                ErrorCode::InfraConfig001,
                "HMAC_SECRET ausente en entorno productivo (o ENVIRONMENT desconocido)",
            )
        })?;
        if secret == DEV_HMAC_SECRET
            || secret == "local-dev-secret-do-not-use-in-prod"
            || secret.len() < 32
        {
            return Err(DomainError::new(
                ErrorCode::InfraConfig001,
                "HMAC_SECRET débil o por defecto en entorno productivo",
            ));
        }
        return Ok(secret.to_string());
    }
    Ok(hmac_secret.unwrap_or(DEV_HMAC_SECRET).to_string())
}

/// Deriva una sub-clave criptográfica independiente para `MbfTenantCryptoSeal` a partir de `HMAC_SECRET`
/// aplicando separación de dominios (`HMAC-SHA256(secret, "metri-mbf-tenant-seal-v1")`).
pub fn derive_mbf_seal_key_from_hmac(hmac_secret: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(hmac_secret.as_bytes()).unwrap_or_else(|_| {
        use hmac::digest::generic_array::GenericArray;
        <HmacSha256 as hmac::digest::KeyInit>::new(GenericArray::from_slice(&[0u8; 32]))
    });
    mac.update(b"metri-mbf-tenant-seal-v1");
    hex::encode(mac.finalize().into_bytes())
}

/// Valida el token de verificación de origen (`ORIGIN_VERIFY_TOKEN`) según el entorno.
///
/// En entornos productivos (cualquier `ENVIRONMENT` distinto de `development`, `dev`,
/// `local` o `test`), exige que el token esté configurado y tenga al menos 32 bytes.
///
/// # Errors
///
/// `ErrorCode::InfraConfig001` si el entorno productivo carece de `ORIGIN_VERIFY_TOKEN`
/// o este tiene menos de 32 bytes.
pub fn resolve_origin_verify_token(
    environment: Option<&str>,
    origin_verify_token: Option<&str>,
) -> Result<Option<String>, DomainError> {
    let known_non_prod = matches!(environment, Some("development" | "dev" | "local" | "test"));
    let normalized = origin_verify_token.map(str::trim).filter(|s| !s.is_empty());
    if !known_non_prod {
        let token = normalized.ok_or_else(|| {
            DomainError::new(
                ErrorCode::InfraConfig001,
                "ORIGIN_VERIFY_TOKEN ausente en entorno productivo (o ENVIRONMENT desconocido)",
            )
        })?;
        if token.len() < 32 {
            return Err(DomainError::new(
                ErrorCode::InfraConfig001,
                "ORIGIN_VERIFY_TOKEN débil (< 32 bytes) en entorno productivo",
            ));
        }
        return Ok(Some(token.to_string()));
    }
    Ok(normalized.map(str::to_string))
}

/// Bypass de autorización para desarrollo y pruebas (`METRI_DEV_AUTH_BYPASS=1`).
///
/// Sustituye al antiguo `METRI_TEST_MODE`: el principal se sintetiza desde
/// cabeceras `test-*`, se omite la evaluación Cedar del camino de mutación y
/// no se expanden hijos de jerarquía vía índice. **Falla cerrado**: solo es
/// válido en entornos no productivos conocidos — el mismo criterio de
/// `resolve_hmac_secret` — y el arranque aborta si se pide fuera de ellos.
/// La decisión se toma UNA vez en la raíz de composición; nada la consulta
/// por petición.
///
/// # Errors
///
/// `ErrorCode::InfraConfig001` si se pide bypass fuera de los entornos no
/// productivos conocidos.
pub fn resolve_dev_auth_bypass(
    environment: Option<&str>,
    requested: bool,
) -> Result<bool, DomainError> {
    let known_non_prod = matches!(environment, Some("development" | "dev" | "local" | "test"));
    if requested && !known_non_prod {
        return Err(DomainError::new(
            ErrorCode::InfraConfig001,
            "METRI_DEV_AUTH_BYPASS solo es válido en entornos no productivos conocidos \
             (development/dev/local/test)",
        ));
    }
    Ok(requested)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_apunta_al_tenant_system() {
        let cfg = EngineConfig::default();
        assert_eq!(cfg.master_tenant_id, "system");
        assert_eq!(cfg.max_list_limit, DEFAULT_MAX_LIST_LIMIT);
    }

    #[test]
    fn el_nombre_canonico_y_el_legado_son_el_mismo_concepto() {
        // from_parts: sin manipular el entorno global (tests en paralelo).
        let cfg = EngineConfig::from_parts("tnt_master", 100);
        assert_eq!(cfg.master_tenant_id, "tnt_master");
        assert_eq!(cfg.max_list_limit, 100);
    }

    #[test]
    fn environment_ausente_falla_cerrado() {
        assert!(resolve_hmac_secret(None, None).is_err());
        assert!(resolve_hmac_secret(None, Some(DEV_HMAC_SECRET)).is_err());
    }

    #[test]
    fn environment_desconocido_se_trata_como_produccion() {
        assert!(resolve_hmac_secret(Some("qa"), None).is_err());
        assert!(resolve_hmac_secret(Some("staging"), Some(DEV_HMAC_SECRET)).is_err());
        assert!(resolve_hmac_secret(
            Some("Production"),
            Some("a-very-strong-production-secret-0001")
        )
        .is_ok());
    }

    #[test]
    fn produccion_exige_secreto_fuerte() {
        assert!(resolve_hmac_secret(Some("production"), Some("short")).is_err());
        assert!(resolve_hmac_secret(
            Some("production"),
            Some("a-very-strong-production-secret-0001")
        )
        .is_ok());
    }

    #[test]
    fn desarrollo_conserva_el_default() {
        assert_eq!(
            resolve_hmac_secret(Some("development"), None).unwrap(),
            DEV_HMAC_SECRET
        );
        assert_eq!(
            resolve_hmac_secret(Some("local"), Some("local-dev-secret-do-not-use-in-prod"))
                .unwrap(),
            "local-dev-secret-do-not-use-in-prod"
        );
    }

    #[test]
    fn bypass_dev_solo_en_entornos_conocidos() {
        // Pedido en no-prod conocido: permitido.
        assert!(resolve_dev_auth_bypass(Some("development"), true).unwrap());
        assert!(resolve_dev_auth_bypass(Some("local"), true).unwrap());
        // No pedido: permitido en cualquier entorno, siempre es false.
        assert!(!resolve_dev_auth_bypass(Some("production"), false).unwrap());
        assert!(!resolve_dev_auth_bypass(None, false).unwrap());
        // Pedido en producción, staging o entorno desconocido: falla cerrado.
        assert!(resolve_dev_auth_bypass(Some("production"), true).is_err());
        assert!(resolve_dev_auth_bypass(Some("staging"), true).is_err());
        assert!(resolve_dev_auth_bypass(Some("qa"), true).is_err());
        assert!(resolve_dev_auth_bypass(None, true).is_err());
    }

    #[test]
    fn origin_verify_token_exigido_en_produccion() {
        assert!(resolve_origin_verify_token(Some("production"), None).is_err());
        assert!(resolve_origin_verify_token(Some("production"), Some("too-short")).is_err());
        assert!(resolve_origin_verify_token(
            Some("production"),
            Some("a-very-strong-origin-verify-token-0001")
        )
        .is_ok());
        assert_eq!(
            resolve_origin_verify_token(Some("development"), None).unwrap(),
            None
        );
    }

    #[test]
    fn derivacion_mbf_seal_key_separa_dominios() {
        let raw = "a-very-strong-production-secret-0001";
        let derived = derive_mbf_seal_key_from_hmac(raw);
        assert_ne!(derived, raw);
        assert_eq!(derived.len(), 64);
    }
}
