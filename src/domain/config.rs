// domain/config.rs — Configuración del engine, leída una sola vez.
//
// La Fase 4 del plan de refactorización: antes, `METRI_MASTER_TENANT_ID` se
// leía con `env::var` dentro del camino caliente (una llamada al sistema por
// consulta), el concepto tenía DOS nombres (`MASTER_TENANT_ID` en la raíz de
// composición y `METRI_MASTER_TENANT_ID` en el executor) y los tests no podían
// fijar configuración sin manipular el entorno global (condición de carrera con
// tests en paralelo).
//
// Ahora: `EngineConfig::from_env()` en el arranque, `OnceLock` estático, y
// `engine_config()` para todo el resto. El guard de seguridad HMAC vive aquí
// como función pura — testeable sin tocar el entorno.

use std::sync::OnceLock;

/// Tope del RPC ListEntities: configuración de servicio, no una constante de
/// código (Fase 4, ítem 5).
pub const DEFAULT_MAX_LIST_LIMIT: i32 = 5_000;

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
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            master_tenant_id: "system".to_string(),
            max_list_limit: DEFAULT_MAX_LIST_LIMIT,
            environment: None,
            hmac_secret: None,
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
        Self {
            master_tenant_id,
            max_list_limit: DEFAULT_MAX_LIST_LIMIT,
            environment: std::env::var("ENVIRONMENT").ok(),
            hmac_secret: std::env::var("HMAC_SECRET").ok(),
        }
    }

    /// Constructor explícito para tests y para la raíz de composición.
    pub fn from_parts(master_tenant_id: impl Into<String>, max_list_limit: i32) -> Self {
        Self {
            master_tenant_id: master_tenant_id.into(),
            max_list_limit,
            environment: None,
            hmac_secret: None,
        }
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
pub fn resolve_hmac_secret(
    environment: Option<&str>,
    hmac_secret: Option<&str>,
) -> Result<String, String> {
    let known_non_prod = matches!(environment, Some("development" | "dev" | "local" | "test"));
    if !known_non_prod {
        let secret = hmac_secret
            .ok_or("HMAC_SECRET ausente en entorno productivo (o ENVIRONMENT desconocido)")?;
        if secret == DEV_HMAC_SECRET
            || secret == "local-dev-secret-do-not-use-in-prod"
            || secret.len() < 32
        {
            return Err("HMAC_SECRET débil o por defecto en entorno productivo".to_string());
        }
        return Ok(secret.to_string());
    }
    Ok(hmac_secret.unwrap_or(DEV_HMAC_SECRET).to_string())
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
pub fn resolve_dev_auth_bypass(environment: Option<&str>, requested: bool) -> Result<bool, String> {
    let known_non_prod = matches!(environment, Some("development" | "dev" | "local" | "test"));
    if requested && !known_non_prod {
        return Err(
            "METRI_DEV_AUTH_BYPASS solo es válido en entornos no productivos conocidos \
             (development/dev/local/test)"
                .to_string(),
        );
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
}
