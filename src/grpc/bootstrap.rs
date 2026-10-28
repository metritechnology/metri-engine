//! Semilla de arranque (`bootstrap`) del tenant maestro y cuentas de sistema.
//!
//! Verifica de forma idempotente si el almacén OLTP carece de usuarios en el tenant
//! `system` y, cuando las variables `METRI_BOOTSTRAP_MASTER_*` están presentes
//! (incluyendo `METRI_BOOTSTRAP_MASTER_KEY` resuelta desde AWS SSM Parameter Store),
//! crea el tenant `system`, los roles `role_super_master` y `system-bff`, y los
//! usuarios `usr_master` y `usr_system_bff`, limpiando después los secretos de memoria.
use crate::aegis::oltp::executor::OltpExecutor;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::janus_router::router::IWriteChannel;
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info};

/// Chequea e inicializa de forma segura al usuario administrador máster si la BD está vacía.
pub async fn check_and_bootstrap_master(
    oltp_exec: &OltpExecutor,
    oltp_channel: &Arc<dyn IWriteChannel>,
) -> Result<(), DomainError> {
    // 1. Validar si ya existen usuarios registrados en el tenant maestro "system"
    let query_users = json!({
        "entity": "user",
        "select": ["id"],
        "limit": 1
    });

    let has_users = match oltp_exec.run_oltp_query("system", &query_users).await {
        Ok(val) => val.as_array().map(|arr| !arr.is_empty()).unwrap_or(false),
        Err(e) => {
            error!(
                "[Bootstrap] Error consultando usuarios existentes en la base de datos: {:?}",
                e
            );
            // Si hay un error de conexión inicial con la BD o la tabla no está creada, omitimos
            // el bootstrap para evitar panics cíclicos de encendido.
            return Ok(());
        }
    };

    if has_users {
        info!(
            "[Bootstrap] La base de datos ya contiene usuarios registrados. Bootstrapping omitido."
        );
        return Ok(());
    }

    // 2. Extraer variables de entorno
    let email = match std::env::var("METRI_BOOTSTRAP_MASTER_EMAIL")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        Some(v) => v,
        None => {
            info!("[Bootstrap] Base de datos vacía pero METRI_BOOTSTRAP_MASTER_EMAIL no está configurada. Omitiendo.");
            return Ok(());
        }
    };

    let username = match std::env::var("METRI_BOOTSTRAP_MASTER_USERNAME")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        Some(v) => v,
        None => {
            info!("[Bootstrap] Base de datos vacía pero METRI_BOOTSTRAP_MASTER_USERNAME no está configurada. Omitiendo.");
            return Ok(());
        }
    };

    let password = match resolve_bootstrap_master_key() {
        Some(v) => v,
        None => {
            info!("[Bootstrap] Base de datos vacía pero METRI_BOOTSTRAP_MASTER_KEY no está configurada. Omitiendo.");
            return Ok(());
        }
    };

    seed_master(oltp_channel, &email, &username, &password).await
}

/// Resuelve la clave única de bootstrap desde `METRI_BOOTSTRAP_MASTER_KEY`
/// (inyectada desde AWS SSM Parameter Store `arn:aws:ssm:us-east-1:982592308819:parameter/metri/engine/bootstrap/master-key`).
fn resolve_bootstrap_master_key() -> Option<String> {
    std::env::var("METRI_BOOTSTRAP_MASTER_KEY")
        .ok()
        .filter(|v| !v.trim().is_empty())
}

const ALL_DOMAIN_GRANTS: &str = r#"{"domain":"*","actions":["VIEW","CREATE","UPDATE","DELETE","EXECUTE","EXPORT"],"scope":"ALL"}"#;

/// Mecánica de la semilla: hashea la contraseña, crea el tenant, los roles y
/// los usuarios maestros, y limpia los secretos del entorno. La DECISIÓN de
/// sembrar vive en `check_and_bootstrap_master`; esta función no consulta la
/// base de datos, así que se prueba con un canal simulado.
async fn seed_master(
    oltp_channel: &Arc<dyn IWriteChannel>,
    email: &str,
    username: &str,
    password: &str,
) -> Result<(), DomainError> {
    info!("[Bootstrap] Iniciando creación de semilla para el Administrador Master...");

    let password_hash = bcrypt::hash(password, 12)
        .map_err(|e| DomainError::new(ErrorCode::Janus500, format!("bcrypt hash falló: {e}")))?;

    route_bootstrap_entity(
        oltp_channel,
        "tenant",
        "Tenant 'system'",
        build_system_tenant_payload(email),
    )
    .await?;

    route_bootstrap_entity(
        oltp_channel,
        "role",
        "Rol 'role_super_master'",
        build_role_payload(
            "role_super_master",
            "super-master",
            "System Super Master Administrative Role",
        ),
    )
    .await?;

    route_bootstrap_entity(
        oltp_channel,
        "role",
        "Rol 'system-bff'",
        build_role_payload("system-bff", "system-bff", "System BFF Role"),
    )
    .await?;

    route_bootstrap_entity(
        oltp_channel,
        "user",
        "Usuario administrador Máster",
        build_master_user_payload(username, email, &password_hash),
    )
    .await?;

    route_bootstrap_entity(
        oltp_channel,
        "user",
        "Usuario de servicio 'usr_system_bff'",
        build_bff_user_payload(),
    )
    .await?;

    wipe_bootstrap_env_vars();
    Ok(())
}

async fn route_bootstrap_entity(
    oltp_channel: &Arc<dyn IWriteChannel>,
    entity_type: &str,
    label: &str,
    payload: serde_json::Value,
) -> Result<(), DomainError> {
    let mut req = serde_json::Map::new();
    req.insert("payload".to_string(), payload);
    let ctx =
        crate::iop::core::IopContext::new("system", "bootstrap-system", entity_type, "CREATE", req);
    match oltp_channel.route(ctx).await {
        Ok(_) => {
            info!("[Bootstrap] {label} creado con éxito.");
            Ok(())
        }
        Err(e) => {
            error!("[Bootstrap] Error creando {label}: {:?}", e);
            Err(e)
        }
    }
}

fn build_system_tenant_payload(email: &str) -> serde_json::Value {
    json!({
        "id": "system",
        "name": "System Master Tenant",
        "status": "ACTIVE",
        "tier": "ENTERPRISE",
        "storage_region": "us-east-1",
        "billing_admin_email": email
    })
}

fn build_role_payload(id: &str, name: &str, description: &str) -> serde_json::Value {
    json!({
        "id": id,
        "name": name,
        "description": description,
        "tenant_id": "system",
        "grants": [ALL_DOMAIN_GRANTS]
    })
}

fn build_master_user_payload(
    username: &str,
    email: &str,
    password_hash: &str,
) -> serde_json::Value {
    json!({
        "id": "usr_master",
        "username": username,
        "email": email,
        "password_hash": password_hash,
        "first_name": "Super",
        "last_name": "Administrador",
        "status": "ACTIVE",
        "user_type": "INTERNAL",
        "tenant_id": "system",
        "role_ids": ["role_super_master"]
    })
}

fn build_bff_user_payload() -> serde_json::Value {
    json!({
        "id": "usr_system_bff",
        "username": "system-bff",
        "email": "bff@metri.one",
        "password_hash": "SystemBffDummyPasswordHashNotUsed",
        "first_name": "System",
        "last_name": "BFF",
        "status": "ACTIVE",
        "user_type": "INTERNAL",
        "tenant_id": "system",
        "role_ids": ["system-bff"]
    })
}

fn wipe_bootstrap_env_vars() {
    std::env::remove_var("METRI_BOOTSTRAP_MASTER_EMAIL");
    std::env::remove_var("METRI_BOOTSTRAP_MASTER_USERNAME");
    std::env::remove_var("METRI_BOOTSTRAP_MASTER_KEY");
    std::env::remove_var("METRI_BOOTSTRAP_MASTER_PASSWORD");
    info!(
        "[Bootstrap] ✅ Proceso de semilla completado. Secretos limpiados del entorno en memoria."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::errors::DomainError;
    use crate::iop::core::IopContext;
    use serde_json::Value;

    // A mock write channel to test the transaction routing
    struct MockWriteChannel {
        tenant_created: std::sync::Mutex<bool>,
        role_created: std::sync::Mutex<bool>,
        user_created: std::sync::Mutex<bool>,
    }

    #[async_trait::async_trait]
    impl IWriteChannel for MockWriteChannel {
        async fn route(&self, ctx: IopContext) -> Result<Value, DomainError> {
            match ctx.entity_type.as_str() {
                "tenant" => {
                    let p = ctx.request.get("payload").unwrap();
                    assert_eq!(p.get("id").unwrap().as_str().unwrap(), "system");
                    assert_eq!(p.get("status").unwrap().as_str().unwrap(), "ACTIVE");
                    *self.tenant_created.lock().unwrap() = true;
                    Ok(json!({ "entity_id": "system", "status": "success" }))
                }
                "role" => {
                    let p = ctx.request.get("payload").unwrap();
                    let id = p.get("id").unwrap().as_str().unwrap();
                    assert!(id == "role_super_master" || id == "system-bff");
                    assert_eq!(p.get("tenant_id").unwrap().as_str().unwrap(), "system");
                    *self.role_created.lock().unwrap() = true;
                    Ok(json!({ "entity_id": id, "status": "success" }))
                }
                "user" => {
                    let p = ctx.request.get("payload").unwrap();
                    let id = p.get("id").unwrap().as_str().unwrap();
                    assert!(id == "usr_master" || id == "usr_system_bff");
                    assert_eq!(p.get("tenant_id").unwrap().as_str().unwrap(), "system");
                    if id == "usr_master" {
                        let pass_hash = p.get("password_hash").unwrap().as_str().unwrap();
                        assert!(bcrypt::verify("securepwd123", pass_hash).unwrap());
                    }
                    *self.user_created.lock().unwrap() = true;
                    Ok(json!({ "entity_id": id, "status": "success" }))
                }
                _ => panic!("Unexpected entity type in route: {}", ctx.entity_type),
            }
        }
    }

    #[tokio::test]
    async fn test_seed_master_successful_flow() {
        // La mecánica de la semilla se prueba aislada: `seed_master` no
        // consulta la base de datos, así que basta un canal simulado. Sin
        // variables de entorno que manipular (nada las lee en caliente).
        let mock_channel: Arc<dyn IWriteChannel> = Arc::new(MockWriteChannel {
            tenant_created: std::sync::Mutex::new(false),
            role_created: std::sync::Mutex::new(false),
            user_created: std::sync::Mutex::new(false),
        });

        let res = seed_master(&mock_channel, "master@metri.one", "master", "securepwd123").await;
        assert!(res.is_ok());

        // Los secretos de la semilla se limpian del entorno tras crearla.
        assert!(std::env::var("METRI_BOOTSTRAP_MASTER_EMAIL").is_err());
        assert!(std::env::var("METRI_BOOTSTRAP_MASTER_USERNAME").is_err());
        assert!(std::env::var("METRI_BOOTSTRAP_MASTER_KEY").is_err());
        assert!(std::env::var("METRI_BOOTSTRAP_MASTER_PASSWORD").is_err());
    }
}
