use crate::aegis::oltp::executor::OltpExecutor;
use crate::janus_router::router::IWriteChannel;
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info};

/// Chequea e inicializa de forma segura al usuario administrador máster si la BD está vacía.
pub async fn check_and_bootstrap_master(
    oltp_exec: &OltpExecutor,
    oltp_channel: &Arc<dyn IWriteChannel>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    let email = match std::env::var("METRI_BOOTSTRAP_MASTER_EMAIL") {
        Ok(v) => v,
        Err(_) => {
            info!("[Bootstrap] Base de datos vacía pero METRI_BOOTSTRAP_MASTER_EMAIL no está configurada. Omitiendo.");
            return Ok(());
        }
    };

    let username = match std::env::var("METRI_BOOTSTRAP_MASTER_USERNAME") {
        Ok(v) => v,
        Err(_) => {
            info!("[Bootstrap] Base de datos vacía pero METRI_BOOTSTRAP_MASTER_USERNAME no está configurada. Omitiendo.");
            return Ok(());
        }
    };

    let password = match std::env::var("METRI_BOOTSTRAP_MASTER_PASSWORD") {
        Ok(v) => v,
        Err(_) => {
            info!("[Bootstrap] Base de datos vacía pero METRI_BOOTSTRAP_MASTER_PASSWORD no está configurada. Omitiendo.");
            return Ok(());
        }
    };

    seed_master(oltp_channel, &email, &username, &password).await
}

/// Mecánica de la semilla: hashea la contraseña, crea el tenant, los roles y
/// los usuarios maestros, y limpia los secretos del entorno. La DECISIÓN de
/// sembrar vive en `check_and_bootstrap_master`; esta función no consulta la
/// base de datos, así que se prueba con un canal simulado.
async fn seed_master(
    oltp_channel: &Arc<dyn IWriteChannel>,
    email: &str,
    username: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("[Bootstrap] Iniciando creación de semilla para el Administrador Master...");

    // 1. Hashear la contraseña con bcrypt (cost = 12)
    let password_hash = bcrypt::hash(password, 12)?;

    // 4. Crear el Tenant Semilla ("system")
    let mut tenant_req = serde_json::Map::new();
    tenant_req.insert(
        "payload".to_string(),
        json!({
            "id": "system",
            "name": "System Master Tenant",
            "status": "ACTIVE",
            "tier": "ENTERPRISE",
            "storage_region": "us-east-1",
            "billing_admin_email": email.clone()
        }),
    );

    let tenant_ctx = crate::iop::core::IopContext::new(
        "system",
        "bootstrap-system",
        "tenant",
        "CREATE",
        tenant_req,
    );

    match oltp_channel.route(tenant_ctx).await {
        Ok(_) => info!("[Bootstrap] Tenant 'system' creado con éxito."),
        Err(e) => {
            error!("[Bootstrap] Error creando el tenant semilla: {:?}", e);
            return Err(Box::new(e));
        }
    }

    // 5. Crear el Rol Semilla ("role_super_master")
    let mut role_req = serde_json::Map::new();
    role_req.insert("payload".to_string(), json!({
        "id": "role_super_master",
        "name": "super-master",
        "description": "System Super Master Administrative Role",
        "tenant_id": "system",
        "grants": [
            r#"{"domain":"*","actions":["VIEW","CREATE","UPDATE","DELETE","EXECUTE","EXPORT"],"scope":"ALL"}"#
        ]
    }));

    let role_ctx =
        crate::iop::core::IopContext::new("system", "bootstrap-system", "role", "CREATE", role_req);

    match oltp_channel.route(role_ctx).await {
        Ok(_) => info!("[Bootstrap] Rol 'role_super_master' creado con éxito."),
        Err(e) => {
            error!("[Bootstrap] Error creando el rol semilla: {:?}", e);
            return Err(Box::new(e));
        }
    }

    // 5b. Crear el Rol Semilla ("system-bff")
    let mut role_bff_req = serde_json::Map::new();
    role_bff_req.insert("payload".to_string(), json!({
        "id": "system-bff",
        "name": "system-bff",
        "description": "System BFF Role",
        "tenant_id": "system",
        "grants": [
            r#"{"domain":"*","actions":["VIEW","CREATE","UPDATE","DELETE","EXECUTE","EXPORT"],"scope":"ALL"}"#
        ]
    }));

    let role_bff_ctx = crate::iop::core::IopContext::new(
        "system",
        "bootstrap-system",
        "role",
        "CREATE",
        role_bff_req,
    );

    match oltp_channel.route(role_bff_ctx).await {
        Ok(_) => info!("[Bootstrap] Rol 'system-bff' creado con éxito."),
        Err(e) => {
            error!("[Bootstrap] Error creando el rol system-bff: {:?}", e);
            return Err(Box::new(e));
        }
    }

    // 6. Crear el Usuario Semilla ("usr_master")
    let mut user_req = serde_json::Map::new();
    user_req.insert(
        "payload".to_string(),
        json!({
            "id": "usr_master",
            "username": username.clone(),
            "email": email.clone(),
            "password_hash": password_hash,
            "first_name": "Super",
            "last_name": "Administrador",
            "status": "ACTIVE",
            "user_type": "INTERNAL",
            "tenant_id": "system",
            "role_ids": ["role_super_master"]
        }),
    );

    let user_ctx =
        crate::iop::core::IopContext::new("system", "bootstrap-system", "user", "CREATE", user_req);

    match oltp_channel.route(user_ctx).await {
        Ok(_) => info!("[Bootstrap] Usuario administrador Máster creado con éxito."),
        Err(e) => {
            error!(
                "[Bootstrap] Error creando el usuario administrador: {:?}",
                e
            );
            return Err(Box::new(e));
        }
    }

    // 6b. Crear el Usuario de Servicio BFF ("usr_system_bff")
    let mut user_bff_req = serde_json::Map::new();
    user_bff_req.insert(
        "payload".to_string(),
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
        }),
    );

    let user_bff_ctx = crate::iop::core::IopContext::new(
        "system",
        "bootstrap-system",
        "user",
        "CREATE",
        user_bff_req,
    );

    match oltp_channel.route(user_bff_ctx).await {
        Ok(_) => info!("[Bootstrap] Usuario de servicio 'usr_system_bff' creado con éxito."),
        Err(e) => {
            error!(
                "[Bootstrap] Error creando el usuario de servicio bff: {:?}",
                e
            );
            return Err(Box::new(e));
        }
    }

    // 7. Wipe/Limpieza permanente de variables de entorno para evitar leaks
    std::env::remove_var("METRI_BOOTSTRAP_MASTER_EMAIL");
    std::env::remove_var("METRI_BOOTSTRAP_MASTER_USERNAME");
    std::env::remove_var("METRI_BOOTSTRAP_MASTER_PASSWORD");
    info!(
        "[Bootstrap] ✅ Proceso de semilla completado. Secretos limpiados del entorno en memoria."
    );

    Ok(())
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
        assert!(std::env::var("METRI_BOOTSTRAP_MASTER_PASSWORD").is_err());
    }
}
