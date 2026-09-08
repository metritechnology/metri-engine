// cedar/request.rs — DTO de petición de autorización y adapter de transporte.
//
// El core de Cedar no conoce tonic: solo necesita estas cabeceras. El adapter
// `from_tonic` vive aquí (capa de transporte del módulo); los tests construyen
// el DTO directamente, sin requests sintéticos.

pub struct AuthRequest<'a> {
    pub authorization: Option<&'a str>,
    pub sid: Option<&'a str>,
    pub action: Option<&'a str>,
    pub entity_type: Option<&'a str>,
    pub entity_id: Option<&'a str>,
    pub domains: Option<&'a str>,
    /// Cabeceras del bypass de desarrollo (decidido en la raíz de composición).
    pub dev_test_tenant: Option<&'a str>,
    pub dev_test_user: Option<&'a str>,
    pub dev_test_roles: Option<&'a str>,
}

impl<'a> AuthRequest<'a> {
    pub fn from_tonic<T>(req: &'a tonic::Request<T>) -> Self {
        let header = |name: &str| req.metadata().get(name).and_then(|v| v.to_str().ok());
        AuthRequest {
            authorization: header("authorization"),
            sid: header("sid"),
            action: header("x-metri-action"),
            entity_type: header("x-metri-entity-type"),
            entity_id: header("x-metri-entity-id"),
            domains: header("x-metri-domains"),
            dev_test_tenant: header("test-tenant"),
            dev_test_user: header("test-user"),
            dev_test_roles: header("test-roles"),
        }
    }

    /// Token de sesión: `authorization` (con o sin `Bearer`) o `sid`.
    pub fn session_token(&self) -> Option<String> {
        if let Some(auth_header) = self.authorization {
            Some(
                auth_header
                    .strip_prefix("Bearer ")
                    .or_else(|| auth_header.strip_prefix("bearer "))
                    .unwrap_or(auth_header)
                    .trim()
                    .to_string(),
            )
        } else {
            self.sid.map(|s| s.trim().to_string())
        }
    }

    /// Acción Cedar pedida (default histórico: QueryMetrics).
    pub fn cedar_action(&self) -> String {
        self.action.unwrap_or("QueryMetrics").to_string()
    }

    /// Recurso Cedar sintetizado desde las cabeceras x-metri-*.
    pub fn cedar_resource(&self) -> serde_json::Value {
        let entity_type = self.entity_type.unwrap_or("project");
        let domains: Vec<String> = self
            .domains
            .map(|s| {
                s.split(',')
                    .map(|d| d.trim().to_string())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_else(|| vec![entity_type.to_string()]);
        let entity_id = self.entity_id.unwrap_or("");

        serde_json::json!({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "domains": domains,
        })
    }
}
