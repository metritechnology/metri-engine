//! DTO de petición de autorización (`AuthRequest`) y adaptador de transporte desde `tonic`.
//!
//! El núcleo de Cedar no acopla con `tonic`: opera únicamente sobre este DTO de cabeceras.
//! El constructor `from_tonic` extrae los metadatos HTTP/2 en la frontera de transporte,
//! mientras que las pruebas construyen `AuthRequest` directamente.

/// DTO agnóstico de transporte con las cabeceras requeridas para autenticar y autorizar una petición.
pub struct AuthRequest<'a> {
    /// Valor de la cabecera `authorization` (por ejemplo, `Bearer mk_...`).
    pub authorization: Option<&'a str>,
    /// Valor de la cabecera alternativa de sesión `sid`.
    pub sid: Option<&'a str>,
    /// Acción solicitada en la cabecera `x-metri-action`.
    pub action: Option<&'a str>,
    /// Tipo de entidad objetivo indicado en `x-metri-entity-type`.
    pub entity_type: Option<&'a str>,
    /// Identificador de la entidad objetivo indicado en `x-metri-entity-id`.
    pub entity_id: Option<&'a str>,
    /// Lista de dominios separados por coma en `x-metri-domains`.
    pub domains: Option<&'a str>,
    /// Cabecera `test-tenant` empleada únicamente cuando el bypass de desarrollo está activo.
    pub dev_test_tenant: Option<&'a str>,
    /// Cabecera `test-user` empleada únicamente cuando el bypass de desarrollo está activo.
    pub dev_test_user: Option<&'a str>,
    /// Cabecera `test-roles` empleada únicamente cuando el bypass de desarrollo está activo.
    pub dev_test_roles: Option<&'a str>,
}

impl<'a> AuthRequest<'a> {
    /// Extrae las cabeceras de autenticación y autorización desde un `tonic::Request<T>`.
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
