use crate::grpc::service::MetriGrpcService;
use crate::janus_router::oltp_channel::extract_entity_id;
use tonic::Status;

// Handlers del MetriGrpcService — fase 2: service.rs delega, aquí vive el cuerpo.

/// Formato canónico de un grant `dominio:accion` — patrón literal.
#[allow(clippy::unwrap_used)] // invariante allowlisted (PLAN_PATRON_RESULT.md R7)
static GRANT_FORMAT_RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
    regex::Regex::new(r"^([a-zA-Z0-9_\-]+|\*):[a-zA-Z0-9_\-*]+$").unwrap()
});

impl MetriGrpcService {
    pub(crate) fn validate_role_grants(
        &self,
        grants_value: &serde_json::Value,
    ) -> Result<(), Status> {
        let re = &*GRANT_FORMAT_RE;
        let validate_pair = |domain: &str, action: &str| -> Result<(), Status> {
            let pair = format!("{}:{}", domain, action);
            if !re.is_match(&pair) {
                return Err(Status::invalid_argument(format!(
                    "Invalid grant format '{}'. Grants must match pattern '^[a-zA-Z0-9_\\-]+:[a-zA-Z0-9_\\-*]+$'.",
                    pair
                )));
            }
            // F5 · Validación semántica: la ACCIÓN debe existir en el registro
            // (las custom de approval incluidas). El dominio desconocido solo
            // advierte — el Códice puede no listar dominios de plugins en alta.
            const KNOWN_ACTIONS: &[&str] = &[
                "VIEW", "CREATE", "UPDATE", "DELETE", "UPSERT", "EXECUTE", "EXPORT",
                "APPROVE", "REJECT", "DELEGATE", "REVOKE",
            ];
            if action != "*" && !KNOWN_ACTIONS.contains(&action) {
                return Err(Status::invalid_argument(format!(
                    "Invalid grant action '{}' in '{}'. Known actions: {}.",
                    action,
                    pair,
                    KNOWN_ACTIONS.join(", ")
                )));
            }
            if !crate::cedar::is_known_domain(domain) {
                tracing::warn!(
                    domain = %domain,
                    "[validate_role_grants] Dominio de grant no conocido por el Códice: '{}' (se acepta, pero revisa el nombre)", domain
                );
            }
            Ok(())
        };

        let elements = if let Some(s) = grants_value.as_str() {
            if s.starts_with('[') {
                serde_json::from_str::<serde_json::Value>(s).map_err(|e| {
                    Status::invalid_argument(format!("Failed to parse grants JSON string: {}", e))
                })?
            } else {
                serde_json::Value::Array(vec![serde_json::Value::String(s.to_string())])
            }
        } else {
            grants_value.clone()
        };

        if let Some(arr) = elements.as_array() {
            for item in arr {
                match item {
                    serde_json::Value::String(s) => {
                        if s.starts_with('{') {
                            if let Ok(serde_json::Value::Object(obj)) =
                                serde_json::from_str::<serde_json::Value>(s)
                            {
                                let domain = obj
                                    .get("domain")
                                    .and_then(|v| v.as_str())
                                    .ok_or_else(|| {
                                        Status::invalid_argument(
                                            "Grant object missing 'domain' field",
                                        )
                                    })?;

                                if let Some(actions_val) = obj.get("actions") {
                                    if let Some(actions_arr) = actions_val.as_array() {
                                        for act_val in actions_arr {
                                            let action = act_val.as_str().ok_or_else(|| {
                                                Status::invalid_argument(
                                                    "Grant action must be a string",
                                                )
                                            })?;
                                            validate_pair(domain, action)?;
                                        }
                                    } else if let Some(action_str) = actions_val.as_str() {
                                        validate_pair(domain, action_str)?;
                                    } else {
                                        return Err(Status::invalid_argument(
                                            "Grant 'actions' field must be an array or string",
                                        ));
                                    }
                                } else {
                                    validate_pair(domain, "*")?;
                                }
                                continue;
                            }
                        }
                        if !re.is_match(s) {
                            return Err(Status::invalid_argument(format!(
                                "Invalid grant format '{}'. Grants must match pattern '^[a-zA-Z0-9_\\-]+:[a-zA-Z0-9_\\-*]+$'.",
                                s
                            )));
                        }
                    }
                    serde_json::Value::Object(obj) => {
                        let domain =
                            obj.get("domain").and_then(|v| v.as_str()).ok_or_else(|| {
                                Status::invalid_argument("Grant object missing 'domain' field")
                            })?;

                        if let Some(actions_val) = obj.get("actions") {
                            if let Some(actions_arr) = actions_val.as_array() {
                                for act_val in actions_arr {
                                    let action = act_val.as_str().ok_or_else(|| {
                                        Status::invalid_argument("Grant action must be a string")
                                    })?;
                                    validate_pair(domain, action)?;
                                }
                            } else if let Some(action_str) = actions_val.as_str() {
                                validate_pair(domain, action_str)?;
                            } else {
                                return Err(Status::invalid_argument(
                                    "Grant 'actions' field must be an array or string",
                                ));
                            }
                        } else {
                            validate_pair(domain, "*")?;
                        }
                    }
                    _ => {
                        return Err(Status::invalid_argument(
                            "Grant item must be a string or object",
                        ));
                    }
                }
            }
        } else if let Some(obj) = elements.as_object() {
            let domain = obj
                .get("domain")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Status::invalid_argument("Grant object missing 'domain' field"))?;
            if let Some(actions_val) = obj.get("actions") {
                if let Some(actions_arr) = actions_val.as_array() {
                    for act_val in actions_arr {
                        let action = act_val.as_str().ok_or_else(|| {
                            Status::invalid_argument("Grant action must be a string")
                        })?;
                        validate_pair(domain, action)?;
                    }
                } else if let Some(action_str) = actions_val.as_str() {
                    validate_pair(domain, action_str)?;
                } else {
                    return Err(Status::invalid_argument(
                        "Grant 'actions' field must be an array or string",
                    ));
                }
            } else {
                validate_pair(domain, "*")?;
            }
        } else {
            return Err(Status::invalid_argument(
                "Grants field must be an array, string, or object",
            ));
        }

        Ok(())
    }

    pub(crate) async fn validate_single_mutation(
        &self,
        tenant_id: &str,
        entity_type: &str,
        action: &str,
        payload: &serde_json::Value,
        principal: &crate::cedar::PrincipalData,
    ) -> Result<(), Status> {
        // Enforce tenant isolation (bypass for master tenant or system BFF account)
        if let Err(e) = crate::cedar::SystemSecurityRules::check_tenant_isolation(
            tenant_id,
            &principal.tenant_id,
            &principal.user_id,
        ) {
            return Err(Status::permission_denied(e.detail));
        }

        let entity_id = extract_entity_id(payload).unwrap_or_default();

        if entity_type == "tenant" {
            if let Err(_) = crate::cedar::SystemSecurityRules::check_tenant_isolation(
                &entity_id,
                &principal.tenant_id,
                &principal.user_id,
            ) {
                return Err(Status::permission_denied(
                    "Auth403: Cannot mutate other tenant",
                ));
            }
        }

        // ── Autoservicio con GRANT (F4 de PLAN_PERMISOS_SYSTEM_CORE.md) ────
        //
        // Dos filas de SISTEMA se gestionan desde el panel del PROPIO tenant:
        //   · `tenant_plugin` — configuración de módulos (/settings/plugins).
        //   · `tenant`        — la propia cuenta (entity_id == llamante):
        //     branding, idioma, moneda, settings operativos.
        // Autorización en DOS condiciones: dueño de la fila (el aislamiento de
        // arriba ya lo garantizó) Y el GRANT `entidad:acción` en los roles del
        // llamante — el grant del System Core tiene dientes, no es decorativo.
        // Master, BFF de sistema y roles admin quedan exentos.
        let own_tenant_row = entity_type == "tenant"
            && (entity_id == principal.tenant_id || tenant_id == principal.tenant_id);
        let is_self_service = crate::cedar::SystemSecurityRules::is_self_service_row(
            entity_type,
            tenant_id,
            &principal.tenant_id,
        ) || own_tenant_row;

        if is_self_service {
            let is_master = crate::cedar::rules::is_master_tenant(&principal.tenant_id)
                || principal.user_id == "usr_system_bff";
            let is_admin_like = principal
                .roles_boundaries
                .iter()
                .any(|b| b.role_id == "admin" || b.role_id == "tenant-admin");

            if !is_master && !is_admin_like {
                let grant_action = if action == "UPSERT" { "UPDATE" } else { action };
                let required = format!("{entity_type}:{grant_action}");
                let granted = crate::cedar::principal_grant_keys(principal);
                if !granted.contains(&required) {
                    return Err(Status::permission_denied(format!(
                        "Auth403: requiere el permiso {required} (solicítalo al administrador del tenant)"
                    )));
                }
            }

            // Gate maestro y ABAC de roles de dominio no aplican al
            // self-service: ninguna política de rol declara entidades de
            // sistema, así que evaluarse aquí solo puede DENY.
            return Ok(());
        }

        // Enforce that only master tenant users or system BFF account can mutate tenants and quotas.
        if let Err(e) = crate::cedar::SystemSecurityRules::check_crud_authorization(
            entity_type,
            &principal.tenant_id,
            &principal.user_id,
            "mutate",
        ) {
            return Err(Status::permission_denied(e.detail));
        }

        let mut resource = serde_json::json!({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "domains": vec![entity_type.to_string()],
        });

        // Hydrate assigned_company_id if possible
        let mut assigned_company_id: Option<String> = None;
        for key in &[
            "assigned_company_id",
            "company_id",
            "assigned_company",
            "company",
        ] {
            if let Some(val) = payload.get(*key).and_then(|v| v.as_str()) {
                assigned_company_id = Some(val.to_string());
                break;
            }
        }

        if assigned_company_id.is_none()
            && (action == "UPDATE" || action == "DELETE")
            && !entity_id.is_empty()
        {
            let eav_reader = self.oltp_executor.pull_reader();
            if let Ok(entity_map) = eav_reader.pull(tenant_id, &entity_id, None).await {
                for key in &[
                    "assigned_company_id",
                    "company_id",
                    "assigned_company",
                    "company",
                ] {
                    if let Some(val) = entity_map
                        .get(*key)
                        .or_else(|| entity_map.get(&format!("{}/{}", entity_type, key)))
                    {
                        match val {
                            crate::eav::types::datom::DatomValue::Str(s) => {
                                assigned_company_id = Some(s.clone());
                                break;
                            }
                            crate::eav::types::datom::DatomValue::Ref(r) => {
                                assigned_company_id = Some(r.to_string());
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        if let Some(comp_id) = assigned_company_id {
            if let Some(obj) = resource.as_object_mut() {
                obj.insert(
                    "assigned_company_id".to_string(),
                    serde_json::json!(comp_id),
                );
            }
        }

        if self.dev_auth_bypass.is_bypass() {
            return Ok(());
        }

        crate::cedar::step4_evaluate_cedar(
            &self.cedar_engine,
            &self.policy_cache,
            principal,
            action,
            &resource,
        )
        .map_err(|err| Status::permission_denied(format!("Auth403: {}", err.detail)))?;

        Ok(())
    }

    pub(crate) async fn validate_user_group_hierarchy_cycle(
        &self,
        tenant_id: &str,
        entity_id: &str,
        actual_payload: &serde_json::Value,
    ) -> Result<(), Status> {
        let parent_id = actual_payload
            .get("parent_user_group_id")
            .or_else(|| actual_payload.get("user_group/parent_user_group_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if !parent_id.is_empty() {
            if !entity_id.is_empty() && parent_id == entity_id {
                return Err(Status::invalid_argument(
                    "Circular dependency detected: a group cannot be its own parent",
                ));
            }

            let eav_reader = self.oltp_executor.pull_reader();
            let mut current_id = parent_id.to_string();
            let mut visited = std::collections::HashSet::new();
            visited.insert(entity_id.to_string());

            for _depth in 0..10 {
                if current_id.is_empty() {
                    break;
                }
                if !visited.insert(current_id.clone()) {
                    return Err(Status::invalid_argument(
                        "Circular dependency detected in user groups",
                    ));
                }

                if let Ok(group_map) = eav_reader.pull(tenant_id, &current_id, None).await {
                    if group_map.is_empty() {
                        break;
                    }
                    let next_parent = group_map
                        .get("parent_user_group_id")
                        .or_else(|| group_map.get("user_group/parent_user_group_id"))
                        .and_then(|v| match v {
                            crate::eav::types::datom::DatomValue::Str(s) => Some(s.clone()),
                            _ => None,
                        })
                        .unwrap_or_default();
                    current_id = next_parent;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn evict_caches_after_mutation(
        &self,
        tenant_id: String,
        entity_type: String,
        entity_id: String,
        actual_payload: &serde_json::Value,
    ) {
        if entity_type == "user" || entity_type == "role" || entity_type == "user_group" {
            let entity_id_opt = if !entity_id.is_empty() {
                Some(entity_id)
            } else {
                extract_entity_id(actual_payload)
            };
            if let Some(eid) = entity_id_opt {
                let msg = crate::cedar::InvalidationMsg {
                    tenant_id,
                    entity_type,
                    entity_id: eid,
                };
                self.invalidation_bus.publish(msg);
            }
        }
    }
}
