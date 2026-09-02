// ═══════════════════════════════════════════════════════════════
// METRI ENGINE — Cedar Policy Store
// Generado por cedar_pipeline.py — role.json + user_group.json
// Generado: 2026-04-30T20:41:27.412782Z
//
// PRINCIPIO: roles son DATOS en Datahike — no hardcodeados en Cedar.
// CedarAuthorizer hidrata principal.grants/scope/locations/assets
// desde Datahike en runtime antes de invocar Cedar.
// ═══════════════════════════════════════════════════════════════

// P00 — Analítica: usuario con grant en el dominio solicitado y scope != NONE. El scope efectivo (ALL/OWN/ASSIGNED/OWN_OR_ASSIGNED) vive en Datahike — Cedar solo verifica su presencia. Janus inyecta el predicado concreto en el AST IR.
permit(
    principal is Metri::User,
    action in Metri::Action::"ActionGroup::\"analytical\"",
    resource is Metri::MetriResource
)
when {
    (principal.grants.contains(resource.required_grant) && principal.query_scope != "NONE") &&
    (principal.tenant_id == "system" || principal.tenant_id == resource.tenant_id)
};

// P01 — Mutacional: usuario con grant sobre el dominio solicitado. domain-boundaries={} (Cedar validó ABAC in-memory — sin RLS Push-Down). El nombre del dominio viene de Datahike — no hardcodeado.
permit(
    principal is Metri::User,
    action in Metri::Action::"ActionGroup::\"mutational\"",
    resource is Metri::MetriResource
)
when {
    (principal.grants.contains(resource.required_grant)) &&
    (principal.tenant_id == "system" || principal.tenant_id == resource.tenant_id)
};

// P02 — Sistema/Infraestructura: usuario con grant sobre el tipo de recurso para acciones de sistema (MatchRoutingRules, etc.). El grant viene de Datahike — ningún nombre de dominio está hardcodeado en Cedar.
permit(
    principal is Metri::User,
    action in Metri::Action::"ActionGroup::\"system\"",
    resource is Metri::MetriResource
)
when {
    (principal.grants.contains(resource.required_grant)) &&
    (principal.tenant_id == "system" || principal.tenant_id == resource.tenant_id)
};

// P03 — Zero-Trust CARDINAL: FORBID cross-tenant. Precedencia sobre todos los permits. NUNCA eliminar.
forbid(
    principal is Metri::User,
    action,
    resource is Metri::MetriResource
)
when {
    principal.tenant_id != "system" &&
    principal.tenant_id != resource.tenant_id
};

// P04 — scope NONE: denegación explícita. role.grants[domain].scope=NONE → Janus emite :JANUS_400
forbid(
    principal is Metri::User,
    action in Metri::Action::"ActionGroup::\"analytical\"",
    resource is Metri::MetriResource
)
when {
    resource.query_scope == "NONE"
};

// P05 — SUSPENDED: usuario bloqueado en Datahike. Efecto inmediato en el próximo request (sin gap de TTL)
forbid(
    principal is Metri::User,
    action,
    resource
)
when {
    principal.status == "SUSPENDED"
};

// P06 — CONTRACTOR-ISOLATED: Prohibir acceso a contratistas si el recurso no está asignado a su compañía
forbid(
    principal is Metri::User,
    action,
    resource is Metri::MetriResource
)
when {
    principal.user_type == "CONTRACTOR" &&
    (! (resource has assigned_company_id) || resource.assigned_company_id != principal.company_id)
};

// P07 — SYSTEM-ONLY ENTITIES: Solo los usuarios del tenant "system" pueden acceder a entidades de sistema (tenant, quota, domain_quota, domain_plugin, tenant_plugin)
forbid(
    principal is Metri::User,
    action,
    resource is Metri::MetriResource
)
when {
    resource has entity_type &&
    (resource.entity_type == "tenant" || resource.entity_type == "quota" || resource.entity_type == "domain_quota" || resource.entity_type == "domain_plugin" || resource.entity_type == "tenant_plugin") &&
    principal.tenant_id != "system"
};



