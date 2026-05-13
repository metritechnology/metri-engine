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
    action in ActionGroup::"analytical",
    resource is Metri::MetriResource
)
when {
    (principal.grants.containsAny(resource.entity_type) && principal.query_scope != "NONE") &&
    principal.tenant_id == resource.tenant_id
};

// P01 — Mutacional: usuario con grant sobre el dominio solicitado. domain-boundaries={} (Cedar validó ABAC in-memory — sin RLS Push-Down). El nombre del dominio viene de Datahike — no hardcodeado.
permit(
    principal is Metri::User,
    action in ActionGroup::"mutational",
    resource is Metri::MetriResource
)
when {
    (principal.grants.containsAny(resource.entity_type)) &&
    principal.tenant_id == resource.tenant_id
};

// P02 — Sistema/Infraestructura: usuario con grant sobre el tipo de recurso para acciones de sistema (MatchRoutingRules, etc.). El grant viene de Datahike — ningún nombre de dominio está hardcodeado en Cedar.
permit(
    principal is Metri::User,
    action in ActionGroup::"system",
    resource is Metri::MetriResource
)
when {
    (principal.grants.containsAny(resource.entity_type)) &&
    principal.tenant_id == resource.tenant_id
};

// P03 — Zero-Trust CARDINAL: FORBID cross-tenant. Precedencia sobre todos los permits. NUNCA eliminar.
forbid(
    principal is Metri::User,
    action,
    resource is Metri::MetriResource
)
when {
    principal.tenant_id != resource.tenant_id
};

// P04 — scope NONE: denegación explícita. role.grants[domain].scope=NONE → Janus emite :JANUS_400
forbid(
    principal is Metri::User,
    action in ActionGroup::"analytical",
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
