"""
cedar_policy_engine.py — Derivador de políticas Cedar desde los modelos del Códice.

Responsabilidad única: dado el conjunto de modelos del Códice (role.json,
user.json, user_group.json) y el contrato Cedar-Janus (cedar-janus-contract-v1.edn),
produce:

  1. CedarDomainPolicy   — política Cedar derivada de role.grants (por dominio)
  2. CedarPrincipalSpec  — EntityType Cedar 'User' con todos sus atributos ZT
  3. CedarActionMatrix   — Actions Cedar derivadas de role.grants.actions
  4. CedarContextSpec    — :metri.cedar/context-invariant como Malli spec Python

Pipeline de derivación:
  ┌─────────────────────────────────────────────────────────┐
  │  role.json (grants: [{domain, actions, scope}])         │
  │  user.json (role_ids, group_ids, tenant_id, status)     │
  │  user_group.json (allowed_locations, time_restrictions) │
  │         │                                               │
  │  CedarPolicyEngine.derive()                             │
  │         │                                               │
  │  ┌──────┴────────────────────────────────────────┐      │
  │  │  CedarEntitySchema  — Principal / Resource    │      │
  │  │  CedarActionMatrix  — por dominio + acción    │      │
  │  │  CedarPolicySet     — permit por scope        │      │
  │  │  CedarContextSpec   — Malli invariante        │      │
  │  └────────────────────────────────────────────────┘     │
  │         │                                               │
  │  emit_cedar_policy_edn() → :metri.cedar/* section       │
  └─────────────────────────────────────────────────────────┘

ALINEACIÓN con cedar-janus-contract-v1.edn:
  • :metri.cedar/query-scope   → ["ALL","OWN","ASSIGNED","OWN_OR_ASSIGNED","NONE"]
  • :metri.cedar/permission-boundary → {query-scope, permitted-locations, permitted-assets}
  • :metri.cedar/cedar-ctx     → {tenant-id, user-id, roles, domain-boundaries, request}

INVARIANTES Zero-Trust:
  • tenant_id en el Principal (User) y Resource (MetriResource) — SIEMPRE
  • FORBID explícito cross-tenant (P_CROSS_TENANT)
  • NONE scope → :JANUS_400 inmediato
  • SUSPENDED user status → :ABAC_403 inmediato
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Constantes del dominio alineadas con el contrato v1
# ─────────────────────────────────────────────────────────────────────────────

# Scopes válidos según :metri.cedar/query-scope
VALID_SCOPES = ("ALL", "OWN", "ASSIGNED", "OWN_OR_ASSIGNED", "NONE")

# Acciones mutacionales que Cedar evalúa en memoria (domain-boundaries = {})
MUTATIONAL_ACTIONS = frozenset({"CREATE", "UPDATE", "DELETE", "UPSERT", "EXECUTE"})

# Acciones analíticas que producen domain-boundaries con RLS Push-Down
ANALYTICAL_ACTIONS = frozenset({"VIEW", "EXPORT"})

# Acciones Cedar canónicas (Cedar usa strings, no keywords)
ALL_DOMAIN_ACTIONS = MUTATIONAL_ACTIONS | ANALYTICAL_ACTIONS


# ─────────────────────────────────────────────────────────────────────────────
# Tipos del dominio Cedar
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CedarAttribute:
    """Un atributo de un EntityType Cedar."""
    name: str
    cedar_type: str       # String | Long | Bool | Set<String> | Entity<Namespace::Type>
    required: bool = True
    doc: str = ""


@dataclass
class CedarEntityType:
    """Un EntityType Cedar con su namespace y atributos."""
    name: str                                   # e.g. "User", "MetriResource"
    namespace: str = "Metri"
    attributes: list[CedarAttribute] = field(default_factory=list)
    member_of: list[str] = field(default_factory=list)
    doc: str = ""

    @property
    def fqn(self) -> str:
        return f"{self.namespace}::{self.name}"


@dataclass
class CedarAction:
    """Una Action Cedar derivada de role.grants.actions."""
    name: str             # e.g. "VIEW_work_order", "CREATE_asset"
    domain: str           # e.g. "work_order", "asset"
    raw_action: str       # e.g. "VIEW", "CREATE"
    group: str            # "analytical" | "mutational"
    applies_to_principals: list[str] = field(default_factory=lambda: ["User"])
    applies_to_resources:  list[str] = field(default_factory=lambda: ["MetriResource"])
    doc: str = ""


@dataclass
class CedarScopePolicy:
    """
    Una política Cedar derivada de un scope + domain + action.

    Mapeo scope → restricción Cedar (alineado con :metres.cedar/query-scope):
      ALL             → sin predicado de propiedad (solo tenant isolation)
      OWN             → when { principal.user_id == resource.owner_field }
      ASSIGNED        → when { principal.user_id == resource.assignee_field }
      OWN_OR_ASSIGNED → when { ... || ... }
      NONE            → policy DENY (fallback) — Janus emite :JANUS_400
    """
    policy_id: str
    effect: str           # "permit" | "forbid"
    domain: str
    raw_action: str       # "VIEW" | "CREATE" | ...
    scope: str            # uno de VALID_SCOPES
    when_clause: str      # expresión Cedar inline
    comment: str = ""


@dataclass
class CedarPolicySet:
    """Colección completa de políticas Cedar derivadas del Códice."""
    entity_types: list[CedarEntityType] = field(default_factory=list)
    actions:      list[CedarAction]     = field(default_factory=list)
    policies:     list[CedarScopePolicy] = field(default_factory=list)

    # Invariantes del contrato v1
    cross_tenant_forbid: bool = True        # P06 equivalente siempre presente
    context_invariant_spec: dict[str, Any] = field(default_factory=dict)

    # Trazabilidad
    source_models: list[str] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Builders de EntityTypes
# ─────────────────────────────────────────────────────────────────────────────

def _build_user_entity(user_model: dict) -> CedarEntityType:
    """
    Cedar EntityType 'User' derivado de user.json.

    Toma los atributos del modelo y los traduce al tipo Cedar correspondiente.
    Los atributos user_id, tenant_id, roles son OBLIGATORIOS (Zero-Trust).
    """
    attrs = [
        CedarAttribute("user_id",   "String",       required=True,
                       doc="UUID único del usuario — identidad soberana"),
        CedarAttribute("tenant_id", "String",       required=True,
                       doc="UUID del tenant — Zero-Trust boundary (del Valkey session)"),
        CedarAttribute("roles",     "Set<String>",  required=True,
                       doc="Nombres de roles activos — derivados de role_ids → role.name"),
        CedarAttribute("status",    "String",       required=True,
                       doc="ACTIVE | INACTIVE | SUSPENDED — SUSPENDED → :ABAC_403"),
        CedarAttribute("email",     "String",       required=False,
                       doc="Email del usuario"),
    ]

    # Añadir atributos dimensionales adicionales del modelo
    for attr_def in user_model.get("attributes", []):
        name = attr_def.get("name", "")
        if name in {"id", "email", "status", "role_ids", "group_ids", "tenant_id",
                    "first_name", "last_name"}:
            continue  # ya cubiertos arriba o no son atributos Cedar
        attrs.append(CedarAttribute(
            name=name,
            cedar_type="String",
            required=attr_def.get("required", False),
        ))

    return CedarEntityType(
        name="User",
        namespace="Metri",
        attributes=attrs,
        doc="Principal Cedar — sujeto de autorización en el ecosistema Metri",
    )


def _build_resource_entity(role_model: dict, user_group_model: dict) -> CedarEntityType:
    """
    Cedar EntityType 'MetriResource' derivado del dominio de roles.

    Los dominios disponibles provienen de role.grants[*].domain.
    Los campos owner_field y assignee_field se resuelven en runtime por Janus
    vía codice/load-schema (según :metre.cedar/scope-field-resolution).
    """
    attrs = [
        CedarAttribute("tenant_id",    "String",  required=True,
                       doc="UUID del tenant del recurso — Zero-Trust boundary"),
        CedarAttribute("entity_type",  "String",  required=True,
                       doc="Tipo de entidad: work_order, asset, location, ..."),
        CedarAttribute("rpc",          "String",  required=True,
                       doc="RPC de origen: Query, Transact, BulkIngest, ..."),
        CedarAttribute("owner_field",  "String",  required=False,
                       doc="Campo del propietario — resuelto por Códice (scope OWN)"),
        CedarAttribute("assignee_field","String", required=False,
                       doc="Campo del asignado — resuelto por Códice (scope ASSIGNED)"),
    ]
    return CedarEntityType(
        name="MetriResource",
        namespace="Metri",
        attributes=attrs,
        doc="Resource Cedar — entidad de negocio con aislamiento multitenant",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Derivación de Actions desde role.grants
# ─────────────────────────────────────────────────────────────────────────────

def _derive_actions_from_grants(role_model: dict) -> list[CedarAction]:
    """
    Deriva las Actions Cedar desde role.json → grants[*].{domain, actions}.

    Cada combinación (domain, action) produce una CedarAction.
    El group se infiere por pertenencia al conjunto MUTATIONAL_ACTIONS.

    Ejemplo:
      grant = {domain: "work_order", actions: ["VIEW", "CREATE"], scope: "ALL"}
      → Action("VIEW_work_order",  domain="work_order", group="analytical")
      → Action("CREATE_work_order", domain="work_order", group="mutational")
    """
    # Extraer los dominios y acciones declaradas en el modelo grants
    grants_def = None
    for attr in role_model.get("attributes", []):
        if attr.get("name") == "grants":
            grants_def = attr
            break

    # Si el modelo tiene la definición de grants, extraemos las actions válidas
    valid_actions: set[str] = set()
    if grants_def:
        try:
            actions_in_schema = (
                grants_def["items"]["keys"]["actions"]["items"]["description"]
            )
        except (KeyError, TypeError):
            pass
        # Las actions vienen descritas en el doc del campo actions
        for action_desc in grants_def.get("items", {}).get("keys", {}) \
                                     .get("actions", {}).get("description", "").split(","):
            tok = action_desc.strip().split()[0]
            if tok in ALL_DOMAIN_ACTIONS:
                valid_actions.add(tok)

    # Fallback: usar el conjunto canónico completo si no hay definición explícita
    if not valid_actions:
        valid_actions = ALL_DOMAIN_ACTIONS

    # Dominios de ejemplo derivados del campo scope
    # En runtime, los dominios vendrán de Datahike (role.grants[*].domain)
    # Aquí generamos las acciones por el conjunto completo de acciones × dominio-placeholder
    actions: list[CedarAction] = []
    for raw_action in sorted(valid_actions):
        group = "mutational" if raw_action in MUTATIONAL_ACTIONS else "analytical"
        # Action genérica (Janus instancia por dominio en runtime con el ctx real)
        actions.append(CedarAction(
            name=raw_action,
            domain="*",           # wildcard — en runtime se especializa por dominio
            raw_action=raw_action,
            group=group,
            doc=f"Acción {raw_action} — group:{group}",
        ))

    return actions


# ─────────────────────────────────────────────────────────────────────────────
# Derivación de Políticas desde el contrato v1 + role.json
# ─────────────────────────────────────────────────────────────────────────────

def _scope_to_cedar_when(scope: str, domain: str = "*") -> str:
    """
    Traduce un query-scope a una expresión 'when { ... }' Cedar.

    Alineado con :metri.cedar/scope-field-resolution del contrato v1:
      ALL             → tenant isolation solo
      OWN             → tenant isolation + creador
      ASSIGNED        → tenant isolation + asignado
      OWN_OR_ASSIGNED → tenant isolation + (creador || asignado)
      NONE            → denegación implícita (el caller emite :JANUS_400)
    """
    base = "principal.tenant_id == resource.tenant_id"
    scope_map = {
        "ALL":             base,
        "OWN":             f"{base} && principal.user_id == resource.owner_field",
        "ASSIGNED":        f"{base} && principal.user_id == resource.assignee_field",
        "OWN_OR_ASSIGNED": (
            f"{base} && "
            f"(principal.user_id == resource.owner_field || "
            f"principal.user_id == resource.assignee_field)"
        ),
        "NONE":            "false",  # denegación explícita
    }
    return scope_map.get(scope, base)


def _derive_scope_policies(role_model: dict) -> list[CedarScopePolicy]:
    """
    Genera una CedarScopePolicy por cada combinación (scope, action) del contrato.

    El contrato v1 define query-scope como:
      ["ALL", "OWN", "ASSIGNED", "OWN_OR_ASSIGNED", "NONE"]

    Para ANALYTICAL actions → permit con domain-boundaries (RLS Push-Down)
    Para MUTATIONAL actions → permit con validación in-memory Cedar
    Para NONE              → forbid explícito (pre-emite :JANUS_400)
    """
    policies: list[CedarScopePolicy] = []

    # ── P00: SuperAdmin universal ────────────────────────────────────────────
    policies.append(CedarScopePolicy(
        policy_id="P00_SUPERADMIN",
        effect="permit",
        domain="*",
        raw_action="*",
        scope="ALL",
        when_clause="principal.roles.contains(\"platform-admin\")",
        comment="P00 — SuperAdmin: acceso universal sin restricción de scope",
    ))

    # ── P01–P06: Un bloque de políticas por scope del contrato v1 ────────────
    scope_actions = [
        # (scope, action_set, policy_id_prefix, comment_suffix)
        ("ALL",             ANALYTICAL_ACTIONS, "P01_ALL_VIEW",
         "Scope ALL → View sin restricciones de propiedad (tenant isolation)"),
        ("ALL",             MUTATIONAL_ACTIONS, "P02_ALL_MUTATE",
         "Scope ALL → Mutación autorizada en su tenant"),
        ("OWN",             ANALYTICAL_ACTIONS, "P03_OWN_VIEW",
         "Scope OWN → View solo de registros creados por el usuario"),
        ("OWN",             MUTATIONAL_ACTIONS, "P04_OWN_MUTATE",
         "Scope OWN → Mutación solo de registros propios"),
        ("ASSIGNED",        ANALYTICAL_ACTIONS, "P05_ASSIGNED_VIEW",
         "Scope ASSIGNED → View solo de registros asignados al usuario"),
        ("ASSIGNED",        MUTATIONAL_ACTIONS, "P06_ASSIGNED_MUTATE",
         "Scope ASSIGNED → Mutación solo de registros asignados"),
        ("OWN_OR_ASSIGNED", ANALYTICAL_ACTIONS, "P07_OWN_OR_ASSIGNED_VIEW",
         "Scope OWN_OR_ASSIGNED → View: propios O asignados"),
        ("OWN_OR_ASSIGNED", MUTATIONAL_ACTIONS, "P08_OWN_OR_ASSIGNED_MUTATE",
         "Scope OWN_OR_ASSIGNED → Mutación: propios O asignados"),
    ]

    for scope, action_set, pid_prefix, comment in scope_actions:
        for action in sorted(action_set):
            policies.append(CedarScopePolicy(
                policy_id=f"{pid_prefix}_{action}",
                effect="permit",
                domain="*",
                raw_action=action,
                scope=scope,
                when_clause=_scope_to_cedar_when(scope),
                comment=f"{comment} [action:{action}]",
            ))

    # ── P_NONE: Denegación cuando scope es NONE ───────────────────────────────
    policies.append(CedarScopePolicy(
        policy_id="P_NONE_DENY",
        effect="forbid",
        domain="*",
        raw_action="*",
        scope="NONE",
        when_clause="resource.query_scope == \"NONE\"",
        comment="P_NONE — scope NONE → denegación explícita (Janus emite :JANUS_400)",
    ))

    # ── P_CROSS_TENANT: Zero-Trust Guard OBLIGATORIO ─────────────────────────
    policies.append(CedarScopePolicy(
        policy_id="P_CROSS_TENANT",
        effect="forbid",
        domain="*",
        raw_action="*",
        scope="*",
        when_clause="principal.tenant_id != resource.tenant_id",
        comment="P_CROSS_TENANT — FORBID cross-tenant: Zero-Trust invariante global",
    ))

    # ── P_SUSPENDED: Bloqueo de usuarios suspendidos ─────────────────────────
    policies.append(CedarScopePolicy(
        policy_id="P_SUSPENDED",
        effect="forbid",
        domain="*",
        raw_action="*",
        scope="*",
        when_clause="principal.status == \"SUSPENDED\"",
        comment="P_SUSPENDED — FORBID usuario suspendido (Datahike source of truth)",
    ))

    return policies


# ─────────────────────────────────────────────────────────────────────────────
# Context Invariant (Malli spec Python)
# ─────────────────────────────────────────────────────────────────────────────

def _build_context_invariant_spec(user_group_model: dict) -> dict[str, Any]:
    """
    Construye la spec Malli/Python del :metri.cedar/context-invariant.

    Alineado con cedar-janus-contract-v1.edn — DOMINIO 4 (:metres.cedar/cedar-ctx):
      :tenant-id         uuid     obligatorio (Valkey session)
      :user-id           uuid     obligatorio (Valkey session)
      :roles             Set<str> obligatorio (role.name expandido en Datahike)
      :domain-boundaries map-of   opcional  (solo en path analítico)
      :request           any      passthrough

    El contrato v1 especifica que domain-boundaries acepta:
      {entity-type → [{query-scope, permitted-locations, permitted-assets}]}
    donde la ausencia de una clave = sub-recurso (herencia Root RLS Guardian).
    """
    # Chequear si user_group declara time_restrictions
    has_time_restrictions = any(
        attr.get("name") == "time_restrictions"
        for attr in user_group_model.get("attributes", [])
    )

    permission_boundary_spec = [":map",
        [":query-scope",         [":enum",
                                  *[f'"{s}"' for s in VALID_SCOPES]]],
        [":permitted-locations", [":vector", ":uuid"]],
        [":permitted-assets",    [":vector", ":uuid"]],
    ]

    context_invariant: dict[str, Any] = {
        ":tenant-id":  {":optional": False, "spec": ":uuid"},
        ":user-id":    {":optional": False, "spec": ":uuid"},
        ":roles":      {":optional": False, "spec": [":set", ":string"]},
        ":domain-boundaries": {
            ":optional": True,
            "spec": [":map-of", ":string",
                     [":vector", {":min": 0}, permission_boundary_spec]],
            "doc": (
                "Root RLS Guardian: solo entidades RAÍZ evaluadas por Cedar. "
                "Sub-recursos heredan el dominio de su entidad raíz. "
                "Vacío {} en path mutacional (Cedar evaluó ACTION+BODY in-memory)."
            ),
        },
        ":request": {":optional": False, "spec": ":any",
                     "doc": "Passthrough opaco del payload gRPC — Cedar no lo modifica"},
    }

    if has_time_restrictions:
        context_invariant[":time-window-valid"] = {
            ":optional": True,
            "spec": ":boolean",
            "doc": "true → ventana temporal validada por step3b (user_group.time_restrictions)",
        }

    return context_invariant


# ─────────────────────────────────────────────────────────────────────────────
# API pública — CedarPolicyEngine
# ─────────────────────────────────────────────────────────────────────────────

def derive_policy_set(
    role_model:       dict,
    user_model:       dict,
    user_group_model: dict,
) -> CedarPolicySet:
    """
    Pipeline principal: deriva el CedarPolicySet completo desde los modelos del Códice.

    Entradas:
      role_model       → role.json (grants, allowed_locations, allowed_assets)
      user_model       → user.json (role_ids, group_ids, status, tenant_id)
      user_group_model → user_group.json (allowed_locations, time_restrictions)

    Salida: CedarPolicySet con EntityTypes, Actions, Policies y context-invariant.
    """
    # ── [1] EntityTypes Cedar ────────────────────────────────────────────────
    user_entity      = _build_user_entity(user_model)
    resource_entity  = _build_resource_entity(role_model, user_group_model)
    action_group     = CedarEntityType(
        name="ActionGroup", namespace="Metri",
        doc="Agrupaciones semánticas de actions: analytical | mutational",
    )

    # ── [2] Actions Cedar desde role.grants ──────────────────────────────────
    actions = _derive_actions_from_grants(role_model)

    # ── [3] Políticas Cedar por scope (contrato v1) ──────────────────────────
    policies = _derive_scope_policies(role_model)

    # ── [4] Context Invariant (Malli spec) ───────────────────────────────────
    ctx_invariant = _build_context_invariant_spec(user_group_model)

    return CedarPolicySet(
        entity_types=[user_entity, resource_entity, action_group],
        actions=actions,
        policies=policies,
        cross_tenant_forbid=True,
        context_invariant_spec=ctx_invariant,
        source_models=["role.json", "user.json", "user_group.json"],
    )


def load_model(models_dir: Path, entity_name: str) -> dict:
    """Carga un modelo JSON del Códice desde el directorio de modelos."""
    path = models_dir / f"{entity_name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Modelo no encontrado: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


# ─────────────────────────────────────────────────────────────────────────────
# Emisor EDN — :metri.cedar/* section para el contrato
# ─────────────────────────────────────────────────────────────────────────────

def emit_policy_edn_section(policy_set: CedarPolicySet) -> str:
    """
    Serializa el CedarPolicySet como un fragmento EDN listo para insertar
    en janus-ast-ir.edn (sección :metri.cedar/*).

    Produce secciones bien marcadas con comentarios para facilitar la lectura
    y el merge posterior por el cedar_pipeline.py.
    """
    lines: list[str] = []
    _sep = ";" * 66

    lines += [
        "",
        f" ;; {_sep}",
        " ;; SECCIÓN CEDAR — Modelo de Autorización Zero-Trust",
        " ;; Derivado de: role.json + user.json + user_group.json → CedarPolicyEngine",
        " ;; Alineado con:  cedar-janus-contract-v1.edn (DOMINIO 2-5)",
        f" ;; {_sep}",
        "",
    ]

    # ── Namespace ────────────────────────────────────────────────────────────
    lines += [
        " :metri.cedar/schema-namespace",
        ' "Metri"',
        "",
    ]

    # ── EntityTypes ──────────────────────────────────────────────────────────
    et_names = " ".join(f":{et.name}" for et in policy_set.entity_types)
    lines += [
        " ;; EntityTypes Cedar derivados de los modelos del Códice",
        " :metri.cedar/entity-types",
        f" [:enum {et_names}]",
        "",
    ]

    # ── Query Scopes (contrato v1 DOMINIO 2) ─────────────────────────────────
    scopes_str = " ".join(f'"{s}"' for s in VALID_SCOPES)
    lines += [
        " ;; Scopes ABAC — :metres.cedar/query-scope (contrato v1 DOMINIO 2)",
        " ;; ALL: sin filtro propiedad | OWN: propietario | ASSIGNED: asignado",
        " ;; OWN_OR_ASSIGNED: ambos | NONE: :JANUS_400 inmediato",
        " :metri.cedar/query-scope",
        f" [:enum {scopes_str}]",
        "",
    ]

    # ── Acciones por grupo ───────────────────────────────────────────────────
    analytical = [a.name for a in policy_set.actions if a.group == "analytical"]
    mutational  = [a.name for a in policy_set.actions if a.group == "mutational"]

    agg_str = "\n".join(f"  :{n}" for n in sorted(analytical))
    mut_str = "\n".join(f"  :{n}" for n in sorted(mutational))

    lines += [
        " ;; Actions analíticas (VIEW, EXPORT) — producen domain-boundaries RLS",
        " :metri.cedar/analytical-actions",
        " [:set",
        agg_str + "]",
        "",
        " ;; Actions mutacionales — Cedar evalúa in-memory, domain-boundaries = {}",
        " :metri.cedar/mutational-actions",
        " [:set",
        mut_str + "]",
        "",
    ]

    # ── Permit policies (por scope) ───────────────────────────────────────────
    permit_ids = " ".join(
        f'"{p.policy_id}"'
        for p in policy_set.policies if p.effect == "permit"
    )
    forbid_ids = " ".join(
        f'"{p.policy_id}"'
        for p in policy_set.policies if p.effect == "forbid"
    )

    lines += [
        " :metri.cedar/permit-policies",
        f" [:set {permit_ids}]",
        "",
        " ;; Políticas de denegación explícita — NUNCA eliminar P_CROSS_TENANT",
        " :metri.cedar/forbid-policies",
        f" [:set {forbid_ids}]",
        "",
    ]

    # ── Scope → Cedar when-clause (scope-field-resolution del contrato v1) ──
    lines += [
        " ;; Resolución scope → predicado Cedar when{}",
        " ;; Consumido por Janus para construir el nodo [:where] del AST IR",
        " :metri.cedar/scope-field-resolution",
        " [:map",
    ]
    for scope in VALID_SCOPES:
        when = _scope_to_cedar_when(scope)
        lines.append(f'  [:{scope.lower()} "{when}"]')
    lines += [" ]", ""]

    # ── Context Invariant (contrato v1 DOMINIO 4) ────────────────────────────
    lines += [
        " ;; Invariante de contexto Cedar — validado ANTES de construir el AST IR",
        " ;; Alineado con cedar-janus-contract-v1.edn :metres.cedar/cedar-ctx",
        " :metri.cedar/context-invariant",
        " [:map",
        "  [:tenant-id  {:optional false} :uuid]   ;; Valkey session — Zero-Trust",
        "  [:user-id    {:optional false} :uuid]   ;; Valkey session — identidad",
        "  [:roles      {:optional false} [:set :string]] ;; role.name expandido",
        "  [:domain-boundaries {:optional true}",
        "   [:map-of :string",
        "    [:vector {:min 0} [:map",
        '     [:query-scope    [:enum "ALL" "OWN" "ASSIGNED" "OWN_OR_ASSIGNED" "NONE"]]',
        "     [:permitted-locations {:optional true} [:vector :uuid]]",
        "     [:permitted-assets    {:optional true} [:vector :uuid]]]]]]",
        "  [:request {:optional false} :any]] ;; Passthrough gRPC — Cedar no modifica",
        "",
    ]

    # ── Permission Boundary spec (contrato v1 DOMINIO 3) ────────────────────
    lines += [
        " ;; Permission Boundary — contrato v1 DOMINIO 3",
        " ;; Root RLS Guardian: domain-boundaries solo contiene entidades RAÍZ.",
        " ;; Sub-recursos (JOINs) heredan el dominio del registro raíz.",
        " :metri.cedar/permission-boundary",
        " [:map",
        '  [:query-scope        [:enum "ALL" "OWN" "ASSIGNED" "OWN_OR_ASSIGNED" "NONE"]]',
        "  [:permitted-locations {:optional true} [:vector :uuid]]",
        "  [:permitted-assets    {:optional true} [:vector :uuid]]]",
        "",
    ]

    # ── Zero-Trust invariantes ───────────────────────────────────────────────
    lines += [
        " ;; Railway: Cedar authorization gate (auth-ok | :JANUS_40x)",
        " :metri.cedar/auth-gate",
        " [:or",
        "  [:map [:principal-ok true] [:action-ok true] [:resource-ok true]]",
        '  [:map [:cedar-error :string] [:janus-code [:enum :JANUS_403 :JANUS_400 :JANUS_401]]]]',
        "",
    ]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# CLI de diagnóstico (python3 -m metres_schema_engine.cedar_policy_engine)
# ─────────────────────────────────────────────────────────────────────────────

def _print_summary(policy_set: CedarPolicySet) -> None:
    sep = "═" * 66
    sep2 = "─" * 62
    print(f"\n{sep}")
    print("  CEDAR POLICY ENGINE — Derivación desde el Códice")
    print(sep)

    print(f"\n  EntityTypes ({len(policy_set.entity_types)}):")
    for et in policy_set.entity_types:
        attrs_str = ", ".join(a.name for a in et.attributes) if et.attributes else "—"
        print(f"    {et.fqn:<30}  attrs: {attrs_str}")

    print(f"\n  Actions ({len(policy_set.actions)}):")
    by_group: dict[str, list[str]] = {}
    for a in policy_set.actions:
        by_group.setdefault(a.group, []).append(a.name)
    for grp, names in sorted(by_group.items()):
        print(f"    {grp:<15}  {', '.join(names)}")

    permits = [p for p in policy_set.policies if p.effect == "permit"]
    forbids = [p for p in policy_set.policies if p.effect == "forbid"]
    print(f"\n  Políticas: {len(policy_set.policies)} "
          f"({len(permits)} permit + {len(forbids)} forbid)")
    for p in policy_set.policies:
        icon = "✅" if p.effect == "permit" else "🚫"
        print(f"    {icon} [{p.policy_id:<38}]  {p.comment[:52]}")

    print(f"\n  Cross-tenant FORBID: {'✅ ACTIVO' if policy_set.cross_tenant_forbid else '❌ INACTIVO'}")
    print(f"  Modelos fuente:      {', '.join(policy_set.source_models)}")
    print(f"\n{sep}\n")


if __name__ == "__main__":
    import sys

    # Ruta por defecto: metros-engine/docs/architecture/models/
    _here        = Path(__file__).resolve()
    _engine_root = _here.parent.parent.parent  # metros-engine/
    _models_dir  = _engine_root / "docs" / "architecture" / "models"

    try:
        role_m       = load_model(_models_dir, "role")
        user_m       = load_model(_models_dir, "user")
        user_group_m = load_model(_models_dir, "user_group")
    except FileNotFoundError as e:
        print(f"❌  {e}", file=sys.stderr)
        sys.exit(1)

    ps = derive_policy_set(role_m, user_m, user_group_m)
    _print_summary(ps)

    edn_out = emit_policy_edn_section(ps)
    out_path = _engine_root / "docs" / "architecture" / "cedar" / "cedar-policy-codice.edn"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(edn_out, encoding="utf-8")
    print(f"  ✓ Escrito: {out_path}  ({out_path.stat().st_size} bytes)")
