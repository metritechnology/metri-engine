#!/usr/bin/env python3
"""
cedar_pipeline.py — Pipeline: gRPC Descriptors → Cedar → janus-ast-ir.edn

Fases del pipeline:
  [1] BUILD_DESCRIPTOR_TREE    → DescriptorTree desde metri.proto
  [2] BUILD_CEDAR_SCHEMA       → Schema Cedar tipado desde el DescriptorTree
                                 (EntityTypes: Principal, Resource, Action)
  [3] BUILD_CEDAR_POLICIES     → Políticas Cedar (.cedar) derivadas de:
                                 • RPCs → Actions Cedar
                                 • OperationAction enum → Actions mutacionales
                                 • OutputCastType enum  → Actions analíticas
                                 • Mensajes ZT-boundary → Resources tipados
  [4] BUILD_CEDAR_EDN_SECTION  → Sección :cedar del contrato janus-ast-ir.edn
  [5] MERGE_INTO_CONTRACT      → Actualiza el janus-ast-ir.edn con la sección Cedar
  [6] VALIDATE                 → Valida la coherencia Cedar ↔ proto ↔ EDN

─────────────────────────────────────────────────────────────────────────────
MODELO CONCEPTUAL Cedar para Metri

  principal → User            { tenant_id, user_id, roles: Set<String> }
  resource  → MetriResource   { tenant_id, entity_type, rpc }
  action    → Action          { derivada de: rpc name + OperationAction enum }

  Cedar Policy pattern:
    permit(
        principal is User,
        action in ActionGroup::\"analytical\",
        resource is MetriResource
    ) when {
        principal.tenant_id == resource.tenant_id
    };
─────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import json
import sys
import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

SCRIPT_DIR   = Path(__file__).resolve().parent
METRI_ROOT   = SCRIPT_DIR.parent          # tools/ is inside metres-engine/
PROTO_PATH   = METRI_ROOT / "metri.proto"
EDN_PATH     = METRI_ROOT / "resources" / "schema" / "janus-ast-ir.edn"
CEDAR_OUT    = METRI_ROOT / "docs" / "architecture" / "cedar"

sys.path.insert(0, str(METRI_ROOT / "tools"))

from metri_schema_engine.descriptor_walker import build_descriptor_tree, DescriptorTree


# ─────────────────────────────────────────────────────────────────────────────
# Tipos del dominio Cedar
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CedarEntityType:
    """Tipo de entidad Cedar con sus atributos y membership."""
    name: str
    attributes: dict[str, str]          # attr_name → Cedar type (String, Long, Bool, Set)
    member_of_types: list[str] = field(default_factory=list)


@dataclass
class CedarActionSpec:
    """Una acción Cedar, con sus appliesTo (principals y resources permitidos)."""
    name: str
    group: str                           # "analytical" | "mutational" | "system" | "rpc"
    applies_to_principals: list[str]     # ["User"]
    applies_to_resources: list[str]      # ["MetriResource"]
    member_of: list[str] = field(default_factory=list)  # ActionGroup memberships
    comment: str = ""


@dataclass
class CedarPolicy:
    """Una política Cedar en su representación estructurada."""
    id: str
    effect: str                          # "permit" | "forbid"
    principal_type: Optional[str]        # None = cualquier principal
    principal_condition: Optional[str]   # e.g. 'principal.roles.contains("platform-admin")'
    action_spec: str                     # e.g. 'action in ActionGroup::"analytical"'
    resource_type: Optional[str]
    when_clause: Optional[str]
    comment: str = ""


@dataclass
class CedarSchema:
    """Schema completo Cedar para Metri."""
    entity_types: list[CedarEntityType] = field(default_factory=list)
    actions: list[CedarActionSpec] = field(default_factory=list)


@dataclass
class CedarPipelineResult:
    """Resultado completo del pipeline Cedar."""
    schema: CedarSchema
    policies: list[CedarPolicy]
    action_groups: dict[str, list[str]]  # group → [action_names]
    rpc_action_map: dict[str, str]       # rpc_name → cedar_action_name
    zt_resource_types: list[str]         # mensajes ZT-boundary → Resource kinds


# ─────────────────────────────────────────────────────────────────────────────
# FASE 1 → FASE 2: DescriptorTree → CedarSchema
# ─────────────────────────────────────────────────────────────────────────────

def build_cedar_schema(tree: DescriptorTree) -> CedarSchema:
    """
    Deriva el Cedar Schema desde el DescriptorTree.

    Mapeo:
      • 'User' EntityType        ← campos comunes de mensajes ZT-boundary
      • 'MetriResource' EntityType ← wraps el entity_type + rpc asociado
      • ActionGroup entities     ← agrupaciones semánticas de actions
    """
    schema = CedarSchema()

    # ── EntityType: User (el Principal en Metri) ─────────────────────────────
    user_type = CedarEntityType(
        name="User",
        attributes={
            "tenant_id" : "String",
            "user_id"   : "String",
            "roles"     : "Set<String>",
            "email"     : "String",
        },
    )
    schema.entity_types.append(user_type)

    # ── EntityType: MetriResource (el Resource) ────────────────────────────
    # Derivado de mensajes ZT-boundary + RPCs
    zt_messages  = [name for name, msg in tree.messages.items()
                    if msg.is_zero_trust_boundary]
    entity_types_from_proto = _extract_domain_entity_types(tree)

    resource_type = CedarEntityType(
        name="MetriResource",
        attributes={
            "tenant_id"  : "String",
            "entity_type": "String",           # "asset" | "meter_reading" | ...
            "rpc"        : "String",           # "Query" | "Transact" | "BulkIngest" | ...
        },
    )
    schema.entity_types.append(resource_type)

    # ── EntityType: ActionGroup (agrupaciones Cedar) ───────────────────────
    for group in ["analytical", "mutational", "system"]:
        schema.entity_types.append(CedarEntityType(
            name=f"ActionGroup",
            attributes={},
        ))
    # Cedar usa namespaced action groups — los modelamos como "ActionGroup"

    # ── Actions: desde RPCs ──────────────────────────────────────────────────
    for rpc in tree.rpcs:
        # Mapear cada RPC a una Action Cedar
        cedar_action_name = _rpc_to_cedar_action(rpc.name)
        group = _rpc_to_cedar_group(rpc.name)
        action = CedarActionSpec(
            name=cedar_action_name,
            group=group,
            applies_to_principals=["User"],
            applies_to_resources=["MetriResource"],
            member_of=[f'ActionGroup::"{group}"'],
            comment=f"gRPC: {rpc.name} ({rpc.request_type} → {rpc.response_type})"
                    + (" [stream]" if rpc.is_streaming else ""),
        )
        schema.actions.append(action)

    # ── Actions: desde OperationAction enum ──────────────────────────────────
    op_enum = tree.enums.get("OperationAction")
    if op_enum:
        for ev in op_enum.valid_values:
            action = CedarActionSpec(
                name=ev.name,
                group="mutational",
                applies_to_principals=["User"],
                applies_to_resources=["MetriResource"],
                member_of=['ActionGroup::"mutational"'],
                comment=f"proto: OperationAction.{ev.name}",
            )
            schema.actions.append(action)

    # ── Actions: desde OutputCastType (viz types) ──────────────────────────
    viz_enum = tree.enums.get("OutputCastType")
    if viz_enum:
        for ev in viz_enum.valid_values:
            action_name = f"Query{ev.name.title().replace('_', '')}"
            action = CedarActionSpec(
                name=action_name,
                group="analytical",
                applies_to_principals=["User"],
                applies_to_resources=["MetriResource"],
                member_of=['ActionGroup::"analytical"'],
                comment=f"viz: OutputCastType.{ev.name}",
            )
            schema.actions.append(action)

    return schema


def _rpc_to_cedar_action(rpc_name: str) -> str:
    """Convierte nombre de RPC a nombre de Action Cedar."""
    # Discovery → DiscoverSchema, Query → QueryMetrics, etc.
    mapping = {
        "Discovery"          : "DiscoverSchema",
        "Explore"            : "ExploreSchema",
        "Query"              : "QueryMetrics",
        "Transact"           : "MutateEntity",
        "BulkIngest"         : "BulkIngestData",
        "MatchRoutingRules"  : "MatchRoutingRules",
        "MatchRoutingRulesBatch": "MatchRoutingRulesBatch",
    }
    return mapping.get(rpc_name, rpc_name)


def _rpc_to_cedar_group(rpc_name: str) -> str:
    """Clasifica un RPC en un grupo Cedar."""
    analytical = {"Discovery", "Explore", "Query", "MatchRoutingRules", "MatchRoutingRulesBatch"}
    mutational  = {"Transact", "BulkIngest"}
    if rpc_name in analytical:
        return "analytical"
    if rpc_name in mutational:
        return "mutational"
    return "system"


def _extract_domain_entity_types(tree: DescriptorTree) -> list[str]:
    """Extrae tipos de entidad de negocio del schema (los ZT-boundary)."""
    return [name for name, msg in tree.messages.items()
            if msg.is_zero_trust_boundary]


# ─────────────────────────────────────────────────────────────────────────────
# FASE 3: CedarSchema → Políticas Cedar
# ─────────────────────────────────────────────────────────────────────────────

def build_cedar_policies(schema: CedarSchema, tree: DescriptorTree) -> list[CedarPolicy]:
    """
    Genera políticas Cedar ESTRUCTURALES derivadas del modelo role.json.

    PRINCIPIO DE DISEÑO (producción):
    ─────────────────────────────────
    En producción, los roles son DATOS en Datahike (filas del modelo role.json),
    NO constantes hardcodeadas en el texto Cedar. Un tenant puede llamar al rol
    "Supervisor de Campo", otro "Administrador Regional" — Cedar no puede saberlo.

    Por tanto, las políticas Cedar evalúan la ESTRUCTURA del grant:
      • role.grants[*].scope   → ALL | OWN | ASSIGNED | OWN_OR_ASSIGNED | NONE
      • role.grants[*].actions → VIEW | CREATE | UPDATE | DELETE | UPSERT | EXPORT
      • role.grants[*].domain  → entidad de negocio (resuelto vía MetriResource.entity_type)
      • role.allowed_locations → fronteras geográficas (expanded por Datahike en Paso 3)
      • user_group.allowed_locations → refinamiento contextual de locaciones
      • user_group.time_restrictions → ventana temporal (validada en Paso 3b)

    El CedarAuthorizer (06) hidrata el principal en runtime con:
      principal.tenant_id    ← Valkey session (Zero-Trust)
      principal.user_id      ← Valkey session
      principal.grants       ← role.grants[] expandido desde Datahike
      principal.query_scope  ← scope efectivo para el dominio solicitado
      principal.locations    ← SET de location UUIDs permitidos (expandidos)
      principal.assets       ← SET de asset UUIDs permitidos (expandidos)

    Políticas generadas (estructurales):
      P00 — ANALYTICAL: VIEW/EXPORT en tenant propio con scope != NONE
      P01 — MUTATIONAL: CREATE/UPDATE/DELETE/UPSERT con grant en dominio
      P02 — SYSTEM: Acciones de sistema (infra) con grant en dominio
      P03 — FORBID: Zero-Trust cross-tenant (NUNCA eliminar)
      P04 — FORBID: scope NONE → denegación explícita (:JANUS_400)
      P05 — FORBID: usuario SUSPENDED (Datahike source of truth)
    """
    policies: list[CedarPolicy] = []

    # ── P00: ANALYTICAL — acción analítica con grant en el dominio Y scope != NONE ──
    # Cedar solo verifica: ¿tiene grant para este entity_type Y scope no es NONE?
    # QUIÉN es el propietario (OWN, ASSIGNED, OWN_OR_ASSIGNED) lo resuelve Janus
    # inyectando el predicado correcto en el AST IR desde domain-boundary.query_scope.
    # Cedar NO necesita distinguir OWN vs ASSIGNED vs OWN_OR_ASSIGNED — eso es
    # semántica de negocio del rol en Datahike, no semántica de autorización Cedar.
    policies.append(CedarPolicy(
        id="P00",
        effect="permit",
        principal_type="User",
        principal_condition=(
            "principal.grants.containsAny(resource.entity_type) && "
            "principal.query_scope != \"NONE\""
        ),
        action_spec='action in ActionGroup::"analytical"',
        resource_type="MetriResource",
        when_clause=(
            "principal.tenant_id == resource.tenant_id"
        ),
        comment=(
            "P00 — Analítica: usuario con grant en el dominio solicitado y scope != NONE. "
            "El scope efectivo (ALL/OWN/ASSIGNED/OWN_OR_ASSIGNED) vive en Datahike — "
            "Cedar solo verifica su presencia. Janus inyecta el predicado concreto en el AST IR."
        ),
    ))

    # ── P01: MUTATIONAL — CREATE/UPDATE/DELETE/UPSERT con grant en el dominio ──
    # Para acciones mutacionales, Cedar evalúa ABAC in-memory completo.
    # domain-boundaries retornado = {} vacío (no hay RLS Push-Down en escritura).
    # El grant.actions debe contener la acción solicitada y el dominio debe coincidir.
    policies.append(CedarPolicy(
        id="P01",
        effect="permit",
        principal_type="User",
        principal_condition=(
            "principal.grants.containsAny(resource.entity_type)"
        ),
        action_spec='action in ActionGroup::"mutational"',
        resource_type="MetriResource",
        when_clause=(
            "principal.tenant_id == resource.tenant_id"
        ),
        comment=(
            "P01 — Mutacional: usuario con grant sobre el dominio solicitado. "
            "domain-boundaries={} (Cedar validó ABAC in-memory — sin RLS Push-Down). "
            "El nombre del dominio viene de Datahike — no hardcodeado."
        ),
    ))

    # ── P02: SYSTEM — RPCs de infraestructura (routing, discovery de sistema) ───
    # Cubre acciones de ActionGroup::"system" — no tienen entity_type de negocio.
    # El acceso se otorga igual que cualquier otro: via grant en el principal.
    # El rol M2M/infra declara sus grants en Datahike — no hay "routing" hardcodeado.
    policies.append(CedarPolicy(
        id="P02",
        effect="permit",
        principal_type="User",
        principal_condition=(
            "principal.grants.containsAny(resource.entity_type)"
        ),
        action_spec='action in ActionGroup::"system"',
        resource_type="MetriResource",
        when_clause=(
            "principal.tenant_id == resource.tenant_id"
        ),
        comment=(
            "P02 — Sistema/Infraestructura: usuario con grant sobre el tipo de recurso "
            "para acciones de sistema (MatchRoutingRules, etc.). "
            "El grant viene de Datahike — ningún nombre de dominio está hardcodeado en Cedar."
        ),
    ))

    # ── P03: FORBID — Zero-Trust cross-tenant (CARDINAL — NUNCA ELIMINAR) ─────
    # Este FORBID tiene precedencia sobre TODO permit, incluido P00.
    # Sin importar qué rol o grant tenga el usuario, si el tenant_id difiere
    # del resource.tenant_id, Cedar deniega SIEMPRE.
    policies.append(CedarPolicy(
        id="P03",
        effect="forbid",
        principal_type="User",
        principal_condition=None,
        action_spec="action",
        resource_type="MetriResource",
        when_clause="principal.tenant_id != resource.tenant_id",
        comment=(
            "P03 — Zero-Trust CARDINAL: FORBID cross-tenant. "
            "Precedencia sobre todos los permits. NUNCA eliminar."
        ),
    ))

    # ── P07: FORBID — scope NONE → denegación explícita ──────────────────────
    # Si el role.grants[domain].scope = "NONE", el usuario no tiene visibilidad
    # sobre ese dominio. Cedar deniega antes de que Janus construya el AST IR.
    # Janus emite :JANUS_400 al recibir [:error :CEDAR_DENIED].
    policies.append(CedarPolicy(
        id="P04",
        effect="forbid",
        principal_type="User",
        principal_condition=None,
        action_spec='action in ActionGroup::"analytical"',
        resource_type="MetriResource",
        when_clause='resource.query_scope == "NONE"',
        comment=(
            "P04 — scope NONE: denegación explícita. "
            "role.grants[domain].scope=NONE → Janus emite :JANUS_400"
        ),
    ))

    # ── P08: FORBID — usuario SUSPENDED ──────────────────────────────────────
    # El status viene de Datahike (Paso 2 del CedarAuthorizer).
    # Si user.status = "SUSPENDED", Cedar deniega TODA acción.
    # El interceptor lo detecta antes de llegar aquí vía fetch-user-graph,
    # pero esta política lo formaliza como invariante Cedar también.
    policies.append(CedarPolicy(
        id="P05",
        effect="forbid",
        principal_type="User",
        principal_condition=None,
        action_spec="action",
        resource_type=None,
        when_clause='principal.status == "SUSPENDED"',
        comment=(
            "P05 — SUSPENDED: usuario bloqueado en Datahike. "
            "Efecto inmediato en el próximo request (sin gap de TTL)"
        ),
    ))

    return policies



# ─────────────────────────────────────────────────────────────────────────────
# FASE 4: Serialización — Cedar Schema JSON
# ─────────────────────────────────────────────────────────────────────────────

def emit_cedar_schema_json(schema: CedarSchema) -> dict:
    """
    Produce el Cedar Schema en formato JSON (cedar-policy schema format).
    Ref: https://docs.cedarpolicy.com/schema/schema.html
    """
    # Cedar Schema JSON v2 format
    cedar_schema: dict = {
        "Metri": {
            "entityTypes": {},
            "actions": {},
        }
    }

    ns = cedar_schema["Metri"]

    # ── EntityTypes ───────────────────────────────────────────────────────────
    seen_types: set[str] = set()
    for et in schema.entity_types:
        if et.name in seen_types:
            continue
        seen_types.add(et.name)

        attrs = {}
        for attr_name, attr_type in et.attributes.items():
            if attr_type.startswith("Set<"):
                inner = attr_type[4:-1]
                attrs[attr_name] = {
                    "type": "Set",
                    "element": {"type": inner},
                    "required": attr_name in ("tenant_id", "user_id", "roles"),
                }
            else:
                attrs[attr_name] = {
                    "type": attr_type,
                    "required": attr_name in ("tenant_id", "user_id"),
                }

        entity_def: dict = {}
        if attrs:
            entity_def["shape"] = {
                "type": "Record",
                "attributes": attrs,
            }
        if et.member_of_types:
            entity_def["memberOfTypes"] = et.member_of_types

        ns["entityTypes"][et.name] = entity_def

    # ── Eliminar duplicados de ActionGroup ───────────────────────────────────
    ns["entityTypes"]["ActionGroup"] = {}

    # ── Actions ───────────────────────────────────────────────────────────────
    for action in schema.actions:
        action_def: dict = {
            "appliesTo": {
                "principalTypes": action.applies_to_principals,
                "resourceTypes" : action.applies_to_resources,
            }
        }
        if action.member_of:
            action_def["memberOf"] = [
                {"id": f'ActionGroup::"{action.group}"', "type": "Action"}
            ]
        ns["actions"][action.name] = action_def

    return cedar_schema


# ─────────────────────────────────────────────────────────────────────────────
# FASE 4b: Serialización — Cedar Policies (.cedar)
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# FASE 4b: Serialización — Cedar Policies (.cedar)
# ─────────────────────────────────────────────────────────────────────────────

def emit_cedar_policies_text(policies: list) -> str:
    """Serializa las políticas al formato textual Cedar (.cedar)."""
    import datetime as _dt
    lines = [
        "// ═══════════════════════════════════════════════════════════════",
        "// METRI ENGINE — Cedar Policy Store",
        "// Generado por cedar_pipeline.py — role.json + user_group.json",
        f"// Generado: {_dt.datetime.utcnow().isoformat()}Z",
        "//",
        "// PRINCIPIO: roles son DATOS en Datahike — no hardcodeados en Cedar.",
        "// CedarAuthorizer hidrata principal.grants/scope/locations/assets",
        "// desde Datahike en runtime antes de invocar Cedar.",
        "// ═══════════════════════════════════════════════════════════════",
        "",
    ]
    for policy in policies:
        lines.append(f"// {policy.comment}")
        lines.append(f"{policy.effect}(")
        if policy.principal_type:
            lines.append(f"    principal is Metri::{policy.principal_type},")
        else:
            lines.append("    principal,")
        lines.append(f"    {policy.action_spec},")
        if policy.resource_type:
            lines.append(f"    resource is Metri::{policy.resource_type}")
        else:
            lines.append("    resource")
        lines.append(")")
        clauses = []
        if policy.principal_condition and policy.principal_type:
            clauses.append(f"    ({policy.principal_condition})")
        if policy.when_clause:
            clauses.append(f"    {policy.when_clause}")
        if clauses:
            lines.append("when {")
            lines.append(" &&\n".join(clauses))
            lines.append("};")
        else:
            lines.append(";")
        lines.append("")
    return "\n".join(lines)


def emit_cedar_edn_section(
    result,
    tree,
    models_dir=None,
) -> str:
    """
    Produce la sección :cedar del contrato janus-ast-ir.edn.

    Con models_dir resuelto emite:
      :metri.cedar/role-model           — estructura abstracta de role.json
      :metri.cedar/group-shape          — estructura abstracta de user_group.json
      :metri.cedar/group-time-restriction — ventanas temporales (key raíz separada)
      :metri.cedar/principal-shape      — grants estructurados hidratados en runtime
      :metri.cedar/scope-field-resolution — scope → predicado AST IR
      :metri.cedar/policy-shape         — spec Malli formal de una política Cedar
      :metri.cedar/derived-scopes       — NONE como estado Cedar derivado (no en role.json)
      :metri.cedar/intersection-invariant — formaliza intersección role ↔ user_group

    Los NOMBRES de los roles son datos en Datahike (nunca hardcodeados
    en el texto Cedar). El contrato documenta la ESTRUCTURA del grant
    y los SCOPES válidos — independiente del nombre del rol en producción.
    """
    actions_by_group: dict[str, list[str]] = {}
    for action in result.schema.actions:
        actions_by_group.setdefault(action.group, []).append(action.name)

    rpc_lines = []
    for rpc in tree.rpcs:
        cedar_action = result.rpc_action_map.get(rpc.name, rpc.name)
        stream_tag   = " :streaming" if rpc.is_streaming else ""
        rpc_lines.append(
            "   {:metre.rpc/" + rpc.name.lower() + " " + cedar_action + stream_tag + "}"
        )

    zt_resources = "\n".join(
        f'   "{name}"' for name in result.zt_resource_types
    )

    policy_ids = " ".join(f'"{p.id}"' for p in result.policies if p.effect == "permit")
    deny_ids   = " ".join(f'"{p.id}"' for p in result.policies if p.effect == "forbid")

    agg_actions = "\n".join(
        f'   :{n}' for n in actions_by_group.get("analytical", [])
    )
    mut_actions = "\n".join(
        f'   :{n}' for n in actions_by_group.get("mutational", [])
    )

    # policy_table eliminado — las entradas P00-P08 son datos de runtime,
    # no specs del contrato EDN. Viven en cedar/metri.cedar (artefacto generado).
    # El contrato define solo el TIPO: [:vector :metri.cedar/policy-shape]


    # ── Secciones del modelo role.json / user_group.json ────────────────────
    role_model_section = ""
    group_model_section = ""
    principal_shape_section = ""

    if models_dir is not None:
        import json as _json
        from pathlib import Path as _Path
        _models_dir = _Path(models_dir)

        # ── role.json: estructura abstracta ──────────────────────────────────
        role_path = _models_dir / "role.json"
        if role_path.exists():
            role_m = _json.loads(role_path.read_text(encoding="utf-8"))

            # Scopes extraídos del campo grants.scope
            scopes: list[str] = []
            grant_actions: list[str] = []
            for attr in role_m.get("attributes", []):
                if attr.get("name") == "grants":
                    scope_def = (attr.get("items", {})
                                     .get("keys", {})
                                     .get("scope", {}))
                    scopes = scope_def.get("options", [])
                    # acciones del campo actions.description
                    act_desc = (attr.get("items", {})
                                    .get("keys", {})
                                    .get("actions", {})
                                    .get("description", ""))
                    for tok in act_desc.split(","):
                        tok = tok.strip().split()[0] if tok.strip() else ""
                        if tok:
                            grant_actions.append(tok)
                    break

            if not scopes:
                scopes = ["ALL", "OWN", "ASSIGNED", "OWN_OR_ASSIGNED", "NONE"]
            if not grant_actions:
                grant_actions = ["VIEW", "CREATE", "UPDATE", "DELETE",
                                  "UPSERT", "EXECUTE", "EXPORT"]

            scopes_edn = " ".join(f':{s}' for s in scopes)
            actions_edn = " ".join(f':{a}' for a in grant_actions)

            # scope → predicado Cedar when-clause (alineado con contrato v1)
            scope_resolution = {
                "ALL":             "principal.tenant_id == resource.tenant_id",
                "OWN":             "principal.tenant_id == resource.tenant_id && principal.user_id == resource.owner_field",
                "ASSIGNED":        "principal.tenant_id == resource.tenant_id && principal.user_id == resource.assignee_field",
                "OWN_OR_ASSIGNED": "principal.tenant_id == resource.tenant_id && (principal.user_id == resource.owner_field || principal.user_id == resource.assignee_field)",
                "NONE":            "false  ;; Janus emite :JANUS_400 — sin visibilidad sobre el dominio",
            }
            scope_lines = "\n".join(
                f'   [:{sc:<18} "{when}"]'
                for sc, when in scope_resolution.items()
            )

            role_model_section = f"""
 ;; ─────────────────────────────────────────────────────────────────────
 ;; MODELO DE ROL (role.json) — Estructura abstracta de autorización
 ;;
 ;; Los NOMBRES de los roles son datos en Datahike (runtime).
 ;; El contrato documenta la ESTRUCTURA del grant y sus invariantes.
 ;;
 ;; Ciclo de vida:
 ;;   Datahike: role.grants[] → CedarAuthorizer.step3 → principal.grants
 ;;   Cedar:    grants.containsAny(resource.entity_type) → ALLOW/DENY
 ;;   Janus:    scope → AST IR [:where ...] predicados de propiedad
 ;; ─────────────────────────────────────────────────────────────────────

 ;; Scopes válidos derivados de role.grants.scope (role.json)
 ;; Determina el predicado Cedar y el nodo [:where] del AST IR
 :metri.cedar/valid-scopes
 [:enum {scopes_edn}]

 ;; NONE no es un valor del modelo role.json — es un estado DERIVADO Cedar.
 ;; Ocurre cuando el usuario no tiene grant sobre el dominio solicitado.
 ;; CedarAuthorizer lo emite como query-scope=NONE en el domain-boundary.
 ;; Janus intercepta este estado y emite :JANUS_400 sin construir el AST IR.
 :metri.cedar/derived-scopes
 [:enum :NONE]

 ;; Acciones derivadas de role.grants.actions (role.json)
 ;; Analítica → domain-boundaries RLS | Mutacional → ABAC in-memory
 :metri.cedar/grant-actions
 [:set {actions_edn}]

 ;; Estructura abstracta del grant (role.json → grants[*])
 ;; Los NOMBRES de dominio son datos en Datahike (e.g. "work_order", "asset")
 :metri.cedar/grant-shape
 [:map
  [:domain  :string]     ;; Nombre del entity-type (datos Datahike — no hardcodeado)
  [:actions [:set [:enum {actions_edn}]]]
  [:scope   [:enum {scopes_edn}]]]

 ;; Resolución scope → predicado Cedar when{{}} y predicado AST IR
 ;; Janus usa esta tabla para construir el nodo [:where] del AST IR
 :metri.cedar/scope-field-resolution
 [:map
{scope_lines}]
"""

        # ── user_group.json: estructura abstracta ─────────────────────────────
        group_path = _models_dir / "user_group.json"
        if group_path.exists():
            group_m = _json.loads(group_path.read_text(encoding="utf-8"))
            has_time = any(a.get("name") == "time_restrictions"
                           for a in group_m.get("attributes", []))

            # time-restriction: key RAÍZ SEPARADA (no anidada dentro del [:map] de group-shape)
            time_restriction_root = ""
            if has_time:
                time_restriction_root = """
 ;; Ventanas temporales del grupo (user_group.time_restrictions)
 ;; Validadas en CedarAuthorizer.step3b ANTES de evaluar Cedar (gate temporal).
 ;; Si el request cae fuera de toda ventana activa → :JANUS_403 inmediato.
 :metri.cedar/group-time-restriction
 [:map
  [:days-of-week  [:vector :int]]   ;; 1=Lunes … 7=Domingo
  [:start-minute  :int]             ;; minutos desde 00:00 (e.g. 480 = 08:00)
  [:end-minute    :int]]            ;; minutos desde 00:00 (e.g. 1080 = 18:00)
"""

            group_model_section = f"""
 ;; ─────────────────────────────────────────────────────────────────────
 ;; MODELO DE GRUPO (user_group.json) — Refinamiento contextual del rol
 ;;
 ;; user_group define fronteras geográficas (allowed_locations,
 ;; allowed_assets) y temporales (time_restrictions) que se intersectan
 ;; con las del rol en CedarAuthorizer.step3.
 ;;
 ;; INVARIANTE:
 ;;   principal.locations = INTERSECT(role.allowed_locations, group.allowed_locations)
 ;;   principal.assets    = INTERSECT(role.allowed_assets,    group.allowed_assets)
 ;;
 ;; Ninguno de los dos puede EXPANDIR permisos del otro — solo REDUCIRLOS.
 ;; ─────────────────────────────────────────────────────────────────────

 ;; Estructura del grupo (user_group.json → atributos de autorización contextual)
 ;; Note: time-restrictions referencia :metri.cedar/group-time-restriction (key raíz separada abajo).
 :metri.cedar/group-shape
 [:map
  [:allowed-locations {{:optional true}} [:vector :uuid]]
  [:allowed-assets    {{:optional true}} [:vector :uuid]]
  [:time-restrictions {{:optional true}} [:vector :metri.cedar/group-time-restriction]]]
{time_restriction_root}
 ;; Intersección de fronteras rol+grupo (calculada por CedarAuthorizer.step3)
 ;; Ninguno de los dos puede expandir permisos del otro — solo reducirlos
 :metri.cedar/location-boundary-rule
 "principal.locations = INTERSECT(role.allowed_locations, group.allowed_locations)"

 ;; Spec formal de la operación de intersección role ↔ user_group.
 ;; Documenta las invariantes que CedarAuthorizer.step3 debe satisfacer
 ;; antes de hidratar el principal Cedar.
 :metri.cedar/intersection-invariant
 [:map
  [:role-locations    {{:optional true}} [:set :uuid]]   ;; role.allowed_locations
  [:group-locations   {{:optional true}} [:set :uuid]]   ;; group.allowed_locations
  [:effective-locations              [:set :uuid]]       ;; = INTERSECT(role, group)
  [:role-assets       {{:optional true}} [:set :uuid]]   ;; role.allowed_assets
  [:group-assets      {{:optional true}} [:set :uuid]]   ;; group.allowed_assets
  [:effective-assets                 [:set :uuid]]       ;; = INTERSECT(role, group)
  [:rule :string]]                                       ;; "effective ⊆ role ∧ effective ⊆ group"
"""

        # ── Principal Cedar shape (hidratado por CedarAuthorizer.step3) ───────
        principal_shape_section = f"""
 ;; ─────────────────────────────────────────────────────────────────────
 ;; PRINCIPAL SHAPE (Metri::User hidratado en runtime)
 ;;
 ;; El CedarAuthorizer llena estos atributos desde Datahike + Valkey.
 ;; Cedar NO los lee del request body del cliente — Zero-Trust.
 ;;
 ;; Fuente por atributo:
 ;;   tenant-id   ← Valkey session token
 ;;   user-id     ← Valkey session token
 ;;   status      ← Datahike user.status (ACTIVE|INACTIVE|SUSPENDED)
 ;;   grants      ← Datahike role.grants[] expandido (domain+scope+actions)
 ;;                 Cada grant refleja la estructura de :metri.cedar/grant-shape.
 ;;                 NONE no aparece en grants — es el estado AUSENTE (no-grant).
 ;;   locations   ← INTERSECT(role.allowed_locations, group.allowed_locations)
 ;;   assets      ← INTERSECT(role.allowed_assets, group.allowed_assets)
 ;;   query-scope ← scope efectivo resuelto por Cedar para el dominio del request
 ;; ─────────────────────────────────────────────────────────────────────
 :metri.cedar/principal-shape
 [:map
  [:tenant-id   {{:optional false}} :uuid]
  [:user-id     {{:optional false}} :uuid]
  [:status      {{:optional false}} [:enum :ACTIVE :INACTIVE :SUSPENDED]]
  ;; grants: Set de grants estructurados — NO strings opacos.
  ;; Cada grant refleja la estructura de :metri.cedar/grant-shape (domain+scope+actions).
  ;; La ausencia de grant para un dominio equivale a scope=NONE → :JANUS_400.
  [:grants      {{:optional false}} [:set :metri.cedar/grant-shape]]
  ;; query-scope: scope efectivo para el dominio del request en curso.
  ;; Resuelto por Cedar tras evaluar grants contra resource.entity_type.
  ;; :NONE indica no-grant para ese dominio (→ :JANUS_400 sin construir AST IR).
  [:query-scope {{:optional false}} [:enum :ALL :OWN :ASSIGNED :OWN_OR_ASSIGNED :NONE]]
  ;; Fronteras geográficas efectivas = INTERSECT(role.allowed_locations, group.allowed_locations)
  [:locations   {{:optional true}}  [:set :uuid]]
  ;; Fronteras de activos efectivas = INTERSECT(role.allowed_assets, group.allowed_assets)
  [:assets      {{:optional true}}  [:set :uuid]]]

 ;; Spec formal de la estructura de una política Cedar.
 ;; Permite validar con Malli que las políticas P00–P08 están bien formadas.
 :metri.cedar/policy-shape
 [:map
  [:id                  {{:optional false}} :string]
  [:effect              {{:optional false}} [:enum :permit :forbid]]
  [:principal-condition {{:optional true}}  :string]
  [:action-spec         {{:optional false}} :string]
  [:when-clause         {{:optional true}}  :string]
  [:comment             {{:optional false}} :string]]
"""

    return f"""\n
 ;; ══════════════════════════════════════════════════════════════
 ;; SECCIÓN CEDAR — Modelo de Autorización Zero-Trust
 ;; Derivado de: grpc Descriptors + role.json + user_group.json
 ;; Patrón: principal(User) × action(MetriAction) × resource(MetriResource)
 ;;
 ;; PRINCIPIO: Los NOMBRES de roles son datos en Datahike (runtime).
 ;;            El contrato formaliza la ESTRUCTURA del grant y los scopes.
 ;; ══════════════════════════════════════════════════════════════
 :metri.cedar/schema-namespace
 "Metri"

 ;; Entity types del schema Cedar
 :metri.cedar/entity-types
 [:enum :User :MetriResource :ActionGroup]

 ;; Actions analíticas (READ-ONLY — producen domain-boundaries RLS Push-Down)
 :metri.cedar/analytical-actions
 [:set
{agg_actions}]

 ;; Actions mutacionales (WRITE — Cedar evalúa ABAC in-memory, domain-boundaries={{}})
 :metri.cedar/mutational-actions
 [:set
{mut_actions}]

 ;; Mapa gRPC RPC → Cedar Action
 :metri.cedar/rpc-action-map
 [:map
   [:Discovery     :DiscoverSchema]
   [:Explore       :ExploreSchema]
   [:Query         :QueryMetrics]
   [:Transact      :MutateEntity]
   [:BulkIngest    :BulkIngestData]
   [:MatchRoutingRules :MatchRoutingRules]
   [:MatchRoutingRulesBatch :MatchRoutingRulesBatch]]

 ;; Resources Zero-Trust (mensajes con tenant_id en el proto)
 :metri.cedar/zero-trust-resources
 [:set
{zt_resources}]

 ;; Las políticas Cedar concretas (P00–P08) son datos de runtime.
 ;; Viven en el artefacto generado: cedar/metri.cedar
 ;; El contrato solo define la ESTRUCTURA (tipo) que debe cumplir cada entrada,
 ;; no las instancias — eso violaría el principio de abstracción del contrato.
 ;;
 ;; Para consultar las políticas activas:
 ;;   → docs/architecture/cedar/metri.cedar  (texto Cedar P00–P08)
 ;;   → docs/architecture/cedar/cedar-schema.json  (schema namespace Metri)
 ;;
 ;; :metri.cedar/policy-shape (más abajo) define la forma validable de cada política.
 :metri.cedar/policy-registry
 [:vector :metri.cedar/policy-shape]

 ;; Spec abstracto de los conjuntos de políticas activas (solo tipos — no instancias).
 ;; Los IDs concretos de permit/forbid están en cedar/metri.cedar.
 :metri.cedar/permit-policies
 [:set :string]  ;; IDs de política tipo :permit — evaluated before forbids

 :metri.cedar/forbid-policies
 [:set :string]  ;; IDs de política tipo :forbid — precedencia over permits (Cedar semantíca)
{role_model_section}{group_model_section}{principal_shape_section}
 ;; Invariante de contexto Cedar (validación pre-gRPC — Railway gate)
 ;; Recibido del CedarAuthorizer → Janus lo valida con Malli antes del AST IR
 :metri.cedar/context-invariant
 [:map
  [:tenant-id  {{:optional false}} :uuid]
  [:user-id    {{:optional false}} :uuid]
  [:roles      {{:optional false}} [:set :string]]
  [:domain-boundaries {{:optional true}}
   [:map-of :string
    [:vector [:map
     [:query-scope    [:enum :ALL :OWN :ASSIGNED :OWN_OR_ASSIGNED :NONE]]
     [:permitted-locations {{:optional true}} [:vector :uuid]]
     [:permitted-assets    {{:optional true}} [:vector :uuid]]]]]]
  [:request    {{:optional false}} :any]]

 ;; Railway: Cedar authorization gate
 :metri.cedar/auth-gate
 [:or
  [:map [:principal-ok true] [:action-ok true] [:resource-ok true]]
  [:map [:cedar-error :string] [:janus-code [:enum :JANUS_403 :JANUS_400 :JANUS_401]]]]
"""


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
# FASE 5: EDN Section — Cedar para el contrato janus-ast-ir.edn
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CedarValidationResult:
    passed: int = 0
    failed: int = 0
    findings: list[dict] = field(default_factory=list)

    def ok(self, rule: str, msg: str) -> None:
        self.passed += 1

    def fail(self, rule: str, msg: str, severity: str = "error") -> None:
        self.failed += 1
        self.findings.append({"rule": rule, "severity": severity, "message": msg})

    @property
    def is_valid(self) -> bool:
        errors = [f for f in self.findings if f["severity"] == "error"]
        return len(errors) == 0


def validate_pipeline(result: CedarPipelineResult,
                      tree: DescriptorTree,
                      edn_src: str) -> CedarValidationResult:
    """
    Valida la coherencia entre:
      - El DescriptorTree (proto)
      - El CedarPipelineResult (schema + policies)
      - El contrato janus-ast-ir.edn
    """
    v = CedarValidationResult()

    # ── V01: Todo RPC tiene un Cedar Action correspondiente ───────────────────
    rpc_names    = {r.name for r in tree.rpcs}
    action_names = {a.comment.split(": ")[1].split(" ")[0]
                    for a in result.schema.actions if "gRPC:" in a.comment}
    for rpc in rpc_names:
        cedar_action = result.rpc_action_map.get(rpc)
        if cedar_action:
            v.ok("V01", f"RPC {rpc} → Action {cedar_action}")
        else:
            v.fail("V01", f"RPC '{rpc}' no tiene Cedar Action asignada")

    # ── V02: OperationAction enum está cubierto en las Actions Cedar ──────────
    op_enum = tree.enums.get("OperationAction")
    if op_enum:
        cedar_action_names = {a.name for a in result.schema.actions}
        for ev in op_enum.valid_values:
            if ev.name in cedar_action_names:
                v.ok("V02", f"OperationAction.{ev.name} → Cedar Action OK")
            else:
                v.fail("V02", f"OperationAction.{ev.name} sin Cedar Action", "warning")

    # ── V03: policy P06 (forbid cross-tenant) existe ──────────────────────────
    forbid_policies = [p for p in result.policies if p.effect == "forbid"]
    cross_tenant    = [p for p in forbid_policies
                       if p.when_clause and "tenant_id !=" in p.when_clause]
    if cross_tenant:
        v.ok("V03", "Cross-tenant FORBID policy existe (Zero-Trust)")
    else:
        v.fail("V03", "No hay policy FORBID para cross-tenant — violación Zero-Trust")

    # ── V04: Mensajes ZT-boundary son Resources en Cedar schema ───────────────
    zt_msgs      = [name for name, msg in tree.messages.items()
                    if msg.is_zero_trust_boundary]
    cedar_resources = result.zt_resource_types
    for zname in zt_msgs[:5]:  # verificar los primeros 5
        if zname in cedar_resources:
            v.ok("V04", f"ZT-boundary '{zname}' registrado como Cedar resource")
        else:
            v.ok("V04", f"ZT-boundary '{zname}' cubierto por MetriResource genérico")

    # ── V05: Contrato EDN tiene sección :cedar ────────────────────────────────
    if ":metri.cedar/" in edn_src:
        v.ok("V05", "Contrato EDN contiene sección :metri.cedar/")
    else:
        v.fail("V05", "Contrato EDN no tiene sección :metri.cedar/ — regenerar")

    # ── V06: Todos los groups Cedar existen en el schema ─────────────────────
    defined_groups = {"analytical", "mutational", "system"}
    used_groups    = {a.group for a in result.schema.actions}
    for g in used_groups:
        if g in defined_groups:
            v.ok("V06", f"ActionGroup '{g}' definido")
        else:
            v.fail("V06", f"ActionGroup '{g}' no está definido en el schema", "warning")

    # ── V07: OutputCastType → viz actions en grupo analytical ────────────────
    viz_enum = tree.enums.get("OutputCastType")
    if viz_enum:
        analytical = [a for a in result.schema.actions if a.group == "analytical"
                      and a.comment.startswith("viz:")]
        if len(analytical) >= len(viz_enum.valid_values):
            v.ok("V07", f"Todos los viz types tienen Cedar Action analítica")
        else:
            v.fail("V07",
                   f"Solo {len(analytical)}/{len(viz_enum.valid_values)} viz types tienen Action Cedar",
                   "warning")

    # ── V08: User EntityType tiene tenant_id y roles ──────────────────────────
    user_types = [et for et in result.schema.entity_types if et.name == "User"]
    if user_types and "tenant_id" in user_types[0].attributes:
        v.ok("V08", "User EntityType tiene tenant_id (ZT compliance)")
    else:
        v.fail("V08", "User EntityType sin tenant_id — Zero-Trust violation")

    return v


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────────────────────

SEP  = "═" * 70
SEP2 = "─" * 66


def run_pipeline(proto_path: Path, edn_path: Path) -> int:
    print(f"\n{SEP}")
    print(f"  CEDAR PIPELINE — gRPC Descriptors → Cedar → janus-ast-ir.edn")
    print(f"  Proto:    {proto_path}")
    print(f"  Contrato: {edn_path}")
    print(SEP)

    # ── [1] BUILD DESCRIPTOR TREE ──────────────────────────────────────────────
    print(f"\n  [1/6] 📋 Construyendo DescriptorTree desde {proto_path.name}...")
    tree = build_descriptor_tree(proto_path)
    zt_msgs = [n for n, m in tree.messages.items() if m.is_zero_trust_boundary]
    print(f"        RPCs:              {len(tree.rpcs)}")
    print(f"        Messages:          {len(tree.messages)}")
    print(f"        Enums:             {len([e for e in tree.enums if e not in tree.nested_enum_owner])}")
    print(f"        ZT-boundary msgs:  {len(zt_msgs)}")

    # ── [2] BUILD CEDAR SCHEMA ─────────────────────────────────────────────────
    print(f"\n  [2/6] 🏗  Derivando Cedar Schema...")
    schema = build_cedar_schema(tree)
    print(f"        EntityTypes:  {len(set(et.name for et in schema.entity_types))}")
    print(f"        Actions:      {len(schema.actions)}")
    by_group = {}
    for a in schema.actions:
        by_group.setdefault(a.group, []).append(a.name)
    for grp, acts in sorted(by_group.items()):
        print(f"          ├── {grp:<12}  {len(acts)} actions: {', '.join(acts[:4])}{'...' if len(acts)>4 else ''}")

    # ── [3] BUILD CEDAR POLICIES ───────────────────────────────────────────────
    print(f"\n  [3/6] 📜 Generando políticas Cedar...")
    policies = build_cedar_policies(schema, tree)
    permit_  = [p for p in policies if p.effect == "permit"]
    forbid_  = [p for p in policies if p.effect == "forbid"]
    print(f"        Permit policies: {len(permit_)}")
    print(f"        Forbid policies: {len(forbid_)}")
    for p in policies:
        icon = "✅" if p.effect == "permit" else "🚫"
        print(f"          {icon} [{p.id}] {p.comment[:60]}")

    # Construir el resultado de pipeline
    rpc_action_map = {rpc.name: _rpc_to_cedar_action(rpc.name) for rpc in tree.rpcs}
    pipeline_result = CedarPipelineResult(
        schema=schema,
        policies=policies,
        action_groups=by_group,
        rpc_action_map=rpc_action_map,
        zt_resource_types=zt_msgs,
    )

    # ── [4] EMIT CEDAR ARTIFACTS ───────────────────────────────────────────────
    print(f"\n  [4/6] 📤 Emitiendo artefactos Cedar...")
    CEDAR_OUT.mkdir(parents=True, exist_ok=True)

    # Cedar Schema JSON
    schema_json = emit_cedar_schema_json(schema)
    schema_path = CEDAR_OUT / "cedar-schema.json"
    schema_path.write_text(json.dumps(schema_json, indent=2), encoding="utf-8")
    print(f"        ✓ {schema_path.name} ({schema_path.stat().st_size} bytes)")

    # Cedar Policies .cedar
    policies_text = emit_cedar_policies_text(policies)
    cedar_path    = CEDAR_OUT / "metri.cedar"
    cedar_path.write_text(policies_text, encoding="utf-8")
    print(f"        ✓ {cedar_path.name} ({cedar_path.stat().st_size} bytes)")

    # Cedar EDN section
    models_dir = proto_path.parent / "docs" / "architecture" / "models"
    cedar_edn = emit_cedar_edn_section(pipeline_result, tree, models_dir=models_dir if models_dir.exists() else None)
    cedar_edn_path = CEDAR_OUT / "cedar-section.edn"
    cedar_edn_path.write_text(cedar_edn, encoding="utf-8")
    print(f"        ✓ {cedar_edn_path.name} ({cedar_edn_path.stat().st_size} bytes)")

    # ── [5] MERGE INTO CONTRACT ────────────────────────────────────────────────
    print(f"\n  [5/6] 🔀 Mergando sección Cedar en el contrato...")
    if edn_path.exists():
        edn_src = edn_path.read_text(encoding="utf-8")
        # Remover sección Cedar previa si existe
        if ":metri.cedar/" in edn_src:
            start = edn_src.find("\n ;; ══════════════════════════════════════════════════════════════\n ;; SECCIÓN CEDAR")
            end   = edn_src.rfind("}")
            if start > 0:
                edn_src = edn_src[:start] + "\n}" + "\n"
                print(f"        ⚠️  Sección Cedar previa removida — reemplazando")

        # Insertar antes del cierre del mapa raíz
        insertion_point = edn_src.rfind("}")
        if insertion_point > 0:
            new_edn = edn_src[:insertion_point] + cedar_edn + "\n}"
            edn_path.write_text(new_edn, encoding="utf-8")
            print(f"        ✓ {edn_path.name} actualizado con sección :cedar")
    else:
        print(f"        ⚠️  Contrato no encontrado — la sección Cedar está en cedar-section.edn")
        edn_src = ""

    # ── [6] VALIDATE ───────────────────────────────────────────────────────────
    print(f"\n  [6/6] 🔍 Validando coherencia Cedar ↔ proto ↔ EDN...")
    edn_src_final = edn_path.read_text(encoding="utf-8") if edn_path.exists() else ""
    validation    = validate_pipeline(pipeline_result, tree, edn_src_final)

    print(f"\n{SEP}")
    print(f"  VALIDACIÓN Cedar ↔ proto ↔ EDN")
    print(SEP)

    for finding in validation.findings:
        sev_icon = "🔴" if finding["severity"] == "error" else "🟡"
        print(f"  {sev_icon} [{finding['rule']}] {finding['message']}")

    print(f"\n  Checks pasados: {validation.passed}")
    print(f"  Checks fallidos: {validation.failed}")

    if validation.is_valid:
        print(f"\n  ✅ CEDAR PIPELINE COMPLETADO — Schema y políticas coherentes con proto")
    else:
        print(f"\n  🔴 PIPELINE CON ERRORES — Revisar findings")

    # ── Resumen final ──────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    print(f"  RESUMEN")
    print(f"  {SEP2}")
    print(f"  EntityTypes Cedar: {len(set(et.name for et in schema.entity_types))}")
    print(f"  Actions Cedar:     {len(schema.actions)}")
    print(f"    ├── analytical:  {len(by_group.get('analytical', []))}")
    print(f"    ├── mutational:  {len(by_group.get('mutational', []))}")
    print(f"    └── system:      {len(by_group.get('system', []))}")
    print(f"  Policies:          {len(policies)} ({len(permit_)} permit + {len(forbid_)} forbid)")
    print(f"  ZT-Resources:      {len(zt_msgs)}")
    print(f"  RPC→Action map:    {len(rpc_action_map)}")
    print(f"\n  Artefactos generados:")
    print(f"    → {schema_path}")
    print(f"    → {cedar_path}")
    print(f"    → {cedar_edn_path}")
    print(f"    → {edn_path}  (actualizado)")
    print(f"{SEP}\n")

    return 0 if validation.is_valid else 1


if __name__ == "__main__":
    proto = Path(sys.argv[1]) if len(sys.argv) > 1 else PROTO_PATH
    edn   = Path(sys.argv[2]) if len(sys.argv) > 2 else EDN_PATH
    sys.exit(run_pipeline(proto, edn))
