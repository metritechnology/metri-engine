"""
edn_emitter.py — Paso 4: Serialización del JanusAegisContract a EDN idiomático.

Convierte el modelo Python a un string EDN que:
- Es válido Clojure/EDN
- Contiene comentarios de trazabilidad (;; proto-source: ...)
- Ordena las specs topológicamente (dependencias primero)
- Usa kebab-case para keywords
- Emite mapas como {:key val} y vectores como [...]
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .contract_synthesizer import JanusAegisContract, SpecEntry, RPCContractEntry
from .type_algebra import MalliSpec


# ─────────────────────────────────────────────────────────────────────────────
# Punto de entrada
# ─────────────────────────────────────────────────────────────────────────────

def emit_edn(contract: JanusAegisContract) -> str:
    """
    Serializa el JanusAegisContract completo a un string EDN idiomático.
    """
    lines: list[str] = []

    # Header
    lines += _emit_header(contract)

    # Apertura del registry
    lines.append("{")
    lines.append(f" :metri.ast/registry")
    lines.append(" {")

    # ── SECCIÓN 1: Enums ────────────────────────────────────────────────────
    enum_specs = [s for s in contract.specs if "enum" in s.tags]
    if enum_specs:
        lines.append("")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        lines.append("  ;; SECCIÓN 1 — Enums (proto3 → [:enum :V1 :V2 ...])")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        for entry in enum_specs:
            lines += _emit_spec_entry(entry, indent=2)

    # ── SECCIÓN 2: Mensajes recursivos (ciclos intencionales) ───────────────
    recursive_specs = [s for s in contract.specs if "recursive" in s.tags]
    if recursive_specs:
        lines.append("")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        lines.append("  ;; SECCIÓN 2 — Schemas Recursivos (ciclos intencionales del DSL)")
        lines.append("  ;; [:schema {:registry {::N ...}} [:ref ::N]]")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        for entry in recursive_specs:
            lines += _emit_spec_entry(entry, indent=2)

    # ── SECCIÓN 3: Mensajes Zero-Trust boundary ─────────────────────────────
    zt_specs = [
        s for s in contract.specs
        if "zero-trust-boundary" in s.tags and "recursive" not in s.tags
    ]
    if zt_specs:
        lines.append("")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        lines.append("  ;; SECCIÓN 3 — Mensajes Zero-Trust (contienen tenant_id)")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        for entry in zt_specs:
            lines += _emit_spec_entry(entry, indent=2)

    # ── SECCIÓN 4: Resto de mensajes ────────────────────────────────────────
    other_specs = [
        s for s in contract.specs
        if "enum" not in s.tags
        and "recursive" not in s.tags
        and "zero-trust-boundary" not in s.tags
    ]
    if other_specs:
        lines.append("")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        lines.append("  ;; SECCIÓN 4 — Mensajes de Dominio")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        for entry in other_specs:
            lines += _emit_spec_entry(entry, indent=2)

    # ── SECCIÓN 5: Railway-Oriented Error Contracts ─────────────────────────
    if contract.railway_specs:
        lines.append("")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        lines.append("  ;; SECCIÓN 5 — Railway-Oriented Error Contracts")
        lines.append("  ;; [:or success-branch failure-branch]")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        for entry in contract.railway_specs:
            lines += _emit_spec_entry(entry, indent=2)

    # ── SECCIÓN 6: Contratos de RPCs ────────────────────────────────────────
    if contract.rpc_contracts:
        lines.append("")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        lines.append("  ;; SECCIÓN 6 — Contratos de RPCs")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        for rpc in contract.rpc_contracts:
            lines += _emit_rpc_entry(rpc, indent=2)

    # ── SECCIÓN 7: Zero-Trust Global ────────────────────────────────────────
    if contract.zero_trust_clauses:
        lines.append("")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        lines.append("  ;; SECCIÓN 7 — Cláusulas Zero-Trust Globales (Janus invariants)")
        lines.append("  ;; ══════════════════════════════════════════════════════════════")
        for k, v in contract.zero_trust_clauses.items():
            lines.append(f"  {k}")
            # P1: valores que son str con ;;-prefix son prose docs — emitir como EDN string,
            # no como comentario colgante (que deja la key sin valor → EDN invalid).
            if isinstance(v, str) and v.startswith(";;"):
                # Convertir en string EDN válido con el mensaje de la regla
                rule_text = v.lstrip(";; ").strip()
                lines.append(f'  "{rule_text}"')
            else:
                lines.append(f"  {malli_to_edn(v)}")
            lines.append("")

    # ── SECCIÓN Cedar: Modelo de Autorización Zero-Trust ────────────────
    lines += _emit_cedar_section()

    # Cierre
    lines.append(" }")
    lines.append("}")
    lines.append("")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Emitters de secciones
# ─────────────────────────────────────────────────────────────────────────────

def _emit_header(contract: JanusAegisContract) -> list[str]:
    return [
        ";; ==========================================================================",
        ";; METRI ENGINE — JANUS-AEGIS CONTRACT",
        ";; ==========================================================================",
        ";; GENERADO AUTOMÁTICAMENTE por proto2edn — NO EDITAR MANUALMENTE",
        f";; Fuente:    {contract.source_proto}",
        f";; Generado: {contract.generated_at}",
        f";; Servicio: {contract.service_name}",
        ";;",
        ";; RACIONAL DE DISEÑO (DIP):",
        ";; ─────────────────────────────────────────────────────────────────────────",
        ";; • Aegis implementa :ast-definition — es ciego a gRPC y tipos de gráficas.",
        ";; • Janus gestiona :orchestration-meta — decora el resultado de Aegis.",
        ";; • Este contrato es la ÚNICA fuente de verdad derivada de metri.proto.",
        ";; ==========================================================================",
        "",
    ]


def _emit_spec_entry(entry: SpecEntry, indent: int = 2) -> list[str]:
    """Emite una spec con comentario de trazabilidad."""
    pad = " " * indent
    lines = []
    lines.append(f"{pad};; proto-source: {entry.source_proto}")
    if entry.tags:
        lines.append(f"{pad};; tags: {' '.join(entry.tags)}")
    spec_str = malli_to_edn(entry.spec, indent=indent)
    lines.append(f"{pad}{entry.key}")
    lines.append(f"{pad}{spec_str}")
    lines.append("")
    return lines


def _emit_rpc_entry(rpc: RPCContractEntry, indent: int = 2) -> list[str]:
    """Emite la spec de contrato de un RPC con key en kebab-case correcto."""
    from .naming import to_kebab
    pad = " " * indent
    lines = []
    tags_str = " ".join(rpc.tags) if rpc.tags else "—"
    lines.append(f"{pad};; rpc: {rpc.name} | tags: {tags_str}")
    # P3: usar kebab-case para la key, no .lower()
    key = f":metri.rpc/{to_kebab(rpc.name)}"
    spec = [":map",
        [":request",  rpc.request_spec_key],
        [":response", rpc.response_spec_key],
        [":streaming",        rpc.streaming],
        [":zero-trust-scoped", rpc.tenant_scoped],
    ]
    lines.append(f"{pad}{key}")
    lines.append(f"{pad}{malli_to_edn(spec, indent=indent)}")
    lines.append("")
    return lines


# ─────────────────────────────────────────────────────────────────────────────
# Serializador de MalliSpec → EDN string
# ─────────────────────────────────────────────────────────────────────────────

def malli_to_edn(spec: MalliSpec, indent: int = 0) -> str:
    """
    Serializa una MalliSpec (Python data) a un string EDN.

    Tipos manejados:
      str   → emitido literal (keywords, atoms)
      bool  → true / false
      int   → número
      float → número
      list  → vector [ ... ]
      dict  → mapa { ... }
      None  → nil
    """
    if spec is None:
        return "nil"
    if isinstance(spec, bool):
        return "true" if spec else "false"
    if isinstance(spec, (int, float)):
        return str(spec)
    if isinstance(spec, str):
        # Si ya es un keyword (:algo) o una expresión que no debe quotearse
        return spec
    if isinstance(spec, list):
        return _list_to_edn(spec, indent)
    if isinstance(spec, dict):
        return _dict_to_edn(spec, indent)
    # Fallback
    return str(spec)


def _list_to_edn(lst: list, indent: int) -> str:
    """[ item1 item2 ... ] con indentación inteligente."""
    if not lst:
        return "[]"

    items = [malli_to_edn(item, indent + 1) for item in lst]

    # Heurística: si es corto, en una sola línea
    single = "[" + " ".join(items) + "]"
    if len(single) <= 80:
        return single

    # Multi-línea
    pad = " " * (indent + 1)
    inner = ("\n" + pad).join(items)
    return f"[{inner}]"


def _dict_to_edn(d: dict, indent: int) -> str:
    """{:k1 v1 :k2 v2} EDN map."""
    if not d:
        return "{}"
    pairs = []
    for k, v in d.items():
        k_str = malli_to_edn(k, indent + 1)
        v_str = malli_to_edn(v, indent + 1)
        pairs.append(f"{k_str} {v_str}")
    inner = ", ".join(pairs)
    if len(inner) + 2 <= 80:
        return "{" + inner + "}"
    # Multi-línea
    pad = " " * (indent + 1)
    inner_ml = ("\n" + pad).join(pairs)
    return "{" + inner_ml + "}"


# ─────────────────────────────────────────────────────────────────────────────
# Sección Cedar — bloque estático derivado del modelo de autorización
# Fuente: role.json + user.json + user_group.json → CedarPolicyEngine
# NO se deriva de metres.proto — se emite siempre como bloque fijo.
# ─────────────────────────────────────────────────────────────────────────────

def _emit_cedar_section() -> list[str]:
    """
    Emite la sección Cedar de autorización Zero-Trust.

    Este bloque es estático: su contenido viene del modelo de políticas
    (Códice roles / users / groups) y NO de los descriptores proto.
    El emitter lo añade siempre al final del contrato para garantizar
    que el fichero generado sea 100% completo sin edición manual.
    """
    return [
        "",
        " ;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;",
        " ;; SECCIÓN CEDAR — Modelo de Autorización Zero-Trust",
        " ;; Derivado de: role.json + user.json + user_group.json → CedarPolicyEngine",
        " ;; Alineado con:  cedar-janus-contract-v1.edn (DOMINIO 2-5)",
        " ;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;",
        "",
        " :metri.cedar/schema-namespace",
        ' "Metri"',
        "",
        " ;; EntityTypes Cedar derivados de los modelos del Códice",
        " :metri.cedar/entity-types",
        " [:enum :User :MetriResource :ActionGroup]",
        "",
        " ;; Scopes ABAC — :metri.cedar/query-scope (contrato v1 DOMINIO 2)",
        " ;; ALL: sin filtro propiedad | OWN: propietario | ASSIGNED: asignado",
        " ;; OWN_OR_ASSIGNED: ambos | NONE: :JANUS_400 inmediato",
        " :metri.cedar/query-scope",
        ' [:enum "ALL" "OWN" "ASSIGNED" "OWN_OR_ASSIGNED" "NONE"]',
        "",
        " ;; Actions analíticas (VIEW, EXPORT) — producen domain-boundaries RLS",
        " :metri.cedar/analytical-actions",
        " [:set]",
        "",
        " ;; Actions mutacionales — Cedar evalúa in-memory, domain-boundaries = {}",
        " :metri.cedar/mutational-actions",
        " [:set",
        "  :CREATE",
        "  :DELETE",
        "  :EXECUTE",
        "  :UPDATE]",
        "",
        " :metri.cedar/permit-policies",
        ' [:set "P00_SUPERADMIN"'
        ' "P01_ALL_VIEW_EXPORT" "P01_ALL_VIEW_VIEW"'
        ' "P02_ALL_MUTATE_CREATE" "P02_ALL_MUTATE_DELETE" "P02_ALL_MUTATE_EXECUTE" "P02_ALL_MUTATE_UPDATE" "P02_ALL_MUTATE_UPSERT"'
        ' "P03_OWN_VIEW_EXPORT" "P03_OWN_VIEW_VIEW"'
        ' "P04_OWN_MUTATE_CREATE" "P04_OWN_MUTATE_DELETE" "P04_OWN_MUTATE_EXECUTE" "P04_OWN_MUTATE_UPDATE" "P04_OWN_MUTATE_UPSERT"'
        ' "P05_ASSIGNED_VIEW_EXPORT" "P05_ASSIGNED_VIEW_VIEW"'
        ' "P06_ASSIGNED_MUTATE_CREATE" "P06_ASSIGNED_MUTATE_DELETE" "P06_ASSIGNED_MUTATE_EXECUTE" "P06_ASSIGNED_MUTATE_UPDATE" "P06_ASSIGNED_MUTATE_UPSERT"'
        ' "P07_OWN_OR_ASSIGNED_VIEW_EXPORT" "P07_OWN_OR_ASSIGNED_VIEW_VIEW"'
        ' "P08_OWN_OR_ASSIGNED_MUTATE_CREATE" "P08_OWN_OR_ASSIGNED_MUTATE_DELETE"'
        ' "P08_OWN_OR_ASSIGNED_MUTATE_EXECUTE" "P08_OWN_OR_ASSIGNED_MUTATE_UPDATE" "P08_OWN_OR_ASSIGNED_MUTATE_UPSERT"]',
        "",
        " ;; Políticas de denegación explícita — NUNCA eliminar P_CROSS_TENANT",
        " :metri.cedar/forbid-policies",
        ' [:set "P_NONE_DENY" "P_CROSS_TENANT" "P_SUSPENDED"]',
        "",
        " ;; Resolución scope → predicado Cedar when{}",
        " ;; Consumido por Janus para construir el nodo [:where] del AST IR",
        " :metri.cedar/scope-field-resolution",
        " [:map",
        '  [:all "principal.tenant_id == resource.tenant_id"]',
        '  [:own "principal.tenant_id == resource.tenant_id && principal.user_id == resource.owner_field"]',
        '  [:assigned "principal.tenant_id == resource.tenant_id && principal.user_id == resource.assignee_field"]',
        '  [:own_or_assigned "principal.tenant_id == resource.tenant_id && (principal.user_id == resource.owner_field || principal.user_id == resource.assignee_field)"]',
        '  [:none "false"]',
        " ]",
        "",
        " ;; Invariante de contexto Cedar — validado ANTES de construir el AST IR",
        " ;; Alineado con cedar-janus-contract-v1.edn :metri.cedar/cedar-ctx",
        " :metri.cedar/context-invariant",
        " [:map",
        "  [:tenant-id  {:optional false} :string]   ;; Valkey session — Zero-Trust",
        "  [:user-id    {:optional false} :string]   ;; Valkey session — identidad",
        "  [:roles      {:optional false} [:set :string]] ;; role.name expandido",
        "  [:domain-boundaries {:optional true}",
        "   [:map-of :string",
        "    [:vector {:min 0} [:map",
        '     [:query-scope    [:enum "ALL" "OWN" "ASSIGNED" "OWN_OR_ASSIGNED" "NONE"]]',
        "     [:permitted-locations {:optional true} [:vector :uuid]]",
        "     [:permitted-assets    {:optional true} [:vector :uuid]]]]]]",
        "  [:request {:optional false} :any]] ;; Passthrough gRPC — Cedar no modifica",
        "",
        " ;; Permission Boundary — contrato v1 DOMINIO 3",
        " ;; Root RLS Guardian: domain-boundaries solo contiene entidades RAÍZ.",
        " ;; Sub-recursos (JOINs) heredan el dominio del registro raíz.",
        " :metri.cedar/domain-boundary",
        " [:map",
        '  [:query-scope        [:enum "ALL" "OWN" "ASSIGNED" "OWN_OR_ASSIGNED" "NONE"]]',
        "  [:permitted-locations {:optional true} [:vector :uuid]]",
        "  [:permitted-assets    {:optional true} [:vector :uuid]]]",
        "",
        " ;; Railway: Cedar authorization gate (auth-ok | :JANUS_40x)",
        " :metri.cedar/auth-gate",
        " [:or",
        "  [:map [:principal-ok true] [:action-ok true] [:resource-ok true]]",
        "  [:map [:cedar-error :string] [:janus-code [:enum :JANUS_403 :JANUS_400 :JANUS_401]]]]",
        "",
    ]
