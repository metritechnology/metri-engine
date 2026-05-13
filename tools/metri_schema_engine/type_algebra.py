"""
type_algebra.py — Paso 2: Álgebra de tipos proto3 → Malli spec.

Responsabilidad única: definir las 15 reglas de mapeo deterministas
que convierten cada tipo proto3 en su especificación Malli equivalente.

Las reglas son funciones PURAS: mismo input → mismo output. Sin estado.
No emite EDN. No toma decisiones semánticas (eso es work de contract_synthesizer).

Reglas implementadas:
  R1  — string   → :string
  R2  — int32    → [:int {:min N :max M}]
  R3  — int64    → :int
  R4  — double   → :double
  R5  — bool     → :boolean
  R6  — bytes    → :bytes
  R7  — Struct/Value/Timestamp → :map/:any/:inst
  R8  — enum E   → [:enum :V1 :V2 ...]  (sin sentinel)
  R9  — repeated T → [:vector <T-spec>]
  R10 — map<K,V> → [:map-of K-spec V-spec]
  R11 — oneof    → [:or [:map [:f1 s1]] [:map [:f2 s2]] ...]
  R12 — Message  → [:ref :metri.spec/message-name]
  R13 — message  → [:map [:key spec] ...]
  R14 — recursive → [:schema {:registry {::N [...]}} [:ref ::N]]
  R15 — google.protobuf.* → spec Malli
"""

from __future__ import annotations

import re
from typing import Any, Optional

from .descriptor_walker import (
    DescriptorTree, EnumDescriptor, FieldDescriptor,
    MessageDescriptor, OneofDescriptor,
)
from .naming import to_kebab, to_malli_key, to_spec_key

# MalliSpec: tipo alias para cualquier estructura serializable a EDN.
#   str   → keyword atom   ":string"
#   list  → vector/form    [":or", ...]
#   dict  → map            {":optional": True}
MalliSpec = Any


# ─────────────────────────────────────────────────────────────────────────────
# R1–R7: Primitivos proto3 → Malli atoms
# ─────────────────────────────────────────────────────────────────────────────

_PRIMITIVE_MAP: dict[str, MalliSpec] = {
    # R1: Cadenas de texto
    "string":   ":string",
    # R2: Enteros 32-bit con bounds físicos del tipo
    "int32":    [":int", {":min": -2147483648, ":max": 2147483647}],
    "uint32":   [":int", {":min": 0,           ":max": 4294967295}],
    "sint32":   [":int", {":min": -2147483648, ":max": 2147483647}],
    "fixed32":  [":int", {":min": 0,           ":max": 4294967295}],
    "sfixed32": [":int", {":min": -2147483648, ":max": 2147483647}],
    # R3: Enteros 64-bit (Malli no tiene bounds para 64-bit por defecto)
    "int64":    ":int",
    "uint64":   ":int",
    "sint64":   ":int",
    "fixed64":  ":int",
    "sfixed64": ":int",
    # R4: Punto flotante IEEE-754
    "double":   ":double",
    "float":    ":double",
    # R5: Booleano
    "bool":     ":boolean",
    # R6: Datos binarios opacos
    "bytes":    ":bytes",
    # R7: Well-known types de google.protobuf (short names)
    "Struct":    ":map",
    "Value":     ":any",
    "Timestamp": ":inst",
    "Any":       ":any",
}


def map_primitive(proto_type: str) -> Optional[MalliSpec]:
    """R1–R7: Mapea un tipo primitivo proto3 a su spec Malli. None si no es primitivo."""
    return _PRIMITIVE_MAP.get(proto_type)


# ─────────────────────────────────────────────────────────────────────────────
# R8: Enums → [:enum :V1 :V2 ...]
# ─────────────────────────────────────────────────────────────────────────────

def map_enum(enum: EnumDescriptor, include_sentinel: bool = False) -> MalliSpec:
    """
    R8: Enum proto3 → [:enum :VALUE1 :VALUE2 ...]

    El sentinel (_UNSPECIFIED = 0) se excluye por defecto porque Janus lo
    rechaza con :JANUS_400 ante cualquier mensaje con valor 0 en un campo enum.

    Args:
        enum: El EnumDescriptor a convertir.
        include_sentinel: Si True incluye el valor 0. Útil para documentación.
    """
    values = enum.values if include_sentinel else enum.valid_values
    kws = [f":{v.name}" for v in values]
    return [":enum"] + kws


# ─────────────────────────────────────────────────────────────────────────────
# R9: repeated T → [:vector spec]
# ─────────────────────────────────────────────────────────────────────────────

def map_repeated(inner_spec: MalliSpec) -> MalliSpec:
    """R9: repeated T → [:vector <inner_spec>]"""
    return [":vector", inner_spec]


# ─────────────────────────────────────────────────────────────────────────────
# R10: map<K,V> → [:map-of K-spec V-spec]
# ─────────────────────────────────────────────────────────────────────────────

def map_map_field(key_type: str, val_spec: MalliSpec) -> MalliSpec:
    """R10: map<K,V> → [:map-of K-malli V-spec]"""
    if key_type == "string":
        key_spec = ":keyword"
    else:
        key_spec = map_primitive(key_type) or ":keyword"
    return [":map-of", key_spec, val_spec]


# ─────────────────────────────────────────────────────────────────────────────
# R11: oneof { T1 f1; T2 f2; } → [:or branch1 branch2 ...]
# ─────────────────────────────────────────────────────────────────────────────

def map_oneof(oneof: OneofDescriptor, branch_specs: list[MalliSpec]) -> MalliSpec:
    """
    R11: oneof → [:or [:map [:f1 s1]] [:map [:f2 s2]] ...]

    Cada branch se envuelve en [:map [:field-name spec]] para que Malli
    pueda distinguir cuál branch está activo en el runtime de validación.
    """
    branches = [
        [":map", [to_malli_key(fd.name), spec]]
        for fd, spec in zip(oneof.branches, branch_specs)
    ]
    return [":or"] + branches


# ─────────────────────────────────────────────────────────────────────────────
# R12: Referencia a Message → [:ref :metri.spec/message-name]
# ─────────────────────────────────────────────────────────────────────────────

def map_message_ref(message_name: str, prefix: str = "metri.spec") -> MalliSpec:
    """R12: MessageType field → [:ref :prefix/message-name]"""
    return [":ref", to_spec_key(message_name, prefix)]


# ─────────────────────────────────────────────────────────────────────────────
# R13: Message completo → [:map [:key spec] [:key2 {:optional true} spec2] ...]
# ─────────────────────────────────────────────────────────────────────────────

def map_message(
    msg: MessageDescriptor,
    tree: DescriptorTree,
    prefix: str = "metri.spec",
) -> MalliSpec:
    """
    R13: message → [:map entry1 entry2 ...]

    Política de opcionalidad:
      - Campos marcados como requeridos por contrato (tenant_id, entity_type, action)
        NO llevan {:optional true}.
      - Todos los demás campos son opcionales (proto3 no tiene required).
      - Los campos oneof se emiten como un único campo opcional con [:or ...].
    """
    entries: list[Any] = [":map"]

    # Nombres de campos que pertenecen a algún oneof (se emiten aparte)
    oneof_field_names = {
        branch.name
        for oneof in msg.oneofs
        for branch in oneof.branches
    }

    # Campos regulares
    for f in msg.fields:
        if f.name in oneof_field_names:
            continue
        spec = resolve_field_spec(f, tree, prefix)
        key  = to_malli_key(f.name)
        if f.is_required_by_contract:
            entries.append([key, spec])
        else:
            entries.append([key, {":optional": True}, spec])

    # Bloques oneof → campo opcional con [:or branch1 branch2 ...]
    for oneof in msg.oneofs:
        branch_specs = [resolve_field_spec(b, tree, prefix) for b in oneof.branches]
        oneof_spec   = map_oneof(oneof, branch_specs)
        entries.append([to_malli_key(oneof.name), {":optional": True}, oneof_spec])

    return entries


# ─────────────────────────────────────────────────────────────────────────────
# R14: Mensajes recursivos → [:schema {:registry {::N [...]}} [:ref ::N]]
# ─────────────────────────────────────────────────────────────────────────────

def map_recursive_message(
    msg: MessageDescriptor,
    tree: DescriptorTree,
    prefix: str = "metri.spec",
) -> MalliSpec:
    """
    R14: Mensajes con auto-referencia (FilterNode, QueryResponse) →
         [:schema {:registry {::N inner-spec}} [:ref ::N]]

    Usa el registry local de Malli para romper el ciclo sin explosión infinita.
    El local_ref (::N) solo existe dentro de este schema form.
    """
    local_ref = f":metri.spec.local/{to_kebab(msg.name)}"
    inner     = map_message(msg, tree, prefix)
    return [":schema", {":registry": {local_ref: inner}}, [":ref", local_ref]]


# ─────────────────────────────────────────────────────────────────────────────
# R15: google.protobuf.* → well-known Malli specs
# ─────────────────────────────────────────────────────────────────────────────

_GOOGLE_MAP: dict[str, MalliSpec] = {
    "google.protobuf.Struct":    ":map",
    "google.protobuf.Value":     ":any",
    "google.protobuf.Timestamp": ":inst",
    "google.protobuf.Any":       ":any",
}


def map_google_type(full_type: str) -> Optional[MalliSpec]:
    """R15: google.protobuf.X → spec Malli equivalente."""
    return _GOOGLE_MAP.get(full_type)


# ─────────────────────────────────────────────────────────────────────────────
# Resolvedor central de campo — aplica R1–R15 en orden
# ─────────────────────────────────────────────────────────────────────────────

def resolve_field_spec(
    f: FieldDescriptor,
    tree: DescriptorTree,
    prefix: str = "metri.spec",
) -> MalliSpec:
    """
    Resuelve la spec Malli completa de un FieldDescriptor.

    Orden de resolución del tipo base:
      1. R15: google.protobuf.*
      2. R1–R7: primitivos proto3
      3. R8: enum
      4. R12: message → ref
      5. Fallback: :any

    Luego aplica cardinalidad:
      - repeated → R9: [:vector ...]
      - map      → R10: [:map-of ...]
      - singular/oneof → tipo base directo
    """
    proto_type = f.proto_type

    # ── Resolver tipo base ──────────────────────────────────────────────────
    base: MalliSpec

    google = map_google_type(proto_type)
    if google:
        base = google
    else:
        prim = map_primitive(proto_type)
        if prim is not None:
            base = prim
        elif proto_type in tree.enums:
            enum = tree.enums[proto_type]
            is_nested = proto_type in tree.nested_enum_owner
            if is_nested:
                # R8: Nested enum — inline los valores directamente.
                # No emite [:ref] porque el nested enum NO está en la Sección 1.
                base = map_enum(enum, include_sentinel=False)
            else:
                # R8 → R12: Enum global — ya está registrado en Sección 1.
                # Emite [:ref :metri.spec/X] para evitar duplicación.
                base = map_message_ref(proto_type, prefix)
        elif proto_type in tree.messages:
            # R12: referencia a otro message
            base = map_message_ref(proto_type, prefix)
        else:
            # Tipo desconocido (google import externo, etc.) → :any
            base = ":any"

    # ── Aplicar cardinalidad ────────────────────────────────────────────────
    if f.cardinality == "repeated":
        return map_repeated(base)        # R9
    if f.cardinality == "map":
        return map_map_field(f.map_key_type or "string", base)  # R10
    # singular | oneof branch → tipo base sin wrapping
    return base
