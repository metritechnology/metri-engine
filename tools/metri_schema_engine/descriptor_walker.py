"""
descriptor_walker.py — Paso 1: Construcción del DescriptorTree enriquecido.

Recorre el ProtoGraph y produce una representación tipada de alto nivel
de cada message, enum y RPC, con resolución completa de tipos y cardinalidades.

El DescriptorTree es el IR (Intermediate Representation) entre el parser
de texto y el ContractSynthesizer semántico.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

# networkx is imported lazily inside topological_order() — not required at module load time.

# ─────────────────────────────────────────────────────────────────────────────
# Tipos de Descriptor
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FieldDescriptor:
    """Representa un campo de un mensaje proto3."""
    name: str
    proto_type: str          # Tipo proto3 raw: "string", "int32", "FilterNode", ...
    cardinality: str         # "singular" | "repeated" | "map" | "oneof"
    field_number: int
    map_key_type: Optional[str] = None   # Solo para campos map<K,V>
    oneof_name: Optional[str] = None     # Solo para campos dentro de oneof
    comment: str = ""                    # Comentario inline extraído del proto

    @property
    def is_primitive(self) -> bool:
        return self.proto_type in _PRIMITIVES

    @property
    def is_zero_trust_key(self) -> bool:
        return self.name in {"tenant_id", "tenant-id"}

    @property
    def is_optional(self) -> bool:
        """En proto3 todo campo singular es opcional por defecto."""
        return self.cardinality == "singular"

    @property
    def is_required_by_contract(self) -> bool:
        """Campos que el contrato Janus-Aegis marca como requeridos."""
        return self.name in {"tenant_id", "entity_type", "action"}


@dataclass
class EnumValueDescriptor:
    name: str
    number: int
    comment: str = ""

    @property
    def is_sentinel(self) -> bool:
        return "UNSPECIFIED" in self.name.upper() and self.number == 0


@dataclass
class EnumDescriptor:
    name: str
    values: list[EnumValueDescriptor] = field(default_factory=list)
    comment: str = ""

    @property
    def sentinel(self) -> Optional[EnumValueDescriptor]:
        return next((v for v in self.values if v.is_sentinel), None)

    @property
    def valid_values(self) -> list[EnumValueDescriptor]:
        """Valores que Janus acepta (excluye el sentinel = 0)."""
        return [v for v in self.values if not v.is_sentinel]


@dataclass
class OneofDescriptor:
    name: str
    branches: list[FieldDescriptor] = field(default_factory=list)


@dataclass
class MessageDescriptor:
    name: str
    fields: list[FieldDescriptor] = field(default_factory=list)
    oneofs: list[OneofDescriptor] = field(default_factory=list)
    nested_enums: list[EnumDescriptor] = field(default_factory=list)
    comment: str = ""
    is_recursive: bool = False        # True si contiene ref a sí mismo
    recursive_fields: list[str] = field(default_factory=list)

    @property
    def tenant_field(self) -> Optional[FieldDescriptor]:
        return next((f for f in self.fields if f.is_zero_trust_key), None)

    @property
    def is_zero_trust_boundary(self) -> bool:
        return self.tenant_field is not None

    @property
    def singular_fields(self) -> list[FieldDescriptor]:
        return [f for f in self.fields if f.cardinality == "singular"]

    @property
    def repeated_fields(self) -> list[FieldDescriptor]:
        return [f for f in self.fields if f.cardinality == "repeated"]

    @property
    def map_fields(self) -> list[FieldDescriptor]:
        return [f for f in self.fields if f.cardinality == "map"]


@dataclass
class RPCDescriptor:
    name: str
    request_type: str
    response_type: str
    request_stream: bool = False
    response_stream: bool = False
    comment: str = ""

    @property
    def is_streaming(self) -> bool:
        return self.response_stream

    @property
    def is_tenant_scoped(self) -> bool:
        """Será resuelto por el DescriptorTree con acceso al tree completo."""
        return True  # Placeholder — resuelto en DescriptorTree.build()


@dataclass
class DescriptorTree:
    """
    Árbol enriquecido de todos los Descriptors del .proto.
    Es el IR central del transpilador.
    """
    messages: dict[str, MessageDescriptor] = field(default_factory=dict)
    enums: dict[str, EnumDescriptor] = field(default_factory=dict)
    # nested_enum_owner["Conjunction"] = "FilterGroup" — para inline resolution
    nested_enum_owner: dict[str, str] = field(default_factory=dict)
    rpcs: list[RPCDescriptor] = field(default_factory=list)
    service_name: str = ""
    source: str = ""   # texto fuente original para trazabilidad

    def resolve_type(self, proto_type: str) -> str:
        """Determina la categoría de un tipo proto3."""
        if proto_type in _PRIMITIVES:
            return "primitive"
        if proto_type in self.enums:
            return "enum"
        if proto_type in self.messages:
            return "message"
        return "unknown"

    def topological_order(self) -> list[str]:
        """
        Ordena los mensajes topológicamente (dependencias primero).
        Implementado con el algoritmo de Kahn (BFS) — sin dependencias externas.
        Ciclos intencionales (FilterNode, QueryResponse) son detectados y
        emitidos como schemas recursivos al final del orden.
        """
        # Construir grafo de adyacencia y detectar ciclos
        adj: dict[str, set[str]] = {n: set() for n in self.messages}
        for name, msg in self.messages.items():
            for f in msg.fields:
                if f.proto_type in self.messages and f.proto_type != name:
                    adj[name].add(f.proto_type)
            for oneof in msg.oneofs:
                for branch in oneof.branches:
                    if branch.proto_type in self.messages and branch.proto_type != name:
                        adj[name].add(branch.proto_type)

        # Detectar nodos en ciclos con DFS
        cyclic: set[str] = set()
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n: WHITE for n in self.messages}

        def dfs_cycle(node: str) -> bool:
            color[node] = GRAY
            for nb in adj[node]:
                if color[nb] == GRAY:
                    cyclic.add(node)
                    cyclic.add(nb)
                elif color[nb] == WHITE:
                    if dfs_cycle(nb):
                        cyclic.add(node)
            color[node] = BLACK
            return node in cyclic

        for n in list(self.messages.keys()):
            if color[n] == WHITE:
                dfs_cycle(n)

        # Kahn's algorithm sobre nodos no-cíclicos
        non_cyclic = [n for n in self.messages if n not in cyclic]
        in_degree  = {n: 0 for n in non_cyclic}
        nc_set     = set(non_cyclic)
        for n in non_cyclic:
            for nb in adj[n]:
                if nb in nc_set:
                    in_degree[nb] = in_degree.get(nb, 0) + 1

        queue  = [n for n in non_cyclic if in_degree[n] == 0]
        result = []
        while queue:
            node = queue.pop(0)
            result.append(node)
            for nb in adj[node]:
                if nb in nc_set:
                    in_degree[nb] -= 1
                    if in_degree[nb] == 0:
                        queue.append(nb)

        # Nodos cíclicos al final (serán emitidos como [:schema ...])
        result_set = set(result)
        for n in self.messages:
            if n not in result_set:
                result.append(n)

        return list(reversed(result))

    def is_recursive_message(self, name: str) -> bool:
        """True si el mensaje tiene referencia directa o indirecta a sí mismo."""
        visited = set()
        def dfs(curr: str) -> bool:
            if curr == name and curr in visited:
                return True
            if curr in visited:
                return False
            visited.add(curr)
            msg = self.messages.get(curr)
            if not msg:
                return False
            for f in msg.fields:
                if f.proto_type in self.messages:
                    if dfs(f.proto_type):
                        return True
            return False
        # Reiniciar para DFS desde vecinos
        msg = self.messages.get(name)
        if not msg:
            return False
        for f in msg.fields:
            if f.proto_type == name:
                return True
            if f.proto_type in self.messages:
                visited = set()
                visited.add(name)
                if dfs(f.proto_type):
                    return True
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Tipos primitivos proto3
# ─────────────────────────────────────────────────────────────────────────────

_PRIMITIVES = {
    "string", "int32", "int64", "uint32", "uint64", "sint32", "sint64",
    "bool", "bytes", "double", "float", "fixed32", "fixed64",
    "sfixed32", "sfixed64",
    # Well-known types
    "Struct", "Timestamp", "Value", "Any",
    # google prefix handled separately
}

_GOOGLE_TYPES = {
    "google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.Timestamp",
    "google.protobuf.Any",
}


# ─────────────────────────────────────────────────────────────────────────────
# Builder — construye el DescriptorTree desde el texto fuente
# ─────────────────────────────────────────────────────────────────────────────

_RE_SERVICE     = re.compile(r"^service\s+(\w+)\s*\{", re.MULTILINE)
_RE_RPC         = re.compile(
    r"rpc\s+(\w+)\s*\(\s*(stream\s+)?(\w+)\s*\)\s*returns\s*\(\s*(stream\s+)?(\w+)\s*\)",
    re.MULTILINE,
)
_RE_MESSAGE     = re.compile(r"^message\s+(\w+)\s*\{", re.MULTILINE)
_RE_ENUM        = re.compile(r"^enum\s+(\w+)\s*\{", re.MULTILINE)
_RE_ENUM_VALUE  = re.compile(r"^\s+(\w+)\s*=\s*(\d+)\s*;", re.MULTILINE)
_RE_MAP_FIELD   = re.compile(
    r"^\s+map<\s*(\w+)\s*,\s*([^>]+)>\s+(\w+)\s*=\s*(\d+)\s*;", re.MULTILINE
)
_RE_REPEATED    = re.compile(
    r"^\s+repeated\s+([^\s]+)\s+(\w+)\s*=\s*(\d+)\s*;", re.MULTILINE
)
_RE_SINGULAR    = re.compile(
    r"^\s+(?!repeated|map|oneof|//)([A-Za-z][\w.]*)\s+(\w+)\s*=\s*(\d+)\s*;", re.MULTILINE
)
_RE_ONEOF_BLOCK = re.compile(r"^\s+oneof\s+(\w+)\s*\{([^}]+)\}", re.MULTILINE | re.DOTALL)
_RE_ONEOF_FIELD = re.compile(
    r"^\s+([A-Za-z][\w.]*)\s+(\w+)\s*=\s*(\d+)\s*;", re.MULTILINE
)
_RE_NESTED_ENUM = re.compile(r"^\s+enum\s+(\w+)\s*\{([^}]+)\}", re.MULTILINE | re.DOTALL)
_RE_LINE_COMMENT= re.compile(r"//\s*(.*?)$", re.MULTILINE)


def build_descriptor_tree(proto_path: str | Path) -> DescriptorTree:
    """
    Punto de entrada principal.
    Lee el .proto y construye el DescriptorTree completo.
    """
    source = Path(proto_path).read_text(encoding="utf-8")
    tree = DescriptorTree(source=source)

    # 1. Service name
    svc_m = _RE_SERVICE.search(source)
    if svc_m:
        tree.service_name = svc_m.group(1)

    # 2. Enums globales
    for m in _RE_ENUM.finditer(source):
        name = m.group(1)
        block_start = m.end()
        body, _ = _extract_block(source, block_start)
        enum = _parse_enum(name, body)
        tree.enums[name] = enum

    # 3. Messages (con nested enums y campos)
    msg_starts = [
        (m.group(1), m.end())
        for m in _RE_MESSAGE.finditer(source)
    ]
    for msg_name, block_start in msg_starts:
        body, _ = _extract_block(source, block_start)
        msg = _parse_message(msg_name, body, tree.enums)
        tree.messages[msg_name] = msg

    # 4. RPCs
    for m in _RE_RPC.finditer(source):
        rpc = RPCDescriptor(
            name=m.group(1),
            request_stream=bool(m.group(2)),
            request_type=m.group(3),
            response_stream=bool(m.group(4)),
            response_type=m.group(5),
        )
        tree.rpcs.append(rpc)

    # 5. Marcar mensajes recursivos y registrar nested enums
    for name, msg in tree.messages.items():
        if tree.is_recursive_message(name):
            msg.is_recursive = True
            msg.recursive_fields = [
                f.name for f in msg.fields if f.proto_type == name
            ]
        # Registrar nested enums con clave corta (e.g. "Conjunction")
        # y clave cualificada (e.g. "FilterGroup.Conjunction") para resolución
        for nested_enum in msg.nested_enums:
            tree.enums[nested_enum.name] = nested_enum
            qualified_key = f"{name}.{nested_enum.name}"
            tree.enums[qualified_key] = nested_enum
            tree.nested_enum_owner[nested_enum.name] = name
            tree.nested_enum_owner[qualified_key] = name

    return tree


def _parse_enum(name: str, body: str) -> EnumDescriptor:
    """Parsea un bloque enum y extrae sus valores."""
    # Remover comentarios para no confundir valores
    clean_body = re.sub(r"//[^\n]*", "", body)
    enum = EnumDescriptor(name=name)
    for m in _RE_ENUM_VALUE.finditer(clean_body):
        val_name = m.group(1)
        val_num  = int(m.group(2))
        enum.values.append(EnumValueDescriptor(name=val_name, number=val_num))
    enum.values.sort(key=lambda v: v.number)
    return enum


def _parse_message(name: str, body: str, known_enums: dict) -> MessageDescriptor:
    """Parsea un bloque message y extrae todos sus campos."""
    msg = MessageDescriptor(name=name)

    # a. Nested enums
    for m in _RE_NESTED_ENUM.finditer(body):
        nested_name = m.group(1)
        nested_body = m.group(2)
        nested_enum = _parse_enum(nested_name, nested_body)
        msg.nested_enums.append(nested_enum)

    # Crear copia sin nested enums ni bloques oneof para parsear campos singulares
    clean_body = re.sub(r"\s+enum\s+\w+\s*\{[^}]+\}", "", body)
    clean_body_no_oneof = re.sub(r"\s+oneof\s+\w+\s*\{[^}]+\}", "", clean_body, flags=re.DOTALL)
    # Remover comentarios
    clean_singular = re.sub(r"//[^\n]*", "", clean_body_no_oneof)

    # b. Map fields
    for m in _RE_MAP_FIELD.finditer(clean_body):
        key_type = m.group(1)
        val_type = m.group(2).strip()
        field_name = m.group(3)
        field_num  = int(m.group(4))
        msg.fields.append(FieldDescriptor(
            name=field_name,
            proto_type=val_type,
            cardinality="map",
            field_number=field_num,
            map_key_type=key_type,
        ))

    # c. Repeated fields (en el body sin oneof, sin nested enums)
    map_field_names = {f.name for f in msg.fields}
    for m in _RE_REPEATED.finditer(clean_singular):
        proto_type = m.group(1)
        field_name = m.group(2)
        field_num  = int(m.group(3))
        if field_name not in map_field_names:
            msg.fields.append(FieldDescriptor(
                name=field_name,
                proto_type=proto_type,
                cardinality="repeated",
                field_number=field_num,
            ))

    # d. Singular fields
    existing_names = {f.name for f in msg.fields}
    for m in _RE_SINGULAR.finditer(clean_singular):
        proto_type = m.group(1)
        field_name = m.group(2)
        field_num  = int(m.group(3))
        if field_name not in existing_names and proto_type not in {"option", "syntax", "package"}:
            msg.fields.append(FieldDescriptor(
                name=field_name,
                proto_type=proto_type,
                cardinality="singular",
                field_number=field_num,
            ))
            existing_names.add(field_name)

    # e. Oneof blocks
    oneof_field_names = set()
    for m in _RE_ONEOF_BLOCK.finditer(clean_body):
        oneof_name = m.group(1)
        oneof_body = re.sub(r"//[^\n]*", "", m.group(2))
        oneof = OneofDescriptor(name=oneof_name)
        for fm in _RE_ONEOF_FIELD.finditer(oneof_body):
            proto_type = fm.group(1)
            field_name = fm.group(2)
            field_num  = int(fm.group(3))
            branch = FieldDescriptor(
                name=field_name,
                proto_type=proto_type,
                cardinality="oneof",
                field_number=field_num,
                oneof_name=oneof_name,
            )
            oneof.branches.append(branch)
            oneof_field_names.add(field_name)
        msg.oneofs.append(oneof)

    # Ordenar campos por field_number
    msg.fields.sort(key=lambda f: f.field_number)
    return msg


def _extract_block(source: str, start: int) -> tuple[str, int]:
    """Extrae contenido entre llaves balanceadas desde `start`."""
    depth = 1
    i = start
    while i < len(source) and depth > 0:
        if source[i] == "{":
            depth += 1
        elif source[i] == "}":
            depth -= 1
        i += 1
    return source[start:i - 1], i
