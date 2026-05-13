"""
contract_synthesizer.py — Paso 3: Síntesis semántica del contrato Janus-Aegis.

Responsabilidad única: aplicar las 7 reglas semánticas del contrato sobre el
DescriptorTree y producir el JanusAegisContract (modelo Python intermedio).

NO serializa a EDN (eso es responsabilidad de edn_emitter.py).
NO mapea tipos (eso es responsabilidad de type_algebra.py).
NO parsea texto proto (eso es responsabilidad de descriptor_walker.py).

Las 7 reglas semánticas:
  R1 — Zero-Trust Tenant Isolation
       Todo mensaje con tenant_id es una frontera Zero-Trust.

  R2 — Railway-Oriented Error Contract
       Todo Response con Status emite un union type [:or ok fail].

  R3 — Ciclos Intencionales (FilterNode, QueryResponse)
       Mensajes recursivos → [:schema {:registry {::N ...}} [:ref ::N]]

  R4 — oneof Exclusividad
       Cada oneof branch como [:map [:field spec]] para validación discriminada.

  R5 — Enum Sentinel Gate
       Enums con _UNSPECIFIED = 0 → tag has-sentinel.
       Janus rechaza campo con valor 0 con :JANUS_400.

  R6 — Streaming RPCs
       RPCs con stream response → :streaming true en el contrato.

  R7 — AST Zero-Trust Invariant
       Cláusula global: todo AST Janus debe contener tenant-id en su where-clause.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any

from .descriptor_walker import DescriptorTree, MessageDescriptor
from .naming import to_kebab, to_spec_key
from .type_algebra import (
    MalliSpec,
    map_enum,
    map_message,
    map_recursive_message,
)


# ─────────────────────────────────────────────────────────────────────────────
# Modelo del contrato (Data classes puras — sin lógica)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SpecEntry:
    """
    Una entrada del registry de Malli.

    key         → :metri.spec/filter-node
    spec        → La spec Malli en Python data (lista/dict/str)
    source_proto → Trazabilidad: "message FilterNode field=3"
    tags        → ["zero-trust-boundary", "recursive", "enum", ...]
    """
    key: str
    spec: MalliSpec
    source_proto: str
    tags: list[str] = field(default_factory=list)


@dataclass
class RPCContractEntry:
    """
    Contrato de un RPC individual con metadata de observabilidad y seguridad.

    streaming      → True si el response usa server-side streaming
    tenant_scoped  → True si el request contiene tenant_id (Zero-Trust gate)
    """
    name: str
    request_spec_key: str
    response_spec_key: str
    streaming: bool = False
    tenant_scoped: bool = False
    tags: list[str] = field(default_factory=list)


@dataclass
class JanusAegisContract:
    """
    Modelo Python completo del contrato Janus-Aegis.

    Producido por synthesize() y consumido por emit_edn().
    Es el IR semántico entre el DescriptorTree y la serialización EDN.

    Secciones:
      specs           → Todas las specs Malli en orden topológico
      rpc_contracts   → Un RPCContractEntry por cada RPC del servicio
      railway_specs   → Union types Railway-Oriented por cada Response
      zero_trust_clauses → Invariantes globales de Janus
    """
    specs: list[SpecEntry] = field(default_factory=list)
    rpc_contracts: list[RPCContractEntry] = field(default_factory=list)
    zero_trust_clauses: dict[str, Any] = field(default_factory=dict)
    railway_specs: list[SpecEntry] = field(default_factory=list)

    source_proto: str = ""
    generated_at: str = ""
    service_name: str = ""

    def find_spec(self, key: str) -> "SpecEntry | None":
        """Busca una spec por key en el registry principal."""
        return next((s for s in self.specs if s.key == key), None)


# ─────────────────────────────────────────────────────────────────────────────
# Configuración del contrato
# ─────────────────────────────────────────────────────────────────────────────

_PREFIX = "metri.spec"

# Mensajes con auto-referencia intencional (detectada por el DescriptorTree
# o conocida por diseño del DSL)
_KNOWN_RECURSIVE: frozenset[str] = frozenset({"FilterNode", "FilterGroup", "QueryResponse"})


def _is_railway_response(msg: MessageDescriptor) -> bool:
    """
    R2: Determina si un mensaje es un Response Railway-Oriented.

    Criterio: tiene al menos un campo de tipo Status Y su nombre
    termina en "Response" o es exactamente "Status".
    No se usa una lista hardcodeada — se infiere del árbol.
    """
    has_status_field = any(f.proto_type == "Status" for f in msg.fields)
    is_response_shape = msg.name.endswith("Response") or msg.name == "Status"
    return has_status_field and is_response_shape


# ─────────────────────────────────────────────────────────────────────────────
# API pública
# ─────────────────────────────────────────────────────────────────────────────

def synthesize(
    tree: DescriptorTree,
    source_proto: str = "",
    generated_at: str = "",
) -> JanusAegisContract:
    """
    Punto de entrada principal del ContractSynthesizer.

    Recibe el DescriptorTree, aplica las 7 reglas semánticas en orden
    y devuelve el JanusAegisContract listo para serialización.

    El orden de aplicación es importante:
      1. Enums       → tipos base sin dependencias
      2. Messages    → en orden topológico (dependencias primero)
      3. RPCs        → contratos de cada método gRPC
      4. Zero-Trust  → clausulas globales derivadas del árbol
      5. Railway     → specs de error inferidas del árbol
    """
    contract = JanusAegisContract(
        source_proto=source_proto,
        generated_at=generated_at or datetime.datetime.utcnow().isoformat() + "Z",
        service_name=tree.service_name,
    )

    # Conjunto local de mensajes que son I/O de RPCs (para referencia futura)
    rpc_io_messages: set[str] = {
        msg_name
        for rpc in tree.rpcs
        for msg_name in (rpc.request_type, rpc.response_type)
    }

    _apply_r5_enums(tree, contract)
    _apply_r1_r3_r4_messages(tree, contract, rpc_io_messages)
    _apply_r6_rpc_contracts(tree, contract)
    _apply_r1_r7_zero_trust(tree, contract)
    _apply_r2_railway_specs(tree, contract)

    return contract


# ─────────────────────────────────────────────────────────────────────────────
# Aplicadores de reglas (una función por regla o grupo cohesivo)
# ─────────────────────────────────────────────────────────────────────────────

def _apply_r5_enums(tree: DescriptorTree, contract: JanusAegisContract) -> None:
    """
    R5 — Enum Sentinel Gate.

    Emite specs SOLO para los enums GLOBALES del proto (top-level).
    Los nested enums (dentro de un message) se inlinean directamente en la spec
    del mensaje padre — no se registran en la Sección 1 del contrato.

    Evita 'ghost specs' en el cross-validator: un nested enum registrado en
    tree.enums NO tiene message o enum top-level correspondiente en el proto,
    por lo que el checker lo marcaría como orphan.
    """
    for name, enum in tree.enums.items():
        # Saltar nested enums — están registrados en nested_enum_owner
        # y se emiten inline en la spec del mensaje padre vía type_algebra.
        if name in tree.nested_enum_owner:
            continue

        tags = ["enum"]
        if enum.sentinel:
            tags.append("has-sentinel")

        contract.specs.append(SpecEntry(
            key=to_spec_key(name, _PREFIX),
            spec=map_enum(enum, include_sentinel=False),
            source_proto=f"enum {name}",
            tags=tags,
        ))


def _apply_r1_r3_r4_messages(
    tree: DescriptorTree,
    contract: JanusAegisContract,
    rpc_io_messages: set[str],
) -> None:
    """
    R1 + R3 + R4 — Zero-Trust, Recursividad y oneof Exclusividad.

    Emite specs para todos los mensajes en orden topológico.
    Los mensajes recursivos usan R14 (map_recursive_message).
    Los oneofs dentro de cada mensaje son manejados por map_message (R11).
    """
    for msg_name in tree.topological_order():
        msg = tree.messages.get(msg_name)
        if msg is None:
            continue

        tags: list[str] = []

        # R1: fronteras Zero-Trust
        if msg.is_zero_trust_boundary:
            tags.append("zero-trust-boundary")

        # R3: ciclos intencionales
        is_recursive = msg.is_recursive or msg.name in _KNOWN_RECURSIVE
        if is_recursive:
            tags.append("recursive")
            spec = map_recursive_message(msg, tree, _PREFIX)
        else:
            spec = map_message(msg, tree, _PREFIX)

        contract.specs.append(SpecEntry(
            key=to_spec_key(msg_name, _PREFIX),
            spec=spec,
            source_proto=f"message {msg_name}",
            tags=tags,
        ))


def _apply_r6_rpc_contracts(
    tree: DescriptorTree,
    contract: JanusAegisContract,
) -> None:
    """
    R6 — Streaming RPCs.

    Un RPCContractEntry por cada RPC del servicio con:
      - :request/:response → las spec keys correspondientes
      - :streaming         → True si el RPC usa server-side streaming
      - :zero-trust-scoped → True si el request tiene tenant_id
    """
    for rpc in tree.rpcs:
        req_msg = tree.messages.get(rpc.request_type)
        is_tenant_scoped = bool(req_msg and req_msg.is_zero_trust_boundary)

        tags: list[str] = []
        if rpc.is_streaming:
            tags.append("streaming")
        if is_tenant_scoped:
            tags.append("zero-trust-scoped")

        contract.rpc_contracts.append(RPCContractEntry(
            name=rpc.name,
            request_spec_key=to_spec_key(rpc.request_type, _PREFIX),
            response_spec_key=to_spec_key(rpc.response_type, _PREFIX),
            streaming=rpc.is_streaming,
            tenant_scoped=is_tenant_scoped,
            tags=tags,
        ))


def _apply_r1_r7_zero_trust(
    tree: DescriptorTree,
    contract: JanusAegisContract,
) -> None:
    """
    R1 + R7 — Cláusulas Zero-Trust globales.

    Produce los invariantes que Janus DEBE verificar en el interceptor:
      - tenant-isolation-rule → todo RPC tenant-scoped rechaza tenant_id vacío
      - ast-tenant-invariant  → todo AST debe contener tenant-id en where-clause
      - zero-trust-boundary-messages → lista de spec keys afectadas
      - enum-sentinel-gate    → Janus rechaza campo enum = 0
    """
    boundary_keys = [
        to_spec_key(name, _PREFIX)
        for name, msg in tree.messages.items()
        if msg.is_zero_trust_boundary
    ]

    contract.zero_trust_clauses = {
        ":janus/tenant-isolation-rule":
            ";; Todo RPC tenant-scoped rechaza requests con tenant_id vacío (:JANUS_400)",
        ":janus/ast-tenant-invariant": [
            ":fn", "every?",
            [":or",
                [":contains?", ":tenant-id"],
                [":contains?", ":entity/tenant-id"],
                [":contains?", ":*/tenant-id"],
            ],
        ],
        ":janus/zero-trust-boundary-messages": boundary_keys,
        ":janus/enum-sentinel-gate":
            ";; Janus rechaza cualquier campo enum con valor = 0 (_UNSPECIFIED) con :JANUS_400",
    }


def _apply_r2_railway_specs(
    tree: DescriptorTree,
    contract: JanusAegisContract,
) -> None:
    """
    R2 — Railway-Oriented Error Contracts.

    Emite UN ÚNICO union type que modela los dos carriles del Railway Pattern
    compartidos por todos los responses de la API.
      - Carril OK:    [:map [:status [:map [:success [:= true]]]]]
      - Carril Error: [:map [:status [:map [:success [:= false]] [:error-code :string]]]]

    En lugar de generar N copias idénticas para cada mensaje, emite un contrato DRY
    al que Janus puede delegar la validación genérica de errores en el pipeline.
    """
    has_railway = any(_is_railway_response(msg) for msg in tree.messages.values())
    if not has_railway:
        return

    railway_spec: MalliSpec = [":or",
        [":map",
         [":status", [":map",
            [":success", [":=", True]],
         ]],
        ],
        [":map",
         [":status", [":map",
            [":success", [":=", False]],
            [":error-code", ":string"],
            [":error-message", {":optional": True}, ":string"],
         ]],
        ],
    ]

    contract.railway_specs.append(SpecEntry(
        key=f"{_PREFIX}/railway-error-union",
        spec=railway_spec,
        source_proto="Global Railway-Oriented Pattern",
        tags=["railway", "error-union"],
    ))
