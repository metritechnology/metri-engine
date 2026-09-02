"""
graph_engine.py — Módulo puente: DescriptorTree → ProtoGraph (NetworkX DiGraph).

Responsabilidad única: convertir el DescriptorTree (IR tipado producido por
descriptor_walker) en un ProtoGraph NetworkX enriquecido, listo para análisis
matemático por graph_metrics, graph_traversal, coherence y domain_partitioner.

Elimina la duplicación de lógica de parsing: antes, proto-graph-analyzer y
proto2edn tenían parsers independientes. Ahora ambos consumen el mismo IR.

Tipos de nodo (atributo `kind`):
    "service"  — MetriService (raíz del grafo)
    "rpc"      — cada método gRPC
    "message"  — cada message proto3
    "enum"     — cada enum proto3 global

Tipos de arista (atributo `edge_type`):
    "rpc_input"    — RPC → Message (request)
    "rpc_output"   — RPC → Message (response)
    "field_ref"    — Message → Message (campo singular)
    "repeated_ref" — Message → Message (campo repeated)
    "map_ref"      — Message → Message (map<k,v>)
    "oneof_ref"    — Message → Message (rama oneof)
    "nested_enum"  — Message → Enum (enum interno)
    "enum_ref"     — Message → Enum global (campo que usa enum global)
    "stream_edge"  — anotación sobre rpc_output: respuesta es stream
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import networkx as nx

from .descriptor_walker import (
    DescriptorTree, build_descriptor_tree,
    MessageDescriptor, EnumDescriptor, RPCDescriptor,
)


# ─────────────────────────────────────────────────────────────────────────────
# Modelo de resultado
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ProtoGraph:
    """
    Grafo NetworkX enriquecido del schema proto3.

    Contiene el grafo dirigido completo y las listas de nodos
    classificados (messages, enums, rpcs) para acceso rápido
    por los módulos de análisis.
    """
    G: nx.DiGraph
    tree: DescriptorTree          # IR fuente de verdad
    service: str                  # nombre del service gRPC
    messages: list[str]           # nombres de todos los messages
    enums: list[str]              # nombres de todos los enums globales
    rpcs: list[str]               # nombres de todos los RPCs
    source_path: str = ""

    @property
    def node_count(self) -> int:
        return self.G.number_of_nodes()

    @property
    def edge_count(self) -> int:
        return self.G.number_of_edges()

    @property
    def graph(self) -> nx.DiGraph:
        """Alias de .G para compatibilidad con los módulos migrados de proto-graph-analyzer."""
        return self.G

    @property
    def services(self) -> list[str]:
        """Alias de compatibilidad: lista con el nodo service del grafo."""
        return [self.service] if self.service else []

    @property
    def recursive_edges(self) -> set[tuple[str, str]]:
        """
        Alias de compatibilidad: conjunto de aristas que forman ciclos intencionales.
        Derivado de los mensajes marcados como is_recursive en el DescriptorTree.
        """
        result: set[tuple[str, str]] = set()
        for msg_name, msg in self.tree.messages.items():
            if msg.is_recursive:
                result.add((msg_name, msg_name))
                for f in msg.fields:
                    if f.proto_type == msg_name:
                        result.add((msg_name, msg_name))
        return result

    def nodes_of_kind(self, kind: str) -> list[str]:
        """Retorna todos los nodos de un kind dado."""
        return [
            n for n, d in self.G.nodes(data=True)
            if d.get("kind") == kind
        ]

    def edges_of_type(self, edge_type: str) -> list[tuple]:
        """Retorna todas las aristas de un edge_type dado."""
        return [
            (u, v, d) for u, v, d in self.G.edges(data=True)
            if d.get("edge_type") == edge_type
        ]


# ─────────────────────────────────────────────────────────────────────────────
# Builder: DescriptorTree → ProtoGraph
# ─────────────────────────────────────────────────────────────────────────────

def build_proto_graph(proto_path: str | Path) -> ProtoGraph:
    """
    Construye el ProtoGraph completo desde un archivo .proto.

    Internamente:
      1. Construye el DescriptorTree (descriptor_walker)
      2. Convierte el tree en un DiGraph NetworkX enriquecido
      3. Retorna el ProtoGraph listo para análisis
    """
    path = Path(proto_path)
    tree = build_descriptor_tree(path)
    G    = _build_digraph(tree)

    return ProtoGraph(
        G=G,
        tree=tree,
        service=tree.service_name,
        messages=list(tree.messages.keys()),
        enums=[
            name for name in tree.enums
            if name not in tree.nested_enum_owner
        ],
        rpcs=[rpc.name for rpc in tree.rpcs],
        source_path=str(path),
    )


def _build_digraph(tree: DescriptorTree) -> nx.DiGraph:
    """
    Convierte el DescriptorTree en un DiGraph NetworkX enriquecido.
    Cada nodo y arista lleva metadata semántica completa.
    """
    G = nx.DiGraph()

    # ── Nodo raíz: Service ──────────────────────────────────────────────────
    service = tree.service_name or "UnknownService"
    G.add_node(service, kind="service", label=service)

    # ── Nodos: Messages ─────────────────────────────────────────────────────
    for msg_name, msg in tree.messages.items():
        G.add_node(msg_name, kind="message",
            label=msg_name,
            field_count=len(msg.fields),
            oneof_count=len(msg.oneofs),
            is_recursive=msg.is_recursive,
            is_zero_trust=msg.is_zero_trust_boundary,
        )

    # ── Nodos: Enums globales ───────────────────────────────────────────────
    for enum_name, enum in tree.enums.items():
        if enum_name in tree.nested_enum_owner:
            continue  # nested enums no son nodos de primer nivel
        G.add_node(enum_name, kind="enum",
            label=enum_name,
            value_count=len(enum.values),
            has_sentinel=bool(enum.sentinel),
        )

    # ── Nodos: RPCs & aristas service → rpc ─────────────────────────────────
    for rpc in tree.rpcs:
        G.add_node(rpc.name, kind="rpc",
            label=rpc.name,
            streaming=rpc.is_streaming,
        )
        G.add_edge(service, rpc.name, edge_type="rpc_uses")
        # rpc → request
        G.add_edge(rpc.name, rpc.request_type,
            edge_type="rpc_input",
            label=f"{rpc.name}→req",
        )
        # rpc → response
        G.add_edge(rpc.name, rpc.response_type,
            edge_type="rpc_output",
            stream_edge=rpc.is_streaming,
            label=f"{rpc.name}→resp",
        )

    # ── Aristas: Message → Message / Enum ───────────────────────────────────
    for msg_name, msg in tree.messages.items():
        _add_field_edges(G, tree, msg_name, msg)
        _add_oneof_edges(G, tree, msg_name, msg)
        _add_nested_enum_edges(G, msg_name, msg)

    return G


def _add_field_edges(G: nx.DiGraph, tree: DescriptorTree,
                     msg_name: str, msg: MessageDescriptor) -> None:
    """Agrega aristas desde un message a sus dependencias por campos."""
    oneof_names = {b.name for oo in msg.oneofs for b in oo.branches}

    for f in msg.fields:
        if f.name in oneof_names:
            continue  # los oneof se procesan aparte

        target = f.proto_type
        if target in tree.messages:
            etype = "repeated_ref" if f.cardinality == "repeated" else \
                    "map_ref"      if f.cardinality == "map"      else \
                    "field_ref"
            if not G.has_edge(msg_name, target):
                G.add_edge(msg_name, target,
                    edge_type=etype,
                    field_name=f.name,
                    cardinality=f.cardinality,
                )
        elif target in tree.enums and target not in tree.nested_enum_owner:
            # Referencia a enum global
            if not G.has_edge(msg_name, target):
                G.add_edge(msg_name, target,
                    edge_type="enum_ref",
                    field_name=f.name,
                )


def _add_oneof_edges(G: nx.DiGraph, tree: DescriptorTree,
                     msg_name: str, msg: MessageDescriptor) -> None:
    """Agrega aristas oneof_ref desde un message."""
    for oneof in msg.oneofs:
        for branch in oneof.branches:
            target = branch.proto_type
            if target in tree.messages and not G.has_edge(msg_name, target):
                G.add_edge(msg_name, target,
                    edge_type="oneof_ref",
                    oneof_name=oneof.name,
                    field_name=branch.name,
                )


def _add_nested_enum_edges(G: nx.DiGraph,
                            msg_name: str, msg: MessageDescriptor) -> None:
    """Agrega aristas nested_enum de message → enum interno."""
    for nested in msg.nested_enums:
        label = f"{msg_name}.{nested.name}"
        # El nodo del nested enum es el label cualificado
        if not G.has_node(label):
            G.add_node(label, kind="enum",
                label=label,
                is_nested=True,
                value_count=len(nested.values),
                has_sentinel=bool(nested.sentinel),
            )
        if not G.has_edge(msg_name, label):
            G.add_edge(msg_name, label,
                edge_type="nested_enum",
                enum_name=nested.name,
            )
