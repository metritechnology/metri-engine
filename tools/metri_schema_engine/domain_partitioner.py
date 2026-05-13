"""
dominator.py — Módulo 6: Partición por Dominios del Grafo Proto.

Estrategia matemática:
    1. Greedy Modularity Communities (≈ Louvain) para detectar particiones naturales.
    2. Spectral Graph Analysis (Laplaciano + Fiedler value) por dominio para medir cohesión.
    3. Boundary Edge Analysis para identificar contratos inter-dominio críticos.

Los 8 dominios canónicos de metri.proto (descubiertos matemáticamente):
    D1  Analytics Core       — 18 nodos (QueryRequest, AnalyticsRequest, MetricDefinition, ...)
    D2  Service & Operations — 13 nodos (MetriService, Transact, BulkIngest, ...)
    D3  Query Response       —  7 nodos (QueryResponse, Pagination, QueryMetadata, ...)
    D4  Visualization        —  7 nodos (VizMeta, ChartDecoration, AnalyticalSignal, ...)
    D5  EDA / Routing        —  7 nodos (MatchRoutingRules*, WebhookTarget, MatchedRule, ...)
    D6  Metadata/Discovery   —  5 nodos (Discovery, EntitySchema, AttributeSchema, ...)
    D7  Filter DSL           —  5 nodos (FilterNode, FilterCriteria, FilterValue, ...)
    D8  RowSet / Payload     —  4 nodos (RowSet, DataRow, DataRowList, ColumnSchema)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import networkx as nx

from .graph_engine import ProtoGraph


# ─────────────────────────────────────────────────────────────────────────────
# Dominios canónicos (etiquetados semánticamente a partir del análisis matemático)
# ─────────────────────────────────────────────────────────────────────────────

CANONICAL_DOMAINS: dict[str, dict[str, Any]] = {
    "D1_ANALYTICS": {
        "label": "Analytics Core",
        "nodes": {
            "AggregationFunction", "AnalyticalComparison", "AnalyticsRequest",
            "BatchContext", "ChronosAlertTask", "DashboardCrossFilterContext",
            "DimensionDefinition", "FilterGroup", "FilterNode", "FormulaEntry",
            "HierarchyContext", "MetricDefinition", "MultiSeriesGroup",
            "OutputCastType", "QueryRequest", "SemanticMetricRef",
            "SortDefinition", "TimeFrameContext",
        },
    },
    "D2_OPERATIONS": {
        "label": "Service & Operations",
        "nodes": {
            "BulkIngest", "BulkRequest", "BulkResponse", "Explore",
            "ExploreRequest", "ExploreResponse", "MetriService", "OperationAction",
            "Query", "Status", "Transact", "TransactionRequest", "TransactionResponse",
        },
    },
    "D3_RESPONSE": {
        "label": "Query Response & Hypermedia",
        "nodes": {
            "Action", "Link", "Pagination", "QueryMetadata",
            "QueryResponse", "TableColumn", "TableMeta",
        },
    },
    "D4_VISUALIZATION": {
        "label": "Visualization",
        "nodes": {
            "AnalyticalSignal", "BreakdownSignal", "ChartDecoration",
            "IndicatorThreshold", "IntelligenceSignal", "TreeMeta", "VizMeta",
        },
    },
    "D5_EDA": {
        "label": "EDA / Event Routing",
        "nodes": {
            "MatchRoutingRulesBatch", "MatchRoutingRulesBatchRequest",
            "MatchRoutingRulesBatchResponse", "MatchRoutingRulesRequest",
            "MatchRoutingRulesResponse", "MatchedRule", "WebhookTarget",
        },
    },
    "D6_METADATA": {
        "label": "Metadata / Discovery",
        "nodes": {
            "AttributeSchema", "Discovery", "DiscoveryRequest",
            "DiscoveryResponse", "EntitySchema",
        },
    },
    "D7_FILTER": {
        "label": "Filter DSL",
        "nodes": {
            "FilterCriteria", "FilterOperator", "FilterValue",
            "FilterValueList", "StringList",
        },
    },
    "D8_ROWSET": {
        "label": "RowSet / Data Payload",
        "nodes": {
            "ColumnSchema", "DataRow", "DataRowList", "RowSet",
        },
    },
}


@dataclass
class DomainProfile:
    """Perfil matemático de un dominio del grafo proto."""
    domain_id: str
    label: str
    nodes: set[str]
    subgraph: nx.DiGraph

    # Métricas estructurales
    n_nodes: int = 0
    n_edges: int = 0
    density: float = 0.0
    fiedler_value: float = 0.0     # λ₂ del Laplaciano — algebraic connectivity
    diameter: int = 0
    avg_degree: float = 0.0
    hub_node: str = ""             # Nodo de mayor fan-in en el dominio

    # Aristas de corte (conexiones a otros dominios)
    boundary_edges: list[tuple[str, str, str]] = field(default_factory=list)
    # Dominios vecinos (a los que este dominio se conecta)
    neighbor_domains: set[str] = field(default_factory=set)


# ─────────────────────────────────────────────────────────────────────────────
# API pública
# ─────────────────────────────────────────────────────────────────────────────

def build_domain_profiles(pg: ProtoGraph) -> dict[str, DomainProfile]:
    """
    Construye el perfil matemático completo de cada dominio canónico.

    Args:
        pg: ProtoGraph parseado.

    Returns:
        Dict {domain_id: DomainProfile} con métricas por dominio.
    """
    G = pg.graph
    profiles: dict[str, DomainProfile] = {}

    # 1. Construir sub-grafo por dominio
    for domain_id, meta in CANONICAL_DOMAINS.items():
        nodes_in_graph = {n for n in meta["nodes"] if G.has_node(n)}
        sub = G.subgraph(nodes_in_graph).copy()

        p = DomainProfile(
            domain_id=domain_id,
            label=meta["label"],
            nodes=nodes_in_graph,
            subgraph=sub,
        )
        p.n_nodes = sub.number_of_nodes()
        p.n_edges = sub.number_of_edges()
        p.density = nx.density(sub)

        if p.n_nodes > 0:
            degrees = [d for _, d in sub.degree()]
            p.avg_degree = sum(degrees) / len(degrees)
            hub = max(sub.nodes(), key=lambda n: sub.in_degree(n), default="")
            p.hub_node = hub

        # 2. Fiedler value (λ₂ del Laplaciano no dirigido)
        p.fiedler_value = _fiedler_value(sub)

        # 3. Diámetro (sobre el sub-grafo no dirigido)
        p.diameter = _diameter(sub)

        profiles[domain_id] = p

    # 4. Aristas de corte inter-dominio
    _compute_boundary_edges(G, profiles)

    return profiles


def discovered_communities(pg: ProtoGraph) -> list[frozenset[str]]:
    """
    Aplica Greedy Modularity Communities sobre el grafo no dirigido
    para descubrir particiones naturales del schema.

    Útil para validar que los dominios canónicos coinciden con los
    grupos matemáticos detectados automáticamente.
    """
    G_und = pg.graph.to_undirected()
    return [
        frozenset(c)
        for c in nx.community.greedy_modularity_communities(G_und)
    ]


def modularity_score(pg: ProtoGraph) -> float:
    """
    Calcula la modularidad Q del particionado canónico.
    Q ∈ [-0.5, 1.0] — valores > 0.3 indican partición significativa.
    """
    G_und = pg.graph.to_undirected()
    partition = [
        {n for n in meta["nodes"] if G_und.has_node(n)}
        for meta in CANONICAL_DOMAINS.values()
    ]
    partition = [p for p in partition if p]  # Eliminar vacíos
    try:
        return nx.community.modularity(G_und, partition)
    except Exception:
        return 0.0


def print_domain_summary(profiles: dict[str, DomainProfile]) -> None:
    """Imprime un resumen de los dominios a stdout."""
    print("\n" + "═" * 70)
    print(f"{'DOMAIN PARTITION SUMMARY':^70}")
    print("═" * 70)
    print(f"{'ID':<18} {'Label':<30} {'N':>4} {'E':>4} {'λ₂':>7} {'Hub':<20}")
    print("─" * 70)
    for p in profiles.values():
        print(
            f"{p.domain_id:<18} {p.label:<30} {p.n_nodes:>4} "
            f"{p.n_edges:>4} {p.fiedler_value:>7.4f} {p.hub_node:<20}"
        )
    print("═" * 70)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers privados
# ─────────────────────────────────────────────────────────────────────────────

def _fiedler_value(sub: nx.DiGraph) -> float:
    """
    Calcula el segundo eigenvalue más pequeño del Laplaciano (λ₂).
    Conocido como 'Algebraic Connectivity' o 'Fiedler value'.
    - λ₂ = 0  → grafo desconectado
    - λ₂ > 0  → grafo conectado (mayor = más cohesivo)
    """
    try:
        import numpy as np
        G_und = sub.to_undirected()
        if G_und.number_of_nodes() < 2:
            return 0.0
        L = nx.laplacian_matrix(G_und).toarray().astype(float)
        eigenvalues = np.linalg.eigvalsh(L)
        # Ordenar y tomar λ₂ (índice 1)
        eigenvalues_sorted = sorted(eigenvalues)
        return float(eigenvalues_sorted[1]) if len(eigenvalues_sorted) > 1 else 0.0
    except Exception:
        return 0.0


def _diameter(sub: nx.DiGraph) -> int:
    """Diámetro del sub-grafo (en grafo no dirigido). 0 si desconectado."""
    try:
        G_und = sub.to_undirected()
        if nx.is_connected(G_und):
            return nx.diameter(G_und)
        # Diámetro del componente más grande
        largest = max(nx.connected_components(G_und), key=len)
        return nx.diameter(G_und.subgraph(largest))
    except Exception:
        return 0


def _compute_boundary_edges(
    G: nx.DiGraph,
    profiles: dict[str, DomainProfile],
) -> None:
    """
    Identifica aristas que cruzan de un dominio a otro (boundary edges).
    Estas son los contratos inter-dominio que Janus/Aegis deben respetar.
    """
    # Construir mapa inverso: nodo → domain_id
    node_to_domain: dict[str, str] = {}
    for domain_id, p in profiles.items():
        for node in p.nodes:
            node_to_domain[node] = domain_id

    for u, v, data in G.edges(data=True):
        domain_u = node_to_domain.get(u)
        domain_v = node_to_domain.get(v)
        if domain_u and domain_v and domain_u != domain_v:
            edge_type = data.get("edge_type", "?")
            profiles[domain_u].boundary_edges.append((u, v, edge_type))
            profiles[domain_u].neighbor_domains.add(domain_v)
            profiles[domain_v].neighbor_domains.add(domain_u)
