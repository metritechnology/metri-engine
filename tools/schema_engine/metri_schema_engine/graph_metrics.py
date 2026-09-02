"""
metrics.py — Módulo 3: Representación matemática del grafo proto.

Calcula métricas de teoría de grafos para describir cuantitativamente
la complejidad y estructura del schema Protobuf.

Métricas expuestas:
    density(pg)              → densidad del grafo [0, 1]
    fan_in_out(pg)           → {nodo: (in_degree, out_degree)}
    pagerank(pg)             → importancia relativa de cada nodo
    betweenness(pg)          → centralidad de paso (bridge nodes)
    degree_stats(pg)         → estadísticas globales (mean, max, min)
    hub_nodes(pg, top_n)     → top N nodos más referenciados
    isolate_nodes(pg)        → nodos sin conexiones (posibles huérfanos)
    depth_profile(pg)        → distribución de nodos por nivel BFS
"""

from __future__ import annotations

import statistics
from typing import Any

import networkx as nx

from .graph_engine import ProtoGraph
from .graph_traversal import bfs_from_service


# ─────────────────────────────────────────────────────────────────────────────
# Métricas estructurales básicas
# ─────────────────────────────────────────────────────────────────────────────

def density(pg: ProtoGraph) -> float:
    """
    Densidad del grafo: proporción de aristas presentes vs. posibles.
    D = m / (n * (n-1))   para grafo dirigido.

    Valor cercano a 0 = grafo disperso (normal en schemas).
    Valor cercano a 1 = grafo muy denso (posible God Object).
    """
    G = pg.graph
    n = G.number_of_nodes()
    m = G.number_of_edges()
    if n <= 1:
        return 0.0
    return m / (n * (n - 1))


def fan_in_out(pg: ProtoGraph) -> dict[str, dict[str, int]]:
    """
    Fan-in (in_degree) y fan-out (out_degree) por nodo.

    Fan-in alto  → mensaje muy referenciado (hub crítico).
    Fan-out alto → mensaje con muchas dependencias (God Object risk).

    Retorna: {nombre_nodo: {"fan_in": int, "fan_out": int}}
    """
    G = pg.graph
    return {
        node: {
            "fan_in": G.in_degree(node),
            "fan_out": G.out_degree(node),
            "kind": G.nodes[node].get("kind", "unknown"),
        }
        for node in G.nodes()
    }


def degree_stats(pg: ProtoGraph) -> dict[str, float]:
    """
    Estadísticas globales de degree (combinado in+out).

    Retorna dict con: mean, stdev, max, min, total_nodes, total_edges.
    """
    G = pg.graph
    degrees = [d for _, d in G.degree()]
    if not degrees:
        return {}
    return {
        "mean_degree":   round(statistics.mean(degrees), 3),
        "stdev_degree":  round(statistics.stdev(degrees) if len(degrees) > 1 else 0.0, 3),
        "max_degree":    max(degrees),
        "min_degree":    min(degrees),
        "total_nodes":   G.number_of_nodes(),
        "total_edges":   G.number_of_edges(),
        "density":       round(density(pg), 6),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Centralidad
# ─────────────────────────────────────────────────────────────────────────────

def pagerank_scores(pg: ProtoGraph) -> dict[str, float]:
    """
    PageRank de cada nodo (importancia relativa en el grafo).

    Un nodo con PageRank alto es "crítico" — muchos otros dependen de él
    directa o transitivamente.  En metri.proto se espera que FilterNode
    tenga el PageRank más alto por sus múltiples referencias.

    Retorna: {nodo: score} ordenado descendente.
    """
    try:
        scores = nx.pagerank(pg.graph, alpha=0.85)
    except Exception:
        scores = {n: 0.0 for n in pg.graph.nodes()}

    return dict(sorted(scores.items(), key=lambda x: x[1], reverse=True))


def betweenness_centrality(pg: ProtoGraph) -> dict[str, float]:
    """
    Centralidad de paso (betweenness): mide cuántos caminos más cortos
    pasan por cada nodo.

    Un nodo con betweenness alto es un "puente estructural" — si se
    elimina, el grafo se fragmenta. Crítico para evaluar fragilidad del schema.
    """
    try:
        scores = nx.betweenness_centrality(pg.graph, normalized=True)
    except Exception:
        scores = {n: 0.0 for n in pg.graph.nodes()}

    return dict(sorted(scores.items(), key=lambda x: x[1], reverse=True))


# ─────────────────────────────────────────────────────────────────────────────
# Nodos Hub e Isolados
# ─────────────────────────────────────────────────────────────────────────────

def hub_nodes(pg: ProtoGraph, top_n: int = 10) -> list[dict[str, Any]]:
    """
    Top-N nodos con mayor fan-in (más referenciados desde otros mensajes).
    Estos son los "hubs" del schema — cambiarlos tiene mayor impacto.
    """
    G = pg.graph
    ranked = sorted(
        G.nodes(),
        key=lambda n: G.in_degree(n),
        reverse=True,
    )[:top_n]

    return [
        {
            "node": n,
            "fan_in": G.in_degree(n),
            "fan_out": G.out_degree(n),
            "kind": G.nodes[n].get("kind", "?"),
        }
        for n in ranked
    ]


def isolate_nodes(pg: ProtoGraph) -> list[str]:
    """
    Nodos sin ninguna arista (ni entrante ni saliente).
    En un schema proto son señales de:
        - Mensajes declarados pero nunca usados.
        - Tipos importados no enlazados al grafo principal.
    """
    G = pg.graph
    return [n for n in G.nodes() if G.degree(n) == 0]


# ─────────────────────────────────────────────────────────────────────────────
# Perfil de profundidad
# ─────────────────────────────────────────────────────────────────────────────

def depth_profile(pg: ProtoGraph) -> dict[str, Any]:
    """
    Distribución de nodos por nivel BFS desde el service raíz.
    Permite visualizar la "pirámide" del schema.

    Retorna:
        {
            "levels": {0: ["MetriService"], 1: ["Query", ...], ...},
            "max_depth": int,
            "nodes_at_max_depth": list[str],
        }
    """
    levels = bfs_from_service(pg)
    if not levels:
        return {"levels": {}, "max_depth": 0, "nodes_at_max_depth": []}

    max_depth = max(levels.keys())
    return {
        "levels": levels,
        "max_depth": max_depth,
        "nodes_at_max_depth": levels.get(max_depth, []),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Resumen completo
# ─────────────────────────────────────────────────────────────────────────────

def full_report(pg: ProtoGraph) -> dict[str, Any]:
    """
    Agrega todas las métricas en un único dict serializable.
    Usado por `visualizer.py` y `coherence.py`.
    """
    return {
        "degree_stats":          degree_stats(pg),
        "fan_in_out":            fan_in_out(pg),
        "pagerank":              pagerank_scores(pg),
        "betweenness":           betweenness_centrality(pg),
        "hub_nodes":             hub_nodes(pg),
        "isolate_nodes":         isolate_nodes(pg),
        "depth_profile":         depth_profile(pg),
        "recursive_edges":       pg.recursive_edges,
    }
