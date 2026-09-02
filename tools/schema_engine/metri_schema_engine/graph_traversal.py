"""
traversal.py — Módulo 2: Algoritmos de recorrido del grafo proto.

Expone:
    bfs_from_service(pg)       → niveles BFS desde el nodo service raíz
    dfs_all_paths(pg, source)  → todos los caminos desde un nodo (DFS)
    detect_cycles(pg)          → lista de ciclos (incluyendo recursivos intencionales)
    topological_order(pg)      → orden topo para el sub-grafo acíclico
    longest_path(pg)           → camino más largo (profundidad máxima de anidamiento)
"""

from __future__ import annotations

from collections import deque
from typing import Any

import networkx as nx

from .graph_engine import ProtoGraph


# ─────────────────────────────────────────────────────────────────────────────
# BFS desde el nodo service raíz
# ─────────────────────────────────────────────────────────────────────────────

def bfs_from_service(pg: ProtoGraph) -> dict[int, list[str]]:
    """
    BFS comenzando desde el nodo service.
    Retorna un dict {nivel: [nodos]} que permite visualizar la
    distancia topológica de cada message/enum desde la interfaz gRPC.

    Complejidad: O(V + E)
    """
    if not pg.services:
        return {}

    root = pg.services[0]
    levels: dict[int, list[str]] = {}
    visited = {root}
    queue: deque[tuple[str, int]] = deque([(root, 0)])

    while queue:
        node, level = queue.popleft()
        levels.setdefault(level, []).append(node)

        for neighbor in pg.graph.successors(node):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, level + 1))

    return levels


# ─────────────────────────────────────────────────────────────────────────────
# DFS — todos los caminos desde un nodo origen
# ─────────────────────────────────────────────────────────────────────────────

def dfs_all_paths(
    pg: ProtoGraph,
    source: str,
    max_depth: int = 15,
) -> list[list[str]]:
    """
    DFS iterativo que retorna todos los caminos desde `source`.
    `max_depth` evita explosión en grafos con ciclos.

    Complejidad: O(V * E) en el peor caso.
    """
    G = pg.graph
    paths: list[list[str]] = []
    stack: list[tuple[list[str], set[str]]] = [([source], {source})]

    while stack:
        path, visited = stack.pop()
        current = path[-1]

        neighbors = list(G.successors(current))
        if not neighbors or len(path) >= max_depth:
            paths.append(path)
            continue

        for neighbor in neighbors:
            if neighbor not in visited:
                stack.append((path + [neighbor], visited | {neighbor}))

    return paths


# ─────────────────────────────────────────────────────────────────────────────
# Detección de ciclos (incluyendo recursivos intencionales)
# ─────────────────────────────────────────────────────────────────────────────

def detect_cycles(pg: ProtoGraph) -> list[dict[str, Any]]:
    """
    Detecta todos los ciclos simples en el grafo.
    Clasifica cada ciclo como:
        - "intentional_recursive": FilterNode-style loops (conocidos)
        - "unexpected": ciclos no documentados (posible bug)

    Retorna lista de dicts con:
        {
            "cycle": ["NodeA", "NodeB", ...],
            "length": int,
            "classification": str,
        }
    """
    # Nodos conocidos que forman ciclos intencionales en metri.proto.
    # Cada par representa una arista (src, dst) que es parte de un ciclo
    # documentado y arquitectónicamente justificado.
    _KNOWN_RECURSIVE_PAIRS = {
        # FilterNode ↔ FilterGroup: árbol de filtros anidados (AND/OR/NOT)
        ("FilterNode",  "FilterGroup"),
        ("FilterGroup", "FilterNode"),

        # QueryResponse → QueryResponse: batch_results map<string, QueryResponse>
        # Patrón Response-as-Tree para dashboards multi-gráfica.
        ("QueryResponse", "QueryResponse"),

        # FilterValue ↔ FilterValueList: mutual recursion para operador BETWEEN/IN
        # FilterValue.range_values = FilterValueList → repeated FilterValue
        ("FilterValue",    "FilterValueList"),
        ("FilterValueList", "FilterValue"),
    }

    result = []
    try:
        for cycle in nx.simple_cycles(pg.graph):
            edges_in_cycle = {
                (cycle[i], cycle[(i + 1) % len(cycle)])
                for i in range(len(cycle))
            }
            is_known = any(pair in _KNOWN_RECURSIVE_PAIRS for pair in edges_in_cycle)
            result.append({
                "cycle": cycle,
                "length": len(cycle),
                "classification": "intentional_recursive" if is_known else "unexpected",
            })
    except Exception:
        pass

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Orden Topológico (sub-grafo acíclico)
# ─────────────────────────────────────────────────────────────────────────────

def topological_order(pg: ProtoGraph) -> list[str]:
    """
    Retorna el orden topológico del grafo.
    Si hay ciclos, los rompe eliminando las aristas marcadas como `recursive=True`.

    Permite determinar el orden de compilación/dependencia de messages.
    """
    G_dag = pg.graph.copy()
    # Eliminar aristas recursivas para hacer el grafo acíclico
    recursive = [
        (u, v) for u, v, d in G_dag.edges(data=True) if d.get("recursive")
    ]
    G_dag.remove_edges_from(recursive)

    try:
        return list(nx.topological_sort(G_dag))
    except nx.NetworkXUnfeasible:
        # Fallback: retornar nodos en orden de degree descendente
        return sorted(G_dag.nodes(), key=lambda n: G_dag.in_degree(n), reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# Camino más largo (profundidad máxima de anidamiento)
# ─────────────────────────────────────────────────────────────────────────────

def longest_path(pg: ProtoGraph) -> tuple[list[str], int]:
    """
    Calcula el camino más largo desde cualquier nodo service/rpc hacia
    los mensajes más profundos del árbol.

    Esto revela el nivel máximo de anidamiento del schema — crítico para
    detectar riesgo OOM en el motor Janus.

    Retorna: (camino: list[str], longitud: int)
    Complejidad: O(V + E) sobre el DAG
    """
    G_dag = pg.graph.copy()
    recursive = [
        (u, v) for u, v, d in G_dag.edges(data=True) if d.get("recursive")
    ]
    G_dag.remove_edges_from(recursive)

    try:
        path = nx.dag_longest_path(G_dag)
        return path, len(path)
    except Exception:
        return [], 0
