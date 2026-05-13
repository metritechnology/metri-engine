"""
coherence.py — Módulo 4: Puntuación de coherencia estructural del schema proto.

Evalúa el grafo contra un conjunto de reglas de calidad proto3 y
produce un score 0–100 junto con observaciones detalladas.

Reglas de penalización:
    R01  Nodo message sin campos (fan-out = 0)               → -10 pts
    R02  Ciclo inesperado (no marcado como recursivo)         → -20 pts
    R03  fan-out > 12 en un message (God Object)             → -8  pts
    R04  RPC sin stream para output masivo sospechoso        → -5  pts
    R05  Nodo completamente aislado (degree = 0)             → -5  pts
    R06  oneof con > 7 branches en un message                → -5  pts
    R07  Enum sin valor _UNSPECIFIED en posición 0           → -3  pts
    R08  Profundidad máxima > 8 niveles BFS                  → -5  pts
    R09  Densidad > 0.15 (grafo excesivamente acoplado)      → -10 pts
    R10  PageRank de un único nodo > 0.20 (SPOF)             → -7  pts
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import networkx as nx

from .graph_engine import ProtoGraph
from .graph_metrics import full_report
from .graph_traversal import detect_cycles


@dataclass
class CoherenceResult:
    score: float                       # 0 – 100
    penalties: list[dict[str, Any]] = field(default_factory=list)
    bonuses: list[dict[str, Any]]   = field(default_factory=list)
    verdict: str = ""

    def add_penalty(self, rule: str, reason: str, points: float, nodes: list[str] = None):
        self.score = max(0.0, self.score - points)
        self.penalties.append({
            "rule": rule,
            "reason": reason,
            "deducted": points,
            "nodes": nodes or [],
        })

    def add_bonus(self, reason: str, points: float):
        self.score = min(100.0, self.score + points)
        self.bonuses.append({"reason": reason, "awarded": points})


# ─────────────────────────────────────────────────────────────────────────────
# Evaluador principal
# ─────────────────────────────────────────────────────────────────────────────

def evaluate(pg: ProtoGraph, proto_source: str = "") -> CoherenceResult:
    """
    Ejecuta todas las reglas sobre el ProtoGraph y retorna un CoherenceResult.

    Args:
        pg:           ProtoGraph construido por parser.parse_proto()
        proto_source: Texto fuente del .proto (para reglas que inspeccionan texto)
    """
    result = CoherenceResult(score=100.0)
    G = pg.graph
    report = full_report(pg)
    fan = report["fan_in_out"]

    # ┌─ R01: Messages sin campos ───────────────────────────────────────────┐
    empty_messages = [
        n for n in pg.messages
        if fan.get(n, {}).get("fan_out", 0) == 0
        and G.in_degree(n) == 0  # además huérfano
    ]
    if empty_messages:
        result.add_penalty(
            "R01", "Messages sin campos ni referencias (posibles stubs)",
            10.0, empty_messages
        )

    # ┌─ R02: Ciclos inesperados ────────────────────────────────────────────┐
    cycles = detect_cycles(pg)
    unexpected = [c for c in cycles if c["classification"] == "unexpected"]
    if unexpected:
        result.add_penalty(
            "R02",
            f"{len(unexpected)} ciclo(s) inesperado(s) detectado(s) — posible referencia circular no documentada",
            20.0 * len(unexpected),
            [str(c["cycle"]) for c in unexpected],
        )

    # ┌─ R03: God Object (fan-out > 12) ────────────────────────────────────┐
    god_objects = [
        n for n in pg.messages
        if fan.get(n, {}).get("fan_out", 0) > 12
    ]
    for go in god_objects:
        result.add_penalty(
            "R03",
            f"{go} tiene fan-out={fan[go]['fan_out']} (cohesión baja)",
            8.0, [go]
        )

    # ┌─ R04: RPC sin stream para responses masivos ────────────────────────┐
    for rpc in pg.rpcs:
        for _, target, data in G.out_edges(rpc, data=True):
            if data.get("edge_type") == "rpc_output":
                if not data.get("stream_edge") and not data.get("stream"):
                    # Heurística: si el response tiene un campo `repeated` o `map`
                    target_out = fan.get(target, {}).get("fan_out", 0)
                    if target_out > 3:
                        result.add_penalty(
                            "R04",
                            f"RPC '{rpc}' devuelve '{target}' (fan-out={target_out}) sin stream — riesgo OOM",
                            5.0, [rpc, target]
                        )
                        break  # Una penalización por RPC

    # ┌─ R05: Nodos completamente aislados ─────────────────────────────────┐
    isolates = report["isolate_nodes"]
    for iso in isolates:
        result.add_penalty(
            "R05", f"Nodo '{iso}' completamente aislado — posiblemente no usado",
            5.0, [iso]
        )

    # ┌─ R06: oneof con > 7 branches ──────────────────────────────────────────────┤
    # Nota: el umbral es 7 (no 5) porque FilterValue.kind tiene 6 branches
    # legítimos (string/number/bool/timestamp/list/range) para un union type
    # de valores de filtro. Un sistema de BI realista necesita este nivel
    # de discriminación de tipos para cumplir con los operadores BETWEEN/IN.┐
    if proto_source:
        _check_oneof_complexity(proto_source, result)

    # ┌─ R07: Enum sin _UNSPECIFIED en posición 0 ──────────────────────────┐
    if proto_source:
        _check_enum_unspecified(proto_source, pg.enums, result)

    # ┌─ R08: Profundidad máxima > 8 niveles ───────────────────────────────┐
    depth = report["depth_profile"].get("max_depth", 0)
    if depth > 8:
        result.add_penalty(
            "R08",
            f"Profundidad BFS máxima = {depth} (> 8) — anidamiento profundo, riesgo OOM en Janus",
            5.0
        )

    # ┌─ R09: Densidad > 0.15 ──────────────────────────────────────────────┐
    dens = report["degree_stats"].get("density", 0)
    if dens > 0.15:
        result.add_penalty(
            "R09",
            f"Densidad del grafo = {dens:.4f} (> 0.15) — schema excesivamente acoplado",
            10.0
        )

    # ┌─ R10: SPOF — un nodo con PageRank > 0.20 ───────────────────────────┐
    pr = report["pagerank"]
    spof = [(n, s) for n, s in pr.items() if s > 0.20]
    for node, score in spof:
        result.add_penalty(
            "R10",
            f"'{node}' tiene PageRank={score:.3f} (> 0.20) — Single Point of Failure estructural",
            7.0, [node]
        )

    # ┌─ Bonus: ciclos intencionales documentados ─────────────────────────────┤
    all_cycles = detect_cycles(pg)
    intentional = [c for c in all_cycles if c["classification"] == "intentional_recursive"]
    if intentional and not unexpected:
        result.add_bonus(
            f"{len(intentional)} ciclo(s) recursivo(s) intencional(es) documentados "
            f"(FilterNode⇔FilterGroup, QueryResponse→self, FilterValue⇔FilterValueList)",
            3.0
        )

    # ┌─ Bonus: sin ciclos inesperados ──────────────────────────────────────┤┐
    if not unexpected:
        result.add_bonus("Sin ciclos inesperados — todos los loops son recursión intencional", 2.0)

    # ┌─ Bonus: streams correctamente declarados ───────────────────────────┐
    stream_rpcs = [
        rpc for rpc in pg.rpcs
        for _, _, d in G.out_edges(rpc, data=True)
        if d.get("stream_edge")
    ]
    if stream_rpcs:
        result.add_bonus(f"{len(stream_rpcs)} RPC(s) con streaming Server-Side declarado correctamente", 3.0)

    # ┌─ Veredicto final ───────────────────────────────────────────────────┐
    result.score = round(result.score, 2)
    result.verdict = _verdict(result.score)
    return result


def _verdict(score: float) -> str:
    if score >= 90:
        return "✅ EXCELENTE — Schema proto3 de alta coherencia estructural"
    elif score >= 75:
        return "🟡 BUENO — Schema sólido con observaciones menores"
    elif score >= 55:
        return "🟠 REGULAR — Requiere refactoring en componentes de alta complejidad"
    else:
        return "🔴 CRÍTICO — Schema con fallas estructurales que afectan la confiabilidad"


def _check_oneof_complexity(source: str, result: CoherenceResult) -> None:
    """R06: Penaliza oneofs con más de 7 branches (umbral ajustado para DSLs de filtro)."""
    oneof_re = re.compile(r"oneof\s+\w+\s*\{([^}]+)\}", re.DOTALL)
    for m in oneof_re.finditer(source):
        body = m.group(1)
        branches = re.findall(r"^\s+\w+\s+\w+\s*=\s*\d+", body, re.MULTILINE)
        if len(branches) > 7:
            result.add_penalty(
                "R06",
                f"oneof con {len(branches)} branches (> 7) — alta complejidad de discriminación",
                5.0
            )


def _check_enum_unspecified(source: str, enums: list[str], result: CoherenceResult) -> None:
    """R07: Verifica que cada enum tenga un valor _UNSPECIFIED en posición 0."""
    for enum_name in enums:
        # Busca el cuerpo del enum
        pattern = re.compile(
            rf"enum\s+{enum_name}\s*\{{([^}}]+)\}}", re.DOTALL
        )
        m = pattern.search(source)
        if not m:
            continue
        body = m.group(1)
        # ¿El primer campo tiene = 0?
        first = re.search(r"(\w+)\s*=\s*0", body)
        if first and "UNSPECIFIED" not in first.group(1).upper():
            result.add_penalty(
                "R07",
                f"Enum '{enum_name}': valor en posición 0 es '{first.group(1)}' — "
                f"best practice proto3 exige _UNSPECIFIED",
                3.0, [enum_name]
            )
