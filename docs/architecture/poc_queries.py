#!/usr/bin/env python3
"""
poc_queries.py — Prueba de Concepto: Consultas Janus AST IR

Genera dos consultas de referencia basadas en los modelos de dominio
asset.json y meter_reading.json, y muestra el árbol AST completo
que Janus produce al parsear cada QueryRequest.

Caso 1: KPI con comparación temporal
  → Activos ACTIVE por status, viz KPI
  → Comparación: período anterior (TIME_SHIFT_RELATIVE)

Caso 2: Multi-serie línea temporal
  → Lectura promedio de medidores (meter_reading.reading_value)
  → Agrupada por asset_id (dimensión) y timestamp (intervalo diario)
  → viz: TIMESERIES (line chart)
  → Multi-serie: una serie por asset.status

Ambas consultas se validan estructuralmente contra el contrato
janus-ast-ir.edn y se imprime el árbol Janus AST IR.
"""

from __future__ import annotations
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Constantes de dominio (derivadas de asset.json y meter_reading.json)
# ─────────────────────────────────────────────────────────────────────────────

TENANT_ID    = "tenant-demo-001"
SCRIPT_DIR   = Path(__file__).resolve().parent         # .../docs/architecture/
METRI_ROOT   = SCRIPT_DIR.parent.parent                 # .../metri-engine/
EDN_CONTRACT = METRI_ROOT / "resources" / "schema" / "janus-ast-ir.edn"
METRI_TOOL   = METRI_ROOT / "tools" / "metri_schema.py"

# ─────────────────────────────────────────────────────────────────────────────
# Builder de QueryRequest (representación Python del proto)
# ─────────────────────────────────────────────────────────────────────────────

def analytics_request(
    entity: str,
    metrics: list[dict],
    dimensions: list[dict] | None = None,
    filters: list[dict] | None = None,
    time_frame: dict | None = None,
    comparisons: list[dict] | None = None,
    sort: list[dict] | None = None,
    viz: str = "KPI",
    limit: int = 1,
) -> dict:
    """Construye un AnalyticsRequest proto-compatible como dict Python."""
    req: dict[str, Any] = {
        "tenant_id": TENANT_ID,
        "entity": entity,
        "metrics": metrics,
        "viz": viz,
    }
    if dimensions:   req["dimensions"]  = dimensions
    if filters:      req["filters"]     = filters
    if time_frame:   req["time_frame"]  = time_frame
    if comparisons:  req["comparisons"] = comparisons
    if sort:         req["sort"]        = sort
    if limit != 1:   req["limit"]       = limit
    return req


def query_request(
    queries: dict[str, dict],
    merge_groups: list[dict] | None = None,
    aliases: dict[str, str] | None = None,
) -> dict:
    """Construye un QueryRequest con múltiples AnalyticsRequests nombrados."""
    req: dict[str, Any] = {
        "tenant_id": TENANT_ID,
        "queries"  : queries,
    }
    if merge_groups: req["merge_groups"] = merge_groups
    if aliases:      req["aliases"]       = aliases
    return req


# ─────────────────────────────────────────────────────────────────────────────
# CASO 1 — KPI con comparación temporal
# ─────────────────────────────────────────────────────────────────────────────
# Pregunta de negocio:
#   "¿Cuántos activos ACTIVOS tengo este mes y cuánto cambió vs el mes anterior?"
#
# Janus route: Query → AnalyticsRequest (single)
# Aegis transmutation: COUNT(asset) WHERE status=ACTIVE, THIS_MONTH
# viz: KPI  →  VizMeta.signal (AnalyticalSignal con IntelligenceSignal)

CASO_1_KPI = query_request(
    queries={
        "active_assets_kpi": analytics_request(
            entity="asset",
            viz="KPI",
            limit=1,
            metrics=[{
                "entity"     : "asset",
                "attribute"  : "id",
                "aggregation": "COUNT",
                "name"       : "total_active_assets",
            }],
            dimensions=[{
                "entity"   : "asset",
                "attribute": "status",       # Dimensión: filtrar KPI por status
            }],
            filters=[{
                "criteria": {
                    "field" : "status",
                    "op_ref": "EQ",
                    "value" : {"string_val": "ACTIVE"},
                }
            }],
            time_frame={
                "type"    : "THIS_MONTH",
                "timezone": "America/Bogota",
            },
            comparisons=[{
                "type"               : "TIME_SHIFT_RELATIVE",
                "label"              : "vs Mes Anterior",
                "relative_granularity": "month",
                "relative_amount"    : -1,
            }],
        )
    }
)


# ─────────────────────────────────────────────────────────────────────────────
# CASO 2 — Multi-serie línea temporal
# ─────────────────────────────────────────────────────────────────────────────
# Pregunta de negocio:
#   "Muéstrame el promedio de lectura de cada medidor (meter_reading.reading_value)
#    agrupado por día en los últimos 30 días, con una línea por activo."
#
# Janus route: Query → QueryRequest con 2 sub-queries y merge_group
#   q1: KPIs por asset (meter_reading agregado por asset_id)
#   q2: Metadata de asset (name, status) para enriquecer labels
# Aegis transmutation:
#   AVG(meter_reading.reading_value) GROUP BY asset_id, DATE_TRUNC('day', timestamp)
# viz: TIMESERIES → VizMeta.chart (ChartDecoration)

CASO_2_MULTISERIE = query_request(
    queries={
        # Sub-query 1: Series temporales de lecturas por activo
        "readings_by_asset": analytics_request(
            entity="meter_reading",
            viz="TIMESERIES",
            limit=10000,
            metrics=[{
                "entity"     : "meter_reading",
                "attribute"  : "reading_value",
                "aggregation": "AVG",
                "name"       : "avg_reading",
            }],
            dimensions=[
                {
                    "entity"   : "meter_reading",
                    "attribute": "asset_id",     # Dimensión 1: una serie por activo
                },
                {
                    "entity"   : "meter_reading",
                    "attribute": "timestamp",
                    "interval" : "day",           # Dimensión 2: agrupación diaria
                },
                {
                    "entity"   : "meter_reading",
                    "attribute": "unit_of_measure",  # Dimensión 3: eje Y semántico
                },
            ],
            filters=[{
                # Filtrar solo lecturas de los últimos 30 días
                "criteria": {
                    "field" : "timestamp",
                    "op_ref": "GTE",
                    "value" : {"string_val": "now-30d"},
                }
            }],
            time_frame={
                "type"    : "LAST_N_DAYS",
                "n_value" : 30,
                "timezone": "America/Bogota",
            },
            sort=[{
                "field"     : "timestamp",
                "descending": False,
            }],
        ),

        # Sub-query 2: Complejidad de activos (dimensión para labels del chart)
        "asset_labels": analytics_request(
            entity="asset",
            viz="TABLE",
            limit=500,
            metrics=[{
                "entity"     : "asset",
                "attribute"  : "id",
                "aggregation": "COUNT",
                "name"       : "count",
            }],
            dimensions=[
                {"entity": "asset", "attribute": "id"},
                {"entity": "asset", "attribute": "name"},
                {"entity": "asset", "attribute": "status"},
                {"entity": "asset", "attribute": "tag"},
            ],
        ),
    },
    # Merge: unir readings_by_asset ↔ asset_labels via asset_id
    merge_groups=[{
        "group_id"   : "asset_timeseries",
        "query_keys" : ["readings_by_asset", "asset_labels"],
        "override_viz": "TIMESERIES",
    }],
    aliases={
        "asset_id"  : "id",  # meter_reading.asset_id → asset.id
        "avg_reading": "Lectura Promedio",
        "name"      : "Activo",
    },
)


# ─────────────────────────────────────────────────────────────────────────────
# AST IR Printer — imprime el árbol Janus AST de una QueryRequest
# ─────────────────────────────────────────────────────────────────────────────

class JanusASTNode:
    """Nodo del árbol AST que Janus construye al parsear una QueryRequest."""

    def __init__(self, kind: str, label: str, meta: dict | None = None):
        self.kind     = kind
        self.label    = label
        self.meta     = meta or {}
        self.children: list[JanusASTNode] = []

    def add(self, node: "JanusASTNode") -> "JanusASTNode":
        self.children.append(node)
        return node

    def print(self, prefix: str = "", is_last: bool = True) -> None:
        connector = "└── " if is_last else "├── "
        kind_icon = _kind_icon(self.kind)
        meta_str  = _meta_str(self.meta)
        print(f"{prefix}{connector}{kind_icon} [{self.kind}] {self.label}{meta_str}")
        child_prefix = prefix + ("    " if is_last else "│   ")
        for i, child in enumerate(self.children):
            child.print(child_prefix, is_last=(i == len(self.children) - 1))


def _kind_icon(kind: str) -> str:
    return {
        "QUERY_REQUEST"    : "🌐",
        "ANALYTICS_REQUEST": "📊",
        "ZERO_TRUST_GATE"  : "🔒",
        "METRIC"           : "📐",
        "DIMENSION"        : "🗂 ",
        "FILTER"           : "🔍",
        "TIME_FRAME"       : "📅",
        "COMPARISON"       : "⚖️ ",
        "SORT"             : "⬆️ ",
        "VIZ"              : "🖼 ",
        "MERGE_GROUP"      : "🔗",
        "ALIAS_MAP"        : "🏷 ",
        "RPC_ROUTE"        : "🚀",
        "RAILWAY_GATE"     : "🚦",
        "OUTPUT_CAST"      : "📤",
        "TENANT_SCOPE"     : "🏢",
    }.get(kind, "•")


def _meta_str(meta: dict) -> str:
    if not meta:
        return ""
    parts = [f"{k}={v}" for k, v in meta.items()]
    return "  " + "  ".join(parts)


def build_ast(query_req: dict, case_name: str) -> JanusASTNode:
    """
    Construye el árbol AST Janus IR desde un QueryRequest dict.
    Simula la etapa de parsing y routing que Janus realiza antes
    de enviar las sub-queries a Aegis.
    """
    root = JanusASTNode("QUERY_REQUEST", case_name)

    # — Zero-Trust Gate (siempre primero en Janus) —
    zt = root.add(JanusASTNode("ZERO_TRUST_GATE", "Validación Zero-Trust", {
        "tenant_id": query_req["tenant_id"],
        "policy"   : "JANUS_400 si tenant_id vacío",
    }))
    zt.add(JanusASTNode("TENANT_SCOPE", f"tenant={query_req['tenant_id']}"))
    zt.add(JanusASTNode("RAILWAY_GATE", "[:or ok fail]", {
        "ok"  : ":status/success=true",
        "fail": ":JANUS_400",
    }))

    # — RPC Route —
    root.add(JanusASTNode("RPC_ROUTE", "MetriService/Query → server-stream"))

    # — Sub-queries (AnalyticsRequests) —
    for q_key, ar in query_req.get("queries", {}).items():
        ar_node = root.add(JanusASTNode(
            "ANALYTICS_REQUEST", q_key,
            {"entity": ar["entity"], "viz": ar.get("viz", "?")}
        ))

        # VizMeta
        ar_node.add(JanusASTNode("OUTPUT_CAST", f"viz={ar.get('viz', 'KPI')}",
            {"cast": _viz_cast(ar.get("viz", "KPI"))}))

        # Métricas
        for m in ar.get("metrics", []):
            ar_node.add(JanusASTNode("METRIC",
                f"{m['aggregation']}({m['entity']}.{m['attribute']}) → {m.get('name', '?')}",
                {"type": m["aggregation"]},
            ))

        # Dimensiones
        for d in ar.get("dimensions", []):
            interval = f" / {d['interval']}" if d.get("interval") else ""
            ar_node.add(JanusASTNode("DIMENSION",
                f"{d['entity']}.{d['attribute']}{interval}"))

        # TimeFrame
        if ar.get("time_frame"):
            tf = ar["time_frame"]
            meta = {"type": tf["type"]}
            if "n_value"  in tf: meta["n"] = tf["n_value"]
            if "timezone" in tf: meta["tz"] = tf["timezone"]
            ar_node.add(JanusASTNode("TIME_FRAME", tf["type"], meta))

        # Comparaciones
        for comp in ar.get("comparisons", []):
            ar_node.add(JanusASTNode("COMPARISON", comp.get("label", "?"), {
                "type"    : comp["type"],
                "offset"  : f"{comp.get('relative_granularity', '')} {comp.get('relative_amount', '')}",
            }))

        # Filtros
        for flt in ar.get("filters", []):
            if "criteria" in flt:
                c = flt["criteria"]
                val = list(c["value"].values())[0] if c.get("value") else "?"
                ar_node.add(JanusASTNode("FILTER",
                    f"{c['field']} {c['op_ref']} {val}"))

        # Sort
        for s in ar.get("sort", []):
            dir_ = "DESC" if s.get("descending") else "ASC"
            ar_node.add(JanusASTNode("SORT", f"{s['field']} {dir_}"))

    # — Merge Groups —
    for mg in query_req.get("merge_groups", []):
        root.add(JanusASTNode("MERGE_GROUP", mg["group_id"], {
            "queries"     : "+".join(mg["query_keys"]),
            "override_viz": mg.get("override_viz", ""),
        }))

    # — Alias Map —
    if query_req.get("aliases"):
        alias_node = root.add(JanusASTNode("ALIAS_MAP", "Resolución de aliases"))
        for src, dst in query_req["aliases"].items():
            alias_node.add(JanusASTNode("ALIAS_MAP", f"{src} → {dst}"))

    return root


def _viz_cast(viz: str) -> str:
    return {
        "KPI"       : "VizMeta.signal → AnalyticalSignal",
        "TIMESERIES": "VizMeta.chart  → ChartDecoration",
        "TABLE"     : "VizMeta.table  → TableMeta",
        "PIE"       : "VizMeta.chart  → ChartDecoration",
        "LINE"      : "VizMeta.chart  → ChartDecoration",
    }.get(viz, "VizMeta.?")


# ─────────────────────────────────────────────────────────────────────────────
# Validador contra el contrato janus-ast-ir.edn
# ─────────────────────────────────────────────────────────────────────────────

def validate_against_contract(query_req: dict, edn_path: Path) -> list[str]:
    """
    Validación estructural lightweight contra el contrato EDN.
    Verifica que los campos clave existan en las specs correspondientes.
    Retorna lista de violaciones encontradas.
    """
    if not edn_path.exists():
        return [f"⚠️  Contrato no encontrado: {edn_path}"]

    edn_src    = edn_path.read_text(encoding="utf-8")
    violations = []

    # R1: tenant_id presente (Zero-Trust)
    if not query_req.get("tenant_id"):
        violations.append("🔴 JANUS_400 — tenant_id vacío (Zero-Trust violation)")

    # R2: Cada sub-query debe tener entity y al menos 1 metric
    for k, ar in query_req.get("queries", {}).items():
        if not ar.get("entity"):
            violations.append(f"🔴 {k} — entity faltante (campo requerido)")
        if not ar.get("metrics"):
            violations.append(f"🟡 {k} — sin metrics definidas")

        # R3: Cada métrica debe tener aggregation válida
        for m in ar.get("metrics", []):
            agg = m.get("aggregation", "")
            # Verificar que el aggregation está en el enum del contrato
            if agg and f":{agg}" not in edn_src:
                violations.append(f"🔴 {k}.metric — aggregation '{agg}' no reconocida en el contrato")

        # R4: viz debe ser OutputCastType válido
        viz = ar.get("viz", "")
        if viz and f":{viz}" not in edn_src:
            violations.append(f"🔴 {k} — viz '{viz}' no reconocido en :metri.spec/output-cast-type")

        # R5: Comparaciones deben tener type válido
        for comp in ar.get("comparisons", []):
            comp_type = comp.get("type", "")
            if comp_type and f":{comp_type}" not in edn_src:
                violations.append(f"🔴 {k}.comparison — type '{comp_type}' no reconocido")

    return violations


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

SEP = "═" * 70

def print_case(
    case_name: str,
    query_req: dict,
    edn_path: Path,
    show_json: bool = True,
) -> None:
    print(f"\n{SEP}")
    print(f"  {case_name}")
    print(SEP)

    # ── JSON del QueryRequest ──────────────────────────────────────────────
    if show_json:
        print(f"\n  📋 QueryRequest (proto-compatible JSON):")
        print(f"  {'─' * 66}")
        json_str = json.dumps(query_req, indent=4, ensure_ascii=False)
        for line in json_str.splitlines():
            print(f"  {line}")

    # ── Árbol AST IR ──────────────────────────────────────────────────────
    print(f"\n  🌳 Janus AST IR — Árbol de procesamiento:")
    print(f"  {'─' * 66}")
    ast = build_ast(query_req, case_name)
    # Imprimir el árbol con indentación propia del CLI
    class IndentPrinter:
        @staticmethod
        def print_tree(node: JanusASTNode, prefix: str = "", is_last: bool = True) -> None:
            connector = "└── " if is_last else "├── "
            icon      = _kind_icon(node.kind)
            meta      = _meta_str(node.meta)
            print(f"  {prefix}{connector}{icon} [{node.kind}] {node.label}{meta}")
            child_prefix = prefix + ("    " if is_last else "│   ")
            for i, child in enumerate(node.children):
                IndentPrinter.print_tree(child, child_prefix, i == len(node.children)-1)

    IndentPrinter.print_tree(ast, "", True)

    # ── Validación contra contrato ─────────────────────────────────────────
    print(f"\n  🔍 Validación contra contrato janus-ast-ir.edn:")
    print(f"  {'─' * 66}")
    violations = validate_against_contract(query_req, edn_path)
    if violations:
        for v in violations:
            print(f"  {v}")
    else:
        print(f"  ✅ VÁLIDO — Todas las specs satisfechas por el contrato AST IR")

    # ── Resumen de routing Janus ──────────────────────────────────────────
    queries    = query_req.get("queries", {})
    n_metrics  = sum(len(ar.get("metrics", [])) for ar in queries.values())
    n_dims     = sum(len(ar.get("dimensions", [])) for ar in queries.values())
    n_filters  = sum(len(ar.get("filters", [])) for ar in queries.values())
    n_comps    = sum(len(ar.get("comparisons", [])) for ar in queries.values())
    n_merges   = len(query_req.get("merge_groups", []))

    print(f"\n  📊 Resumen del plan de ejecución Janus:")
    print(f"  {'─' * 66}")
    print(f"  Sub-queries:   {len(queries)}")
    print(f"  Métricas:      {n_metrics}")
    print(f"  Dimensiones:   {n_dims}")
    print(f"  Filtros:       {n_filters}")
    print(f"  Comparaciones: {n_comps}")
    print(f"  Merge groups:  {n_merges}")
    print(f"  Tenant scope:  {query_req['tenant_id']}")
    print()


def main() -> None:
    print(f"\n{SEP}")
    print(f"  PRUEBA DE CONCEPTO — Janus AST IR")
    print(f"  Contrato: {EDN_CONTRACT}")
    print(SEP)

    if not EDN_CONTRACT.exists():
        print(f"\n  ⚠️  Contrato no encontrado en {EDN_CONTRACT}. Generando...")
        import subprocess, sys as _sys
        subprocess.run(
            [_sys.executable, str(METRI_TOOL), "generate",
             str(METRI_ROOT / "metri.proto")],
            check=True,
        )

    print_case(
        "CASO 1 — KPI Temporal: Activos ACTIVOS vs Mes Anterior",
        CASO_1_KPI,
        EDN_CONTRACT,
    )

    print_case(
        "CASO 2 — Multi-Serie Temporal: Lecturas Promedio por Activo (30 días)",
        CASO_2_MULTISERIE,
        EDN_CONTRACT,
    )

    print(f"{SEP}")
    print(f"  ✅ Prueba de concepto completada")
    print(f"{SEP}\n")


if __name__ == "__main__":
    main()
