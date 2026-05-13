#!/usr/bin/env python3
"""
janus_ast_reliability_test.py — Suite de confiabilidad Janus AST IR (500 casos)

Genera y valida 500 consultas QueryRequest cubriendo:
  - Todos los viz types del contrato: KPI, TIMESERIES, TABLE, PIE, BUBBLE, CSV_EXPORT
  - Todos los aggregations: COUNT, SUM, AVG, MIN, MAX, MEDIAN, STD_DEV, VARIANCE,
                             PERCENTILE_90/95/99, CORRELATION, LINEAR_REGRESSION
  - Multi-series (QueryRequest compuesto con merge_groups)
  - Search (filtros FTS sobre campos string)
  - Select (queries de tabla con proyecciones dimensionales)
  - Casos de error intencional (Zero-Trust violations, specs inválidas)
  - Comparaciones temporales (TIME_SHIFT_RELATIVE, TIME_SHIFT_ABSOLUTE, BENCHMARK)
  - Todos los time_frame types: THIS_WEEK, LAST_N_DAYS, MONTH_TO_DATE, ALL_TIME, etc.
  - Filtros compuestos (FilterGroup AND/OR/NOT con FilterNode anidados)

Retorna un informe de confiabilidad con tasas de pass/fail por categoría.
"""
from __future__ import annotations

import json
import random
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR   = Path(__file__).resolve().parent
METRI_ROOT   = SCRIPT_DIR.parent.parent
EDN_CONTRACT = METRI_ROOT / "resources" / "schema" / "janus-ast-ir.edn"

# ─────────────────────────────────────────────────────────────────────────────
# Valores válidos del contrato janus-ast-ir.edn
# ─────────────────────────────────────────────────────────────────────────────

VALID_VIZ          = ["KPI", "TIMESERIES", "TABLE", "PIE", "BUBBLE", "CSV_EXPORT"]
VALID_AGGREGATIONS = [
    "COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN", "STD_DEV",
    "VARIANCE", "PERCENTILE_90", "PERCENTILE_95", "PERCENTILE_99",
    "CORRELATION", "LINEAR_REGRESSION",
]
VALID_OPERATORS    = ["EQ", "NEQ", "GT", "GTE", "LT", "LTE",
                      "IN", "BETWEEN", "LIKE", "IS_NULL", "IS_NOT_NULL", "MATCHES"]
VALID_TIME_FRAMES  = [
    "CUSTOM_RANGE", "LAST_N_MINUTES", "LAST_N_HOURS", "LAST_N_DAYS",
    "NEXT_N_DAYS", "THIS_WEEK", "LAST_WEEK", "NEXT_WEEK", "LAST_N_WEEKS",
    "NEXT_N_WEEKS", "WEEK_TO_DATE", "THIS_MONTH", "LAST_MONTH", "NEXT_MONTH",
    "LAST_N_MONTHS", "NEXT_N_MONTHS", "MONTH_TO_DATE", "LAST_N_QUARTERS",
    "QUARTER_TO_DATE", "THIS_QUARTER", "THIS_YEAR", "LAST_YEAR", "LAST_N_YEARS",
    "YEAR_TO_DATE", "ALL_TIME", "TODAY", "YESTERDAY", "TOMORROW",
]
VALID_COMPARISON_TYPES = [
    "TIME_SHIFT_RELATIVE", "TIME_SHIFT_ABSOLUTE", "TIME_SHIFT_SHORTCUT", "BENCHMARK", "SMART",
]
VALID_CONJUNCTIONS = ["AND", "OR", "NOT"]
VALID_INTERVALS    = ["minute", "hour", "day", "week", "month", "quarter", "year"]

# Entidades y atributos de dominio
ENTITIES = {
    "asset"           : ["id", "name", "serial_number", "tag", "status", "location_id", "omniclass_category"],
    "meter_reading"   : ["id", "asset_id", "reading_value", "unit_of_measure", "timestamp", "metadata"],
    "location"        : ["id", "name", "code", "parent_id", "level"],
    "work_order"      : ["id", "code", "status", "asset_id", "priority", "created_at", "closed_at"],
    "tenant"          : ["id", "name", "plan", "status", "created_at"],
}
MEASURE_ATTRS = {
    "asset"        : ["id"],
    "meter_reading": ["reading_value", "id"],
    "location"     : ["id"],
    "work_order"   : ["id"],
    "tenant"       : ["id"],
}
DIM_ATTRS = {
    "asset"        : ["status", "location_id", "omniclass_category"],
    "meter_reading": ["asset_id", "unit_of_measure", "timestamp", "metadata"],
    "location"     : ["name", "code", "level"],
    "work_order"   : ["status", "priority", "asset_id"],
    "tenant"       : ["plan", "status"],
}
STRING_ATTRS = {
    "asset"        : ["name", "serial_number", "tag", "omniclass_category"],
    "meter_reading": ["unit_of_measure"],
    "location"     : ["name", "code"],
    "work_order"   : ["code"],
    "tenant"       : ["name"],
}

TENANT_IDS = ["tenant-001", "tenant-002", "tenant-abc", "org-beta-99", "demo-tenant"]


# ─────────────────────────────────────────────────────────────────────────────
# Generadores de partes de query (helpers)
# ─────────────────────────────────────────────────────────────────────────────

rng = random.Random(42)   # seed fija → resultados reproducibles


def pick(lst):  return rng.choice(lst)
def pickn(lst, n): return rng.sample(lst, min(n, len(lst)))


def gen_metric(entity: str, aggregation: str | None = None) -> dict:
    attr = pick(MEASURE_ATTRS[entity])
    agg  = aggregation or pick(VALID_AGGREGATIONS)
    return {
        "entity"     : entity,
        "attribute"  : attr,
        "aggregation": agg,
        "name"       : f"{agg.lower()}_{attr}",
    }


def gen_dimension(entity: str, with_interval: bool = False) -> dict:
    attrs = DIM_ATTRS[entity]
    attr  = pick(attrs)
    d: dict = {"entity": entity, "attribute": attr}
    if with_interval and attr == "timestamp":
        d["interval"] = pick(VALID_INTERVALS)
    return d


def gen_time_frame(tf_type: str | None = None) -> dict:
    t = tf_type or pick(VALID_TIME_FRAMES)
    tf: dict = {"type": t, "timezone": "America/Bogota"}
    if "N_DAYS"    in t: tf["n_value"] = rng.randint(1, 90)
    if "N_HOURS"   in t: tf["n_value"] = rng.randint(1, 48)
    if "N_MINUTES" in t: tf["n_value"] = rng.randint(5, 120)
    if "N_WEEKS"   in t: tf["n_value"] = rng.randint(1, 12)
    if "N_MONTHS"  in t: tf["n_value"] = rng.randint(1, 24)
    if "N_QUARTERS" in t: tf["n_value"] = rng.randint(1, 8)
    if "N_YEARS"   in t: tf["n_value"] = rng.randint(1, 5)
    if t == "CUSTOM_RANGE":
        tf["absolute_start_ts"] = 1700000000
        tf["absolute_end_ts"]   = 1710000000
    return tf


def gen_filter_criteria(entity: str) -> dict:
    op   = pick(["EQ", "NEQ", "GT", "GTE", "LT", "LTE", "LIKE", "IN", "IS_NULL", "IS_NOT_NULL"])
    attr = pick(list(ENTITIES[entity]))
    val: dict = {}
    if op in ("EQ", "NEQ", "LIKE"):
        val = {"string_val": pick(["ACTIVE", "INACTIVE", "KWH", "CEL", "A-1234567"])}
    elif op in ("GT", "GTE", "LT", "LTE"):
        val = {"number_val": str(rng.uniform(0, 1000))}
    elif op in ("IS_NULL", "IS_NOT_NULL"):
        val = {}
    elif op == "IN":
        val = {"string_val": "ACTIVE,INACTIVE"}
    return {"field": attr, "op_ref": op, **({"value": val} if val else {})}


def gen_filter_node(entity: str, depth: int = 0) -> dict:
    if depth >= 2 or rng.random() < 0.6:
        return {"criteria": gen_filter_criteria(entity)}
    return {
        "group": {
            "conjunction": pick(VALID_CONJUNCTIONS),
            "nodes": [gen_filter_node(entity, depth + 1)
                      for _ in range(rng.randint(2, 3))]
        }
    }


def gen_comparison(comp_type: str | None = None) -> dict:
    t = comp_type or pick(VALID_COMPARISON_TYPES)
    c: dict = {"type": t, "label": f"vs {t.replace('_', ' ').title()}"}
    if t == "TIME_SHIFT_RELATIVE":
        c["relative_granularity"] = pick(["day", "week", "month", "quarter", "year"])
        c["relative_amount"]      = rng.choice([-1, -2, -3, -7, -30])
    elif t == "BENCHMARK":
        c["benchmark_value"] = rng.uniform(100, 10000)
    return c


def gen_sort(entity: str) -> dict:
    attr = pick(list(ENTITIES[entity]))
    return {"field": attr, "descending": rng.choice([True, False])}


def gen_analytics_request(entity: str, viz: str,
                           n_metrics: int = 1, n_dims: int = 0,
                           with_time: bool = True, with_filter: bool = False,
                           with_comparison: bool = False, with_sort: bool = False,
                           n_groups: int = 0,
                           aggregation: str | None = None) -> dict:
    req: dict = {
        "tenant_id": pick(TENANT_IDS),
        "entity"   : entity,
        "metrics"  : [gen_metric(entity, aggregation) for _ in range(max(1, n_metrics))],
        "viz"      : viz,
    }
    dims = []
    for _ in range(n_dims):
        dims.append(gen_dimension(entity, with_interval=(viz == "TIMESERIES")))
    if dims:
        req["dimensions"] = dims
    if with_time:
        req["time_frame"] = gen_time_frame()
    if with_filter:
        req["filters"] = [gen_filter_node(entity) for _ in range(rng.randint(1, 3))]
    if with_comparison:
        req["comparisons"] = [gen_comparison()]
    if with_sort:
        req["sort"] = [gen_sort(entity)]
    if viz != "KPI" and rng.random() < 0.3:
        req["limit"] = rng.choice([10, 50, 100, 500, 1000, 10000])
    return req


def gen_query_request(queries: dict[str, dict],
                      merge_groups: list | None = None,
                      aliases: dict | None = None) -> dict:
    req: dict = {"tenant_id": pick(TENANT_IDS), "queries": queries}
    if merge_groups: req["merge_groups"] = merge_groups
    if aliases:      req["aliases"]       = aliases
    return req


# ─────────────────────────────────────────────────────────────────────────────
# Validador contra el contrato
# ─────────────────────────────────────────────────────────────────────────────

def validate(query_req: dict, edn_src: str) -> list[str]:
    """Valida el QueryRequest contra el contrato janus-ast-ir.edn."""
    violations: list[str] = []

    # Zero-Trust
    if not query_req.get("tenant_id"):
        violations.append("JANUS_400:tenant_id_empty")

    for q_key, ar in query_req.get("queries", {}).items():
        if not ar.get("entity"):
            violations.append(f"{q_key}:entity_missing")
        if not ar.get("metrics"):
            violations.append(f"{q_key}:metrics_empty")

        for m in ar.get("metrics", []):
            agg = m.get("aggregation", "")
            if agg and f":{agg}" not in edn_src:
                violations.append(f"{q_key}:aggregation_invalid:{agg}")

        viz = ar.get("viz", "")
        if viz and f":{viz}" not in edn_src:
            violations.append(f"{q_key}:viz_invalid:{viz}")

        tf = ar.get("time_frame", {})
        if tf:
            tf_type = tf.get("type", "")
            if tf_type and f":{tf_type}" not in edn_src:
                violations.append(f"{q_key}:time_frame_invalid:{tf_type}")

        for comp in ar.get("comparisons", []):
            ct = comp.get("type", "")
            if ct and f":{ct}" not in edn_src:
                violations.append(f"{q_key}:comparison_invalid:{ct}")

        for fnode in ar.get("filters", []):
            _validate_filter_node(fnode, q_key, edn_src, violations)

    return violations


def _validate_filter_node(node: dict, prefix: str, edn_src: str, violations: list) -> None:
    if "criteria" in node:
        c  = node["criteria"]
        op = c.get("op_ref", "")
        if op and f":{op}" not in edn_src:
            violations.append(f"{prefix}:op_invalid:{op}")
    elif "group" in node:
        conj = node["group"].get("conjunction", "")
        if conj and f":{conj}" not in edn_src:
            violations.append(f"{prefix}:conjunction_invalid:{conj}")
        for child in node["group"].get("nodes", []):
            _validate_filter_node(child, prefix, edn_src, violations)


# ─────────────────────────────────────────────────────────────────────────────
# Fábricas de casos por categoría
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TestCase:
    name    : str
    category: str
    query   : dict
    expect_violations: list[str] = field(default_factory=list)  # si es caso de error intencional

    def is_negative(self) -> bool:
        return bool(self.expect_violations)


def factory_kpi(n: int) -> list[TestCase]:
    cases = []
    entities = list(ENTITIES.keys())
    aggs_suitable = ["COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN"]
    for i in range(n):
        entity = pick(entities)
        agg    = pick(aggs_suitable)
        with_comp = rng.random() < 0.5
        q = gen_query_request({
            "main": gen_analytics_request(
                entity, "KPI",
                n_metrics=1, n_dims=rng.randint(0, 2),
                with_time=True, with_filter=rng.random() < 0.6,
                with_comparison=with_comp,
                aggregation=agg,
            )
        })
        cases.append(TestCase(f"kpi_{i:03d}", "KPI", q))
    return cases


def factory_timeseries(n: int) -> list[TestCase]:
    cases = []
    for i in range(n):
        entity = pick(["meter_reading", "asset", "work_order"])
        n_metrics = rng.randint(1, 3)
        q = gen_query_request({
            "series": gen_analytics_request(
                entity, "TIMESERIES",
                n_metrics=n_metrics,
                n_dims=rng.randint(1, 3),
                with_time=True, with_filter=rng.random() < 0.4,
                with_sort=True,
            )
        })
        cases.append(TestCase(f"timeseries_{i:03d}", "TIMESERIES", q))
    return cases


def factory_area(n: int) -> list[TestCase]:
    """AREA usa TIMESERIES como viz base — multiserie temporal con área."""
    cases = []
    for i in range(n):
        entity = pick(["meter_reading", "work_order"])
        agg    = pick(["SUM", "AVG", "COUNT"])
        q = gen_query_request({
            "area": gen_analytics_request(
                entity, "TIMESERIES",   # área es un subtipo de TIMESERIES en el contrato
                n_metrics=1, n_dims=2,
                with_time=True, with_filter=rng.random() < 0.5,
                aggregation=agg,
            )
        })
        q["queries"]["area"]["chart_subtype"] = "AREA"  # meta adicional
        cases.append(TestCase(f"area_{i:03d}", "AREA", q))
    return cases


def factory_pie(n: int) -> list[TestCase]:
    cases = []
    for i in range(n):
        entity = pick(["asset", "work_order", "location"])
        agg    = pick(["COUNT", "SUM"])
        q = gen_query_request({
            "pie": gen_analytics_request(
                entity, "PIE",
                n_metrics=1, n_dims=rng.randint(1, 2),
                with_time=rng.random() < 0.5,
                with_filter=rng.random() < 0.4,
                aggregation=agg,
            )
        })
        cases.append(TestCase(f"pie_{i:03d}", "PIE", q))
    return cases


def factory_bubble(n: int) -> list[TestCase]:
    cases = []
    for i in range(n):
        entity = pick(["meter_reading", "work_order"])
        q = gen_query_request({
            "bubble": gen_analytics_request(
                entity, "BUBBLE",
                n_metrics=rng.randint(2, 3),  # necesita x, y y tamaño
                n_dims=rng.randint(1, 2),
                with_time=True,
            )
        })
        cases.append(TestCase(f"bubble_{i:03d}", "BUBBLE", q))
    return cases


def factory_table(n: int) -> list[TestCase]:
    """TABLE: proyección dimensional con múltiples columnas y sort."""
    cases = []
    for i in range(n):
        entity = pick(list(ENTITIES.keys()))
        n_dims = rng.randint(2, 5)
        q = gen_query_request({
            "table": gen_analytics_request(
                entity, "TABLE",
                n_metrics=rng.randint(1, 3),
                n_dims=n_dims,
                with_time=rng.random() < 0.6,
                with_filter=rng.random() < 0.7,
                with_sort=True,
            )
        })
        cases.append(TestCase(f"table_{i:03d}", "TABLE", q))
    return cases


def factory_csv_export(n: int) -> list[TestCase]:
    cases = []
    for i in range(n):
        entity = pick(list(ENTITIES.keys()))
        q = gen_query_request({
            "export": gen_analytics_request(
                entity, "CSV_EXPORT",
                n_metrics=rng.randint(1, 4),
                n_dims=rng.randint(2, 6),
                with_time=True, with_filter=rng.random() < 0.5,
                with_sort=True,
            )
        })
        q["queries"]["export"]["limit"] = rng.choice([1000, 5000, 50000])
        cases.append(TestCase(f"csv_{i:03d}", "CSV_EXPORT", q))
    return cases


def factory_multiseries(n: int) -> list[TestCase]:
    """Multi-series: 2 sub-queries + merge_group."""
    cases = []
    for i in range(n):
        e1 = "meter_reading"
        e2 = "asset"
        q  = gen_query_request(
            queries={
                "readings": gen_analytics_request(
                    e1, "TIMESERIES", n_metrics=1, n_dims=2,
                    with_time=True, with_filter=rng.random() < 0.5, with_sort=True,
                ),
                "labels": gen_analytics_request(
                    e2, "TABLE", n_metrics=1, n_dims=rng.randint(2, 4),
                    with_time=False,
                ),
            },
            merge_groups=[{
                "group_id"    : f"multi_{i}",
                "query_keys"  : ["readings", "labels"],
                "override_viz": "TIMESERIES",
            }],
            aliases={"asset_id": "id"},
        )
        cases.append(TestCase(f"multiseries_{i:03d}", "MULTI_SERIES", q))
    return cases


def factory_search(n: int) -> list[TestCase]:
    """SEARCH: TABLE query con filtros CONTAINS sobre campos FTS."""
    cases = []
    terms = ["pump", "valve", "motor-A", "2024", "INACTIVE", "KWH", "building-3"]
    for i in range(n):
        entity = pick(["asset", "location", "work_order"])
        sattr  = pick(STRING_ATTRS[entity])
        q = gen_query_request({
            "search": {
                "tenant_id": pick(TENANT_IDS),
                "entity"   : entity,
                "metrics"  : [gen_metric(entity, "COUNT")],
                "viz"      : "TABLE",
                "dimensions": [
                    {"entity": entity, "attribute": a}
                    for a in pickn(list(ENTITIES[entity]), 3)
                ],
                "filters": [{
                    "criteria": {
                        "field" : sattr,
                        "op_ref": "LIKE",          # FTS → LIKE en el contrato
                        "value" : {"string_val": f"%{pick(terms)}%"},
                    }
                }],
                "limit": rng.choice([10, 25, 50]),
                "sort" : [gen_sort(entity)],
            }
        })
        cases.append(TestCase(f"search_{i:03d}", "SEARCH", q))
    return cases


def factory_select(n: int) -> list[TestCase]:
    """SELECT: TABLE query proyectando dimensiones específicas (drill-down)."""
    cases = []
    for i in range(n):
        entity = pick(list(ENTITIES.keys()))
        dims   = pickn(list(ENTITIES[entity]), rng.randint(2, 5))
        q = gen_query_request({
            "select": {
                "tenant_id" : pick(TENANT_IDS),
                "entity"    : entity,
                "metrics"   : [gen_metric(entity, "COUNT")],
                "viz"       : "TABLE",
                "dimensions": [{"entity": entity, "attribute": a} for a in dims],
                "filters"   : [gen_filter_node(entity)] if rng.random() < 0.6 else [],
                "sort"      : [gen_sort(entity)],
                "limit"     : rng.choice([10, 25, 50, 100]),
            }
        })
        cases.append(TestCase(f"select_{i:03d}", "SELECT", q))
    return cases


def factory_advanced_agg(n: int) -> list[TestCase]:
    """Tests específicos para aggregations avanzadas (percentiles, std_dev, etc.)"""
    cases = []
    adv_aggs = ["MEDIAN", "STD_DEV", "VARIANCE",
                "PERCENTILE_90", "PERCENTILE_95", "PERCENTILE_99",
                "CORRELATION", "LINEAR_REGRESSION"]
    for i in range(n):
        agg    = pick(adv_aggs)
        entity = pick(["meter_reading", "work_order"])
        viz    = pick(["KPI", "TIMESERIES", "TABLE"])
        q = gen_query_request({
            "adv": gen_analytics_request(
                entity, viz,
                n_metrics=1, n_dims=rng.randint(0, 2),
                with_time=True, aggregation=agg,
            )
        })
        cases.append(TestCase(f"adv_agg_{i:03d}", "ADVANCED_AGG", q))
    return cases


def factory_comparison_all_types(n: int) -> list[TestCase]:
    """Tests de todos los tipos de comparación temporal."""
    cases = []
    for i in range(n):
        comp_type = VALID_COMPARISON_TYPES[i % len(VALID_COMPARISON_TYPES)]
        entity    = pick(["asset", "meter_reading", "work_order"])
        q = gen_query_request({
            "comp": gen_analytics_request(
                entity, "KPI",
                with_time=True, with_comparison=True,
                aggregation="COUNT",
            )
        })
        q["queries"]["comp"]["comparisons"] = [gen_comparison(comp_type)]
        cases.append(TestCase(f"comparison_{i:03d}", "COMPARISON", q))
    return cases


def factory_all_time_frames(n: int) -> list[TestCase]:
    """Tests de todos los time_frame types."""
    cases = []
    for i in range(n):
        tf_type = VALID_TIME_FRAMES[i % len(VALID_TIME_FRAMES)]
        entity  = pick(list(ENTITIES.keys()))
        q = gen_query_request({
            "tf": gen_analytics_request(
                entity, pick(["KPI", "TIMESERIES", "TABLE"]),
                with_time=True,
            )
        })
        q["queries"]["tf"]["time_frame"] = gen_time_frame(tf_type)
        cases.append(TestCase(f"timeframe_{i:03d}", "TIME_FRAME", q))
    return cases


def factory_complex_filters(n: int) -> list[TestCase]:
    """Tests de filtros compuestos AND/OR/NOT anidados."""
    cases = []
    for i in range(n):
        entity = pick(list(ENTITIES.keys()))
        # Generar un FilterGroup multi-nivel
        fnode  = {
            "group": {
                "conjunction": pick(VALID_CONJUNCTIONS),
                "nodes": [
                    gen_filter_node(entity, 0),
                    gen_filter_node(entity, 0),
                    {
                        "group": {
                            "conjunction": pick(VALID_CONJUNCTIONS),
                            "nodes": [
                                gen_filter_node(entity, 1),
                                gen_filter_node(entity, 1),
                            ]
                        }
                    }
                ]
            }
        }
        q = gen_query_request({
            "filtered": gen_analytics_request(
                entity, pick(["TABLE", "KPI", "TIMESERIES"]),
                with_time=True, with_filter=False,
            )
        })
        q["queries"]["filtered"]["filters"] = [fnode]
        cases.append(TestCase(f"complex_filter_{i:03d}", "COMPLEX_FILTER", q))
    return cases


def factory_negative_cases(n: int) -> list[TestCase]:
    """Casos negativos deliberados — deben producir violations conocidas."""
    cases = []
    for i in range(n):
        error_type = i % 5
        if error_type == 0:
            # Sin tenant_id
            q = gen_query_request({"q": gen_analytics_request("asset", "KPI")})
            q["tenant_id"] = ""
            cases.append(TestCase(f"neg_zt_{i:03d}", "NEGATIVE_ZT", q,
                                  expect_violations=["JANUS_400:tenant_id_empty"]))
        elif error_type == 1:
            # viz inválido
            q = gen_query_request({"q": gen_analytics_request("asset", "KPI")})
            q["queries"]["q"]["viz"] = "RADAR_CHART"
            cases.append(TestCase(f"neg_viz_{i:03d}", "NEGATIVE_VIZ", q,
                                  expect_violations=["q:viz_invalid:RADAR_CHART"]))
        elif error_type == 2:
            # aggregation inválida
            q = gen_query_request({"q": gen_analytics_request("asset", "TABLE")})
            q["queries"]["q"]["metrics"][0]["aggregation"] = "GEOMETRIC_MEAN"
            cases.append(TestCase(f"neg_agg_{i:03d}", "NEGATIVE_AGG", q,
                                  expect_violations=["q:aggregation_invalid:GEOMETRIC_MEAN"]))
        elif error_type == 3:
            # time_frame type inválido
            q = gen_query_request({"q": gen_analytics_request("asset", "KPI")})
            q["queries"]["q"]["time_frame"] = {"type": "FOREVER", "timezone": "UTC"}
            cases.append(TestCase(f"neg_tf_{i:03d}", "NEGATIVE_TF", q,
                                  expect_violations=["q:time_frame_invalid:FOREVER"]))
        else:
            # tenant_id None
            q = gen_query_request({"q": gen_analytics_request("meter_reading", "TIMESERIES")})
            del q["tenant_id"]
            cases.append(TestCase(f"neg_notenant_{i:03d}", "NEGATIVE_NOTENANT", q,
                                  expect_violations=["JANUS_400:tenant_id_empty"]))
    return cases


# ─────────────────────────────────────────────────────────────────────────────
# Runner de confiabilidad
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TestResult:
    case    : TestCase
    passed  : bool
    violations: list[str]
    expected_matched: bool
    latency_us: float

@dataclass
class ReliabilityReport:
    total   : int = 0
    passed  : int = 0
    failed  : int = 0
    by_category: dict = field(default_factory=lambda: defaultdict(lambda: {"pass": 0, "fail": 0, "n": 0}))
    failures: list[TestResult] = field(default_factory=list)
    latencies_us: list[float] = field(default_factory=list)

    def record(self, result: TestResult) -> None:
        self.total += 1
        cat = self.by_category[result.case.category]
        cat["n"] += 1
        self.latencies_us.append(result.latency_us)
        if result.passed:
            self.passed += 1
            cat["pass"] += 1
        else:
            self.failed += 1
            cat["fail"] += 1
            self.failures.append(result)

    @property
    def pass_rate(self) -> float:
        return self.passed / max(1, self.total) * 100


def run_case(case: TestCase, edn_src: str) -> TestResult:
    t0         = time.perf_counter()
    violations = validate(case.query, edn_src)
    latency    = (time.perf_counter() - t0) * 1_000_000  # microseconds

    if case.is_negative():
        # Para casos negativos: pasamos si detectamos las violations esperadas
        expected_found = all(
            any(ev in v for v in violations)
            for ev in case.expect_violations
        )
        passed = expected_found
        expected_matched = expected_found
    else:
        # Para casos positivos: pasamos si no hay violations
        passed           = len(violations) == 0
        expected_matched = True

    return TestResult(
        case=case, passed=passed,
        violations=violations,
        expected_matched=expected_matched,
        latency_us=latency,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Construcción del suite
# ─────────────────────────────────────────────────────────────────────────────

def build_suite() -> list[TestCase]:
    suite: list[TestCase] = []
    # Distribución de 500 casos:
    suite += factory_kpi(60)             #  60 — KPI simple y con comparaciones
    suite += factory_timeseries(55)      #  55 — Series temporales
    suite += factory_area(35)            #  35 — Área (subtipo TIMESERIES)
    suite += factory_pie(35)             #  35 — Pie charts
    suite += factory_bubble(25)          #  25 — Bubble charts
    suite += factory_table(55)           #  55 — Tablas con proyecciones
    suite += factory_csv_export(25)      #  25 — Exportaciones CSV
    suite += factory_multiseries(40)     #  40 — Multi-series con merge
    suite += factory_search(35)          #  35 — Search FTS
    suite += factory_select(35)          #  35 — Select drill-down
    suite += factory_advanced_agg(30)    #  30 — Aggregations avanzadas
    suite += factory_comparison_all_types(25)  # 25 — Todos los tipos de comparación
    suite += factory_all_time_frames(30) #  30 — Todos los time_frame types
    suite += factory_complex_filters(25) #  25 — Filtros compuestos AND/OR/NOT
    suite += factory_negative_cases(30)  #  30 — Casos negativos intencionales
    # Total: 560 → shuffle y tomar 500
    rng.shuffle(suite)
    return suite[:500]


# ─────────────────────────────────────────────────────────────────────────────
# Reporter
# ─────────────────────────────────────────────────────────────────────────────

SEP  = "═" * 72
SEP2 = "─" * 68

def print_report(report: ReliabilityReport) -> None:
    lats = sorted(report.latencies_us)
    p50  = lats[int(len(lats) * 0.50)]
    p95  = lats[int(len(lats) * 0.95)]
    p99  = lats[int(len(lats) * 0.99)]
    avg  = sum(lats) / len(lats)

    print(f"\n{SEP}")
    print(f"  JANUS AST IR — RELIABILITY REPORT")
    print(f"  Suite de confiabilidad: {report.total} casos")
    print(SEP)

    rate = report.pass_rate
    bar  = "█" * int(rate / 2) + "░" * (50 - int(rate / 2))
    status = "✅ CONFIABLE" if rate >= 99 else "🟡 ACEPTABLE" if rate >= 95 else "🔴 INESTABLE"
    print(f"\n  RESULTADO GLOBAL  {status}")
    print(f"  {SEP2}")
    print(f"  [{bar}]")
    print(f"  Pass:  {report.passed:>4} / {report.total}  ({rate:.2f}%)")
    print(f"  Fail:  {report.failed:>4} / {report.total}  ({100 - rate:.2f}%)")

    print(f"\n  LATENCIA DEL VALIDADOR (μs)")
    print(f"  {SEP2}")
    print(f"  Media:  {avg:>8.2f} μs")
    print(f"  P50:    {p50:>8.2f} μs")
    print(f"  P95:    {p95:>8.2f} μs")
    print(f"  P99:    {p99:>8.2f} μs")

    print(f"\n  RESULTADOS POR CATEGORÍA")
    print(f"  {SEP2}")
    cat_width = max(len(c) for c in report.by_category)
    for cat, stats in sorted(report.by_category.items()):
        n    = stats["n"]
        ok   = stats["pass"]
        fail = stats["fail"]
        pct  = ok / max(1, n) * 100
        bar2 = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        icon = "✅" if pct == 100 else "🟡" if pct >= 90 else "🔴"
        print(f"  {icon} {cat:<{cat_width}}  [{bar2}] {pct:>6.1f}%  ({ok}/{n})")

    if report.failures:
        neg_fails  = [f for f in report.failures if f.case.is_negative()]
        pos_fails  = [f for f in report.failures if not f.case.is_negative()]
        print(f"\n  FALLOS ({len(report.failures)} total)")
        print(f"  {SEP2}")
        if pos_fails:
            print(f"\n  🔴 Falsos negativos (debían pasar, fallaron):")
            for r in pos_fails[:10]:
                print(f"    [{r.case.category}] {r.case.name}")
                for v in r.violations[:3]:
                    print(f"      → {v}")
        if neg_fails:
            print(f"\n  🟡 Casos negativos no detectados:")
            for r in neg_fails[:5]:
                print(f"    [{r.case.category}] {r.case.name}")
                print(f"      Esperado: {r.case.expect_violations}")
                print(f"      Obtenido: {r.violations}")

    print(f"\n{SEP}")
    verdict = (
        "✅ JANUS AST IR — CONTRATO VERIFICADO. Confiabilidad de producción."
        if rate >= 99 else
        "🟡 JANUS AST IR — Confiable con observaciones menores."
        if rate >= 95 else
        "🔴 JANUS AST IR — Revisar contrato. Tasa de fallo inaceptable."
    )
    print(f"  {verdict}")
    print(f"{SEP}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    print(f"\n{SEP}")
    print(f"  JANUS AST IR — Suite de Confiabilidad (500 casos)")
    print(f"  Contrato: {EDN_CONTRACT}")
    print(SEP)

    if not EDN_CONTRACT.exists():
        print(f"\n  ERROR: Contrato no encontrado en {EDN_CONTRACT}")
        print(f"  Ejecuta primero: python3 tools/metri_schema.py generate metri.proto")
        return 1

    edn_src = EDN_CONTRACT.read_text(encoding="utf-8")
    suite   = build_suite()

    print(f"\n  Ejecutando {len(suite)} casos de prueba...\n")
    report  = ReliabilityReport()

    # Ejecutar con barra de progreso simple
    bar_width = 50
    for i, case in enumerate(suite):
        result = run_case(case, edn_src)
        report.record(result)
        # Progreso inline
        pct    = (i + 1) / len(suite)
        filled = int(pct * bar_width)
        bar    = "█" * filled + "░" * (bar_width - filled)
        icon   = "✅" if result.passed else "🔴"
        print(f"\r  [{bar}] {i+1:>3}/{len(suite)}  {icon} {case.category:<18} {case.name}",
              end="", flush=True)

    print()  # nueva línea tras la barra
    print_report(report)
    return 0 if report.pass_rate >= 95 else 1


if __name__ == "__main__":
    sys.exit(main())
