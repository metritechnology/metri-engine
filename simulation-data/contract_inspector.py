#!/usr/bin/env python3
"""
METRES ENGINE — CONTRACT INSPECTOR
====================================
Analiza el grafo completo del contrato metri.proto sobre una respuesta real
y detecta brechas de cobertura entre lo que el motor emite y lo que el
contrato exige.

Mensajes auditados (contrato completo de QueryResponse):
  - Status         → { success, error_code, error_message }
  - RowSet         → { columns[], rows_json }
  - VizMeta        → { type, payload: signal|chart|table|breakdown|tree }
  - QueryMetadata  → { engine, execution_time_ms, total_count, query_id... }
  - Pagination     → { page_size, has_next, has_previous, next_cursor, links[] }
  - Links[]        → { rel, href, method } — HATEOAS first/next/prev/last
"""

import struct
import logging
import requests
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import metri_pb2
import google.protobuf.json_format as json_format

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
FUNCTION_URL = "https://engine.metri.one/"


# ─────────────────────────────────────────────────────────────────────────────
# Contract: expected fields per message (derived from metri.proto + ast-ir.edn)
# ─────────────────────────────────────────────────────────────────────────────

CONTRACT = {
    "QueryResponse": {
        "required": ["status"],
        "optional": ["data", "viz_ext", "batch_results", "metadata", "pagination", "links"],
    },
    "Status": {
        "required": ["success"],
        "optional": ["error_code", "error_message"],
    },
    "QueryMetadata": {
        "required": ["engine"],
        "optional": [
            "execution_time_ms", "total_count", "query_id",
            "cache_hits", "cache_ttl_seconds", "is_semantic",
            "parallelism_factor", "total_queries"
        ],
    },
    "VizMeta": {
        "required": ["type"],
        "optional": [],
        "one_of": {
            "payload": ["signal", "chart", "table", "breakdown", "tree"]
        }
    },
    "AnalyticalSignal": {
        "required": ["value"],
        "optional": ["previous_value", "intelligence", "history", "thresholds", "unit", "status_label"],
    },
    "IntelligenceSignal": {
        "required": ["direction"],
        "optional": ["percentage", "delta_abs", "label", "is_anomaly", "z_score", "represents_initial", "previous_value"],
    },
    "ChartDecoration": {
        "required": ["x_dimension"],
        "optional": ["y_dimensions", "color_scheme", "show_legend", "show_tooltip", "title", "stacked", "smooth"],
    },
    "TableMeta": {
        "required": ["columns"],
        "optional": ["row_actions", "global_links"],
    },
    "TableColumn": {
        "required": ["key", "label", "sortable"],
        "optional": ["type", "align", "format", "metadata", "entity_ref"],
    },
    "Pagination": {
        "required": ["page_size"],
        "optional": ["has_next", "has_previous", "next_cursor", "previous_cursor", "links"],
    },
    "Link": {
        "required": ["rel", "href", "method"],
        "optional": [],
    },
    "RowSet": {
        "required": ["columns"],
        "optional": [],
        "one_of": {
            "payload_strategy": ["rows_json", "arrow_binary_blob", "presigned_csv_url"]
        }
    },
}

# HATEOAS links that every QueryResponse with data MUST expose
HATEOAS_REL = {"self", "next", "prev", "first", "last", "refresh"}


@dataclass
class ContractViolation:
    path: str
    severity: str  # CRITICAL | WARNING | INFO
    message: str


@dataclass
class AuditReport:
    query_key: str
    violations: List[ContractViolation] = field(default_factory=list)
    coverage_score: float = 0.0
    field_presence: Dict[str, bool] = field(default_factory=dict)
    raw_response: Dict[str, Any] = field(default_factory=dict)

    def add(self, path: str, severity: str, message: str):
        self.violations.append(ContractViolation(path, severity, message))


def _check_message(report: AuditReport, prefix: str, data: dict, spec_key: str):
    spec = CONTRACT.get(spec_key, {})
    for field_name in spec.get("required", []):
        present = field_name in data and data[field_name] not in (None, "", [], {})
        report.field_presence[f"{prefix}.{field_name}"] = present
        if not present:
            report.add(f"{prefix}.{field_name}", "CRITICAL",
                       f"[{spec_key}] Campo requerido ausente: '{field_name}'")

    for field_name in spec.get("optional", []):
        present = field_name in data and data[field_name] not in (None, "", [], {})
        report.field_presence[f"{prefix}.{field_name}"] = present
        if not present:
            report.add(f"{prefix}.{field_name}", "WARNING",
                       f"[{spec_key}] Campo opcional no poblado: '{field_name}'")

    for oneof_name, variants in spec.get("one_of", {}).items():
        found = any(v in data for v in variants)
        report.field_presence[f"{prefix}.{oneof_name}"] = found
        if not found:
            report.add(f"{prefix}.{oneof_name}", "CRITICAL",
                       f"[{spec_key}] oneof '{oneof_name}' sin ninguna variante presente. Esperado: {variants}")


def audit_status(report: AuditReport, status: dict):
    _check_message(report, "status", status, "Status")
    if not status.get("success"):
        report.add("status.error_code", "CRITICAL" if not status.get("error_code") else "INFO",
                   f"Status no-ok: code={status.get('error_code')} msg={status.get('error_message')}")


def audit_metadata(report: AuditReport, meta: dict):
    _check_message(report, "metadata", meta, "QueryMetadata")
    if not meta.get("query_id"):
        report.add("metadata.query_id", "WARNING", "Falta query_id para trazabilidad distribuida (OTel/Sherlog)")


def audit_pagination(report: AuditReport, pag: dict, has_data: bool):
    if not pag:
        if has_data:
            report.add("pagination", "WARNING",
                       "Respuesta con datos no incluye Pagination. UI no puede ofrecer 'Cargar Más'.")
        return
    _check_message(report, "pagination", pag, "Pagination")
    links = pag.get("links", [])
    if not links:
        report.add("pagination.links", "WARNING",
                   "Pagination sin Links HATEOAS (first/next/prev/last)")
    else:
        rels = {l.get("rel") for l in links}
        for expected in ["first", "last"]:
            if expected not in rels:
                report.add(f"pagination.links.{expected}", "WARNING",
                           f"Falta Link HATEOAS rel='{expected}'")


def audit_viz_meta(report: AuditReport, viz: dict):
    if not viz:
        report.add("viz_ext", "WARNING", "VizMeta ausente — UI SDUI no puede renderizar sin contrato visual")
        return
    _check_message(report, "viz_ext", viz, "VizMeta")
    viz_type = viz.get("type", "")

    if "signal" in viz:
        signal = viz["signal"]
        _check_message(report, "viz_ext.signal", signal, "AnalyticalSignal")
        intel = signal.get("intelligence", {})
        if intel:
            _check_message(report, "viz_ext.signal.intelligence", intel, "IntelligenceSignal")
            dir_ = intel.get("direction", "")
            if dir_ not in ("up", "down", "neutral", "UP", "DOWN", "FLAT"):
                report.add("viz_ext.signal.intelligence.direction", "CRITICAL",
                           f"direction='{dir_}' inválido. Debe ser 'up'|'down'|'neutral'")

    elif "chart" in viz:
        _check_message(report, "viz_ext.chart", viz["chart"], "ChartDecoration")

    elif "table" in viz:
        table = viz["table"]
        _check_message(report, "viz_ext.table", table, "TableMeta")
        for i, col in enumerate(table.get("columns", [])):
            _check_message(report, f"viz_ext.table.columns[{i}]", col, "TableColumn")

    elif "breakdown" in viz:
        breakdown = viz.get("breakdown", {})
        signals = breakdown.get("signals", {})
        if not signals:
            report.add("viz_ext.breakdown.signals", "CRITICAL",
                       "BreakdownSignal.signals vacío — pie/donut sin datos")
        for key, sig in signals.items():
            _check_message(report, f"viz_ext.breakdown.signals.{key}", sig, "AnalyticalSignal")

    elif "tree" in viz:
        pass  # TreeMeta es válido aunque sin campos obligatorios requeridos extra


def audit_links(report: AuditReport, links: list):
    if not links:
        report.add("links", "INFO",
                   "QueryResponse sin Links HATEOAS de nivel raíz (recomendado para APIs discoverables)")
        return
    rels = {l.get("rel") for l in links}
    for expected_rel in ["self", "refresh"]:
        if expected_rel not in rels:
            report.add(f"links.{expected_rel}", "INFO",
                       f"Falta Link HATEOAS raíz rel='{expected_rel}'")
    for i, link in enumerate(links):
        _check_message(report, f"links[{i}]", link, "Link")


def audit_row_set(report: AuditReport, data: dict):
    if not data:
        report.add("data", "INFO", "RowSet vacío (puede ser válido para KPI puro)")
        return
    _check_message(report, "data", data, "RowSet")
    payload_variants = ["rows_json", "arrow_binary_blob", "presigned_csv_url"]
    found_variant = any(v in data for v in payload_variants)
    if not found_variant:
        report.add("data.payload_strategy", "CRITICAL",
                   "RowSet sin payload_strategy (rows_json/arrow/presigned). Datos sin transmitir.")


def audit_query_response(key: str, qr: dict) -> AuditReport:
    report = AuditReport(query_key=key, raw_response=qr)
    status = qr.get("status", {})
    audit_status(report, status)

    if not status.get("success"):
        # Error path — solo validamos status
        report.coverage_score = 0.0
        return report

    audit_metadata(report, qr.get("metadata", {}))
    audit_pagination(report, qr.get("pagination"), bool(qr.get("data")))
    audit_viz_meta(report, qr.get("vizExt") or qr.get("viz_ext") or {})
    audit_links(report, qr.get("links", []))
    audit_row_set(report, qr.get("data", {}))

    total = len(report.field_presence)
    ok = sum(1 for v in report.field_presence.values() if v)
    report.coverage_score = round(100 * ok / total, 1) if total else 100.0
    return report


# ─────────────────────────────────────────────────────────────────────────────
# gRPC-Web invocation
# ─────────────────────────────────────────────────────────────────────────────

def invoke_grpc_web(proto_req) -> Optional[dict]:
    proto_bytes = proto_req.SerializeToString()
    framed_data = struct.pack("!BI", 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()
    headers = {
        "Content-Type": "application/grpc-web+proto",
        "x-grpc-web": "1",
        "x-metri-origin-token": "antigravity-dev-test",
        "x-amz-content-sha256": payload_hash,
    }
    resp = requests.post(
        f"{FUNCTION_URL}metri.MetriService/Query",
        data=framed_data, headers=headers, stream=True, timeout=120
    )
    if resp.status_code != 200:
        logging.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
        return None

    buffer = b""
    responses = []
    for chunk in resp.iter_content(chunk_size=4096):
        buffer += chunk
        while len(buffer) >= 5:
            flags, length = struct.unpack("!BI", buffer[:5])
            if len(buffer) < 5 + length:
                break
            msg_bytes = buffer[5:5 + length]
            buffer = buffer[5 + length:]
            if flags == 0:
                proto_resp = metri_pb2.QueryResponse()
                proto_resp.ParseFromString(msg_bytes)
                responses.append(json_format.MessageToDict(proto_resp, preserving_proto_field_name=True))

    for r in reversed(responses):
        if r.get("batch_results"):
            return r
    return responses[-1] if responses else None


# ─────────────────────────────────────────────────────────────────────────────
# Test cases — different viz patterns
# ─────────────────────────────────────────────────────────────────────────────

def build_requests() -> List[tuple]:
    """Builds a set of representative queries covering all viz paths."""
    cases = []

    # 1. OLAP Indicator (KPI)
    req1 = metri_pb2.QueryRequest()
    req1.tenant_id = "golden-tenant"
    q1 = req1.queries["kpi_olap"]
    q1.tenant_id = "golden-tenant"
    q1.entity = "meter_reading"
    q1.output_cast = metri_pb2.KPI
    m = q1.metrics.add(); m.entity = "meter_reading"; m.attribute = "reading_value"; m.aggregation = metri_pb2.SUM
    q1.time_frame.type = metri_pb2.TimeFrameContext.ALL_TIME
    cases.append(("OLAP Indicator/KPI", req1))

    # 2. OLAP Timeseries
    req2 = metri_pb2.QueryRequest()
    req2.tenant_id = "golden-tenant"
    q2 = req2.queries["olap_timeseries"]
    q2.tenant_id = "golden-tenant"
    q2.entity = "meter_reading"
    q2.output_cast = metri_pb2.TIMESERIES; q2.viz = "line"
    m2 = q2.metrics.add(); m2.entity = "meter_reading"; m2.attribute = "reading_value"; m2.aggregation = metri_pb2.SUM
    d2 = q2.dimensions.add(); d2.entity = "meter_reading"; d2.attribute = "timestamp"; d2.interval = "day"
    q2.time_frame.type = metri_pb2.TimeFrameContext.ALL_TIME; q2.time_frame.n_value = 6
    q2.time_frame.timezone = "America/Bogota"
    cases.append(("OLAP Timeseries Line", req2))

    # 3. OLAP Pie
    req3 = metri_pb2.QueryRequest()
    req3.tenant_id = "golden-tenant"
    q3 = req3.queries["olap_pie"]
    q3.tenant_id = "golden-tenant"
    q3.entity = "meter_reading"
    q3.output_cast = metri_pb2.PIE; q3.viz = "pie"
    m3 = q3.metrics.add(); m3.entity = "meter_reading"; m3.attribute = "reading_value"; m3.aggregation = metri_pb2.SUM
    d3 = q3.dimensions.add(); d3.entity = "meter_reading"; d3.attribute = "unit_of_measure"
    q3.time_frame.type = metri_pb2.TimeFrameContext.ALL_TIME; q3.time_frame.n_value = 6
    q3.time_frame.timezone = "America/Bogota"
    cases.append(("OLAP Pie/Donut", req3))

    # 4. OLTP Table (paginated)
    req4 = metri_pb2.QueryRequest()
    req4.tenant_id = "golden-tenant"
    q4 = req4.queries["oltp_table"]
    q4.tenant_id = "golden-tenant"
    q4.entity = "asset"; q4.limit = 5
    cases.append(("OLTP Table (paginated)", req4))

    # 5. OLAP Scatter
    req5 = metri_pb2.QueryRequest()
    req5.tenant_id = "golden-tenant"
    q5 = req5.queries["olap_scatter"]
    q5.tenant_id = "golden-tenant"
    q5.entity = "meter_reading"; q5.output_cast = metri_pb2.BUBBLE; q5.viz = "scatter"
    m5a = q5.metrics.add(); m5a.entity = "meter_reading"; m5a.attribute = "reading_value"; m5a.aggregation = metri_pb2.AVG
    m5b = q5.metrics.add(); m5b.entity = "meter_reading"; m5b.attribute = "reading_value"; m5b.aggregation = metri_pb2.MAX
    d5 = q5.dimensions.add(); d5.entity = "meter_reading"; d5.attribute = "timestamp"
    q5.time_frame.type = metri_pb2.TimeFrameContext.ALL_TIME; q5.time_frame.n_value = 3
    q5.time_frame.timezone = "America/Bogota"
    cases.append(("OLAP Scatter/Bubble", req5))

    # 6. Comparisons (Time Shift)
    req6 = metri_pb2.QueryRequest()
    req6.tenant_id = "golden-tenant"
    q6 = req6.queries["olap_comparison"]
    q6.tenant_id = "golden-tenant"
    q6.entity = "meter_reading"
    m6 = q6.metrics.add(); m6.entity = "meter_reading"; m6.attribute = "reading_value"; m6.aggregation = metri_pb2.SUM
    q6.time_frame.type = metri_pb2.TimeFrameContext.ALL_TIME
    comp = q6.comparisons.add()
    comp.type = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    comp.shortcut = metri_pb2.AnalyticalComparison.PREVIOUS_PERIOD
    cases.append(("Comparison: PREVIOUS_PERIOD", req6))

    # 7. Aggregation Functions
    req7 = metri_pb2.QueryRequest()
    req7.tenant_id = "golden-tenant"
    q7 = req7.queries["olap_agg"]
    q7.tenant_id = "golden-tenant"
    q7.entity = "meter_reading"
    for agg in [metri_pb2.SUM, metri_pb2.AVG, metri_pb2.COUNT, metri_pb2.MAX, metri_pb2.MIN]:
        m = q7.metrics.add(); m.entity = "meter_reading"; m.attribute = "reading_value"; m.aggregation = agg
    q7.time_frame.type = metri_pb2.TimeFrameContext.ALL_TIME
    cases.append(("Aggregation Functions", req7))

    # 8. Filter Operators
    req8 = metri_pb2.QueryRequest()
    req8.tenant_id = "golden-tenant"
    q8 = req8.queries["olap_filter"]
    q8.tenant_id = "golden-tenant"
    q8.entity = "meter_reading"
    m8 = q8.metrics.add(); m8.entity = "meter_reading"; m8.attribute = "reading_value"; m8.aggregation = metri_pb2.SUM
    f8 = q8.filters.add().criteria
    f8.field = "reading_value"
    f8.op_ref = metri_pb2.FilterOperator.GT
    f8.value.number_val = 100.0
    cases.append(("Filter Operator GT", req8))

    # 9. Search (Omnisearch)
    req9 = metri_pb2.QueryRequest()
    req9.tenant_id = "golden-tenant"
    q9 = req9.queries["oltp_search"]
    q9.tenant_id = "golden-tenant"
    q9.entity = "asset"
    q9.search = "Asset 1"
    q9.limit = 5
    cases.append(("Search Omnisearch", req9))

    return cases


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run_inspection():
    logging.info("=" * 65)
    logging.info("METRES CONTRACT INSPECTOR — Análisis de Cobertura QueryResponse")
    logging.info("=" * 65)

    all_reports: List[AuditReport] = []
    cases = build_requests()

    for label, req in cases:
        logging.info(f"\n▶  {label}")
        t0 = time.time()
        result = invoke_grpc_web(req)
        elapsed = round((time.time() - t0) * 1000)

        if not result:
            logging.error(f"   ✗ Sin respuesta del motor ({elapsed}ms)")
            continue

        batch = result.get("batch_results") or result.get("batchResults", {})
        if not batch:
            # Flat response
            batch = {list(req.queries.keys())[0]: result}

        for qk, qr in batch.items():
            report = audit_query_response(qk, qr)
            all_reports.append(report)
            score_icon = "✅" if report.coverage_score >= 90 else ("⚠️" if report.coverage_score >= 60 else "❌")
            logging.info(f"   {score_icon} [{qk}] Cobertura: {report.coverage_score}% | {elapsed}ms")

            criticals = [v for v in report.violations if v.severity == "CRITICAL"]
            warnings  = [v for v in report.violations if v.severity == "WARNING"]
            infos     = [v for v in report.violations if v.severity == "INFO"]

            for v in criticals:
                logging.error(f"     ❌ CRITICAL [{v.path}]: {v.message}")
            for v in warnings:
                logging.warning(f"     ⚠️  WARNING [{v.path}]: {v.message}")
            for v in infos:
                logging.info(f"     ℹ️  INFO [{v.path}]: {v.message}")

    # ── Summary ───────────────────────────────────────────────────────────────
    logging.info("\n" + "=" * 65)
    logging.info("RESUMEN GLOBAL")
    logging.info("=" * 65)
    if not all_reports:
        logging.error("Sin reportes — verifique conectividad o seedding de datos")
        return

    avg_cov = round(sum(r.coverage_score for r in all_reports) / len(all_reports), 1)
    total_critical = sum(len([v for v in r.violations if v.severity == "CRITICAL"]) for r in all_reports)
    total_warnings = sum(len([v for v in r.violations if v.severity == "WARNING"]) for r in all_reports)

    logging.info(f"Consultas auditadas  : {len(all_reports)}")
    logging.info(f"Cobertura promedio   : {avg_cov}%")
    logging.info(f"Violaciones CRITICAL : {total_critical}")
    logging.info(f"Violaciones WARNING  : {total_warnings}")

    # JSON report
    report_data = {
        "summary": {
            "average_coverage": avg_cov,
            "total_audited": len(all_reports),
            "critical_violations": total_critical,
            "warning_violations": total_warnings,
        },
        "reports": [
            {
                "query_key": r.query_key,
                "coverage_score": r.coverage_score,
                "violations": [{"path": v.path, "severity": v.severity, "message": v.message} for v in r.violations],
                "field_presence": r.field_presence,
                "raw_response": r.raw_response,
            }
            for r in all_reports
        ]
    }
    out_path = os.path.join(os.path.dirname(__file__), "contract_inspection_report.json")
    with open(out_path, "w") as f:
        json.dump(report_data, f, indent=2)
    logging.info(f"\nInforme JSON guardado en: {out_path}")


if __name__ == "__main__":
    run_inspection()
