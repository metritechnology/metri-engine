"""
test_kpi_indicator_suite.py
============================
Suite de validación OLAP para los viz KPI e Indicator con:
  - time_frame (LAST_N_DAYS, THIS_MONTH, ALL_TIME, CUSTOM_RANGE)
  - comparisons (PREVIOUS_PERIOD, SAME_PERIOD_LAST_YEAR)
  - IntelligenceSignal (direction, percentage, delta_abs, previous_value)

Soporta LOCAL (gRPC insecure) y PRODUCCIÓN (gRPC-Web HTTPS).

Uso:
  python test_kpi_indicator_suite.py           # → producción
  python test_kpi_indicator_suite.py --local   # → localhost:50051
  python test_kpi_indicator_suite.py --local --port 9090
"""

import sys
import json
import struct
import hashlib
import logging
import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import metri_pb2

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ─── Configuración ────────────────────────────────────────────────────────────

PROD_URL   = "https://engine.metri.one/"
PROD_TOKEN = "datalog-golden-tenant"
TENANT_ID  = "golden-tenant"

REPORT_FILE = Path(__file__).parent / "kpi_indicator_suite_report.json"


# ─── Transport Helpers ────────────────────────────────────────────────────────

def invoke_grpc_native(req: metri_pb2.QueryRequest, host: str, port: int) -> list:
    """gRPC nativo (local dev). Requiere grpcio."""
    import grpc
    from google.protobuf.json_format import MessageToDict

    target = f"{host}:{port}"
    log.info(f"[LOCAL] Conectando a {target}...")
    with grpc.insecure_channel(target) as channel:
        from metri_pb2_grpc import MetriServiceStub
        stub = MetriServiceStub(channel)
        results = []
        for resp in stub.Query(req):
            results.append(MessageToDict(resp, preserving_proto_field_name=True))
        return results


def invoke_grpc_web(req: metri_pb2.QueryRequest, base_url: str, token: str,
                    retries: int = 3, backoff: int = 10) -> list:
    """gRPC-Web sobre HTTPS (producción)."""
    from google.protobuf.json_format import MessageToDict

    proto_bytes  = req.SerializeToString()
    framed_data  = struct.pack("!BI", 0, len(proto_bytes)) + proto_bytes
    payload_hash = hashlib.sha256(framed_data).hexdigest()

    endpoint = f"{base_url}metri.MetriService/Query"
    headers  = {
        "Content-Type":         "application/grpc-web+proto",
        "X-Metri-Origin-Token": token,
        "x-amz-content-sha256": payload_hash,
    }

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(endpoint, data=framed_data, headers=headers, timeout=90)
            if resp.status_code in (502, 503, 504):
                log.warning(f"  [{attempt}/{retries}] HTTP {resp.status_code}. Retry en {backoff}s...")
                time.sleep(backoff)
                continue
            if resp.status_code != 200:
                log.error(f"HTTP {resp.status_code}: {resp.text[:300]}")
                return []

            rb, idx, frames = resp.content, 0, []
            while idx + 5 <= len(rb):
                flag, length = struct.unpack("!BI", rb[idx:idx + 5])
                idx += 5
                if flag == 0x00:
                    frames.append(rb[idx:idx + length])
                idx += length

            results = []
            for frame in frames:
                r = metri_pb2.QueryResponse()
                r.ParseFromString(frame)
                results.append(MessageToDict(r, preserving_proto_field_name=True))
            return results

        except requests.exceptions.Timeout:
            log.warning(f"  [{attempt}/{retries}] Timeout. Retry en {backoff}s...")
            time.sleep(backoff)

    log.error("Máximo de reintentos alcanzado.")
    return []


# ─── Builders ─────────────────────────────────────────────────────────────────

def _base_kpi(tenant: str, entity: str = "meter_reading",
              attribute: str = "reading_value",
              agg=None) -> metri_pb2.AnalyticsRequest:
    if agg is None:
        agg = metri_pb2.AVG
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = tenant
    q.entity      = entity
    q.output_cast = metri_pb2.KPI
    q.viz         = "kpi"
    m = q.metrics.add()
    m.attribute   = attribute
    m.aggregation = agg
    return q


def _base_indicator(tenant: str, entity: str = "meter_reading",
                    attribute: str = "reading_value") -> metri_pb2.AnalyticsRequest:
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = tenant
    q.entity      = entity
    q.output_cast = metri_pb2.KPI   # el motor usa output_cast=KPI + viz=indicator
    q.viz         = "indicator"
    m = q.metrics.add()
    m.attribute   = attribute
    m.aggregation = metri_pb2.AVG
    return q


def build_all_queries(tenant: str) -> metri_pb2.QueryRequest:
    req = metri_pb2.QueryRequest()
    req.tenant_id = tenant

    # ── 1. KPI sin time_frame (ALL_TIME baseline) ────────────────────────────
    q = _base_kpi(tenant, agg=metri_pb2.SUM)
    q.time_frame.type = metri_pb2.TimeFrameContext.ALL_TIME
    req.queries["kpi_all_time"].CopyFrom(q)

    # ── 2. KPI LAST_N_DAYS = 30 ───────────────────────────────────────────────
    q = _base_kpi(tenant, agg=metri_pb2.AVG)
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30
    req.queries["kpi_last_30d"].CopyFrom(q)

    # ── 3. KPI THIS_MONTH ─────────────────────────────────────────────────────
    q = _base_kpi(tenant, agg=metri_pb2.COUNT)
    q.time_frame.type     = metri_pb2.TimeFrameContext.THIS_MONTH
    q.time_frame.timezone = "America/Bogota"
    req.queries["kpi_this_month"].CopyFrom(q)

    # ── 4. KPI LAST_N_MONTHS = 3, múltiples métricas ─────────────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = tenant
    q.entity      = "meter_reading"
    q.output_cast = metri_pb2.KPI
    q.viz         = "kpi"
    for agg_fn, alias in [(metri_pb2.SUM, "sum_rv"), (metri_pb2.AVG, "avg_rv"), (metri_pb2.MIN, "min_rv"), (metri_pb2.MAX, "max_rv")]:
        m = q.metrics.add()
        m.attribute   = "reading_value"
        m.aggregation = agg_fn
        m.name        = alias
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_MONTHS
    q.time_frame.n_value = 3
    req.queries["kpi_last_3months_multi"].CopyFrom(q)

    # ── 5. KPI CUSTOM_RANGE (últimos 60 días en epoch ms) ────────────────────
    now_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - (60 * 86400 * 1000)
    q = _base_kpi(tenant, agg=metri_pb2.SUM)
    q.time_frame.type     = metri_pb2.TimeFrameContext.CUSTOM_RANGE
    q.time_frame.start_ts = start_ms
    q.time_frame.end_ts   = now_ms
    req.queries["kpi_custom_60d"].CopyFrom(q)

    # ── 6. INDICATOR + PREVIOUS_PERIOD comparison ─────────────────────────────
    q = _base_indicator(tenant)
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30
    cmp = q.comparisons.add()
    cmp.type     = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    cmp.shortcut = metri_pb2.AnalyticalComparison.PREVIOUS_PERIOD
    cmp.label    = "vs Período Anterior"
    req.queries["indicator_prev_period"].CopyFrom(q)

    # ── 7. INDICATOR + SAME_PERIOD_LAST_YEAR ─────────────────────────────────
    q = _base_indicator(tenant)
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30
    cmp = q.comparisons.add()
    cmp.type     = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    cmp.shortcut = metri_pb2.AnalyticalComparison.SAME_PERIOD_LAST_YEAR
    cmp.label    = "vs Mismo Período Año Anterior"
    req.queries["indicator_yoy"].CopyFrom(q)

    # ── 8. INDICATOR + TIME_SHIFT_RELATIVE (-1 mes) ───────────────────────────
    q = _base_indicator(tenant)
    q.time_frame.type    = metri_pb2.TimeFrameContext.THIS_MONTH
    cmp = q.comparisons.add()
    cmp.type                 = metri_pb2.AnalyticalComparison.TIME_SHIFT_RELATIVE
    cmp.relative_granularity = "month"
    cmp.relative_amount      = -1
    cmp.label                = "vs Mes Anterior"
    req.queries["indicator_relative_month"].CopyFrom(q)

    # ── 9. INDICATOR + BENCHMARK ──────────────────────────────────────────────
    q = _base_indicator(tenant)
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30    # avg_30d=150, benchmark=120 -> up +25%
    cmp = q.comparisons.add()
    cmp.type            = metri_pb2.AnalyticalComparison.BENCHMARK
    cmp.benchmark_value = 120.0
    cmp.label           = "vs Benchmark 120"
    req.queries["indicator_benchmark"].CopyFrom(q)

    # ── 10. INDICATOR + LAST_N_DAYS=30 (KPI viz de control sin comparación) ──
    q = _base_indicator(tenant)
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30
    req.queries["indicator_no_cmp"].CopyFrom(q)

    # ── 11. OLTP KPI ALL_TIME ─────────────────────────────────────────────────
    q = _base_kpi(tenant, entity="inventory_movement",
                  attribute="quantity", agg=metri_pb2.SUM)
    q.time_frame.type = metri_pb2.TimeFrameContext.ALL_TIME
    req.queries["oltp_kpi_all_time"].CopyFrom(q)

    # ── 12. OLTP KPI LAST_N_DAYS = 30 ────────────────────────────────────────
    q = _base_kpi(tenant, entity="inventory_movement",
                  attribute="quantity", agg=metri_pb2.AVG)
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30
    req.queries["oltp_kpi_last_30d"].CopyFrom(q)

    # ── 13. OLTP KPI THIS_MONTH ───────────────────────────────────────────────
    q = _base_kpi(tenant, entity="inventory_movement",
                  attribute="quantity", agg=metri_pb2.COUNT)
    q.time_frame.type     = metri_pb2.TimeFrameContext.THIS_MONTH
    q.time_frame.timezone = "America/Bogota"
    req.queries["oltp_kpi_this_month"].CopyFrom(q)

    # ── 14. OLTP KPI LAST_N_MONTHS = 3, múltiples métricas ───────────────────
    q = metri_pb2.AnalyticsRequest()
    q.tenant_id   = tenant
    q.entity      = "inventory_movement"
    q.output_cast = metri_pb2.KPI
    q.viz         = "kpi"
    for agg_fn, alias in [(metri_pb2.SUM, "sum_qty"), (metri_pb2.AVG, "avg_qty"),
                          (metri_pb2.MIN, "min_qty"), (metri_pb2.MAX, "max_qty")]:
        m = q.metrics.add()
        m.attribute   = "quantity"
        m.aggregation = agg_fn
        m.name        = alias
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_MONTHS
    q.time_frame.n_value = 3
    req.queries["oltp_kpi_last_3months_multi"].CopyFrom(q)

    # ── 15. OLTP KPI CUSTOM_RANGE (60 días) ──────────────────────────────────
    now_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - (60 * 86400 * 1000)
    q = _base_kpi(tenant, entity="inventory_movement",
                  attribute="quantity", agg=metri_pb2.SUM)
    q.time_frame.type     = metri_pb2.TimeFrameContext.CUSTOM_RANGE
    q.time_frame.start_ts = start_ms
    q.time_frame.end_ts   = now_ms
    req.queries["oltp_kpi_custom_60d"].CopyFrom(q)

    # ── 16. OLTP INDICATOR + PREVIOUS_PERIOD ─────────────────────────────────
    q = _base_indicator(tenant, entity="inventory_movement", attribute="quantity")
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30
    cmp = q.comparisons.add()
    cmp.type     = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    cmp.shortcut = metri_pb2.AnalyticalComparison.PREVIOUS_PERIOD
    cmp.label    = "vs Período Anterior"
    req.queries["oltp_indicator_prev_period"].CopyFrom(q)

    # ── 17. OLTP INDICATOR + SAME_PERIOD_LAST_YEAR ───────────────────────────
    q = _base_indicator(tenant, entity="inventory_movement", attribute="quantity")
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30
    cmp = q.comparisons.add()
    cmp.type     = metri_pb2.AnalyticalComparison.TIME_SHIFT_SHORTCUT
    cmp.shortcut = metri_pb2.AnalyticalComparison.SAME_PERIOD_LAST_YEAR
    cmp.label    = "vs Mismo Período Año Anterior"
    req.queries["oltp_indicator_yoy"].CopyFrom(q)

    # ── 18. OLTP INDICATOR + TIME_SHIFT_RELATIVE (-1 mes) ────────────────────
    q = _base_indicator(tenant, entity="inventory_movement", attribute="quantity")
    q.time_frame.type    = metri_pb2.TimeFrameContext.THIS_MONTH
    cmp = q.comparisons.add()
    cmp.type                 = metri_pb2.AnalyticalComparison.TIME_SHIFT_RELATIVE
    cmp.relative_granularity = "month"
    cmp.relative_amount      = -1
    cmp.label                = "vs Mes Anterior"
    req.queries["oltp_indicator_relative_month"].CopyFrom(q)

    # ── 19. OLTP INDICATOR + BENCHMARK ───────────────────────────────────────
    q = _base_indicator(tenant, entity="inventory_movement", attribute="quantity")
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30    # avg_30d=150, benchmark=100 -> up +50%
    cmp = q.comparisons.add()
    cmp.type            = metri_pb2.AnalyticalComparison.BENCHMARK
    cmp.benchmark_value = 100.0
    cmp.label           = "vs Benchmark 100"
    req.queries["oltp_indicator_benchmark"].CopyFrom(q)

    # ── 20. OLTP INDICATOR sin comparación ───────────────────────────────────
    q = _base_indicator(tenant, entity="inventory_movement", attribute="quantity")
    q.time_frame.type    = metri_pb2.TimeFrameContext.LAST_N_DAYS
    q.time_frame.n_value = 30
    req.queries["oltp_indicator_no_cmp"].CopyFrom(q)

    return req



# ─── Validación de IntelligenceSignal ─────────────────────────────────────────

def _check_intelligence(signal_dict: dict | None, query_key: str) -> list:
    """
    Valida que IntelligenceSignal esté presente y bien formado.
    Retorna lista de issues (vacía = OK).

    NOTA proto3: MessageToDict omite campos con valor DEFAULT (string "", number 0.0).
    Por eso usamos la presencia del objeto :intelligence en el dict del signal como
    indicador de que el engine lo calculó. Si el sub-dict existe pero está vacío de
    campos clave, se considera "vacío" (engine no calculó delta).
    """
    issues = []
    if not signal_dict:
        issues.append("⚠️  intelligence es None/ausente")
        return issues

    # direction acepta tanto mayúsculas (translator) como minúsculas (normalizer)
    # MessageToDict omite direction si es "" (proto3 default string) → None aquí
    direction = signal_dict.get("direction", "")
    valid_dirs = ("up", "down", "neutral", "UP", "DOWN", "FLAT")
    if direction and direction not in valid_dirs:
        issues.append(f"⚠️  direction inválida: '{direction}'")
    elif not direction:
        issues.append("⚠️  direction vacía (engine no calculó el signal)")

    # percentage: MessageToDict omite 0.0 → None aquí, pero 0.0 es válido
    # Sólo es problema si direction también está vacía (signal no calculado)
    pct = signal_dict.get("percentage")
    if pct is None and not direction:
        issues.append("⚠️  percentage ausente")

    # previous_value: MessageToDict omite 0.0, pero si hay direction es válido
    prev = signal_dict.get("previous_value")
    if prev is None and direction:
        # El engine calculó direction, así que previous_value probablemente es 0.0
        # (proto3 default omitido por MessageToDict) — no es error
        pass
    elif prev is None and not direction:
        issues.append("⚠️  previous_value ausente")

    # label: MessageToDict lo omite si es "" — no crítico si direction está presente
    label = signal_dict.get("label", "")
    if not label and not direction:
        issues.append("⚠️  label del signal vacío")

    return issues


# ─── Análisis de resultados ───────────────────────────────────────────────────

# Queries que deberían llevar IntelligenceSignal (tienen comparison o son indicator)
EXPECTS_INTELLIGENCE = {
    # OLAP
    "indicator_prev_period",
    "indicator_yoy",
    "indicator_relative_month",
    "indicator_benchmark",
    # OLTP
    "oltp_indicator_prev_period",
    "oltp_indicator_yoy",
    "oltp_indicator_relative_month",
    "oltp_indicator_benchmark",
}

QUERY_ORDER = [
    # ── OLAP (10) ──────────────────────────────────────────────────────────
    "kpi_all_time",
    "kpi_last_30d",
    "kpi_this_month",
    "kpi_last_3months_multi",
    "kpi_custom_60d",
    "indicator_prev_period",
    "indicator_yoy",
    "indicator_relative_month",
    "indicator_benchmark",
    "indicator_no_cmp",
    # ── OLTP (10) ──────────────────────────────────────────────────────────
    "oltp_kpi_all_time",
    "oltp_kpi_last_30d",
    "oltp_kpi_this_month",
    "oltp_kpi_last_3months_multi",
    "oltp_kpi_custom_60d",
    "oltp_indicator_prev_period",
    "oltp_indicator_yoy",
    "oltp_indicator_relative_month",
    "oltp_indicator_benchmark",
    "oltp_indicator_no_cmp",
]


def analyse(raw_frames: list) -> dict:
    """Combina todos los frames de QueryResponse en un único batch_results dict."""
    combined = {}
    for frame in raw_frames:
        if "batch_results" in frame:
            combined.update(frame["batch_results"])
        # Respuesta unitaria (no batch)
        elif frame.get("status", {}).get("success"):
            combined["_single"] = frame
    return combined


def validate_result(key: str, result: dict) -> dict:
    """
    Valida un resultado individual.
    Retorna un dict con el análisis completo.
    """
    status    = result.get("status", {})
    success   = status.get("success", False)
    viz_ext   = result.get("viz_ext", {})
    data      = result.get("data", {})
    columns   = data.get("columns", [])
    rows_iter = data.get("rows_json", {}).get("iter", [])
    metadata  = result.get("metadata", {})

    # ── IntelligenceSignal ────────────────────────────────────────────────────
    # Proto structure: viz_ext.signal.intelligence (nested AnalyticalSignal → IntelligenceSignal)
    # MessageToDict serializa: viz_ext["signal"]["intelligence"]["direction"]
    signal_wrapper = viz_ext.get("signal") if isinstance(viz_ext, dict) else None
    # El signal puede tener intelligence como sub-dict, o los campos directamente
    intelligence_dict = None
    if isinstance(signal_wrapper, dict):
        intelligence_dict = signal_wrapper.get("intelligence")
        if not intelligence_dict:
            # Fallback: algunos engines ponen direction/percentage directo en signal
            if signal_wrapper.get("direction"):
                intelligence_dict = signal_wrapper
    intelligence_issues = []
    if key in EXPECTS_INTELLIGENCE:
        intelligence_issues = _check_intelligence(intelligence_dict, key)

    # ── Validaciones generales ────────────────────────────────────────────────
    issues = []
    if not success:
        issues.append(f"❌ status.success=false: {status.get('error_message','?')}")
    if not viz_ext:
        issues.append("❌ viz_ext vacío")
    if not columns:
        issues.append("❌ data.columns vacío → el frontend no puede renderizar")
    if not rows_iter:
        issues.append("❌ rows_json.iter vacío → resultado sin datos (posible data gap)")

    issues.extend(intelligence_issues)

    ok = success and bool(viz_ext) and bool(columns) and bool(rows_iter) and not intelligence_issues

    # Sample rows (primeras 3)
    sample = []
    col_keys = [c.get("key", str(c)) if isinstance(c, dict) else str(c) for c in columns]
    for row in rows_iter[:3]:
        vals = []
        if isinstance(row, dict):
            for v in row.get("values", []):
                vals.append(v.get("string_value") or v.get("number_value") if isinstance(v, dict) else v)
        elif isinstance(row, (list, tuple)):
            vals = list(row)
        sample.append({col_keys[i]: vals[i] for i in range(min(len(col_keys), len(vals)))})

    return {
        "status":       "✅ OK" if ok else "❌ FAIL",
        "ok":           ok,
        "success":      success,
        "viz_type":     viz_ext.get("type") if isinstance(viz_ext, dict) else None,
        "columns":      col_keys,
        "row_count":    len(rows_iter),
        "engine":       metadata.get("engine"),
        "exec_ms":      metadata.get("execution_time_ms"),
        "signal":       signal_wrapper,
        "intelligence": intelligence_dict,
        "issues":       issues,
        "sample_rows":  sample,
    }


# ─── Reporte ──────────────────────────────────────────────────────────────────

def print_report(combined: dict, env: str):
    width = 62
    print(f"\n{'═' * width}")
    print(f"  KPI / INDICATOR SUITE — {env}")
    print(f"{'═' * width}")

    ok_total = fail_total = 0
    for key in QUERY_ORDER:
        if key not in combined:
            print(f"\n❌ [{key}] AUSENTE en batch_results")
            fail_total += 1
            continue

        v = validate_result(key, combined[key])
        tag = v["status"]
        if v["ok"]:
            ok_total += 1
        else:
            fail_total += 1

        print(f"\n{tag} [{key.upper()}]")
        print(f"   viz_type : {v['viz_type']}")
        print(f"   engine   : {v['engine']}  |  exec_ms: {v['exec_ms']}")
        print(f"   columns  : {v['columns']}")
        print(f"   rows     : {v['row_count']}")

        if v["intelligence"]:
            sig = v["intelligence"]
            print(f"   🧠 IntelligenceSignal:")
            print(f"      direction      = {sig.get('direction')}")
            print(f"      percentage     = {sig.get('percentage')}")
            print(f"      delta_abs      = {sig.get('delta_abs')}")
            print(f"      previous_value = {sig.get('previous_value')}")
            print(f"      label          = {sig.get('label')}")
            print(f"      is_anomaly     = {sig.get('is_anomaly', False)}")
            print(f"      z_score        = {sig.get('z_score', 0)}")
        elif v.get("signal"):
            sig = v["signal"]
            print(f"   📊 signal (sin intelligence anidado):")
            print(f"      value          = {sig.get('value')}")
            print(f"      previous_value = {sig.get('previous_value')}")
        elif key in EXPECTS_INTELLIGENCE:
            print(f"   🧠 IntelligenceSignal: ⚠️  AUSENTE (se esperaba)")

        if v["issues"]:
            for issue in v["issues"]:
                print(f"   {issue}")

        if v["sample_rows"]:
            print(f"   sample_rows[0]: {v['sample_rows'][0]}")

    print(f"\n{'═' * width}")
    print(f"  TOTAL: {ok_total}/{ok_total + fail_total} OK  |  {fail_total} FALLIDOS")
    print(f"{'═' * width}\n")

    return ok_total, fail_total


def save_report(combined: dict, env: str, ok: int, fail: int):
    report_entries = []
    for key in QUERY_ORDER:
        result = combined.get(key)
        if result:
            v = validate_result(key, result)
        else:
            v = {"status": "❌ AUSENTE", "ok": False, "issues": ["ausente en batch_results"],
                 "columns": [], "row_count": 0, "signal": None, "intelligence": None,
                 "sample_rows": [], "viz_type": None, "engine": None, "exec_ms": None, "success": False}
        report_entries.append({
            "query_key":          key,
            "expects_signal":     key in EXPECTS_INTELLIGENCE,
            **{k: v[k] for k in ("status", "ok", "success", "viz_type", "columns",
                                  "row_count", "engine", "exec_ms",
                                  "signal", "intelligence", "issues", "sample_rows")},
        })

    report = {
        "meta": {
            "suite":        "kpi_indicator_suite",
            "description":  "Validación KPI/Indicator con time_frame, comparisons e IntelligenceSignal",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "environment":  env,
            "tenant_id":    TENANT_ID,
        },
        "summary": {
            "total":   ok + fail,
            "passed":  ok,
            "failed":  fail,
            "pass_pct": round(ok / (ok + fail) * 100, 1) if (ok + fail) else 0,
            "intelligence_queries": len(EXPECTS_INTELLIGENCE),
        },
        "queries": report_entries,
    }

    REPORT_FILE.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    log.info(f"📄 Reporte guardado en: {REPORT_FILE}")
    print(f"\n📄 Reporte JSON → {REPORT_FILE}\n")


# ─── Entry point ──────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="KPI/Indicator viz test suite")
    parser.add_argument("--local",  action="store_true", help="Usar gRPC nativo en localhost")
    parser.add_argument("--host",   default="localhost",  help="Host del servidor local")
    parser.add_argument("--port",   type=int, default=50051, help="Puerto gRPC local")
    parser.add_argument("--tenant", default=TENANT_ID, help="Tenant ID a utilizar")
    return parser.parse_args()


def main():
    args  = parse_args()
    env   = f"LOCAL ({args.host}:{args.port})" if args.local else "PRODUCCIÓN"
    log.info(f"=== KPI/Indicator Suite | Entorno: {env} ===")

    req = build_all_queries(args.tenant)

    if args.local:
        raw = invoke_grpc_native(req, args.host, args.port)
    else:
        raw = invoke_grpc_web(req, PROD_URL, PROD_TOKEN)

    if not raw:
        log.error("No se recibió ningún frame del engine. Abortando.")
        sys.exit(1)

    combined = analyse(raw)
    ok, fail = print_report(combined, env)
    save_report(combined, env, ok, fail)

    sys.exit(0 if fail == 0 else 1)


if __name__ == "__main__":
    main()
