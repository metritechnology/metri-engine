#!/usr/bin/env python3
"""
Metri Engine — Integration E2E Test Suite
Tests: KPI + comparison/time_frame, PIE, LINE
Requiere: grpcurl instalado, engine corriendo en :9090
"""

import subprocess
import json
import sys
import time
import random
import hmac
import hashlib
import base64
import uuid
from datetime import datetime, timedelta

PROTO_DIR  = "/Users/macuser/projects/metri/metri-engine"
PROTO_FILE = "metri.proto"
ENDPOINT   = "localhost:9090"
TENANT     = "demo"

GREEN = "\033[92m"
RED   = "\033[91m"
YELLOW= "\033[93m"
BLUE  = "\033[94m"
RESET = "\033[0m"
BOLD  = "\033[1m"

passed = []
failed = []

# Secreto local estático alineado con docker-compose.yml
HMAC_SECRET = "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"

def generate_signed_token(secret: str, tenant_id: str, user_id: str, ttl_seconds: int = 3600) -> str:
    """Genera un token de sesión firmado localmente con HMAC-SHA256."""
    now = int(time.time())
    claims = {
        "tid": tenant_id,
        "uid": user_id,
        "iat": now,
        "exp": now + ttl_seconds,
        "jti": str(uuid.uuid4())
    }
    payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode('utf-8').rstrip('=')
    
    mac = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256)
    sig_bytes = mac.digest()
    sig_b64 = base64.urlsafe_b64encode(sig_bytes).decode('utf-8').rstrip('=')
    
    return f"mk_{payload_b64}.{sig_b64}"

# Token de sesión del sistema para autenticar todas las peticiones gRPC en el test suite
TOKEN = generate_signed_token(HMAC_SECRET, TENANT, "usr_system_bff")

def grpc(service: str, payload: dict) -> dict:
    """Ejecuta una llamada gRPC y retorna el JSON parseado con la cabecera de autenticación."""
    cmd = [
        "grpcurl", "-plaintext",
        "-import-path", PROTO_DIR,
        "-proto", PROTO_FILE,
        "-rpc-header", f"sid: {TOKEN}",
        "-d", json.dumps(payload),
        ENDPOINT,
        f"metri.MetriService/{service}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"gRPC {service} falló: {result.stderr[:300]}")
    raw = result.stdout.strip()
    if not raw:
        return {}
    # grpcurl puede retornar múltiples JSON objects en streaming — parsear todos
    objects = []
    decoder = json.JSONDecoder()
    idx = 0
    while idx < len(raw):
        # Saltar whitespace
        while idx < len(raw) and raw[idx] in ' \t\n\r':
            idx += 1
        if idx >= len(raw):
            break
        try:
            obj, end_idx = decoder.raw_decode(raw, idx)
            objects.append(obj)
            idx = idx + end_idx - idx  # avanzar end_idx relativo a raw
            # raw_decode retorna end_idx como posición absoluta en raw
            idx = end_idx
        except json.JSONDecodeError:
            break
    if not objects:
        return {}
    if len(objects) == 1:
        return objects[0]
    # Merge múltiples objetos streaming (batch_results se acumula)
    merged = {}
    for o in objects:
        if isinstance(o, dict):
            # Merge inteligente de batchResults
            for k, v in o.items():
                if k == "batchResults" and isinstance(v, dict) and k in merged:
                    merged[k].update(v)
                else:
                    merged[k] = v
    return merged


def assert_ok(name: str, cond: bool, detail: str = ""):
    if cond:
        print(f"  {GREEN}✓{RESET} {name}")
        passed.append(name)
    else:
        print(f"  {RED}✗{RESET} {name}: {detail}")
        failed.append(f"{name}: {detail}")


def transact(entity: str, action: str, payload: dict) -> dict:
    return grpc("Transact", {
        "tenant_id":   TENANT,
        "entity_type": entity,
        "action":      action,
        "payload":     payload,
    })


def query(queries: dict) -> dict:
    return grpc("Query", {"tenant_id": TENANT, "queries": queries})


# ─────────────────────────────────────────────────────────────────────────────
# FASE 1: Seed data
# ─────────────────────────────────────────────────────────────────────────────

def seed_data():
    print(f"\n{BOLD}{BLUE}═══ FASE 1: Sembrando datos de prueba ═══{RESET}")
    areas   = ["Mecánica", "Eléctrica", "Hidráulica", "Neumática", "Civil"]
    now_ts  = int(time.time())
    prev_ts = now_ts - (30 * 24 * 3600)  # 30 días atrás

    asset_ids = []
    for i in range(20):
        area  = areas[i % len(areas)]
        ts    = prev_ts + random.randint(0, 30 * 24 * 3600)
        value = random.uniform(50_000, 500_000)
        r = transact("asset", "CREATE", {
            "name":          f"Activo-E2E-{i:03d}",
            "area":          area,
            "area_value":    round(value, 2),
            "status":        random.choice(["active", "inactive", "maintenance"]),
            "timestamp":     ts,
            "created_at":    ts,
            "purchase_date": ts,
            "health_score":  round(random.uniform(35.0, 98.0), 1),
            "criticality":   random.choice(["A", "B", "C"]),
        })
        eid = r.get("entityId", "")
        if eid:
            asset_ids.append(eid)

    print(f"  → {len(asset_ids)} assets creados")

    # Work orders: mitad del período anterior, mitad del actual
    wo_ids = []
    for i in range(15):
        is_old = i < 7
        ts = prev_ts + random.randint(0, 14 * 24 * 3600) if is_old else \
             now_ts  - random.randint(0, 14 * 24 * 3600)
        r = transact("work_order", "CREATE", {
            "title":      f"OT-E2E-{i:03d}",
            "status":     random.choice(["open", "in_progress", "closed"]),
            "priority":   random.choice(["low", "medium", "high", "critical"]),
            "created_at": ts,
            "cost":       round(random.uniform(500, 15000), 2),
        })
        eid = r.get("entityId", "")
        if eid:
            wo_ids.append(eid)

    print(f"  → {len(wo_ids)} work_orders creados")

    # Labor logs
    for i in range(10):
        ts = now_ts - random.randint(0, 7 * 24 * 3600)
        transact("labor_log", "CREATE", {
            "technician":   f"Tech-{i % 4:02d}",
            "hours":        round(random.uniform(1, 8), 1),
            "labor_cost":   round(random.uniform(100, 1200), 2),
            "area":         areas[i % len(areas)],
            "created_at":   ts,
        })

    print(f"  → 10 labor_logs creados")
    return asset_ids, wo_ids


# ─────────────────────────────────────────────────────────────────────────────
# FASE 2: KPI tests
# ─────────────────────────────────────────────────────────────────────────────

def test_kpi_basic():
    print(f"\n{BOLD}{BLUE}═══ FASE 2a: KPI básico (COUNT total assets) ═══{RESET}")
    r = query({"kpi_total_assets": {
        "tenant_id":   TENANT,
        "entity":      "asset",
        "metrics":     [{"aggregation": "COUNT", "attribute": "id", "name": "total"}],
        "output_cast": "KPI",
        "viz":         "kpi",
        "limit":       1000,
    }})
    chunk = r.get("batchResults", {}).get("kpi_total_assets", {})
    viz   = chunk.get("vizExt", {})
    sig   = viz.get("signal", {})
    val   = sig.get("value", None)

    assert_ok("KPI response success",      chunk.get("status", {}).get("success"), f"chunk={chunk.get('status')}")
    assert_ok("vizExt.type == indicator",  viz.get("type") == "indicator",         f"got {viz.get('type')}")
    assert_ok("signal.value es número",    val is not None,                        f"signal={sig}")
    assert_ok("signal.value > 0",          val is not None and val > 0,            f"value={val} — ¿datos sembrados?")

    print(f"    → signal.value = {val}")
    return val


def test_kpi_with_time_frame():
    print(f"\n{BOLD}{BLUE}═══ FASE 2b: KPI con time_frame LAST_N_DAYS ═══{RESET}")
    r = query({"kpi_last30": {
        "tenant_id":   TENANT,
        "entity":      "asset",
        "metrics":     [{"aggregation": "COUNT", "attribute": "id", "name": "count_last30"}],
        "time_frame":  {"type": "LAST_N_DAYS", "n_value": 30, "timezone": "UTC"},
        "output_cast": "KPI",
        "viz":         "kpi",
        "limit":       1000,
    }})
    chunk = r.get("batchResults", {}).get("kpi_last30", {})
    viz   = chunk.get("vizExt", {})
    sig   = viz.get("signal", {})
    val   = sig.get("value", None)

    assert_ok("KPI+time_frame success",   chunk.get("status", {}).get("success"), f"status={chunk.get('status')}")
    assert_ok("vizExt.type indicator",    viz.get("type") == "indicator",          f"type={viz.get('type')}")
    assert_ok("signal.value presente",    val is not None,                         f"signal={sig}")
    assert_ok("signal.value >= 0",        val is not None and val >= 0,            f"value={val}")
    print(f"    → signal.value (last 30d) = {val}")
    return val


def test_kpi_with_comparison():
    print(f"\n{BOLD}{BLUE}═══ FASE 2c: KPI con comparison TIME_SHIFT_RELATIVE ═══{RESET}")
    r = query({"kpi_comparison": {
        "tenant_id":   TENANT,
        "entity":      "asset",
        "metrics":     [{"aggregation": "COUNT", "attribute": "id", "name": "count_assets"}],
        "time_frame":  {"type": "LAST_N_DAYS", "n_value": 14, "timezone": "UTC"},
        "comparisons": [{
            "type":                "TIME_SHIFT_RELATIVE",
            "label":               "vs 14d anterior",
            "relative_granularity":"day",
            "relative_amount":     14,
        }],
        "output_cast": "KPI",
        "viz":         "kpi",
        "limit":       1000,
    }})
    chunk = r.get("batchResults", {}).get("kpi_comparison", {})
    viz   = chunk.get("vizExt", {})
    sig   = viz.get("signal", {})
    intel = sig.get("intelligence", {})
    val   = sig.get("value",          None)
    prev  = sig.get("previousValue",  None)
    dir_  = intel.get("direction",    None)
    pct   = intel.get("percentage",   None)
    label = intel.get("label",        None)

    assert_ok("KPI+comparison success",     chunk.get("status", {}).get("success"),  f"status={chunk.get('status')}")
    assert_ok("vizExt.type indicator",      viz.get("type") == "indicator",           f"type={viz.get('type')}")
    assert_ok("signal.value presente",      val is not None,                          f"signal={sig}")
    assert_ok("signal.previousValue existe",prev is not None,                          f"sig keys={list(sig.keys())}")
    assert_ok("intelligence.direction",     dir_ in ("up","down","neutral"),           f"direction={dir_}")
    assert_ok("intelligence.percentage",    pct is not None,                           f"pct={pct}")
    assert_ok("intelligence.label",         label is not None,                         f"label={label}")
    assert_ok("intelligence.isAnomaly",
        any(k in intel for k in ("isAnomaly", "is_anomaly", "representsInitial")),
        f"intel keys={list(intel.keys())}")

    print(f"    → value={val}  previousValue={prev}  direction={dir_}  pct={pct}%  label={label}")
    return intel


def test_kpi_benchmark():
    print(f"\n{BOLD}{BLUE}═══ FASE 2d: KPI con comparison BENCHMARK ═══{RESET}")
    r = query({"kpi_benchmark": {
        "tenant_id":   TENANT,
        "entity":      "work_order",
        "metrics":     [{"aggregation": "COUNT", "attribute": "id", "name": "count_wo"}],
        "comparisons": [{"type": "BENCHMARK", "label": "meta mensual", "benchmark_value": 20.0}],
        "output_cast": "KPI",
        "viz":         "gauge",
        "limit":       1000,
    }})
    chunk = r.get("batchResults", {}).get("kpi_benchmark", {})
    viz   = chunk.get("vizExt", {})
    sig   = viz.get("signal", {})
    intel = sig.get("intelligence", {})
    val   = sig.get("value", None)
    prev  = sig.get("previousValue", None)
    pct   = intel.get("percentage", None)

    assert_ok("KPI+benchmark success",         chunk.get("status", {}).get("success"))
    assert_ok("signal.value presente",          val is not None,    f"signal={sig}")
    assert_ok("signal.previousValue=benchmark", prev is not None,   f"keys={list(sig.keys())}")
    assert_ok("intelligence.percentage",        pct is not None,    f"intel={intel}")

    print(f"    → value={val}  benchmark(prev)={prev}  pct={pct}%")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 3: PIE tests
# ─────────────────────────────────────────────────────────────────────────────

def test_pie_breakdown():
    print(f"\n{BOLD}{BLUE}═══ FASE 3: PIE breakdown por status (asset) ═══{RESET}")
    r = query({"pie_asset_status": {
        "tenant_id":   TENANT,
        "entity":      "asset",
        "metrics":     [{"aggregation": "COUNT", "attribute": "id", "name": "count"}],
        "dimensions":  [{"attribute": "status"}],
        "output_cast": "PIE",
        "viz":         "pie",
        "limit":       1000,
    }})
    chunk = r.get("batchResults", {}).get("pie_asset_status", {})
    viz   = chunk.get("vizExt", {})
    bd    = viz.get("breakdown", {})
    sigs  = bd.get("signals", {})

    assert_ok("PIE success",               chunk.get("status", {}).get("success"), f"status={chunk.get('status')}")
    assert_ok("vizExt.type == pie",         viz.get("type") == "pie",               f"type={viz.get('type')}")
    assert_ok("breakdown existe",           bool(bd),                                f"vizExt={viz}")
    assert_ok("breakdown.signals no vacío", len(sigs) > 0,                           f"signals={sigs}")
    assert_ok("signals tienen value",
        # Proto3 omite value=0.0 → signal puede ser null o dict con value
        # Acepta: null (value=0), dict con 'value' key
        all((v is None) or (isinstance(v, dict) and v.get("value") is not None)
            for v in sigs.values()) if sigs else False,
        f"signals sample={dict(list(sigs.items())[:1])}")

    for k, v in list(sigs.items())[:3]:
        val = (v or {}).get('value', '?') if isinstance(v, dict) else (v if v is not None else 0)
        print(f"    → {k}: {val}")
    return sigs


def test_pie_donut():
    print(f"\n{BOLD}{BLUE}═══ FASE 3b: DONUT breakdown por status work_order ═══{RESET}")
    r = query({"donut_wo_status": {
        "tenant_id":   TENANT,
        "entity":      "work_order",
        "metrics":     [{"aggregation": "COUNT", "attribute": "id", "name": "count"}],
        "dimensions":  [{"attribute": "status"}],
        "output_cast": "PIE",
        "viz":         "donut",
        "limit":       1000,
    }})
    chunk = r.get("batchResults", {}).get("donut_wo_status", {})
    viz   = chunk.get("vizExt", {})
    bd    = viz.get("breakdown", {})
    sigs  = bd.get("signals", {})

    assert_ok("DONUT success",             chunk.get("status", {}).get("success"))
    assert_ok("breakdown.signals > 0",     len(sigs) > 0,                f"signals={sigs}")

    for k, v in list(sigs.items())[:3]:
        val = (v or {}).get('value', '?') if isinstance(v, dict) else (0 if v is None else v)
        print(f"    → {k}: {val}")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 4: LINE chart tests
# ─────────────────────────────────────────────────────────────────────────────

def test_line_timeseries():
    print(f"\n{BOLD}{BLUE}═══ FASE 4: LINE chart TIMESERIES (assets por día) ═══{RESET}")
    r = query({"line_assets_ts": {
        "tenant_id":   TENANT,
        "entity":      "asset",
        "metrics":     [{"aggregation": "COUNT", "attribute": "id", "name": "count"}],
        "dimensions":  [{"attribute": "created_at", "interval": "day"}],
        "time_frame":  {"type": "LAST_N_DAYS", "n_value": 30, "timezone": "UTC"},
        "output_cast": "TIMESERIES",
        "viz":         "line",
        "limit":       1000,
    }})
    chunk = r.get("batchResults", {}).get("line_assets_ts", {})
    viz   = chunk.get("vizExt", {})
    chart = viz.get("chart", {})
    cols  = chunk.get("data", {}).get("columns", [])
    rows  = chunk.get("data", {}).get("rowsJson", {}).get("iter", [])

    assert_ok("LINE TIMESERIES success",     chunk.get("status", {}).get("success"), f"status={chunk.get('status')}")
    assert_ok("vizExt.type == line",         viz.get("type") == "line",               f"type={viz.get('type')}")
    assert_ok("chart existe",                bool(chart),                              f"vizExt={viz}")
    # gRPC JSON serializa snake_case como camelCase: x_dimension→xDimension
    assert_ok("chart.x_dimension",           bool(chart.get("xDimension") or chart.get("x_dimension")),          f"chart={chart}")
    assert_ok("chart.y_dimensions",          len(chart.get("yDimensions", chart.get("y_dimensions", []))) > 0,  f"chart={chart}")
    assert_ok("chart.fill_gaps == true",     chart.get("fillGaps", chart.get("fill_gaps")) == True,          f"fill_gaps={chart.get('fillGaps')}")
    assert_ok("chart.show_legend == true",   chart.get("showLegend", chart.get("show_legend")) == True,        f"show_legend={chart.get('showLegend')}")
    assert_ok("chart.show_tooltip == true",  chart.get("showTooltip", chart.get("show_tooltip")) == True,       f"show_tooltip={chart.get('showTooltip')}")
    assert_ok("data.columns presentes",      len(cols) > 0,                           f"cols={cols}")
    assert_ok("data.rows presentes",         len(rows) > 0,                           f"rows count={len(rows)}")

    x = chart.get("xDimension") or chart.get("x_dimension")
    y = chart.get("yDimensions") or chart.get("y_dimensions", [])
    print(f"    → x={x}  y={y}  fill_gaps={chart.get('fillGaps')}  rows={len(rows)}")
    return rows


def test_bar_by_area():
    print(f"\n{BOLD}{BLUE}═══ FASE 4b: BAR chart assets por área ═══{RESET}")
    r = query({"bar_areas": {
        "tenant_id":   TENANT,
        "entity":      "asset",
        "metrics":     [{"aggregation": "COUNT", "attribute": "id", "name": "count"}],
        "dimensions":  [{"attribute": "area"}],
        "output_cast": "TABLE",
        "viz":         "bar",
        "limit":       1000,
    }})
    chunk = r.get("batchResults", {}).get("bar_areas", {})
    viz   = chunk.get("vizExt", {})
    chart = viz.get("chart", {})
    rows  = chunk.get("data", {}).get("rowsJson", {}).get("iter", [])

    assert_ok("BAR success",               chunk.get("status", {}).get("success"))
    assert_ok("vizExt.type == bar",        viz.get("type") == "bar",              f"type={viz.get('type')}")
    assert_ok("chart existe",              bool(chart),                            f"vizExt={viz}")
    # Proto3 omite booleanos false (default) → ausencia == false
    assert_ok("chart.stacked == false",    chart.get("stacked", False) != True,   f"stacked={chart.get('stacked')}")
    assert_ok("chart.smooth == false",     chart.get("smooth",  False) != True,   f"smooth={chart.get('smooth')}")
    assert_ok("data.rows presentes",       len(rows) > 0,                         f"rows={len(rows)}")

    print(f"    → rows={len(rows)}  chart={json.dumps(chart, ensure_ascii=False)[:120]}")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 5: Mega-batch (todos juntos)
# ─────────────────────────────────────────────────────────────────────────────

def test_mega_batch():
    print(f"\n{BOLD}{BLUE}═══ FASE 5: Mega-batch (KPI+PIE+LINE simultáneos) ═══{RESET}")
    r = query({
        "kpi_total":   {"tenant_id": TENANT, "entity": "asset",
                        "metrics": [{"aggregation": "COUNT", "attribute": "id", "name": "total"}],
                        "output_cast": "KPI", "viz": "kpi", "limit": 1000},
        "pie_status":  {"tenant_id": TENANT, "entity": "work_order",
                        "metrics": [{"aggregation": "COUNT", "attribute": "id", "name": "count"}],
                        "dimensions": [{"attribute": "status"}],
                        "output_cast": "PIE", "viz": "pie", "limit": 1000},
        "line_ts":     {"tenant_id": TENANT, "entity": "asset",
                        "metrics": [{"aggregation": "COUNT", "attribute": "id", "name": "n"}],
                        "dimensions": [{"attribute": "created_at", "interval": "day"}],
                        "time_frame": {"type": "LAST_N_DAYS", "n_value": 30, "timezone": "UTC"},
                        "output_cast": "TIMESERIES", "viz": "line", "limit": 1000},
    })
    br = r.get("batchResults", {})

    assert_ok("Mega-batch 3 keys",         len(br) == 3,                           f"keys={list(br.keys())}")
    assert_ok("kpi_total success",         br.get("kpi_total",  {}).get("status", {}).get("success"))
    assert_ok("pie_status success",        br.get("pie_status", {}).get("status", {}).get("success"))
    assert_ok("line_ts success",           br.get("line_ts",    {}).get("status", {}).get("success"))

    kpi_sig  = br.get("kpi_total",  {}).get("vizExt", {}).get("signal",    {})
    pie_sigs = br.get("pie_status", {}).get("vizExt", {}).get("breakdown", {}).get("signals", {})
    line_ch  = br.get("line_ts",    {}).get("vizExt", {}).get("chart",     {})

    assert_ok("kpi_total signal.value",    kpi_sig.get("value")  is not None,      f"signal={kpi_sig}")
    assert_ok("pie_status has signals",    len(pie_sigs) > 0,                      f"signals={pie_sigs}")
    assert_ok("line_ts has chart",         bool(line_ch.get("xDimension") or line_ch.get("x_dimension")),        f"chart={line_ch}")
    assert_ok("line_ts fill_gaps active",  line_ch.get("fillGaps", line_ch.get("fill_gaps")) == True,        f"fill_gaps={line_ch.get('fillGaps')}")

    print(f"    → KPI={kpi_sig.get('value')}  PIE sectors={len(pie_sigs)}  LINE x={line_ch.get('xDimension') or line_ch.get('x_dimension')}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{BOLD}{'═'*60}{RESET}")
    print(f"{BOLD}  Metri Engine — Integration E2E Test Suite{RESET}")
    print(f"{BOLD}{'═'*60}{RESET}")
    print(f"  Engine: {ENDPOINT}  Tenant: {TENANT}")

    try:
        seed_data()
        time.sleep(1)  # dar tiempo al engine para indexar

        test_kpi_basic()
        test_kpi_with_time_frame()
        test_kpi_with_comparison()
        test_kpi_benchmark()
        test_pie_breakdown()
        test_pie_donut()
        test_line_timeseries()
        test_bar_by_area()
        test_mega_batch()

    except Exception as e:
        print(f"\n{RED}ERROR CRÍTICO: {e}{RESET}")
        import traceback; traceback.print_exc()
        failed.append(str(e))

    # Resultado final
    total = len(passed) + len(failed)
    print(f"\n{BOLD}{'═'*60}{RESET}")
    print(f"{BOLD}  RESULTADO: {GREEN}{len(passed)}{RESET}/{total} tests pasaron{RESET}")
    if failed:
        print(f"\n{RED}{BOLD}  FALLOS ({len(failed)}):{RESET}")
        for f in failed:
            print(f"  {RED}✗{RESET} {f}")
    else:
        print(f"\n{GREEN}{BOLD}  🎉 TODOS LOS TESTS PASARON{RESET}")
    print(f"{BOLD}{'═'*60}{RESET}\n")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
