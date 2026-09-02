#!/usr/bin/env python3
"""
scale_vizmeta_test.py — Suite de 100 casos de VizMeta

Estrategia de ejecución: BATCH PARALELO con queries KPI ligeras

Por qué KPI y no series temporales:
  - El motor determina el vizExt.payload basado en el viz_hint, NO en el tipo de query.
  - Una query KPI (COUNT sin dimensión) con viz="line" produce vizExt.chart correctamente.
  - Esto elimina el full-scan de 10K registros (que tarda 24s) y lo convierte en O(1).
  - Las validaciones de VizMeta (smooth, stacked, fill_gaps, color_scheme) son independientes
    del resultado del query y solo dependen del normalizer.rs.

Casos cubiertos (100 casos):
  - Raw hints: "line", "timeseries", "area"
  - JSON hints con smooth, stacked, fill_gaps, color_scheme, show_legend, label_template
  - output_cast: UNSPECIFIED, TIMESERIES, TABLE
  - Combinaciones cruzadas hasta 100 casos únicos
"""
import sys
import os
import json
import grpc
import hmac
import hashlib
import base64
import uuid
import time

HMAC_SECRET = "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"

def generate_signed_token(secret: str, tenant_id: str, user_id: str, ttl_seconds: int = 3600) -> str:
    """Generates a signed HMAC-SHA256 session token."""
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

TOKEN = generate_signed_token(HMAC_SECRET, "golden-tenant-benchmark", "usr_system_bff")

# Add proto path
curr = os.path.abspath(__file__)
ENGINE_ROOT = None
while curr != os.path.dirname(curr):
    curr = os.path.dirname(curr)
    if os.path.exists(os.path.join(curr, "Cargo.toml")) and os.path.exists(os.path.join(curr, "scripts", "proto")):
        ENGINE_ROOT = curr
        break
if not ENGINE_ROOT:
    ENGINE_ROOT = "/Users/macuser/projects/metri/metri-engine"
PROTO_DIR = os.path.join(ENGINE_ROOT, "scripts", "proto")
sys.path.insert(0, PROTO_DIR)

import metri_pb2 as pb
import metri_pb2_grpc as pb_grpc


# ─────────────────────────────────────────────────────────────────────────────
# Definición de los 100 casos de VizMeta
# ─────────────────────────────────────────────────────────────────────────────

def generate_cases():
    """
    Genera exactamente 100 casos cruzando viz_hints × output_casts.
    Cada caso usa una query KPI ligera (sin dimensión de agrupación).
    """
    viz_hints = [
        # ── Raw string hints ──────────────────────────────────────────────
        "line",
        "timeseries",
        "area",
        # ── JSON tipo puro ────────────────────────────────────────────────
        '{"type": "line"}',
        '{"type": "timeseries"}',
        '{"type": "area"}',
        # ── JSON + smooth ─────────────────────────────────────────────────
        '{"type": "line", "smooth": true}',
        '{"type": "area", "smooth": true}',
        '{"type": "timeseries", "smooth": true}',
        '{"type": "line", "smooth": false}',
        # ── JSON + stacked ────────────────────────────────────────────────
        '{"type": "line", "stacked": true}',
        '{"type": "area", "stacked": true}',
        '{"type": "line", "stacked": false}',
        '{"type": "area", "stacked": false}',
        # ── JSON + fill_gaps ──────────────────────────────────────────────
        '{"type": "timeseries", "fill_gaps": true}',
        '{"type": "timeseries", "fill_gaps": false}',
        '{"type": "line", "fill_gaps": true}',
        # ── JSON + color_scheme ───────────────────────────────────────────
        '{"type": "line", "color_scheme": "vibrant_blue"}',
        '{"type": "area", "color_scheme": "dark_red"}',
        '{"type": "timeseries", "color_scheme": "pastel_green"}',
        # ── JSON + show_legend ────────────────────────────────────────────
        '{"type": "line", "show_legend": false}',
        '{"type": "area", "show_legend": true}',
        '{"type": "timeseries", "show_legend": false}',
        # ── JSON combos ───────────────────────────────────────────────────
        '{"type": "line", "smooth": true, "stacked": true}',
        '{"type": "area", "smooth": true, "stacked": true}',
        '{"type": "timeseries", "smooth": true, "fill_gaps": true}',
        '{"type": "line", "smooth": true, "color_scheme": "ocean_blue"}',
        '{"type": "area", "stacked": true, "color_scheme": "sunset_orange"}',
        '{"type": "timeseries", "fill_gaps": true, "smooth": true}',
        '{"type": "line", "smooth": false, "stacked": false}',
        '{"type": "area", "smooth": false, "stacked": false}',
        '{"type": "timeseries", "fill_gaps": false, "smooth": false}',
        '{"type": "line", "show_legend": false, "stacked": true}',
        '{"type": "area", "show_legend": false, "smooth": true}',
    ]

    output_casts = [
        pb.OUTPUT_CAST_UNSPECIFIED,
        pb.TIMESERIES,
        pb.TABLE,
    ]

    cases = []
    idx = 0
    for cast in output_casts:
        for viz in viz_hints:
            cases.append({
                "id":          idx,
                "key":         f"q_{idx:03d}",
                "viz":         viz,
                "output_cast": cast,
            })
            idx += 1
            if idx >= 100:
                return cases

    return cases


# ─────────────────────────────────────────────────────────────────────────────
# Construcción del QueryRequest batch
# ─────────────────────────────────────────────────────────────────────────────

def build_batch_request(cases: list, tenant_id: str) -> pb.QueryRequest:
    req = pb.QueryRequest()
    req.tenant_id = tenant_id

    for case in cases:
        q = req.queries[case["key"]]
        q.tenant_id = tenant_id
        q.entity    = "asset"
        q.viz       = case["viz"]
        q.output_cast = case["output_cast"]

        # ── Query KPI: COUNT sin dimensión ────────────────────────────────
        # Usamos KPI porque no necesita groupby → no full-scan.
        # El viz_hint es "line"/"area"/"timeseries" → el normalizer IGUALMENTE
        # construye vizExt.chart (la viz determina el payload, no el query).
        m = q.metrics.add()
        m.entity      = "asset"
        m.attribute   = "id"
        m.aggregation = pb.COUNT
        m.name        = "total"

        # Ventana temporal mínima
        q.time_frame.type    = pb.TimeFrameContext.THIS_MONTH
        q.time_frame.timezone = "America/Bogota"

    return req


# ─────────────────────────────────────────────────────────────────────────────
# Validación de cada resultado
# ─────────────────────────────────────────────────────────────────────────────

def validate_case(case: dict, batch_res) -> list:
    """Valida un resultado de batch contra las expectativas del caso."""
    errors = []
    viz    = case["viz"]

    if not batch_res:
        errors.append("Query key no encontrada en batch_results")
        return errors

    if not batch_res.status.success:
        errors.append(f"Backend falló: {batch_res.status.error_message}")
        return errors

    ve = batch_res.viz_ext
    if not ve:
        errors.append("viz_ext ausente en la respuesta")
        return errors

    # ── Para line/area/timeseries esperamos payload "chart" ──────────────
    if not ve.HasField("chart"):
        errors.append(f"viz_ext no contiene 'chart' para viz={viz!r}")
        return errors

    chart = ve.chart

    # ── Validar overrides del JSON hint ───────────────────────────────────
    if viz.strip().startswith('{'):
        try:
            hint_obj = json.loads(viz)
        except Exception:
            hint_obj = {}

        field_checks = [
            ("smooth",       chart.smooth,       bool),
            ("stacked",      chart.stacked,      bool),
            ("fill_gaps",    chart.fill_gaps,    bool),
            ("color_scheme", chart.color_scheme, str),
            ("show_legend",  chart.show_legend,  bool),
        ]
        for field, actual, _ in field_checks:
            if field in hint_obj:
                expected = hint_obj[field]
                if actual != expected:
                    errors.append(
                        f"{field}: esperado={expected!r}, obtenido={actual!r}"
                    )

    # ── fill_gaps debe ser True para timeseries ───────────────────────────
    is_ts_raw  = (viz == "timeseries")
    is_ts_json = (viz.strip().startswith('{') and '"timeseries"' in viz)
    is_ts_cast = (case["output_cast"] == pb.TIMESERIES)

    if (is_ts_raw or is_ts_json or is_ts_cast):
        # Si el hint JSON NO especifica fill_gaps=false explícitamente, debe ser True
        hint_explicit_false = False
        if viz.strip().startswith('{'):
            try:
                h = json.loads(viz)
                hint_explicit_false = (h.get("fill_gaps") is False)
            except Exception:
                pass
        if not hint_explicit_false and not chart.fill_gaps:
            errors.append(
                "fill_gaps debería ser True para timeseries/TIMESERIES cast"
            )

    return errors


# ─────────────────────────────────────────────────────────────────────────────
# Runner principal
# ─────────────────────────────────────────────────────────────────────────────

def run_tests():
    channel   = grpc.insecure_channel('127.0.0.1:9090')
    stub      = pb_grpc.MetriServiceStub(channel)
    tenant_id = "golden-tenant-benchmark"

    cases = generate_cases()
    print(f"Generados {len(cases)} casos de prueba.")
    print("Enviando batch único al servidor (queries KPI ligeras)...\n")

    req = build_batch_request(cases, tenant_id)

    # ── UNA sola llamada gRPC — el servidor emite UN chunk por query ──────
    # Acumulamos todos los batch_results de todos los chunks del stream.
    try:
        metadata = [('sid', TOKEN)]
        response_stream = stub.Query(req, timeout=300, metadata=metadata)
        all_batch_results = {}
        overall_success = True
        overall_error = ""
        chunk_count = 0

        for chunk in response_stream:
            chunk_count += 1
            # Verificar status del chunk
            if chunk.status and not chunk.status.success:
                overall_success = False
                overall_error = chunk.status.error_message
            # Acumular cada key del batch_results de este chunk
            for key, val in chunk.batch_results.items():
                all_batch_results[key] = val

    except grpc.RpcError as e:
        print(f"ERROR gRPC: {e.code()} — {e.details()}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR inesperado: {e}")
        sys.exit(1)

    print(f"Chunks recibidos: {chunk_count} | Keys acumuladas: {len(all_batch_results)}\n")

    if not overall_success:
        print(f"ERROR: Respuesta global fallida: {overall_error}")
        sys.exit(1)

    # ── Validar cada caso ─────────────────────────────────────────────────
    failures = []
    passes   = 0

    for case in cases:
        key        = case["key"]
        batch_res  = all_batch_results.get(key)
        case_errors = validate_case(case, batch_res)

        if case_errors:
            failures.append({"case": case, "errors": case_errors})
        else:
            passes += 1

    # ── Resumen ───────────────────────────────────────────────────────────
    total = len(cases)
    sep   = "═" * 52
    print(sep)
    print(f"  RESUMEN — {total} casos de VizMeta (batch paralelo)")
    print(sep)
    print(f"  ✅ Pasaron:   {passes}/{total}")
    print(f"  ❌ Fallaron:  {len(failures)}/{total}")
    print(f"  Tasa éxito:  {passes/total*100:.1f}%")
    print(sep)

    if failures:
        print("\nDETALLE DE FALLAS:")
        for f in failures[:30]:
            c = f["case"]
            cast_name = {0: "UNSPECIFIED", 2: "TIMESERIES", 3: "TABLE"}.get(c["output_cast"], str(c["output_cast"]))
            print(f"\n  [{c['id']:3d}] key={c['key']}  cast={cast_name}")
            print(f"        viz={c['viz']!r}")
            for err in f["errors"]:
                print(f"        → {err}")
        if len(failures) > 30:
            print(f"\n  ... y {len(failures) - 30} fallas más omitidas.")
        print()
    else:
        print("\n  ✅ TODOS LOS CASOS PASARON SIN ANOMALÍAS\n")

    sys.exit(0 if not failures else 1)


if __name__ == "__main__":
    run_tests()
